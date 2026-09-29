from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import UTC, datetime
from time import monotonic
from typing import Any
from urllib.parse import urlsplit

import httpx

from app.config import Settings
from app.models import (
    NotificationCategory,
    NotificationEvent,
    NotificationLevel,
    NotificationPriority,
)
from app.monitoring import Metrics, Notification
from app.storage import Store

logger = logging.getLogger("notifications")

PRIORITIES = (
    NotificationPriority.CRITICAL,
    NotificationPriority.TIME_SENSITIVE,
    NotificationPriority.ACTIVE,
    NotificationPriority.PASSIVE,
)
BARK_LEVEL = {
    NotificationPriority.PASSIVE: "passive",
    NotificationPriority.ACTIVE: "active",
    NotificationPriority.TIME_SENSITIVE: "timeSensitive",
    NotificationPriority.CRITICAL: "critical",
}


class BarkNotification(Notification):
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None) -> None:
        server = urlsplit(settings.bark_server)
        if (
            server.scheme not in {"http", "https"} or not server.hostname
            or server.username or server.password or server.path not in {"", "/"}
            or server.query or server.fragment
        ):
            raise ValueError("invalid Bark server base URL")
        self.url = settings.bark_server.rstrip("/") + "/push"
        self._device_key = settings.bark_device_key.get_secret_value()
        self.group = settings.bark_group
        self.timeout = settings.bark_timeout_seconds
        self.sound = settings.bark_sound
        self.critical_sound = settings.bark_critical_sound
        self.critical_volume = settings.bark_critical_volume
        self.client = client or httpx.AsyncClient(timeout=self.timeout)
        self._owns_client = client is None

    async def send(self, event: NotificationEvent) -> None:
        payload: dict[str, Any] = {
            "device_key": self._device_key,
            "title": event.title,
            "body": event.message,
            "group": self.group,
            "level": BARK_LEVEL[event.priority],
        }
        sound = self.critical_sound if event.priority == NotificationPriority.CRITICAL else self.sound
        if sound:
            payload["sound"] = sound
        if event.priority == NotificationPriority.CRITICAL:
            payload["volume"] = str(self.critical_volume)
        async with asyncio.timeout(self.timeout):
            response = await self.client.post(self.url, json=payload)
            response.raise_for_status()
        try:
            result = response.json()
        except ValueError:
            return
        if isinstance(result, dict) and str(result.get("code", "200")) not in {"0", "200"}:
            raise RuntimeError("Bark rejected notification")

    async def send_alert(self, level: str, title: str, message: str) -> None:
        await self.send(
            NotificationEvent(
                level=NotificationLevel(level),
                category=NotificationCategory.SYSTEM,
                title=title,
                message=message,
            )
        )

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()


class NotificationManager:
    def __init__(
        self,
        channels: list[Notification],
        metrics: Metrics,
        *,
        store: Store | None = None,
        max_queue: int = 1000,
        dedup_seconds: float = 60,
        retry_delays: tuple[float, ...] = (1, 2, 5),
    ) -> None:
        if max_queue < 1:
            raise ValueError("notification queue must have capacity")
        self.channels = channels
        self.metrics = metrics
        self.store = store
        self.max_queue = max_queue
        self.dedup_seconds = dedup_seconds
        self.retry_delays = retry_delays
        self._pending: dict[NotificationPriority, deque[NotificationEvent]] = {
            priority: deque() for priority in PRIORITIES
        }
        self._size = 0
        self._wakeup = asyncio.Event()
        self._worker: asyncio.Task[None] | None = None
        self._stopping = False
        self._accepting = True
        self._recent: dict[tuple[str, str, str, str], float] = {}
        self._last_stateful: dict[str, tuple[str, str, str, str]] = {}
        self._published = 0
        self.metrics.notification_queue_size.set(0)

    @property
    def queue_size(self) -> int:
        return self._size

    def start(self) -> None:
        if self._worker is not None and not self._worker.done():
            return
        self._stopping = False
        self._accepting = True
        self._worker = asyncio.create_task(self._run(), name="notification-worker")
        self._worker.add_done_callback(self._worker_done)

    async def publish(self, event: NotificationEvent) -> None:
        """Enqueue without awaiting network, persistence, or queue capacity."""
        try:
            if event.level == NotificationLevel.CRITICAL and event.priority != NotificationPriority.CRITICAL:
                event = event.model_copy(update={"priority": NotificationPriority.CRITICAL})
            elif event.level == NotificationLevel.ERROR and event.priority in {
                NotificationPriority.PASSIVE, NotificationPriority.ACTIVE,
            }:
                event = event.model_copy(update={"priority": NotificationPriority.TIME_SENSITIVE})
            if not self._accepting:
                self._drop(event)
                return
            now = monotonic()
            fingerprint = (
                event.dedup_key or "",
                event.level.value,
                event.title,
                event.message,
            )
            stateful = bool(event.metadata.get("recovery") or event.metadata.get("transition"))
            if event.dedup_key:
                if stateful and self._last_stateful.get(event.dedup_key) == fingerprint:
                    return
                if not stateful:
                    previous = self._recent.get(fingerprint)
                    if previous is not None and now - previous < self.dedup_seconds:
                        return
            if self._size >= self.max_queue and not self._make_room(event):
                self._drop(event)
                return
            self._pending[event.priority].append(event)
            self._size += 1
            self.metrics.notification_queue_size.set(self._size)
            self._wakeup.set()
            if event.dedup_key:
                self._recent[fingerprint] = now
                if stateful:
                    self._last_stateful[event.dedup_key] = fingerprint
            self._published += 1
            if self._published % 1000 == 0:
                cutoff = now - max(self.dedup_seconds, 60)
                self._recent = {key: when for key, when in self._recent.items() if when >= cutoff}
        except Exception as exc:
            logger.error("notification enqueue failed: %s", type(exc).__name__)

    def _drop(self, event: NotificationEvent) -> None:
        self.metrics.notification_dropped.labels(
            priority=event.priority.value, category=event.category.value
        ).inc()

    def _make_room(self, incoming: NotificationEvent) -> bool:
        incoming_rank = PRIORITIES.index(incoming.priority)
        for priority in reversed(PRIORITIES):
            if PRIORITIES.index(priority) > incoming_rank and self._pending[priority]:
                self._drop(self._pending[priority].popleft())
                self._size -= 1
                return True
        if incoming.priority == NotificationPriority.CRITICAL and self._pending[incoming.priority]:
            self._drop(self._pending[incoming.priority].popleft())
            self._size -= 1
            return True
        return False

    def _pop(self) -> NotificationEvent | None:
        for priority in PRIORITIES:
            if self._pending[priority]:
                event = self._pending[priority].popleft()
                self._size -= 1
                self.metrics.notification_queue_size.set(self._size)
                return event
        return None

    async def _run(self) -> None:
        while True:
            event = self._pop()
            if event is None:
                if self._stopping:
                    return
                self._wakeup.clear()
                await self._wakeup.wait()
                continue
            await self._deliver(event)

    async def _deliver(self, event: NotificationEvent) -> None:
        for channel in self.channels:
            channel_name = type(channel).__name__.removesuffix("Notification").lower()
            labels = {
                "channel": channel_name,
                "priority": event.priority.value,
                "category": event.category.value,
            }
            started = monotonic()
            attempts = 0
            error_type = ""
            sent = False
            for delay in (0, *self.retry_delays):
                if delay:
                    await asyncio.sleep(delay)
                attempts += 1
                try:
                    await channel.send(event)
                    sent = True
                    break
                except Exception as exc:
                    error_type = type(exc).__name__
            self.metrics.notification_latency.labels(**labels).observe(monotonic() - started)
            if sent:
                self.metrics.notification_sent.labels(**labels).inc()
            else:
                self.metrics.notification_failed.labels(**labels).inc()
                logger.error(
                    "notification delivery failed: channel=%s priority=%s error_type=%s",
                    channel_name, event.priority.value, error_type,
                )
            await self._audit(event, channel_name, sent, attempts, error_type)

    async def _audit(
        self, event: NotificationEvent, channel: str, sent: bool, attempts: int,
        error_type: str,
    ) -> None:
        if self.store is None:
            return
        payload = {
            "event_id": event.id,
            "created_at": event.timestamp.isoformat(),
            "category": event.category.value,
            "priority": event.priority.value,
            "title": event.title,
            "channel": channel,
            "status": "SENT" if sent else "FAILED",
            "attempts": attempts,
            "sent_at": datetime.now(UTC).isoformat() if sent else None,
            "error_type": error_type if not sent else "",
        }
        try:
            async with asyncio.timeout(0.5):
                await self.store.append("notification_events", payload, reference_id=event.id)
        except Exception as exc:
            logger.error("notification audit unavailable: %s", type(exc).__name__)

    def _worker_done(self, task: asyncio.Task[None]) -> None:
        if self._stopping:
            return
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            exc = None
        if exc is not None:
            logger.error("notification worker restarted: %s", type(exc).__name__)
        self._worker = asyncio.create_task(self._run(), name="notification-worker")
        self._worker.add_done_callback(self._worker_done)
        self._wakeup.set()

    async def stop(self, *, drain_seconds: float = 3) -> None:
        self._accepting = False
        self._stopping = True
        self._wakeup.set()
        worker = self._worker
        if worker is not None:
            try:
                await asyncio.wait_for(worker, timeout=drain_seconds)
            except TimeoutError:
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
                logger.warning("notification drain timed out")

    async def close(self) -> None:
        await self.stop(drain_seconds=0.1)
        for channel in self.channels:
            close = getattr(channel, "close", None)
            if close is not None:
                try:
                    await close()
                except Exception as exc:
                    logger.error("notification channel close failed: %s", type(exc).__name__)
