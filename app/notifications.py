from __future__ import annotations

import asyncio
import heapq
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from time import monotonic
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx
from pydantic import SecretStr

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


class BarkConfig(Protocol):
    bark_server: str
    bark_device_key: SecretStr
    bark_group: str
    bark_timeout_seconds: float
    bark_sound: str
    bark_critical_sound: str
    bark_critical_volume: int


class BarkDeliveryError(RuntimeError):
    def __init__(self, status_code: int | None = None) -> None:
        super().__init__("Bark response did not confirm delivery")
        self.status_code = status_code


class BarkNotification(Notification):
    def __init__(self, settings: BarkConfig, client: httpx.AsyncClient | None = None) -> None:
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
        if not 200 <= response.status_code < 300:
            raise BarkDeliveryError(response.status_code)
        try:
            result = response.json()
        except ValueError:
            raise BarkDeliveryError(response.status_code) from None
        if (
            not isinstance(result, dict)
            or type(result.get("code")) not in {int, str}
            or str(result["code"]) not in {"0", "200"}
        ):
            raise BarkDeliveryError(response.status_code)

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


@dataclass
class _Delivery:
    event: NotificationEvent
    started: float = field(default_factory=monotonic)
    attempts: int = 0
    error_type: str = ""
    status_code: int | None = None


@dataclass
class _ChannelState:
    channel: Notification
    name: str
    pending: dict[NotificationPriority, deque[_Delivery]] = field(
        default_factory=lambda: {priority: deque() for priority in PRIORITIES}
    )
    delayed: list[tuple[float, int, _Delivery]] = field(default_factory=list)
    size: int = 0
    wakeup: asyncio.Event = field(default_factory=asyncio.Event)
    worker: asyncio.Task[None] | None = None


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
        channel_timeout_seconds: float = 5,
    ) -> None:
        if max_queue < 1:
            raise ValueError("notification queue must have capacity")
        self.channels = channels
        self.metrics = metrics
        self.store = store
        self.max_queue = max_queue
        self.dedup_seconds = dedup_seconds
        self.retry_delays = retry_delays
        self.channel_timeout_seconds = channel_timeout_seconds
        self._channels = [
            _ChannelState(channel, type(channel).__name__.removesuffix("Notification").lower())
            for channel in channels
        ]
        self._retry_sequence = 0
        self._pending: dict[NotificationPriority, deque[NotificationEvent]] = {
            priority: deque() for priority in PRIORITIES
        }
        self._size = 0
        self._wakeup = asyncio.Event()
        self._worker: asyncio.Task[None] | None = None
        self._stopping = False
        self._dispatcher_done = False
        self._accepting = True
        self._recent: dict[tuple[str, str, str, str], float] = {}
        self._last_stateful: dict[str, tuple[str, str, str, str]] = {}
        self._published = 0
        self.metrics.notification_queue_size.set(0)

    @property
    def queue_size(self) -> int:
        return self._size + sum(channel.size for channel in self._channels)

    def _update_queue_metric(self) -> None:
        self.metrics.notification_queue_size.set(self.queue_size)

    def start(self) -> None:
        if self._worker is not None and not self._worker.done():
            return
        self._stopping = False
        self._dispatcher_done = False
        self._accepting = True
        for state in self._channels:
            if state.worker is None or state.worker.done():
                state.worker = asyncio.create_task(
                    self._run_channel(state), name=f"notification-{state.name}-worker"
                )
                state.worker.add_done_callback(partial(self._channel_worker_done, state))
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
            self._update_queue_metric()
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
        if event.priority == NotificationPriority.CRITICAL:
            logger.error("critical notification dropped: category=%s", event.category.value)

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
                self._update_queue_metric()
                return event
        return None

    async def _run(self) -> None:
        try:
            while True:
                event = self._pop()
                if event is None:
                    if self._stopping:
                        return
                    self._wakeup.clear()
                    await self._wakeup.wait()
                    continue
                await self._deliver(event)
        finally:
            self._dispatcher_done = True
            for state in self._channels:
                state.wakeup.set()

    async def _deliver(self, event: NotificationEvent) -> None:
        for state in self._channels:
            try:
                self._enqueue_channel(state, _Delivery(event))
            except Exception as exc:
                logger.error("notification channel enqueue failed: channel=%s error_type=%s",
                             state.name, type(exc).__name__)

    def _enqueue_channel(self, state: _ChannelState, delivery: _Delivery) -> None:
        if state.size >= self.max_queue and not self._make_channel_room(state, delivery.event):
            self._drop(delivery.event)
            return
        state.pending[delivery.event.priority].append(delivery)
        state.size += 1
        self._update_queue_metric()
        state.wakeup.set()

    def _make_channel_room(self, state: _ChannelState, incoming: NotificationEvent) -> bool:
        incoming_rank = PRIORITIES.index(incoming.priority)
        for priority in reversed(PRIORITIES):
            if PRIORITIES.index(priority) > incoming_rank and self._evict_channel_priority(
                state, priority
            ):
                return True
        if incoming.priority == NotificationPriority.CRITICAL:
            return self._evict_channel_priority(state, NotificationPriority.CRITICAL)
        return False

    def _evict_channel_priority(
        self, state: _ChannelState, priority: NotificationPriority
    ) -> bool:
        ready = state.pending[priority]
        delayed = [
            (index, item) for index, (_, _, item) in enumerate(state.delayed)
            if item.event.priority == priority
        ]
        ready_oldest = ready[0] if ready else None
        delayed_oldest = min(delayed, key=lambda pair: pair[1].started) if delayed else None
        if ready_oldest is None and delayed_oldest is None:
            return False
        if ready_oldest is not None and (
            delayed_oldest is None or ready_oldest.started <= delayed_oldest[1].started
        ):
            removed = ready.popleft()
        else:
            assert delayed_oldest is not None
            removed = state.delayed.pop(delayed_oldest[0])[2]
            heapq.heapify(state.delayed)
        self._drop(removed.event)
        state.size -= 1
        self._update_queue_metric()
        return True

    def _next_channel_delivery(self, state: _ChannelState) -> _Delivery | None:
        now = monotonic()
        while state.delayed and state.delayed[0][0] <= now:
            _, _, delivery = heapq.heappop(state.delayed)
            state.pending[delivery.event.priority].append(delivery)
        for priority in PRIORITIES:
            if state.pending[priority]:
                delivery = state.pending[priority].popleft()
                state.size -= 1
                self._update_queue_metric()
                return delivery
        return None

    async def _run_channel(self, state: _ChannelState) -> None:
        while True:
            delivery = self._next_channel_delivery(state)
            if delivery is not None:
                try:
                    await self._send_once(state, delivery)
                except Exception:
                    self._enqueue_channel(state, delivery)
                    raise
                continue
            if self._stopping and self._dispatcher_done and state.size == 0:
                return
            state.wakeup.clear()
            delay = max(0, state.delayed[0][0] - monotonic()) if state.delayed else None
            try:
                if delay is None:
                    await state.wakeup.wait()
                else:
                    await asyncio.wait_for(state.wakeup.wait(), timeout=delay)
            except TimeoutError:
                pass

    async def _send_once(self, state: _ChannelState, delivery: _Delivery) -> None:
        delivery.attempts += 1
        event = delivery.event
        labels = {
            "channel": state.name,
            "priority": event.priority.value,
            "category": event.category.value,
        }
        try:
            timeout = float(getattr(state.channel, "timeout", self.channel_timeout_seconds))
            async with asyncio.timeout(timeout):
                await state.channel.send(event)
        except Exception as exc:
            delivery.error_type = type(exc).__name__
            delivery.status_code = getattr(exc, "status_code", None)
            if delivery.attempts <= len(self.retry_delays):
                if state.size < self.max_queue or self._make_channel_room(state, event):
                    self._retry_sequence += 1
                    heapq.heappush(
                        state.delayed,
                        (monotonic() + self.retry_delays[delivery.attempts - 1],
                         self._retry_sequence, delivery),
                    )
                    state.size += 1
                    self._update_queue_metric()
                    state.wakeup.set()
                    return
                self._drop(event)
            self.metrics.notification_failed.labels(**labels).inc()
            logger.error(
                "notification delivery failed: channel=%s priority=%s error_type=%s status=%s",
                state.name, event.priority.value, delivery.error_type, delivery.status_code,
            )
            sent = False
        else:
            self.metrics.notification_sent.labels(**labels).inc()
            sent = True
        self.metrics.notification_latency.labels(**labels).observe(monotonic() - delivery.started)
        await self._audit(event, state.name, sent, delivery.attempts, delivery.error_type)

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
        self._dispatcher_done = False
        self._worker = asyncio.create_task(self._run(), name="notification-worker")
        self._worker.add_done_callback(self._worker_done)
        self._wakeup.set()

    def _channel_worker_done(self, state: _ChannelState, task: asyncio.Task[None]) -> None:
        if self._stopping:
            return
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            exc = None
        if exc is not None:
            logger.error("notification channel worker restarted: channel=%s error_type=%s",
                         state.name, type(exc).__name__)
        state.worker = asyncio.create_task(
            self._run_channel(state), name=f"notification-{state.name}-worker"
        )
        state.worker.add_done_callback(partial(self._channel_worker_done, state))
        state.wakeup.set()

    async def stop(self, *, drain_seconds: float = 3) -> None:
        self._accepting = False
        self._stopping = True
        self._wakeup.set()
        for state in self._channels:
            state.wakeup.set()
        tasks = [task for task in (
            self._worker, *(state.worker for state in self._channels)
        ) if task is not None]
        if not tasks:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=drain_seconds
            )
        except TimeoutError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.warning("notification drain timed out")
            self._discard_pending()

    def _discard_pending(self) -> None:
        for ingress in self._pending.values():
            while ingress:
                self._drop(ingress.popleft())
        self._size = 0
        for state in self._channels:
            for channel_pending in state.pending.values():
                while channel_pending:
                    self._drop(channel_pending.popleft().event)
            while state.delayed:
                self._drop(heapq.heappop(state.delayed)[2].event)
            state.size = 0
        self._update_queue_metric()

    async def close(self) -> None:
        await self.stop(drain_seconds=0.1)
        for channel in self.channels:
            close = getattr(channel, "close", None)
            if close is not None:
                try:
                    await close()
                except Exception as exc:
                    logger.error("notification channel close failed: %s", type(exc).__name__)
