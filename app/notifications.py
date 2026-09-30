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

from app.incidents import IncidentManager
from app.models import (
    NotificationCategory,
    NotificationEvent,
    NotificationLevel,
    NotificationPriority,
)
from app.monitoring import Metrics, Notification
from app.notification_policy import NotificationPolicy
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
        policy: NotificationPolicy | None = None,
        incidents: IncidentManager | None = None,
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
        self.policy = policy
        self.incidents = incidents
        self._incident_task: asyncio.Task[None] | None = None
        self._incident_audits: set[asyncio.Task[None]] = set()
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
        if self.incidents is not None:
            self._incident_task = asyncio.create_task(
                self._run_incidents(), name="notification-incident-worker"
            )

    async def _run_incidents(self) -> None:
        assert self.incidents is not None
        while not self._stopping:
            await asyncio.sleep(0.25)
            try:
                generated = self.incidents.due()
                self._record_incident_changes()
                for event in generated:
                    await self.publish(event)
            except Exception as exc:
                logger.error("notification incident worker error_type=%s", type(exc).__name__)

    def _record_incident_changes(self) -> None:
        if self.incidents is None:
            return
        for incident, transition in self.incidents.take_changes():
            risk_incident = incident.key.startswith(("risk:", "auto-recovery:"))
            labels = {
                "category": "risk" if risk_incident else "infrastructure",
                "severity": incident.severity,
                "component": (
                    "emergency" if incident.key == "risk:emergency" else
                    "safety" if risk_incident else "trading"
                ),
            }
            if transition == "opened":
                self.metrics.incidents_total.labels(**labels).inc()
                self.metrics.incidents_open.labels(**labels).inc()
            elif transition == "resolved":
                self.metrics.incidents_resolved.labels(**labels).inc()
                self.metrics.incidents_open.labels(**labels).dec()
                self.metrics.incident_duration.labels(**labels).observe(
                    incident.duration_seconds or 0
                )
            if transition in {"alert_failed", "recovery_failed"}:
                self.metrics.incident_delivery_failure.labels(
                    severity=incident.severity,
                    phase="open" if transition == "alert_failed" else "resolved",
                ).inc()
            elif transition in {"alert_retry_queued", "recovery_retry_queued"}:
                self.metrics.incident_delivery_retry.labels(
                    severity=incident.severity,
                    phase="open" if transition == "alert_retry_queued" else "resolved",
                ).inc()
            elif transition == "retrospective_sent":
                self.metrics.incident_retrospective.labels(
                    severity=incident.severity, phase="resolved"
                ).inc()
            if self.store is not None:
                task = asyncio.create_task(
                    self._audit_incident(incident.record(transition), incident.id)
                )
                self._incident_audits.add(task)
                task.add_done_callback(self._incident_audits.discard)

    async def _audit_incident(self, payload: dict[str, object], incident_id: str) -> None:
        assert self.store is not None
        try:
            async with asyncio.timeout(0.5):
                await self.store.append("system_events", payload, reference_id=incident_id)
        except Exception as exc:
            logger.error("incident audit unavailable: %s", type(exc).__name__)

    async def publish(self, event: NotificationEvent) -> None:
        """Enqueue without awaiting network, persistence, or queue capacity."""
        try:
            if (
                NotificationPolicy.is_critical(event)
                and event.priority != NotificationPriority.CRITICAL
            ):
                event = event.model_copy(update={"priority": NotificationPriority.CRITICAL})
            elif event.level == NotificationLevel.ERROR and event.priority in {
                NotificationPriority.PASSIVE, NotificationPriority.ACTIVE,
            }:
                event = event.model_copy(update={"priority": NotificationPriority.TIME_SENSITIVE})
            if not self._accepting:
                self._drop(event)
                return
            generated: list[NotificationEvent] = []
            if self.incidents is not None:
                try:
                    generated = self.incidents.observe(event)
                    self._record_incident_changes()
                except Exception as exc:
                    logger.error(
                        "notification incident observation error_type=%s", type(exc).__name__
                    )
            for derived in generated:
                await self.publish(derived)
            now = monotonic()
            fingerprint = (
                event.dedup_key or "",
                event.level.value,
                event.title,
                event.message,
            )
            stateful = bool(event.metadata.get("recovery") or event.metadata.get("transition"))
            incident_retry = bool(event.metadata.get("incident_retry"))
            if event.dedup_key and not incident_retry:
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
            if event.dedup_key and not incident_retry:
                self._recent[fingerprint] = now
                if stateful:
                    self._last_stateful[event.dedup_key] = fingerprint
            self._published += 1
            if self._published % 1000 == 0:
                cutoff = now - max(self.dedup_seconds, 60)
                self._recent = {key: when for key, when in self._recent.items() if when >= cutoff}
        except Exception as exc:
            logger.error("notification enqueue failed: %s", type(exc).__name__)

    def _drop(self, event: NotificationEvent, channel: str | None = None) -> None:
        self.metrics.notification_dropped.labels(
            priority=event.priority.value, category=event.category.value
        ).inc()
        if event.priority == NotificationPriority.CRITICAL:
            logger.error("critical notification dropped: category=%s", event.category.value)
        if (
            self.incidents is not None
            and (channel is None or channel == "bark")
            and any(state.name == "bark" for state in self._channels)
        ):
            try:
                self.incidents.delivery_failed(event)
                self._record_incident_changes()
            except Exception as exc:
                logger.error("incident drop receipt error_type=%s", type(exc).__name__)

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
                routed = event
                if self.policy is not None:
                    try:
                        decision = self.policy.evaluate(event, state.name)
                    except Exception as exc:
                        logger.error(
                            "notification policy error_type=%s channel=%s",
                            type(exc).__name__, state.name,
                        )
                        decision = None
                    if decision is None:
                        if state.name == "bark" and not NotificationPolicy.is_critical(event):
                            self.metrics.notification_policy_suppressed.labels(
                                channel=state.name, category=event.category.value
                            ).inc()
                            continue
                        if state.name == "bark" and NotificationPolicy.is_critical(event):
                            routed = event.model_copy(update={
                                "priority": NotificationPriority.CRITICAL
                            })
                    elif not decision.send and not (
                        state.name == "bark" and NotificationPolicy.is_critical(event)
                        and not event.metadata.get("incident_duplicate")
                    ):
                        self.metrics.notification_policy_suppressed.labels(
                            channel=state.name, category=event.category.value
                        ).inc()
                        continue
                    elif decision.priority is not None and decision.priority != event.priority:
                        routed = event.model_copy(update={"priority": decision.priority})
                self._enqueue_channel(state, _Delivery(routed))
            except Exception as exc:
                logger.error("notification channel enqueue failed: channel=%s error_type=%s",
                             state.name, type(exc).__name__)

    def _enqueue_channel(self, state: _ChannelState, delivery: _Delivery) -> None:
        if state.size >= self.max_queue and not self._make_channel_room(state, delivery.event):
            self._drop(delivery.event, state.name)
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
        self._drop(removed.event, state.name)
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
        event = delivery.event
        if (
            state.name == "bark" and self.incidents is not None
            and self.incidents.should_supersede_open(event)
        ):
            self.incidents.supersede_open(event, delivery.attempts)
            self._record_incident_changes()
            generated = self.incidents.due()
            self._record_incident_changes()
            for derived in generated:
                await self.publish(derived)
            return
        delivery.attempts += 1
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
                # The final failure receipt below handles this delivery once.
                self._drop(event, "retry-capacity")
            self.metrics.notification_failed.labels(**labels).inc()
            if state.name == "bark" and self.incidents is not None:
                try:
                    self.incidents.delivery_failed(event)
                    self._record_incident_changes()
                except Exception as incident_exc:
                    logger.error(
                        "incident failure receipt error_type=%s", type(incident_exc).__name__
                    )
            if self.policy is not None:
                try:
                    self.policy.failed(event, state.name)
                except Exception as policy_exc:
                    logger.error(
                        "notification policy failure receipt error_type=%s",
                        type(policy_exc).__name__,
                    )
            logger.error(
                "notification delivery failed: channel=%s priority=%s error_type=%s status=%s",
                state.name, event.priority.value, delivery.error_type, delivery.status_code,
            )
            sent = False
        else:
            self.metrics.notification_sent.labels(**labels).inc()
            if self.policy is not None:
                try:
                    pending_recovery = self.policy.delivered(event, state.name)
                    if pending_recovery is not None:
                        self._enqueue_channel(state, _Delivery(pending_recovery))
                except Exception as exc:
                    logger.error("notification policy receipt error_type=%s", type(exc).__name__)
            if state.name == "bark" and self.incidents is not None:
                try:
                    generated = self.incidents.delivery_confirmed(event)
                    self._record_incident_changes()
                    for derived in generated:
                        await self.publish(derived)
                except Exception as exc:
                    logger.error("incident receipt error_type=%s", type(exc).__name__)
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
        if self._incident_task is not None:
            self._incident_task.cancel()
            await asyncio.gather(self._incident_task, return_exceptions=True)
            self._incident_task = None
        self._wakeup.set()
        for state in self._channels:
            state.wakeup.set()
        tasks = [task for task in (
            self._worker, *(state.worker for state in self._channels)
        ) if task is not None]
        if not tasks:
            await self._drain_incident_audits(drain_seconds)
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
        await self._drain_incident_audits(drain_seconds)

    async def _drain_incident_audits(self, drain_seconds: float) -> None:
        if not self._incident_audits:
            return
        pending = tuple(self._incident_audits)
        done, unfinished = await asyncio.wait(pending, timeout=min(drain_seconds, 0.5))
        for task in done:
            try:
                task.result()
            except Exception as exc:
                logger.error("incident audit task error_type=%s", type(exc).__name__)
        for task in unfinished:
            task.cancel()
        if unfinished:
            await asyncio.gather(*unfinished, return_exceptions=True)
            logger.warning("incident audit drain timed out")

    def _discard_pending(self) -> None:
        for ingress in self._pending.values():
            while ingress:
                self._drop(ingress.popleft())
        self._size = 0
        for state in self._channels:
            for channel_pending in state.pending.values():
                while channel_pending:
                    self._drop(channel_pending.popleft().event, state.name)
            while state.delayed:
                self._drop(heapq.heappop(state.delayed)[2].event, state.name)
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
