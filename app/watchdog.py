from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.models import (
    NotificationCategory,
    NotificationEvent,
    NotificationLevel,
    NotificationPriority,
)
from app.monitoring import ConsoleNotification, Metrics, Notification
from app.notification_events import event_code
from app.notification_policy import NotificationPolicy
from app.notifications import BarkNotification, NotificationManager

logger = logging.getLogger("watchdog")
ALLOWED_RISK_STATES = {"NORMAL", "CAUTION", "REDUCE", "HALT", "EMERGENCY"}


class WatchdogSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    watchdog_enabled: bool = True
    watchdog_status_url: str = "http://app:8000/status"
    watchdog_status_token: SecretStr = SecretStr("")
    watchdog_interval_seconds: float = Field(default=60, gt=0)
    watchdog_failure_threshold: int = Field(default=3, ge=1)
    watchdog_recovery_threshold: int = Field(default=2, ge=1)
    watchdog_startup_grace_seconds: float = Field(default=120, ge=0)
    watchdog_unhealthy_alert_seconds: float = Field(default=60, ge=0)
    watchdog_bark_group: str = ""
    bark_incident_retry_initial_seconds: float = Field(default=30, gt=0)
    bark_incident_retry_max_seconds: float = Field(default=300, gt=0)
    bark_server: str = "https://api.day.app"
    bark_device_key: SecretStr = SecretStr("")
    bark_group: str = "OKX Quant"
    bark_timeout_seconds: float = Field(default=5, gt=0)
    bark_sound: str = ""
    bark_critical_sound: str = "alarm"
    bark_critical_volume: int = Field(default=5, ge=0, le=10)

    @field_validator("watchdog_status_url")
    @classmethod
    def only_status_endpoint(cls, value: str) -> str:
        url = urlsplit(value)
        if (
            url.scheme not in {"http", "https"} or not url.hostname
            or url.username or url.password or url.path != "/status"
            or url.query or url.fragment
        ):
            raise ValueError("WATCHDOG_STATUS_URL must point to /status")
        return value


@dataclass
class WatchdogOutage:
    id: str
    generation: int = 0
    event: NotificationEvent | None = None
    confirmed: bool = False
    ever_delivered: bool = False
    pending: bool = False
    delivery_failures: int = 0
    retry_at: float = 0
    resolved: bool = False
    recovery_bark_queued: bool = False
    previous_kind: str = "APP_DOWN"


class WatchdogMonitor:
    def __init__(
        self,
        settings: WatchdogSettings,
        client: httpx.AsyncClient,
        notifications: NotificationManager,
        *,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self.settings = settings
        self.client = client
        self.notifications = notifications
        self.clock = clock
        self.started_at = clock()
        self.failures = 0
        self.successes = 0
        self.failure_kind: str | None = None
        self.alerted_kind: str | None = None
        self.unhealthy_since: float | None = None
        self.unhealthy_bark_eligible = False
        if settings.bark_incident_retry_max_seconds < settings.bark_incident_retry_initial_seconds:
            raise ValueError("invalid watchdog notification retry interval")
        self._outage: WatchdogOutage | None = None
        notifications.add_delivery_observer(self)

    @property
    def outage_id(self) -> str | None:
        return self._outage.id if self._outage else None

    @property
    def alert_delivery_confirmed(self) -> bool:
        return bool(self._outage and self._outage.confirmed)

    @property
    def alert_delivery_pending(self) -> bool:
        return bool(self._outage and self._outage.pending)

    @property
    def alert_delivery_failures(self) -> int:
        return self._outage.delivery_failures if self._outage else 0

    @property
    def next_alert_retry_at(self) -> float:
        return self._outage.retry_at if self._outage else 0

    def _recovery_event(self, outage: WatchdogOutage) -> NotificationEvent:
        return NotificationEvent(
            event_code="WATCHDOG_RECOVERED", level=NotificationLevel.INFO,
            category=NotificationCategory.INFRASTRUCTURE,
            title="✅ Trading App Recovered",
            message=f"Status endpoint healthy again\nPrevious: {outage.previous_kind}",
            priority=NotificationPriority.ACTIVE,
            dedup_key="watchdog:app-state",
            metadata={"recovery": True, "watchdog_outage_id": outage.id,
                      "watchdog_delivery_confirmed": outage.ever_delivered},
        )

    def _matches(self, event: NotificationEvent) -> bool:
        return bool(self._outage and event.metadata.get("watchdog_outage_id") == self._outage.id)

    def should_send(self, event: NotificationEvent, channel: str) -> bool:
        if channel != "bark" or "watchdog_outage_id" not in event.metadata:
            return True
        if not self._matches(event):
            return False
        assert self._outage is not None
        if event_code(event) in {"WATCHDOG_OFFLINE", "WATCHDOG_UNHEALTHY"}:
            return (
                not self._outage.resolved
                and event.metadata.get("watchdog_alert_generation") == self._outage.generation
            )
        return True

    def delivered(self, event: NotificationEvent, channel: str) -> NotificationEvent | None:
        if channel != "bark" or not self._matches(event):
            return None
        if event_code(event) not in {"WATCHDOG_OFFLINE", "WATCHDOG_UNHEALTHY"}:
            return None
        assert self._outage is not None
        outage = self._outage
        outage.ever_delivered = True
        if event.metadata.get("watchdog_alert_generation") == outage.generation:
            outage.confirmed, outage.pending = True, False
        # An HTTP request already in flight may succeed after the recovery check.
        if outage.resolved and not outage.recovery_bark_queued:
            outage.recovery_bark_queued = True
            recovery = self._recovery_event(outage)
            recovery.metadata["notification_retry"] = True
            return recovery
        return None

    def failed(self, event: NotificationEvent, channel: str) -> None:
        if channel != "bark" or not self._matches(event):
            return
        assert self._outage is not None
        outage = self._outage
        if (
            event_code(event) not in {"WATCHDOG_OFFLINE", "WATCHDOG_UNHEALTHY"}
            or event.metadata.get("watchdog_alert_generation") != outage.generation
            or outage.resolved or outage.confirmed
        ):
            return
        outage.pending = False
        outage.delivery_failures += 1
        delay = min(
            self.settings.bark_incident_retry_max_seconds,
            self.settings.bark_incident_retry_initial_seconds
            * 2 ** min(outage.delivery_failures - 1, 30),
        )
        outage.retry_at = self.clock() + delay
        logger.error("watchdog Bark delivery failed: failures=%s retry_seconds=%s",
                     outage.delivery_failures, delay)

    async def check(self) -> None:
        kind, error, status = await self._probe()
        if kind is None:
            self.failures = 0
            self.failure_kind = None
            self.successes += 1
            if self.alerted_kind and self.successes >= self.settings.watchdog_recovery_threshold:
                self.alerted_kind = None
                if self._outage is not None:
                    self._outage.resolved = True
                    self._outage.pending = False
                    self._outage.recovery_bark_queued = self._outage.ever_delivered
                    await self.notifications.publish(self._recovery_event(self._outage))
            if self.successes >= self.settings.watchdog_recovery_threshold:
                self.unhealthy_since = None
                self.unhealthy_bark_eligible = False
            return
        self.successes = 0
        if self.clock() - self.started_at < self.settings.watchdog_startup_grace_seconds:
            self.failures = 0
            self.failure_kind = None
            return
        if kind == "APP_ALIVE_BUT_UNHEALTHY" and self.failure_kind != kind:
            self.unhealthy_since = self.clock()
        self.failures = self.failures + 1 if self.failure_kind == kind else 1
        self.failure_kind = kind
        if self.failures < self.settings.watchdog_failure_threshold:
            return
        bark_eligible = (
            kind == "APP_ALIVE_BUT_UNHEALTHY"
            and self.unhealthy_since is not None
            and self.clock() - self.unhealthy_since
            >= self.settings.watchdog_unhealthy_alert_seconds
        )
        if self.alerted_kind == kind and (not bark_eligible or self.unhealthy_bark_eligible):
            outage = self._outage
            if (
                outage is not None and outage.event is not None and not outage.resolved
                and not outage.confirmed and not outage.pending
                and (kind == "APP_DOWN" or self.unhealthy_bark_eligible)
                and self.clock() >= outage.retry_at
            ):
                retry = outage.event.model_copy(deep=True)
                retry.metadata["notification_retry"] = True
                outage.pending = True
                await self.notifications.publish(retry)
            return
        self.unhealthy_bark_eligible |= bark_eligible
        self.alerted_kind = kind
        if self._outage is None or self._outage.resolved:
            self._outage = WatchdogOutage(id=uuid4().hex)
        outage = self._outage
        outage.generation += 1
        outage.previous_kind = kind
        outage.confirmed = False
        outage.pending = kind == "APP_DOWN" or bark_eligible
        outage.delivery_failures = 0
        outage.retry_at = 0
        if kind == "APP_DOWN":
            title = "🚨 Trading App Offline"
            message = (
                f"Status endpoint unavailable\nFailures: {self.failures}"
                f"\nLast error: {error}"
            )
            level, priority = NotificationLevel.CRITICAL, NotificationPriority.CRITICAL
        else:
            title = "⚠️ Trading App Unhealthy"
            risk = status.get("risk_state", "unknown") if status else "unknown"
            message = f"App responds but is not ready\nFailures: {self.failures}\nRisk: {risk}"
            level, priority = NotificationLevel.ERROR, NotificationPriority.TIME_SENSITIVE
        outage.event = NotificationEvent(
            event_code="WATCHDOG_OFFLINE" if kind == "APP_DOWN" else "WATCHDOG_UNHEALTHY",
            level=level,
            category=NotificationCategory.INFRASTRUCTURE,
            title=title,
            message=message,
            priority=priority,
            dedup_key="watchdog:app-state",
            metadata={"transition": True, "bark_eligible": bark_eligible,
                      "watchdog_outage_id": outage.id,
                      "watchdog_alert_generation": outage.generation},
        )
        await self.notifications.publish(outage.event.model_copy(deep=True))

    async def _probe(self) -> tuple[str | None, str, dict[str, Any] | None]:
        try:
            token = self.settings.watchdog_status_token.get_secret_value()
            headers = {"Authorization": "Bearer " + token} if token else {}
            response = await self.client.get(self.settings.watchdog_status_url, headers=headers)
        except Exception as exc:
            return "APP_DOWN", type(exc).__name__, None
        if response.status_code != 200:
            return "APP_DOWN", f"HTTP {response.status_code}", None
        try:
            status = response.json()
        except ValueError:
            return "APP_DOWN", "InvalidJSON", None
        if not isinstance(status, dict):
            return "APP_DOWN", "InvalidStatus", None
        if status.get("running") is not True:
            return "APP_ALIVE_BUT_UNHEALTHY", "NotRunning", status
        if status.get("synchronized") is not True:
            return "APP_ALIVE_BUT_UNHEALTHY", "NotSynchronized", status
        if status.get("risk_state") not in ALLOWED_RISK_STATES:
            return "APP_ALIVE_BUT_UNHEALTHY", "InvalidRiskState", status
        sockets = status.get("websockets")
        if not isinstance(sockets, list) or not sockets:
            return "APP_ALIVE_BUT_UNHEALTHY", "WebSocketStale", status
        for socket in sockets:
            if not isinstance(socket, dict):
                return "APP_ALIVE_BUT_UNHEALTHY", "InvalidWebSocketStatus", status
            modern_fields = {"transport_healthy", "critical_data_fresh", "processing_healthy",
                             "reconciliation_required"}
            if modern_fields.intersection(socket):
                if not modern_fields.issubset(socket):
                    return "APP_ALIVE_BUT_UNHEALTHY", "InvalidWebSocketStatus", status
                if socket.get("transport_healthy") is not True:
                    return "APP_ALIVE_BUT_UNHEALTHY", "WebSocketTransportUnavailable", status
                if socket.get("critical_data_fresh") is not True:
                    return "APP_ALIVE_BUT_UNHEALTHY", "MarketDataStale", status
                if socket.get("processing_healthy") is not True:
                    return "APP_ALIVE_BUT_UNHEALTHY", "WebSocketProcessingBacklog", status
                if socket.get("reconciliation_required") is not False:
                    return "APP_ALIVE_BUT_UNHEALTHY", "WebSocketReconciliationPending", status
            elif socket.get("fresh") is not True:
                # Existing deployed status schema remains readable during upgrades.
                return "APP_ALIVE_BUT_UNHEALTHY", "WebSocketStale", status
        return None, "", status


async def run_watchdog() -> None:
    settings = WatchdogSettings()
    if not settings.watchdog_enabled:
        logger.info("watchdog disabled")
        await asyncio.Event().wait()
        return
    channels: list[Notification] = [ConsoleNotification()]
    if settings.bark_device_key.get_secret_value():
        bark_settings = settings.model_copy(update={
            "bark_group": settings.watchdog_bark_group or settings.bark_group
        })
        try:
            channels.append(BarkNotification(bark_settings))
        except Exception as exc:
            logger.error("watchdog Bark disabled: %s", type(exc).__name__)
    else:
        logger.error("watchdog Bark disabled: device key is missing")
    notifications = NotificationManager(channels, Metrics(), policy=NotificationPolicy())
    notifications.start()
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            monitor = WatchdogMonitor(settings, client, notifications)
            while True:
                await monitor.check()
                await asyncio.sleep(settings.watchdog_interval_seconds)
    finally:
        await notifications.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_watchdog())
