from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from time import monotonic
from typing import Any
from urllib.parse import urlsplit

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
from app.notification_policy import NotificationPolicy
from app.notifications import BarkNotification, NotificationManager

logger = logging.getLogger("watchdog")
ALLOWED_RISK_STATES = {"NORMAL", "CAUTION", "REDUCE", "HALT", "EMERGENCY"}


class WatchdogSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    watchdog_enabled: bool = True
    watchdog_status_url: str = "http://app:8000/status"
    watchdog_interval_seconds: float = Field(default=60, gt=0)
    watchdog_failure_threshold: int = Field(default=3, ge=1)
    watchdog_recovery_threshold: int = Field(default=2, ge=1)
    watchdog_startup_grace_seconds: float = Field(default=120, ge=0)
    watchdog_unhealthy_alert_seconds: float = Field(default=60, ge=0)
    watchdog_bark_group: str = ""
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

    async def check(self) -> None:
        kind, error, status = await self._probe()
        if kind is None:
            self.failures = 0
            self.failure_kind = None
            self.successes += 1
            if self.alerted_kind and self.successes >= self.settings.watchdog_recovery_threshold:
                previous = self.alerted_kind
                self.alerted_kind = None
                await self.notifications.publish(NotificationEvent(
                    level=NotificationLevel.INFO,
                    category=NotificationCategory.INFRASTRUCTURE,
                    title="✅ Trading App Recovered",
                    message=f"Status endpoint healthy again\nPrevious: {previous}",
                    priority=NotificationPriority.ACTIVE,
                    dedup_key="watchdog:app-state",
                    metadata={"recovery": True},
                ))
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
            return
        self.unhealthy_bark_eligible |= bark_eligible
        self.alerted_kind = kind
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
        await self.notifications.publish(NotificationEvent(
            level=level,
            category=NotificationCategory.INFRASTRUCTURE,
            title=title,
            message=message,
            priority=priority,
            dedup_key="watchdog:app-state",
            metadata={"transition": True, "bark_eligible": bark_eligible},
        ))

    async def _probe(self) -> tuple[str | None, str, dict[str, Any] | None]:
        try:
            response = await self.client.get(self.settings.watchdog_status_url)
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
        if not isinstance(sockets, list) or not sockets or not all(
            isinstance(socket, dict) and socket.get("fresh") is True for socket in sockets
        ):
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
