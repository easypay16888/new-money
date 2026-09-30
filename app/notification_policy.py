from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic

from app.models import (
    NotificationCategory,
    NotificationEvent,
    NotificationLevel,
    NotificationPriority,
)
from app.notification_events import event_code, reason_code
from app.recovery import AUTO_RECOVERABLE_REASONS


@dataclass(frozen=True)
class NotificationDecision:
    send: bool
    priority: NotificationPriority | None = None


class NotificationPolicy:
    """Channel routing only. It has no handle to trading or risk controls."""

    def __init__(
        self, *, heartbeat_enabled: bool = False, entry_submitted: bool = False,
        system_stopping: bool = False, trade_enabled: bool = True,
        risk_enabled: bool = True, daily_enabled: bool = True,
        webhook_verbose: bool = True, clock: Callable[[], float] = monotonic,
    ) -> None:
        self.heartbeat_enabled = heartbeat_enabled
        self.entry_submitted = entry_submitted
        self.system_stopping = system_stopping
        self.trade_enabled = trade_enabled
        self.risk_enabled = risk_enabled
        self.daily_enabled = daily_enabled
        self.webhook_verbose = webhook_verbose
        self.clock = clock
        self._trade_seen: dict[tuple[str, str], float] = {}
        self._daily_seen: set[str] = set()
        self._watchdog_delivered: dict[str, bool] = {}
        self._watchdog_alert_pending: dict[str, bool] = {}
        self._watchdog_recovery_pending: dict[str, NotificationEvent] = {}

    @staticmethod
    def halt_reason(event: NotificationEvent) -> str:
        return reason_code(event)

    @staticmethod
    def is_critical(event: NotificationEvent) -> bool:
        reason = NotificationPolicy.halt_reason(event) if event_code(event) == "RISK_HALT" else ""
        safety_halt = (
            reason in {
                "position mismatch", "startup position mismatch", "startup fill size mismatch",
                "margin ratio danger", "protective stop invalid",
            }
            or "unprotected" in reason
            or "protective stop" in reason
            or (
                bool(event.metadata.get("has_exposure"))
                and reason in {
                    "foreign risk-increasing pending order", "unexpected algo order",
                    "unexpected order",
                }
            )
        )
        return (
            event.level == NotificationLevel.CRITICAL
            or event.priority == NotificationPriority.CRITICAL
            or event_code(event) in {"RISK_EMERGENCY", "WATCHDOG_OFFLINE"}
            or safety_halt
        )

    def evaluate(self, event: NotificationEvent, channel: str) -> NotificationDecision:
        if channel == "console" or (channel == "webhook" and self.webhook_verbose):
            return NotificationDecision(True)
        if channel not in {"bark", "webhook"}:
            return NotificationDecision(True)
        if event.metadata.get("incident_duplicate"):
            return NotificationDecision(False)
        if self.is_critical(event):
            if event_code(event) == "WATCHDOG_OFFLINE":
                self._watchdog_alert_pending[channel] = True
            return NotificationDecision(True, NotificationPriority.CRITICAL)
        if event.metadata.get("incident_notification"):
            return NotificationDecision(True)
        code = event_code(event)
        if code == "RISK_HALT":
            reason = self.halt_reason(event)
            if reason in AUTO_RECOVERABLE_REASONS or reason in {
                "startup reconciliation pending", "manual stop", "auto recovery circuit breaker",
            }:
                return NotificationDecision(False)
            return NotificationDecision(self.risk_enabled)
        if code in {"RISK_RECOVERED", "AUTO_RECOVERY_COMPLETED"}:
            return NotificationDecision(False)
        if code == "WATCHDOG_OFFLINE":
            return NotificationDecision(True, NotificationPriority.CRITICAL)
        if code == "WATCHDOG_UNHEALTHY":
            eligible = bool(event.metadata.get("bark_eligible"))
            if eligible:
                self._watchdog_alert_pending[channel] = True
            return NotificationDecision(eligible)
        if code == "WATCHDOG_RECOVERED":
            if "watchdog_delivery_confirmed" in event.metadata:
                return NotificationDecision(bool(event.metadata["watchdog_delivery_confirmed"]))
            if not self._watchdog_delivered.get(channel) and self._watchdog_alert_pending.get(channel):
                self._watchdog_recovery_pending[channel] = event
            return NotificationDecision(self._watchdog_delivered.get(channel, False))
        if event.category == NotificationCategory.INFRASTRUCTURE:
            return NotificationDecision(False)
        if event.category == NotificationCategory.HEARTBEAT:
            return NotificationDecision(self.heartbeat_enabled)
        if event.category == NotificationCategory.DAILY_REPORT:
            if channel == "bark" and event.dedup_key:
                if event.dedup_key in self._daily_seen:
                    return NotificationDecision(False)
                self._daily_seen.add(event.dedup_key)
            return NotificationDecision(self.daily_enabled)
        if code == "SYSTEM_STOPPING":
            return NotificationDecision(self.system_stopping)
        if code in {"SYSTEM_STARTED", "SYSTEM_STOPPED"}:
            return NotificationDecision(True)
        if event.category == NotificationCategory.TRADE:
            if not self.trade_enabled:
                return NotificationDecision(False)
            if code == "ENTRY_SUBMITTED":
                return NotificationDecision(self.entry_submitted)
            if code in {"TRADE_FILLED", "PROTECTION_ACTIVE", "POSITION_CLOSED"}:
                if event.dedup_key:
                    now = self.clock()
                    trade_key = (channel, event.dedup_key)
                    if trade_key in self._trade_seen:
                        return NotificationDecision(False)
                    self._trade_seen[trade_key] = now
                    if len(self._trade_seen) > 10000:
                        cutoff = now - 86400
                        self._trade_seen = {
                            key: when for key, when in self._trade_seen.items() if when >= cutoff
                        }
                return NotificationDecision(True)
            return NotificationDecision(False)
        if event.category == NotificationCategory.RISK:
            return NotificationDecision(self.risk_enabled and event.level in {
                NotificationLevel.ERROR, NotificationLevel.CRITICAL,
            })
        return NotificationDecision(event.level in {NotificationLevel.ERROR, NotificationLevel.CRITICAL})

    def delivered(self, event: NotificationEvent, channel: str) -> NotificationEvent | None:
        if channel in {"bark", "webhook"}:
            if event_code(event) in {"WATCHDOG_OFFLINE", "WATCHDOG_UNHEALTHY"}:
                self._watchdog_delivered[channel] = True
                self._watchdog_alert_pending[channel] = False
                return self._watchdog_recovery_pending.pop(channel, None)
            if event_code(event) == "WATCHDOG_RECOVERED":
                self._watchdog_delivered[channel] = False
        return None

    def failed(self, event: NotificationEvent, channel: str) -> None:
        if channel in {"bark", "webhook"} and event_code(event) in {"WATCHDOG_OFFLINE", "WATCHDOG_UNHEALTHY"}:
            self._watchdog_alert_pending[channel] = False
            self._watchdog_recovery_pending.pop(channel, None)
