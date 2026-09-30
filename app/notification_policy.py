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
        return next(
            (line.removeprefix("Reason: ") for line in event.message.splitlines()
             if line.startswith("Reason: ")), "",
        )

    @staticmethod
    def is_critical(event: NotificationEvent) -> bool:
        reason = NotificationPolicy.halt_reason(event) if event.title == "🚨 HALT" else ""
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
            or event.title in {"🚨 EMERGENCY", "🚨 Trading App Offline"}
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
            if event.title == "🚨 Trading App Offline":
                self._watchdog_alert_pending[channel] = True
            return NotificationDecision(True, NotificationPriority.CRITICAL)
        if event.metadata.get("incident_notification"):
            return NotificationDecision(True)
        title = event.title
        if title == "🚨 HALT":
            reason = self.halt_reason(event)
            if reason in AUTO_RECOVERABLE_REASONS or reason in {
                "startup reconciliation pending", "manual stop", "auto recovery circuit breaker",
            }:
                return NotificationDecision(False)
            return NotificationDecision(self.risk_enabled)
        if title in {"✅ Risk State Recovered", "✅ Auto Recovery Completed"}:
            return NotificationDecision(False)
        if title == "🚨 Trading App Offline":
            return NotificationDecision(True, NotificationPriority.CRITICAL)
        if title == "⚠️ Trading App Unhealthy":
            eligible = bool(event.metadata.get("bark_eligible"))
            if eligible:
                self._watchdog_alert_pending[channel] = True
            return NotificationDecision(eligible)
        if title == "✅ Trading App Recovered":
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
        if title == "🟡 Quant System Stopping":
            return NotificationDecision(self.system_stopping)
        if title in {"🟢 Quant System Started", "⚪ Quant System Stopped"}:
            return NotificationDecision(True)
        if event.category == NotificationCategory.TRADE:
            if not self.trade_enabled:
                return NotificationDecision(False)
            if "Entry Submitted" in title:
                return NotificationDecision(self.entry_submitted)
            if any(word in title for word in (" Filled", "Protection Active", "Position Closed")):
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
            if event.title in {"🚨 Trading App Offline", "⚠️ Trading App Unhealthy"}:
                self._watchdog_delivered[channel] = True
                self._watchdog_alert_pending[channel] = False
                return self._watchdog_recovery_pending.pop(channel, None)
            if event.title == "✅ Trading App Recovered":
                self._watchdog_delivered[channel] = False
        return None

    def failed(self, event: NotificationEvent, channel: str) -> None:
        if channel in {"bark", "webhook"} and event.title in {
            "🚨 Trading App Offline", "⚠️ Trading App Unhealthy",
        }:
            self._watchdog_alert_pending[channel] = False
            self._watchdog_recovery_pending.pop(channel, None)
