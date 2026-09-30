from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import monotonic
from uuid import uuid4

from app.models import (
    NotificationCategory,
    NotificationEvent,
    NotificationLevel,
    NotificationPriority,
)
from app.notification_policy import NotificationPolicy
from app.recovery import AUTO_RECOVERABLE_REASONS


@dataclass
class Incident:
    id: str
    key: str
    opened_at: datetime
    opened_tick: float
    last_updated_at: datetime
    severity: str = "IMPORTANT"
    components: set[str] = field(default_factory=set)
    recovered_components: set[str] = field(default_factory=set)
    reasons: set[str] = field(default_factory=set)
    open_queued: bool = False
    notified_open: bool = False
    resolved_queued: bool = False
    notified_resolved: bool = False
    had_halt: bool = False
    risk_state: str = "UNKNOWN"
    resolved_at: datetime | None = None
    duration_seconds: float | None = None

    def record(self, transition: str) -> dict[str, object]:
        return {
            "event": "notification_incident", "transition": transition,
            "incident_id": self.id, "key": self.key,
            "opened_at": self.opened_at.isoformat(),
            "last_updated_at": self.last_updated_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "severity": self.severity, "components": sorted(self.components),
            "reasons": sorted(self.reasons), "notification_open_sent": self.notified_open,
            "notification_resolved_sent": self.notified_resolved,
            "notification_open_queued": self.open_queued,
            "notification_resolved_queued": self.resolved_queued,
            "duration_seconds": self.duration_seconds,
        }


class IncidentManager:
    """Aggregates notification observations; it cannot change runtime state."""

    def __init__(
        self, *, delay_seconds: float = 60, merge_window_seconds: float = 300,
        notify_fast_recovery: bool = False, clock: Callable[[], float] = monotonic,
    ) -> None:
        self.delay_seconds = delay_seconds
        self.merge_window_seconds = merge_window_seconds
        self.notify_fast_recovery = notify_fast_recovery
        self.clock = clock
        self.active: Incident | None = None
        self.history: deque[Incident] = deque(maxlen=1000)
        self.changes: list[tuple[Incident, str]] = []
        self._last_resolved_tick: float | None = None
        self._last_resolved_key: str | None = None

    @staticmethod
    def _component(event: NotificationEvent) -> str | None:
        title = event.title.lower()
        if "websocket" in title or "ws " in title:
            return "websocket"
        if "redis" in title:
            return "redis"
        if "caa" in title or "cancel-all-after" in title:
            return "caa"
        if "reconciliation" in title:
            return "reconciliation"
        return None

    def _open_or_update(self, component: str, reason: str, *, halt: bool,
                        risk_state: str, key: str = "infra:trading") -> None:
        now = datetime.now(UTC)
        if self.active is None:
            self.active = Incident(
                id=uuid4().hex, key=key, opened_at=now,
                opened_tick=self.clock(), last_updated_at=now,
            )
            self.history.append(self.active)
            transition = "opened"
        else:
            transition = "updated"
        incident = self.active
        incident.last_updated_at = now
        incident.components.add(component)
        incident.recovered_components.discard(component)
        incident.reasons.add(reason)
        incident.had_halt |= halt
        incident.risk_state = risk_state
        self.changes.append((incident, transition))

    def _immediate(self, event: NotificationEvent, component: str, reason: str,
                   key: str) -> None:
        new_incident = self.active is None
        self._open_or_update(
            component, reason, halt=True,
            risk_state=str(event.metadata.get("risk_state", "HALT")), key=key,
        )
        assert self.active is not None
        self.active.open_queued = True
        if new_incident:
            self.active.severity = (
                "CRITICAL" if NotificationPolicy.is_critical(event) else "IMPORTANT"
            )
        event.metadata["incident_id"] = self.active.id
        self.changes.append((self.active, "alert_queued"))

    def observe(self, event: NotificationEvent) -> list[NotificationEvent]:
        if event.metadata.get("incident_notification"):
            return []
        if event.title == "🚨 HALT":
            reason = NotificationPolicy.halt_reason(event)
            if reason in AUTO_RECOVERABLE_REASONS:
                halt_component = {
                    "reconciliation failed": "reconciliation",
                    "WebSocket disconnected or stale": "websocket",
                    "Redis unavailable": "redis",
                    "dead man switch unavailable": "caa",
                }[reason]
                self._open_or_update(
                    halt_component, reason, halt=True,
                    risk_state=str(event.metadata.get("risk_state", "HALT")),
                )
            elif reason == "auto recovery circuit breaker":
                self._open_or_update(
                    "auto_recovery", reason, halt=True,
                    risk_state=str(event.metadata.get("risk_state", "HALT")),
                    key="risk:auto-recovery-circuit-breaker",
                )
            elif reason not in {"startup reconciliation pending", "manual stop"}:
                self._immediate(event, "safety", reason, f"risk:{reason}")
            return []
        if event.title == "🚨 EMERGENCY":
            reason = NotificationPolicy.halt_reason(event) or "emergency"
            self._immediate(event, "emergency", reason, "risk:emergency")
            return []
        if event.title == "🚨 Auto Recovery Disabled":
            if self.active is not None:
                self.active.open_queued = True
                event.metadata["incident_id"] = self.active.id
                self.changes.append((self.active, "alert_queued"))
            return []
        if event.category == NotificationCategory.INFRASTRUCTURE:
            component = self._component(event)
            if component:
                if event.metadata.get("recovery"):
                    if self.active is not None:
                        self.active.last_updated_at = datetime.now(UTC)
                        self.active.recovered_components.add(component)
                        self.changes.append((self.active, "updated"))
                        if not self.active.had_halt and self._all_components_recovered(event):
                            return self._resolve()
                else:
                    self._open_or_update(
                        component, component + " unavailable", halt=False,
                        risk_state=str(event.metadata.get("risk_state", "UNKNOWN")),
                    )
            return []
        if event.title in {"✅ Risk State Recovered", "✅ Auto Recovery Completed"}:
            if self.active is not None and event.metadata.get("recovery"):
                return self._resolve()
        return []

    def _all_components_recovered(self, event: NotificationEvent) -> bool:
        if self.active is None:
            return False
        recovered = self._component(event)
        return (
            recovered is not None
            and self.active.components <= self.active.recovered_components
            and event.metadata.get("risk_state") == "NORMAL"
        )

    def due(self) -> list[NotificationEvent]:
        incident = self.active
        if incident is None or incident.open_queued:
            return []
        if incident.had_halt and incident.risk_state == "NORMAL":
            return []
        duration = self.clock() - incident.opened_tick
        if duration < self.delay_seconds:
            return []
        if (
            self._last_resolved_tick is not None
            and self._last_resolved_key == incident.key
            and self.clock() - self._last_resolved_tick < self.merge_window_seconds
        ):
            return []
        incident.open_queued = True
        self.changes.append((incident, "alert_queued"))
        components = ", ".join(sorted(incident.components))
        reasons = ", ".join(sorted(incident.reasons))
        title = (
            "⚠️ Trading Temporarily Halted" if incident.risk_state == "HALT"
            else "⚠️ Trading Infrastructure Unhealthy"
        )
        message = (
            f"Components: {components}\nReason: {reasons}\n"
            f"Duration: {int(duration)}s+\nRisk: {incident.risk_state}"
        )
        if incident.risk_state == "HALT":
            message += "\nNew entries: blocked"
        return [NotificationEvent(
            level=NotificationLevel.WARNING,
            category=NotificationCategory.INFRASTRUCTURE,
            title=title,
            message=message,
            priority=NotificationPriority.TIME_SENSITIVE,
            dedup_key=f"incident:{incident.id}:open",
            metadata={"incident_notification": True, "transition": True,
                      "incident_id": incident.id},
        )]

    def _resolve(self) -> list[NotificationEvent]:
        incident = self.active
        if incident is None:
            return []
        incident.resolved_at = datetime.now(UTC)
        incident.last_updated_at = incident.resolved_at
        incident.duration_seconds = max(0, self.clock() - incident.opened_tick)
        self._last_resolved_tick = self.clock()
        self._last_resolved_key = incident.key
        self.active = None
        self.changes.append((incident, "resolved"))
        if not incident.notified_open and not self.notify_fast_recovery:
            return []
        return self._recovery_event(incident)

    def _recovery_event(self, incident: Incident) -> list[NotificationEvent]:
        if incident.resolved_queued:
            return []
        incident.resolved_queued = True
        self.changes.append((incident, "recovery_queued"))
        previous = "HALT" if incident.had_halt else "INFRASTRUCTURE_UNHEALTHY"
        return [NotificationEvent(
            level=NotificationLevel.INFO,
            category=NotificationCategory.INFRASTRUCTURE,
            title="✅ Trading Recovered",
            message=f"Previous: {previous}\nReason: {', '.join(sorted(incident.reasons))}"
                    f"\nDowntime: {int(incident.duration_seconds or 0)}s\nRisk: NORMAL",
            priority=NotificationPriority.ACTIVE,
            dedup_key=f"incident:{incident.id}:resolved",
            metadata={"incident_notification": True, "recovery": True,
                      "incident_id": incident.id},
        )]

    def delivery_confirmed(self, event: NotificationEvent) -> list[NotificationEvent]:
        incident_id = event.metadata.get("incident_id")
        if not isinstance(incident_id, str):
            return []
        incident = next((item for item in reversed(self.history) if item.id == incident_id), None)
        if incident is None:
            return []
        if event.title == "✅ Trading Recovered":
            incident.notified_resolved = True
            self.changes.append((incident, "recovery_sent"))
            return []
        incident.notified_open = True
        self.changes.append((incident, "alert_sent"))
        if incident.resolved_at is not None:
            return self._recovery_event(incident)
        return []

    def take_changes(self) -> list[tuple[Incident, str]]:
        changes, self.changes = self.changes, []
        return changes
