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
from app.notification_events import COMPONENTS, event_code
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
    components_recovered_tick: float | None = None
    reasons: set[str] = field(default_factory=set)
    open_queued: bool = False
    notified_open: bool = False
    resolved_queued: bool = False
    notified_resolved: bool = False
    had_halt: bool = False
    risk_state: str = "UNKNOWN"
    resolved_at: datetime | None = None
    duration_seconds: float | None = None
    alert_threshold_met: bool = False
    resolved_before_open_delivery: bool = False
    open_delivery_failures: int = 0
    resolved_delivery_failures: int = 0
    next_open_retry_at: float = 0
    next_resolved_retry_at: float = 0
    recurrence_of: str | None = None
    series_id: str = ""
    open_event: NotificationEvent | None = field(default=None, repr=False)
    resolved_event: NotificationEvent | None = field(default=None, repr=False)

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
            "open_delivery_failures": self.open_delivery_failures,
            "resolved_delivery_failures": self.resolved_delivery_failures,
            "resolved_before_open_delivery": self.resolved_before_open_delivery,
            "alert_threshold_met": self.alert_threshold_met,
            "recurrence_of": self.recurrence_of, "series_id": self.series_id,
            "duration_seconds": self.duration_seconds,
        }


class IncidentManager:
    """Bounded notification state. It has no trading or risk-control capability."""

    def __init__(
        self, *, delay_seconds: float = 60, merge_window_seconds: float = 300,
        notify_fast_recovery: bool = False, retry_initial_seconds: float = 30,
        retry_max_seconds: float = 300, clock: Callable[[], float] = monotonic,
    ) -> None:
        if retry_initial_seconds <= 0 or retry_max_seconds < retry_initial_seconds:
            raise ValueError("invalid incident retry interval")
        self.delay_seconds = delay_seconds
        self.merge_window_seconds = merge_window_seconds
        self.notify_fast_recovery = notify_fast_recovery
        self.retry_initial_seconds = retry_initial_seconds
        self.retry_max_seconds = retry_max_seconds
        self.clock = clock
        self.active: dict[str, Incident] = {}
        self.history: deque[Incident] = deque(maxlen=1000)
        self.changes: list[tuple[Incident, str]] = []
        self._last_resolved_by_key: dict[str, tuple[str, str, float]] = {}

    @staticmethod
    def _component(event: NotificationEvent) -> str | None:
        component = event.metadata.get("component")
        return component if isinstance(component, str) else COMPONENTS.get(event_code(event))

    def _open_or_update(
        self, key: str, component: str, reason: str, *, halt: bool, risk_state: str,
    ) -> Incident:
        now = datetime.now(UTC)
        incident = self.active.get(key)
        if incident is None:
            incident_id = uuid4().hex
            previous = self._last_resolved_by_key.get(key)
            related = previous if previous and self.clock() - previous[2] < self.merge_window_seconds else None
            incident = Incident(
                id=incident_id, key=key, opened_at=now, opened_tick=self.clock(),
                last_updated_at=now, recurrence_of=related[0] if related else None,
                series_id=related[1] if related else incident_id,
            )
            self.active[key] = incident
            self.history.append(incident)
            transition = "opened"
        else:
            transition = "updated"
        incident.last_updated_at = now
        incident.components.add(component)
        incident.recovered_components.discard(component)
        incident.components_recovered_tick = None
        incident.reasons.add(reason)
        incident.had_halt |= halt
        incident.risk_state = risk_state
        self.changes.append((incident, transition))
        return incident

    def _immediate(
        self, event: NotificationEvent, key: str, component: str, reason: str,
    ) -> None:
        incident = self._open_or_update(
            key, component, reason, halt=True,
            risk_state=str(event.metadata.get("risk_state", "HALT")),
        )
        if incident.open_event is not None:
            event.metadata["incident_duplicate"] = True
            return
        incident.severity = "CRITICAL" if NotificationPolicy.is_critical(event) else "IMPORTANT"
        incident.alert_threshold_met = True
        incident.open_queued = True
        event.metadata.update({
            "incident_notification": True, "incident_id": incident.id, "incident_phase": "open",
        })
        incident.open_event = event.model_copy(deep=True)
        self.changes.append((incident, "alert_queued"))

    def observe(self, event: NotificationEvent) -> list[NotificationEvent]:
        if event.metadata.get("incident_notification"):
            return []
        if event_code(event) == "RISK_HALT":
            reason = NotificationPolicy.halt_reason(event)
            if reason in AUTO_RECOVERABLE_REASONS:
                halt_component = {
                    "reconciliation failed": "reconciliation",
                    "WebSocket disconnected or stale": "websocket",
                    "Redis unavailable": "redis",
                    "dead man switch unavailable": "caa",
                }[reason]
                self._open_or_update(
                    "infra:trading", halt_component, reason, halt=True,
                    risk_state=str(event.metadata.get("risk_state", "HALT")),
                )
            elif reason == "auto recovery circuit breaker":
                incident = self._open_or_update(
                    "auto-recovery:circuit-breaker", "auto_recovery", reason,
                    halt=True, risk_state=str(event.metadata.get("risk_state", "HALT")),
                )
                incident.severity = "CRITICAL"
            elif reason not in {"startup reconciliation pending", "manual stop"}:
                self._immediate(event, f"risk:{reason}", "safety", reason)
            return []
        if event_code(event) == "RISK_EMERGENCY":
            reason = NotificationPolicy.halt_reason(event) or "emergency"
            self._immediate(event, "risk:emergency", "emergency", reason)
            return []
        if event_code(event) == "AUTO_RECOVERY_DISABLED":
            self._immediate(
                event, "auto-recovery:circuit-breaker", "auto_recovery",
                "auto recovery circuit breaker",
            )
            return []
        if event.category == NotificationCategory.INFRASTRUCTURE:
            component = self._component(event)
            if component:
                if event.metadata.get("recovery"):
                    active_infra = self.active.get("infra:trading")
                    if active_infra is not None:
                        active_infra.last_updated_at = datetime.now(UTC)
                        active_infra.recovered_components.add(component)
                        if (
                            active_infra.components <= active_infra.recovered_components
                            and active_infra.components_recovered_tick is None
                        ):
                            active_infra.components_recovered_tick = self.clock()
                        self.changes.append((active_infra, "updated"))
                        if (
                            not active_infra.had_halt
                            and active_infra.components <= active_infra.recovered_components
                            and event.metadata.get("risk_state") == "NORMAL"
                        ):
                            return self._resolve("infra:trading")
                else:
                    self._open_or_update(
                        "infra:trading", component, component + " unavailable",
                        halt=False, risk_state=str(event.metadata.get("risk_state", "UNKNOWN")),
                    )
            return []
        if event_code(event) == "AUTO_RECOVERY_COMPLETED" and event.metadata.get("recovery"):
            return self._resolve("infra:trading")
        if event_code(event) == "RISK_RECOVERED" and event.metadata.get("recovery"):
            generated: list[NotificationEvent] = []
            for key in tuple(self.active):
                incident = self.active[key]
                if incident.had_halt or (
                    key == "infra:trading" and incident.components_recovered_tick is not None
                ):
                    generated.extend(self._resolve(key))
            return generated
        return []

    def _backoff(self, failures: int) -> float:
        return min(self.retry_max_seconds, self.retry_initial_seconds * 2 ** min(failures - 1, 30))

    def _find(self, incident_id: str) -> Incident | None:
        for incident in self.active.values():
            if incident.id == incident_id:
                return incident
        return next((item for item in reversed(self.history) if item.id == incident_id), None)

    def _open_notification(self, incident: Incident) -> NotificationEvent:
        components = ", ".join(sorted(incident.components))
        reasons = ", ".join(sorted(incident.reasons))
        title = (
            "⚠️ Trading Temporarily Halted" if incident.risk_state == "HALT"
            else "⚠️ Trading Infrastructure Unhealthy"
        )
        message = (
            f"Components: {components}\nReason: {reasons}\n"
            f"Duration: {int(self.clock() - incident.opened_tick)}s+"
            f"\nRisk: {incident.risk_state}"
        )
        if incident.risk_state == "HALT":
            message += "\nNew entries: blocked"
        return NotificationEvent(
            event_code="INCIDENT_OPEN",
            level=NotificationLevel.WARNING, category=NotificationCategory.INFRASTRUCTURE,
            title=title, message=message, priority=NotificationPriority.TIME_SENSITIVE,
            dedup_key=f"incident:{incident.id}:open",
            metadata={
                "incident_notification": True, "incident_id": incident.id,
                "incident_phase": "open", "transition": True, "risk_state": incident.risk_state,
            },
        )

    def _queue_open(self, incident: Incident) -> NotificationEvent:
        if incident.open_event is None:
            incident.open_event = self._open_notification(incident)
        retry = incident.open_delivery_failures > 0
        event = incident.open_event.model_copy(deep=True)
        if retry:
            event.metadata["incident_retry"] = True
        incident.open_queued = True
        incident.alert_threshold_met = True
        self.changes.append((incident, "alert_retry_queued" if retry else "alert_queued"))
        return event

    def _resolve(self, key: str) -> list[NotificationEvent]:
        incident = self.active.pop(key, None)
        if incident is None:
            return []
        incident.resolved_at = datetime.now(UTC)
        incident.last_updated_at = incident.resolved_at
        end_tick = (
            incident.components_recovered_tick
            if not incident.had_halt and incident.components_recovered_tick is not None
            else self.clock()
        )
        incident.duration_seconds = max(0, end_tick - incident.opened_tick)
        if incident.duration_seconds >= self.delay_seconds:
            incident.alert_threshold_met = True
        self._last_resolved_by_key[key] = (incident.id, incident.series_id, self.clock())
        if len(self._last_resolved_by_key) > 1000:
            oldest = min(self._last_resolved_by_key, key=lambda item: self._last_resolved_by_key[item][2])
            del self._last_resolved_by_key[oldest]
        self.changes.append((incident, "resolved"))
        if incident.notified_open or (
            not incident.open_queued and self.notify_fast_recovery
        ):
            return [self._queue_resolved(incident)]
        if incident.alert_threshold_met and not incident.open_queued:
            incident.resolved_before_open_delivery = True
            incident.next_resolved_retry_at = max(
                incident.next_resolved_retry_at, incident.next_open_retry_at
            )
        return []

    def _resolved_notification(self, incident: Incident) -> NotificationEvent:
        retrospective = not incident.notified_open and incident.alert_threshold_met
        if retrospective:
            if incident.key == "risk:emergency":
                title = "🚨 Emergency Incident Resolved"
            elif incident.severity == "CRITICAL":
                title = "⚠️ Safety Incident Resolved"
            else:
                title = "ℹ️ Trading Incident Resolved"
            level = NotificationLevel.CRITICAL if incident.severity == "CRITICAL" else NotificationLevel.INFO
            priority = (
                NotificationPriority.CRITICAL if incident.severity == "CRITICAL"
                else NotificationPriority.ACTIVE
            )
            message = (
                "Alert delivery was delayed; incident occurred while notifications "
                "were unavailable\n"
                f"Reason: {', '.join(sorted(incident.reasons))}\n"
                f"Duration: {int(incident.duration_seconds or 0)}s\nCurrent Risk: NORMAL"
            )
        else:
            title = "✅ Trading Recovered"
            level, priority = NotificationLevel.INFO, NotificationPriority.ACTIVE
            previous = "HALT" if incident.had_halt else "INFRASTRUCTURE_UNHEALTHY"
            message = (
                f"Previous: {previous}\nReason: {', '.join(sorted(incident.reasons))}"
                f"\nDowntime: {int(incident.duration_seconds or 0)}s\nRisk: NORMAL"
            )
        return NotificationEvent(
            event_code=(
                "EMERGENCY_RETROSPECTIVE" if retrospective and incident.key == "risk:emergency"
                else "SAFETY_RETROSPECTIVE" if retrospective and incident.severity == "CRITICAL"
                else "INCIDENT_RETROSPECTIVE" if retrospective else "INCIDENT_RESOLVED"
            ),
            level=level, category=NotificationCategory.INFRASTRUCTURE,
            title=title, message=message, priority=priority,
            dedup_key=f"incident:{incident.id}:resolved",
            metadata={
                "incident_notification": True, "incident_id": incident.id,
                "incident_phase": "resolved", "retrospective": retrospective,
                "recovery": True,
            },
        )

    def _queue_resolved(self, incident: Incident) -> NotificationEvent:
        if incident.resolved_event is None:
            incident.resolved_event = self._resolved_notification(incident)
        retry = incident.resolved_delivery_failures > 0
        event = incident.resolved_event.model_copy(deep=True)
        if retry:
            event.metadata["incident_retry"] = True
        incident.resolved_queued = True
        self.changes.append((
            incident, "recovery_retry_queued" if retry else "recovery_queued"
        ))
        return event

    def due(self) -> list[NotificationEvent]:
        now = self.clock()
        generated: list[NotificationEvent] = []
        for incident in tuple(self.active.values()):
            if incident.open_queued or incident.notified_open:
                continue
            if not incident.had_halt and incident.components_recovered_tick is not None:
                continue
            if incident.had_halt and incident.risk_state == "NORMAL":
                continue
            if now - incident.opened_tick < self.delay_seconds and not incident.alert_threshold_met:
                continue
            if now < incident.next_open_retry_at:
                continue
            generated.append(self._queue_open(incident))
        for incident in tuple(self.history):
            if incident.resolved_at is None or incident.notified_resolved or incident.resolved_queued:
                continue
            if not incident.alert_threshold_met and not self.notify_fast_recovery:
                continue
            if incident.open_queued or now < incident.next_resolved_retry_at:
                continue
            if not incident.notified_open:
                incident.resolved_before_open_delivery = True
            generated.append(self._queue_resolved(incident))
        return generated

    def delivery_failed(self, event: NotificationEvent) -> None:
        if not event.metadata.get("incident_notification"):
            return
        incident_id = event.metadata.get("incident_id")
        if not isinstance(incident_id, str):
            return
        incident = self._find(incident_id)
        if incident is None:
            return
        phase = event.metadata.get("incident_phase")
        if phase == "open" and not incident.notified_open:
            incident.open_queued = False
            incident.open_delivery_failures += 1
            retry_at = self.clock() + self._backoff(incident.open_delivery_failures)
            if incident.resolved_at is None:
                incident.next_open_retry_at = retry_at
            else:
                incident.resolved_before_open_delivery = True
                incident.next_resolved_retry_at = retry_at
            self.changes.append((incident, "alert_failed"))
        elif phase == "resolved" and not incident.notified_resolved:
            incident.resolved_queued = False
            incident.resolved_delivery_failures += 1
            incident.next_resolved_retry_at = (
                self.clock() + self._backoff(incident.resolved_delivery_failures)
            )
            self.changes.append((incident, "recovery_failed"))

    def should_supersede_open(self, event: NotificationEvent) -> bool:
        if (
            not event.metadata.get("incident_notification")
            or event.metadata.get("incident_phase") != "open"
        ):
            return False
        incident_id = event.metadata.get("incident_id")
        incident = self._find(incident_id) if isinstance(incident_id, str) else None
        return bool(incident and not incident.notified_open and (
            incident.resolved_at is not None or (
                incident.key == "infra:trading" and not incident.had_halt
                and incident.components_recovered_tick is not None
            )
        ))

    def supersede_open(self, event: NotificationEvent, attempts: int) -> None:
        incident_id = event.metadata.get("incident_id")
        incident = self._find(incident_id) if isinstance(incident_id, str) else None
        if incident is None or incident.notified_open:
            return
        incident.open_queued = False
        incident.resolved_before_open_delivery = True
        if attempts:
            incident.next_resolved_retry_at = max(
                incident.next_resolved_retry_at,
                self.clock() + self.retry_initial_seconds,
            )
        self.changes.append((incident, "alert_superseded"))

    def delivery_confirmed(self, event: NotificationEvent) -> list[NotificationEvent]:
        if not event.metadata.get("incident_notification"):
            return []
        incident_id = event.metadata.get("incident_id")
        if not isinstance(incident_id, str):
            return []
        incident = self._find(incident_id)
        if incident is None:
            return []
        phase = event.metadata.get("incident_phase")
        if phase == "resolved":
            if not incident.notified_resolved:
                incident.resolved_queued = False
                incident.notified_resolved = True
                self.changes.append((
                    incident, "retrospective_sent" if event.metadata.get("retrospective")
                    else "recovery_sent",
                ))
            return []
        if phase == "open" and not incident.notified_open:
            incident.open_queued = False
            incident.notified_open = True
            self.changes.append((incident, "alert_sent"))
            if incident.resolved_at is not None and not incident.resolved_queued:
                return [self._queue_resolved(incident)]
        return []

    def take_changes(self) -> list[tuple[Incident, str]]:
        changes, self.changes = self.changes, []
        return changes
