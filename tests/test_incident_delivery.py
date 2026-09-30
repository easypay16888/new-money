from __future__ import annotations

import asyncio

import pytest

from app.config import Settings
from app.incidents import IncidentManager
from app.models import (
    NotificationCategory,
    NotificationEvent,
    NotificationLevel,
    NotificationPriority,
)
from app.monitoring import Metrics, Notification
from app.notification_policy import NotificationPolicy
from app.notifications import NotificationManager, _Delivery
from app.runtime import TradingRuntime


class BarkNotification(Notification):
    def __init__(self, *, failing: bool = False) -> None:
        self.failing = failing
        self.attempts: list[NotificationEvent] = []
        self.sent: list[NotificationEvent] = []

    async def send(self, event: NotificationEvent) -> None:
        self.attempts.append(event)
        if self.failing:
            raise ConnectionError("Bark unavailable")
        self.sent.append(event)

    async def send_alert(self, level: str, title: str, message: str) -> None:
        raise AssertionError("event send expected")


class ConsoleNotification(BarkNotification):
    pass


class WebhookNotification(BarkNotification):
    pass


def halt(reason: str) -> NotificationEvent:
    return NotificationEvent(
        level=NotificationLevel.ERROR, category=NotificationCategory.RISK,
        title="🚨 HALT", message=f"Reason: {reason}\nNew entries: blocked",
        priority=NotificationPriority.TIME_SENSITIVE,
        dedup_key=f"risk:{reason}",
        metadata={"risk_state": "HALT", "transition": True},
    )


def risk_recovered() -> NotificationEvent:
    return NotificationEvent(
        level=NotificationLevel.INFO, category=NotificationCategory.RISK,
        title="✅ Risk State Recovered", message="Current: NORMAL",
        metadata={"risk_state": "NORMAL", "recovery": True},
    )


def auto_recovered() -> NotificationEvent:
    return NotificationEvent(
        level=NotificationLevel.INFO, category=NotificationCategory.RISK,
        title="✅ Auto Recovery Completed", message="Current: NORMAL",
        metadata={"risk_state": "NORMAL", "recovery": True},
    )


async def wait_for(condition) -> None:
    async def poll() -> None:
        while not condition():
            await asyncio.sleep(0)

    await asyncio.wait_for(poll(), 1)


@pytest.mark.asyncio
async def test_incident_open_rearms_after_final_bark_failure() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    bark = BarkNotification(failing=True)
    notifier = NotificationManager(
        [bark], Metrics(), incidents=incidents, policy=NotificationPolicy(),
        retry_delays=(0, 0, 0),
    )
    notifier.start()
    await notifier.publish(halt("reconciliation failed"))
    now[0] = 60
    first = incidents.due()
    assert len(first) == 1
    await notifier.publish(first[0])
    incident = incidents.active["infra:trading"]
    await wait_for(lambda: incident.open_delivery_failures == 1)
    assert len(bark.attempts) == 4
    assert not incident.open_queued and not incident.notified_open
    assert incident.next_open_retry_at == 90
    now[0] = 89
    assert incidents.due() == []
    now[0] = 90
    retry = incidents.due()
    assert len(retry) == 1
    assert retry[0].metadata["incident_id"] == first[0].metadata["incident_id"]
    assert retry[0].metadata["incident_retry"] is True
    await notifier.stop(drain_seconds=0.01)


@pytest.mark.asyncio
async def test_ingress_queue_eviction_rearms_critical_incident() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    notifier = NotificationManager(
        [BarkNotification()], Metrics(), incidents=incidents,
        policy=NotificationPolicy(), max_queue=1,
    )
    await notifier.publish(halt("position mismatch"))
    incident = incidents.active["risk:position mismatch"]
    assert incident.open_queued
    await notifier.publish(NotificationEvent(
        level=NotificationLevel.CRITICAL, category=NotificationCategory.SYSTEM,
        title="Other critical event", message="Another alert",
        priority=NotificationPriority.CRITICAL,
    ))
    assert incident.open_delivery_failures == 1
    assert not incident.open_queued and not incident.notified_open
    now[0] = 30
    retry = incidents.due()
    assert len(retry) == 1
    assert retry[0].metadata["incident_id"] == incident.id


def test_bark_channel_eviction_rearms_critical_incident() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    notifier = NotificationManager(
        [BarkNotification()], Metrics(), incidents=incidents,
        policy=NotificationPolicy(), max_queue=1,
    )
    first = halt("position mismatch")
    first.priority = NotificationPriority.CRITICAL
    incidents.observe(first)
    state = notifier._channels[0]
    notifier._enqueue_channel(state, _Delivery(first))
    notifier._enqueue_channel(state, _Delivery(NotificationEvent(
        level=NotificationLevel.CRITICAL, category=NotificationCategory.SYSTEM,
        title="Other critical event", message="Another alert",
        priority=NotificationPriority.CRITICAL,
    )))
    incident = incidents.active["risk:position mismatch"]
    assert incident.open_delivery_failures == 1
    assert not incident.open_queued
    now[0] = 30
    assert incidents.due()[0].metadata["incident_id"] == incident.id


@pytest.mark.asyncio
async def test_incident_open_retries_after_bark_recovers() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    bark = BarkNotification(failing=True)
    metrics = Metrics()
    notifier = NotificationManager(
        [bark], metrics, incidents=incidents, policy=NotificationPolicy(), retry_delays=(),
    )
    notifier.start()
    await notifier.publish(halt("Redis unavailable"))
    now[0] = 60
    for event in incidents.due():
        await notifier.publish(event)
    incident = incidents.active["infra:trading"]
    await wait_for(lambda: incident.open_delivery_failures == 1)
    now[0] = 90
    bark.failing = False
    for event in incidents.due():
        await notifier.publish(event)
    await wait_for(lambda: incident.notified_open)
    assert [event.title for event in bark.sent] == ["⚠️ Trading Temporarily Halted"]
    assert metrics.incident_delivery_failure.labels(
        severity="IMPORTANT", phase="open"
    )._value.get() == 1
    assert metrics.incident_delivery_retry.labels(
        severity="IMPORTANT", phase="open"
    )._value.get() == 1
    assert incidents.due() == []
    await notifier.stop(drain_seconds=1)


@pytest.mark.asyncio
async def test_incident_recovery_rearms_after_final_failure() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    bark = BarkNotification()
    notifier = NotificationManager(
        [bark], Metrics(), incidents=incidents, policy=NotificationPolicy(), retry_delays=(),
    )
    notifier.start()
    await notifier.publish(halt("Redis unavailable"))
    now[0] = 60
    for event in incidents.due():
        await notifier.publish(event)
    incident = incidents.active["infra:trading"]
    await wait_for(lambda: incident.notified_open)
    bark.failing = True
    now[0] = 90
    await notifier.publish(auto_recovered())
    await wait_for(lambda: incident.resolved_delivery_failures == 1)
    assert not incident.resolved_queued and not incident.notified_resolved
    assert incident.next_resolved_retry_at == 120
    now[0] = 119
    assert incidents.due() == []
    now[0] = 120
    retry = incidents.due()
    assert len(retry) == 1 and retry[0].metadata["incident_retry"] is True
    await notifier.stop(drain_seconds=0.01)


@pytest.mark.asyncio
async def test_recovery_retries_after_bark_recovers() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    bark = BarkNotification()
    notifier = NotificationManager(
        [bark], Metrics(), incidents=incidents, policy=NotificationPolicy(), retry_delays=(),
    )
    notifier.start()
    await notifier.publish(halt("Redis unavailable"))
    now[0] = 60
    for event in incidents.due():
        await notifier.publish(event)
    incident = incidents.active["infra:trading"]
    await wait_for(lambda: incident.notified_open)
    bark.failing = True
    now[0] = 90
    await notifier.publish(auto_recovered())
    await wait_for(lambda: incident.resolved_delivery_failures == 1)
    now[0] = 120
    bark.failing = False
    for event in incidents.due():
        await notifier.publish(event)
    await wait_for(lambda: incident.notified_resolved)
    assert [event.title for event in bark.sent] == [
        "⚠️ Trading Temporarily Halted", "✅ Trading Recovered",
    ]
    await notifier.stop(drain_seconds=1)


def test_recurrent_incident_over_60_seconds_alerts_again() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], delay_seconds=60, merge_window_seconds=300)
    incidents.observe(halt("Redis unavailable"))
    now[0] = 60
    first = incidents.due()[0]
    incidents.delivery_confirmed(first)
    now[0] = 120
    incidents.observe(auto_recovered())
    now[0] = 130
    incidents.observe(halt("Redis unavailable"))
    now[0] = 191
    second = incidents.due()
    assert len(second) == 1
    assert second[0].metadata["incident_id"] != first.metadata["incident_id"]
    assert incidents.active["infra:trading"].recurrence_of == first.metadata["incident_id"]


def test_recurrent_incident_under_60_seconds_stays_silent() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], delay_seconds=60, merge_window_seconds=300)
    incidents.observe(halt("Redis unavailable"))
    now[0] = 60
    incidents.delivery_confirmed(incidents.due()[0])
    now[0] = 120
    incidents.observe(auto_recovered())
    now[0] = 130
    incidents.observe(halt("Redis unavailable"))
    now[0] = 160
    assert incidents.observe(auto_recovered()) == []
    assert incidents.due() == []
    assert incidents.history[-1].recurrence_of == incidents.history[-2].id


def test_infrastructure_and_safety_incidents_are_separate() -> None:
    incidents = IncidentManager()
    incidents.observe(halt("reconciliation failed"))
    mismatch = halt("position mismatch")
    incidents.observe(mismatch)
    assert set(incidents.active) == {"infra:trading", "risk:position mismatch"}
    infra, safety = incidents.active["infra:trading"], incidents.active["risk:position mismatch"]
    assert infra.id != safety.id
    assert infra.severity == "IMPORTANT"
    assert safety.severity == "CRITICAL"
    assert mismatch.metadata["incident_id"] == safety.id
    assert safety.open_queued and not infra.open_queued


def test_each_incident_has_independent_severity() -> None:
    incidents = IncidentManager()
    incidents.observe(halt("Redis unavailable"))
    incidents.observe(halt("position mismatch"))
    assert incidents.active["infra:trading"].severity == "IMPORTANT"
    assert incidents.active["risk:position mismatch"].severity == "CRITICAL"


def test_emergency_does_not_merge_into_infrastructure_incident() -> None:
    incidents = IncidentManager()
    incidents.observe(halt("Redis unavailable"))
    emergency = NotificationEvent(
        level=NotificationLevel.CRITICAL, category=NotificationCategory.RISK,
        title="🚨 EMERGENCY", message="Reason: unprotected position",
        priority=NotificationPriority.CRITICAL,
        metadata={"risk_state": "EMERGENCY", "transition": True},
    )
    incidents.observe(emergency)
    assert set(incidents.active) == {"infra:trading", "risk:emergency"}
    assert incidents.active["risk:emergency"].severity == "CRITICAL"
    assert emergency.metadata["incident_id"] == incidents.active["risk:emergency"].id


@pytest.mark.asyncio
async def test_same_safety_incident_barks_once() -> None:
    incidents = IncidentManager()
    bark, console = BarkNotification(), ConsoleNotification()
    notifier = NotificationManager(
        [console, bark], Metrics(), incidents=incidents,
        policy=NotificationPolicy(), retry_delays=(),
    )
    notifier.start()
    first = halt("position mismatch")
    second = halt("position mismatch")
    second.message += "\nObservation: repeated"
    await notifier.publish(first)
    await notifier.publish(second)
    await notifier.stop(drain_seconds=1)
    assert len(bark.sent) == 1
    assert len(console.sent) == 2
    assert incidents.active["risk:position mismatch"].notified_open


def test_critical_incident_retries_same_id() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    emergency = NotificationEvent(
        level=NotificationLevel.CRITICAL, category=NotificationCategory.RISK,
        title="🚨 EMERGENCY", message="Reason: unprotected position",
        priority=NotificationPriority.CRITICAL,
    )
    incidents.observe(emergency)
    original_id = emergency.metadata["incident_id"]
    incidents.delivery_failed(emergency)
    now[0] = 30
    retry = incidents.due()[0]
    assert retry.metadata["incident_id"] == original_id
    assert retry.priority == NotificationPriority.CRITICAL
    incidents.delivery_confirmed(retry)
    assert incidents.active["risk:emergency"].notified_open


@pytest.mark.asyncio
async def test_safety_halt_bypasses_existing_infra_incident() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0])
    bark = BarkNotification()
    notifier = NotificationManager(
        [bark], Metrics(), incidents=incidents, policy=NotificationPolicy(), retry_delays=(),
    )
    notifier.start()
    await notifier.publish(halt("Redis unavailable"))
    await notifier.publish(halt("position mismatch"))
    await wait_for(lambda: len(bark.sent) == 1)
    assert bark.sent[0].title == "🚨 HALT"
    assert bark.sent[0].priority == NotificationPriority.CRITICAL
    assert not incidents.active["infra:trading"].open_queued
    await notifier.stop(drain_seconds=1)


@pytest.mark.asyncio
async def test_resolved_before_open_delivery_sends_single_retrospective() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    bark = BarkNotification(failing=True)
    metrics = Metrics()
    notifier = NotificationManager(
        [bark], metrics, incidents=incidents, policy=NotificationPolicy(), retry_delays=(),
    )
    notifier.start()
    await notifier.publish(halt("reconciliation failed"))
    now[0] = 60
    for event in incidents.due():
        await notifier.publish(event)
    incident = incidents.active["infra:trading"]
    await wait_for(lambda: incident.open_delivery_failures == 1)
    now[0] = 180
    await notifier.publish(auto_recovered())
    await wait_for(lambda: incident.resolved_at is not None)
    bark.failing = False
    retrospective = incidents.due()
    assert len(retrospective) == 1
    assert retrospective[0].title == "ℹ️ Trading Incident Resolved"
    assert retrospective[0].metadata["retrospective"] is True
    await notifier.publish(retrospective[0])
    await wait_for(lambda: incident.notified_resolved)
    assert not incident.notified_open
    assert [event.title for event in bark.sent] == ["ℹ️ Trading Incident Resolved"]
    assert metrics.incident_retrospective.labels(
        severity="IMPORTANT", phase="resolved"
    )._value.get() == 1
    await notifier.stop(drain_seconds=1)


def test_critical_retrospective_preserves_severity() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    emergency = NotificationEvent(
        level=NotificationLevel.CRITICAL, category=NotificationCategory.RISK,
        title="🚨 EMERGENCY", message="Reason: unprotected position",
        priority=NotificationPriority.CRITICAL, metadata={"risk_state": "EMERGENCY"},
    )
    incidents.observe(emergency)
    incidents.delivery_failed(emergency)
    now[0] = 10
    incidents.observe(risk_recovered())
    now[0] = 29
    assert incidents.due() == []
    now[0] = 30
    retrospective = incidents.due()
    assert len(retrospective) == 1
    assert retrospective[0].title == "🚨 Emergency Incident Resolved"
    assert retrospective[0].priority == NotificationPriority.CRITICAL
    assert retrospective[0].metadata["incident_id"] == emergency.metadata["incident_id"]


def test_retrospective_does_not_send_stale_open_then_recovery() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    incidents.observe(halt("Redis unavailable"))
    now[0] = 60
    stale_open = incidents.due()[0]
    now[0] = 80
    assert incidents.observe(auto_recovered()) == []
    assert incidents.should_supersede_open(stale_open)
    incidents.supersede_open(stale_open, attempts=1)
    now[0] = 109
    assert incidents.due() == []
    now[0] = 110
    retrospective = incidents.due()
    assert [event.title for event in retrospective] == ["ℹ️ Trading Incident Resolved"]
    incident = incidents.history[-1]
    assert incident.resolved_before_open_delivery
    assert not incident.notified_open


def test_retrospective_failure_retries_same_incident() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    incidents.observe(halt("Redis unavailable"))
    now[0] = 60
    opened = incidents.due()[0]
    incidents.delivery_failed(opened)
    now[0] = 90
    incidents.observe(auto_recovered())
    retrospective = incidents.due()[0]
    incidents.delivery_failed(retrospective)
    incident = incidents.history[-1]
    assert not incident.notified_resolved and not incident.resolved_queued
    now[0] = 120
    retry = incidents.due()[0]
    assert retry.title == retrospective.title
    assert retry.metadata["incident_id"] == retrospective.metadata["incident_id"]
    assert retry.metadata["incident_retry"] is True
    incidents.delivery_confirmed(retry)
    assert incident.notified_resolved and not incident.notified_open


@pytest.mark.asyncio
async def test_incident_sent_semantics_are_bark_only() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    bark = BarkNotification(failing=True)
    console, webhook = ConsoleNotification(), WebhookNotification()
    notifier = NotificationManager(
        [console, webhook, bark], Metrics(), incidents=incidents,
        policy=NotificationPolicy(), retry_delays=(),
    )
    notifier.start()
    await notifier.publish(halt("Redis unavailable"))
    now[0] = 60
    for event in incidents.due():
        await notifier.publish(event)
    incident = incidents.active["infra:trading"]
    await wait_for(lambda: incident.open_delivery_failures == 1)
    assert console.sent and webhook.sent
    assert not incident.notified_open
    await notifier.stop(drain_seconds=1)


@pytest.mark.asyncio
async def test_incident_delivery_failure_does_not_change_governor() -> None:
    runtime = TradingRuntime(Settings(_env_file=None))
    original_state = runtime.governor.state
    incidents = IncidentManager(retry_initial_seconds=30)
    bark = BarkNotification(failing=True)
    notifier = NotificationManager(
        [bark], runtime.metrics, incidents=incidents,
        policy=NotificationPolicy(), retry_delays=(),
    )
    runtime.notifications = notifier
    notifier.start()
    try:
        await runtime.alert(
            "CRITICAL", "🚨 EMERGENCY", "Reason: unprotected position",
            category=NotificationCategory.RISK, priority=NotificationPriority.CRITICAL,
        )
        incident = incidents.active["risk:emergency"]
        await wait_for(lambda: incident.open_delivery_failures == 1)
        assert runtime.governor.state == original_state
        assert not incident.notified_open
    finally:
        await notifier.stop(drain_seconds=1)
        await runtime.client.close()


@pytest.mark.asyncio
async def test_incident_retry_does_not_block_runtime() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    bark = BarkNotification(failing=True)
    notifier = NotificationManager(
        [bark], Metrics(), incidents=incidents, policy=NotificationPolicy(), retry_delays=(),
    )
    notifier.start()
    try:
        await notifier.publish(halt("Redis unavailable"))
        now[0] = 60
        for event in incidents.due():
            await notifier.publish(event)
        incident = incidents.active["infra:trading"]
        await wait_for(lambda: incident.open_delivery_failures == 1)
        now[0] = 90
        retry = incidents.due()[0]
        await asyncio.wait_for(notifier.publish(retry), 0.05)
        await wait_for(lambda: incident.open_delivery_failures == 2)
    finally:
        await notifier.stop(drain_seconds=1)


def test_incident_retry_backoff_is_bounded() -> None:
    now = [0.0]
    incidents = IncidentManager(
        clock=lambda: now[0], retry_initial_seconds=30, retry_max_seconds=300,
    )
    emergency = NotificationEvent(
        level=NotificationLevel.CRITICAL, category=NotificationCategory.RISK,
        title="🚨 EMERGENCY", message="Reason: unprotected position",
        priority=NotificationPriority.CRITICAL,
    )
    incidents.observe(emergency)
    incident = incidents.active["risk:emergency"]
    for failure, delay in enumerate((30, 60, 120, 240, 300, 300), start=1):
        incidents.delivery_failed(emergency)
        assert incident.open_delivery_failures == failure
        assert incident.next_open_retry_at == now[0] + delay
        now[0] += delay
        assert len(incidents.due()) == 1


def test_incident_lifecycle_records_failure_and_retry_transitions() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], retry_initial_seconds=30)
    incidents.observe(halt("Redis unavailable"))
    now[0] = 60
    opened = incidents.due()[0]
    incidents.delivery_failed(opened)
    now[0] = 90
    incidents.delivery_confirmed(incidents.due()[0])
    now[0] = 120
    recovery = incidents.observe(auto_recovered())[0]
    incidents.delivery_failed(recovery)
    now[0] = 150
    incidents.delivery_confirmed(incidents.due()[0])
    transitions = [transition for _, transition in incidents.take_changes()]
    assert transitions == [
        "opened", "alert_queued", "alert_failed", "alert_retry_queued", "alert_sent",
        "resolved", "recovery_queued", "recovery_failed", "recovery_retry_queued",
        "recovery_sent",
    ]
