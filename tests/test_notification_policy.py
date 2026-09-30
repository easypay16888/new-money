from __future__ import annotations

import asyncio

import httpx
import pytest

from app.incidents import IncidentManager
from app.models import (
    NotificationCategory,
    NotificationEvent,
    NotificationLevel,
    NotificationPriority,
)
from app.monitoring import Metrics, Notification
from app.notification_policy import NotificationPolicy
from app.notifications import NotificationManager
from app.watchdog import WatchdogMonitor, WatchdogSettings


class BarkNotification(Notification):
    def __init__(self) -> None:
        self.events: list[NotificationEvent] = []

    async def send(self, event: NotificationEvent) -> None:
        self.events.append(event)

    async def send_alert(self, level: str, title: str, message: str) -> None:
        raise AssertionError("event send expected")


class ConsoleNotification(BarkNotification):
    pass


class WebhookNotification(BarkNotification):
    pass


def notice(
    title: str, *, message: str = "status",
    category: NotificationCategory = NotificationCategory.INFRASTRUCTURE,
    level: NotificationLevel = NotificationLevel.WARNING,
    priority: NotificationPriority = NotificationPriority.ACTIVE,
    metadata: dict | None = None, dedup_key: str | None = None,
) -> NotificationEvent:
    return NotificationEvent(
        title=title, message=message, category=category, level=level,
        priority=priority, metadata=metadata or {}, dedup_key=dedup_key,
    )


def halt(reason: str) -> NotificationEvent:
    return notice(
        "🚨 HALT", message=f"Reason: {reason}\nNew entries: blocked",
        category=NotificationCategory.RISK, level=NotificationLevel.ERROR,
        priority=NotificationPriority.TIME_SENSITIVE,
        metadata={"transition": True, "risk_state": "HALT"},
    )


def recovered() -> NotificationEvent:
    return notice(
        "✅ Auto Recovery Completed", category=NotificationCategory.RISK,
        level=NotificationLevel.INFO,
        metadata={"recovery": True, "risk_state": "NORMAL"},
    )


def risk_recovered() -> NotificationEvent:
    return notice(
        "✅ Risk State Recovered", category=NotificationCategory.RISK,
        level=NotificationLevel.INFO,
        metadata={"recovery": True, "risk_state": "NORMAL"},
    )


async def route(*events: NotificationEvent, policy: NotificationPolicy | None = None
                ) -> tuple[list[NotificationEvent], list[NotificationEvent], Metrics]:
    bark, console, metrics = BarkNotification(), ConsoleNotification(), Metrics()
    notifier = NotificationManager(
        [console, bark], metrics, policy=policy or NotificationPolicy(), retry_delays=(),
    )
    notifier.start()
    for event in events:
        await notifier.publish(event)
    await notifier.stop(drain_seconds=1)
    return bark.events, console.events, metrics


@pytest.mark.asyncio
@pytest.mark.parametrize("title", [
    "🚨 Reconciliation Unhealthy", "🚨 WebSocket Disconnected",
    "🚨 CAA Unavailable", "🚨 Redis Unavailable",
])
async def test_single_infrastructure_failure_does_not_bark(title: str) -> None:
    bark, console, metrics = await route(notice(title))
    assert bark == []
    assert [event.title for event in console] == [title]
    assert metrics.notification_policy_suppressed.labels(
        channel="bark", category="INFRASTRUCTURE"
    )._value.get() == 1


@pytest.mark.asyncio
async def test_heartbeat_and_entry_submitted_are_opt_in() -> None:
    heartbeat = notice("❤️ Quant Heartbeat", category=NotificationCategory.HEARTBEAT)
    entry = notice("📤 BTC Entry Submitted", category=NotificationCategory.TRADE)
    bark, console, _ = await route(heartbeat, entry)
    assert bark == []
    assert len(console) == 2
    bark, _, _ = await route(
        heartbeat, entry,
        policy=NotificationPolicy(heartbeat_enabled=True, entry_submitted=True),
    )
    assert [event.title for event in bark] == [heartbeat.title, entry.title]


@pytest.mark.asyncio
async def test_trade_notifications_and_system_stop_are_low_noise() -> None:
    entries = [
        notice("🟡 Quant System Stopping", category=NotificationCategory.SYSTEM),
        notice("⚪ Quant System Stopped", category=NotificationCategory.SYSTEM),
        notice("🟢 BTC LONG Filled", category=NotificationCategory.TRADE,
               dedup_key="entry-filled:one"),
        notice("🟢 BTC LONG Filled", category=NotificationCategory.TRADE,
               dedup_key="entry-filled:one", message="new text"),
        notice("🛡 BTC Protection Active", category=NotificationCategory.TRADE,
               dedup_key="protection:one"),
        notice("💰 BTC Position Closed", category=NotificationCategory.TRADE),
    ]
    bark, console, _ = await route(*entries)
    assert [event.title for event in bark] == [
        "⚪ Quant System Stopped", "🟢 BTC LONG Filled",
        "🛡 BTC Protection Active", "💰 BTC Position Closed",
    ]
    assert len(console) == len(entries)


@pytest.mark.asyncio
async def test_critical_bypasses_policy_failure() -> None:
    class BrokenPolicy(NotificationPolicy):
        def evaluate(self, event: NotificationEvent, channel: str):
            raise RuntimeError("broken")

    emergency = notice(
        "🚨 EMERGENCY", category=NotificationCategory.RISK,
        level=NotificationLevel.CRITICAL, priority=NotificationPriority.CRITICAL,
    )
    offline = notice("🚨 Trading App Offline", level=NotificationLevel.CRITICAL)
    bark, _, _ = await route(emergency, offline, policy=BrokenPolicy())
    assert [event.title for event in bark] == [emergency.title, offline.title]


@pytest.mark.asyncio
async def test_critical_bypasses_incident_observation_failure() -> None:
    class BrokenIncidents(IncidentManager):
        def observe(self, event: NotificationEvent) -> list[NotificationEvent]:
            raise RuntimeError("broken")

    bark = BarkNotification()
    notifier = NotificationManager(
        [bark], Metrics(), policy=NotificationPolicy(),
        incidents=BrokenIncidents(), retry_delays=(),
    )
    notifier.start()
    await notifier.publish(notice(
        "🚨 EMERGENCY", category=NotificationCategory.RISK,
        level=NotificationLevel.CRITICAL,
    ))
    await notifier.stop(drain_seconds=1)
    assert [event.title for event in bark.events] == ["🚨 EMERGENCY"]


@pytest.mark.asyncio
async def test_safety_halt_bypasses_transient_delay() -> None:
    bark, _, _ = await route(halt("position mismatch"), halt("Redis unavailable"))
    assert [event.title for event in bark] == ["🚨 HALT"]
    assert bark[0].priority == NotificationPriority.CRITICAL


@pytest.mark.asyncio
async def test_foreign_order_with_exposure_is_critical() -> None:
    foreign = halt("foreign risk-increasing pending order")
    foreign.metadata["has_exposure"] = True
    bark, _, _ = await route(foreign)
    assert len(bark) == 1
    assert bark[0].priority == NotificationPriority.CRITICAL


@pytest.mark.asyncio
async def test_daily_report_only_barks_once_per_date() -> None:
    first = notice(
        "📊 Daily Trading Report", category=NotificationCategory.DAILY_REPORT,
        dedup_key="daily-report:2026-09-30", message="first",
    )
    second = first.model_copy(update={"message": "revised"})
    bark, console, _ = await route(first, second)
    assert len(bark) == 1
    assert len(console) == 2


@pytest.mark.asyncio
async def test_webhook_can_use_low_noise_policy_without_hiding_console() -> None:
    bark, console, webhook = BarkNotification(), ConsoleNotification(), WebhookNotification()
    notifier = NotificationManager(
        [console, bark, webhook], Metrics(),
        policy=NotificationPolicy(webhook_verbose=False), retry_delays=(),
    )
    notifier.start()
    await notifier.publish(notice("📤 BTC Entry Submitted", category=NotificationCategory.TRADE))
    await notifier.publish(notice(
        "🟢 BTC LONG Filled", category=NotificationCategory.TRADE,
        dedup_key="entry-filled:one",
    ))
    await notifier.stop(drain_seconds=1)
    assert len(console.events) == 2
    assert [event.title for event in bark.events] == ["🟢 BTC LONG Filled"]
    assert [event.title for event in webhook.events] == ["🟢 BTC LONG Filled"]


def test_persistent_incident_sends_one_open_and_one_recovery() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], delay_seconds=60)
    incidents.observe(halt("reconciliation failed"))
    assert incidents.due() == []
    now[0] = 61
    opened = incidents.due()
    assert [event.title for event in opened] == ["⚠️ Trading Temporarily Halted"]
    incidents.delivery_confirmed(opened[0])
    incidents.observe(halt("WebSocket disconnected or stale"))
    assert incidents.due() == []
    resolved = incidents.observe(recovered())
    assert [event.title for event in resolved] == ["✅ Trading Recovered"]
    incidents.delivery_confirmed(resolved[0])
    assert incidents.observe(recovered()) == []
    assert incidents.history[0].components == {"reconciliation", "websocket"}
    assert incidents.history[0].notified_open
    assert incidents.history[0].notified_resolved
    assert incidents.history[0].duration_seconds == 61


def test_fast_transient_recovery_is_recorded_without_bark() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], delay_seconds=60)
    incidents.observe(halt("Redis unavailable"))
    now[0] = 20
    assert incidents.observe(recovered()) == []
    assert incidents.active == {}
    assert incidents.history[0].duration_seconds == 20
    assert incidents.history[0].resolved_at is not None
    assert not incidents.history[0].notified_open


@pytest.mark.asyncio
async def test_incident_audit_failure_does_not_block_alert() -> None:
    class BrokenStore:
        async def append(self, table: str, payload: dict, *, reference_id: str) -> None:
            raise OSError("database unavailable")

    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], delay_seconds=60)
    bark = BarkNotification()
    metrics = Metrics()
    notifier = NotificationManager(
        [bark], metrics, incidents=incidents, policy=NotificationPolicy(),
        store=BrokenStore(), retry_delays=(),  # type: ignore[arg-type]
    )
    notifier.start()
    await notifier.publish(halt("reconciliation failed"))
    now[0] = 61
    for event in incidents.due():
        await notifier.publish(event)
    await notifier.stop(drain_seconds=1)
    assert [event.title for event in bark.events] == ["⚠️ Trading Temporarily Halted"]
    assert metrics.incidents_total.labels(
        category="infrastructure", severity="IMPORTANT", component="trading"
    )._value.get() == 1


def test_component_recoveries_do_not_resolve_halted_incident() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], delay_seconds=60)
    incidents.observe(halt("WebSocket disconnected or stale"))
    incidents.observe(notice(
        "✅ WS Recovered", level=NotificationLevel.INFO,
        metadata={"recovery": True, "risk_state": "HALT"},
    ))
    assert incidents.active is not None
    now[0] = 61
    opened = incidents.due()
    assert len(opened) == 1
    incidents.delivery_confirmed(opened[0])
    assert len(incidents.observe(recovered())) == 1


def test_emergency_incident_is_immediate_and_resolves_once() -> None:
    incidents = IncidentManager(delay_seconds=60)
    emergency = notice(
        "🚨 EMERGENCY", message="Reason: unprotected position",
        category=NotificationCategory.RISK, level=NotificationLevel.CRITICAL,
        priority=NotificationPriority.CRITICAL,
        metadata={"transition": True, "risk_state": "EMERGENCY"},
    )
    assert incidents.observe(emergency) == []
    assert incidents.due() == []
    incidents.delivery_confirmed(emergency)
    recovery = incidents.observe(risk_recovered())
    assert [item.title for item in recovery] == ["✅ Trading Recovered"]
    assert incidents.observe(risk_recovered()) == []


def test_merge_window_does_not_delay_persistent_recurrence() -> None:
    now = [0.0]
    incidents = IncidentManager(
        clock=lambda: now[0], delay_seconds=60, merge_window_seconds=300,
    )
    incidents.observe(halt("Redis unavailable"))
    now[0] = 61
    opened = incidents.due()
    assert len(opened) == 1
    incidents.delivery_confirmed(opened[0])
    incidents.observe(recovered())
    now[0] = 70
    incidents.observe(halt("Redis unavailable"))
    now[0] = 140
    second = incidents.due()
    assert len(second) == 1
    assert second[0].metadata["incident_id"] != opened[0].metadata["incident_id"]
    assert incidents.history[-1].recurrence_of == incidents.history[-2].id


@pytest.mark.asyncio
async def test_auto_recovery_produces_single_trading_recovered_bark() -> None:
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0], delay_seconds=60)
    bark, console = BarkNotification(), ConsoleNotification()
    notifier = NotificationManager(
        [console, bark], Metrics(), policy=NotificationPolicy(),
        incidents=incidents, retry_delays=(),
    )
    notifier.start()
    await notifier.publish(halt("reconciliation failed"))
    now[0] = 61
    for event in incidents.due():
        await notifier.publish(event)
    async def wait_for_open() -> None:
        while not incidents.active["infra:trading"].notified_open:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait_for_open(), 1)
    await notifier.publish(notice(
        "✅ Reconciliation Recovered", level=NotificationLevel.INFO,
        metadata={"recovery": True, "risk_state": "HALT"},
    ))
    await notifier.publish(recovered())
    await asyncio.sleep(0.01)
    await notifier.stop(drain_seconds=1)
    assert [event.title for event in bark.events] == [
        "⚠️ Trading Temporarily Halted", "✅ Trading Recovered",
    ]
    assert "✅ Reconciliation Recovered" in [event.title for event in console.events]


@pytest.mark.asyncio
async def test_notification_policy_does_not_wait_for_bark() -> None:
    class SlowBark(BarkNotification):
        async def send(self, event: NotificationEvent) -> None:
            await asyncio.sleep(1)

    # Name-based routing is exercised by the regular Bark recorder tests above.
    notifier = NotificationManager([SlowBark()], Metrics(), policy=NotificationPolicy())
    notifier.start()
    await asyncio.wait_for(notifier.publish(halt("position mismatch")), 0.05)
    await notifier.stop(drain_seconds=0.01)


@pytest.mark.asyncio
async def test_watchdog_short_unhealthy_period_does_not_bark_recovery() -> None:
    now = [0.0]
    responses = [False, False, False, True, True]

    def respond(_: httpx.Request) -> httpx.Response:
        running = responses.pop(0)
        return httpx.Response(200, json={
            "running": running, "synchronized": True, "risk_state": "NORMAL",
            "websockets": [{"fresh": True}],
        })

    bark, console = BarkNotification(), ConsoleNotification()
    notifier = NotificationManager(
        [console, bark], Metrics(), policy=NotificationPolicy(), retry_delays=(),
    )
    notifier.start()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        monitor = WatchdogMonitor(
            WatchdogSettings(
                _env_file=None, watchdog_startup_grace_seconds=0,
                watchdog_unhealthy_alert_seconds=60,
            ), client, notifier, clock=lambda: now[0],
        )
        for second in (0, 1, 2, 3, 4):
            now[0] = second
            await monitor.check()
    await notifier.stop(drain_seconds=1)
    assert bark.events == []
    assert [event.title for event in console.events] == [
        "⚠️ Trading App Unhealthy", "✅ Trading App Recovered",
    ]


@pytest.mark.asyncio
async def test_watchdog_persistent_unhealthy_barks_then_recovers() -> None:
    now = [0.0]
    responses = [False, False, False, False, True, True]

    def respond(_: httpx.Request) -> httpx.Response:
        running = responses.pop(0)
        return httpx.Response(200, json={
            "running": running, "synchronized": True, "risk_state": "NORMAL",
            "websockets": [{"fresh": True}],
        })

    bark = BarkNotification()
    notifier = NotificationManager([bark], Metrics(), policy=NotificationPolicy(), retry_delays=())
    notifier.start()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        monitor = WatchdogMonitor(
            WatchdogSettings(
                _env_file=None, watchdog_startup_grace_seconds=0,
                watchdog_unhealthy_alert_seconds=60,
            ), client, notifier, clock=lambda: now[0],
        )
        for second in (0, 1, 2, 61):
            now[0] = second
            await monitor.check()
        await asyncio.sleep(0.01)
        for second in (62, 63):
            now[0] = second
            await monitor.check()
    await notifier.stop(drain_seconds=1)
    assert [event.title for event in bark.events] == [
        "⚠️ Trading App Unhealthy", "✅ Trading App Recovered",
    ]


@pytest.mark.asyncio
async def test_watchdog_recovery_waits_for_actual_bark_delivery() -> None:
    started, release = asyncio.Event(), asyncio.Event()
    responses = [503, 503, 503, 200, 200]

    def respond(_: httpx.Request) -> httpx.Response:
        status = responses.pop(0)
        if status == 503:
            return httpx.Response(status)
        return httpx.Response(200, json={
            "running": True, "synchronized": True, "risk_state": "NORMAL",
            "websockets": [{"fresh": True}],
        })

    bark = BarkNotification()

    async def delayed_send(event: NotificationEvent) -> None:
        if event.title == "🚨 Trading App Offline":
            started.set()
            await release.wait()
        bark.events.append(event)

    bark.send = delayed_send  # type: ignore[method-assign]
    notifier = NotificationManager([bark], Metrics(), policy=NotificationPolicy())
    notifier.start()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        monitor = WatchdogMonitor(
            WatchdogSettings(_env_file=None, watchdog_startup_grace_seconds=0),
            client, notifier,
        )
        for _ in range(3):
            await monitor.check()
        await asyncio.wait_for(started.wait(), 1)
        await monitor.check()
        await monitor.check()
        assert bark.events == []
        release.set()
    await notifier.stop(drain_seconds=1)
    assert [event.title for event in bark.events] == [
        "🚨 Trading App Offline", "✅ Trading App Recovered",
    ]


@pytest.mark.asyncio
async def test_watchdog_failed_offline_does_not_send_false_recovery() -> None:
    responses = [503, 503, 503, 200, 200]

    def respond(_: httpx.Request) -> httpx.Response:
        status = responses.pop(0)
        if status == 503:
            return httpx.Response(status)
        return httpx.Response(200, json={
            "running": True, "synchronized": True, "risk_state": "NORMAL",
            "websockets": [{"fresh": True}],
        })

    bark, console, metrics = BarkNotification(), ConsoleNotification(), Metrics()
    attempts: list[NotificationEvent] = []

    async def failed_send(event: NotificationEvent) -> None:
        attempts.append(event)
        raise ConnectionError("Bark unavailable")

    bark.send = failed_send  # type: ignore[method-assign]
    notifier = NotificationManager(
        [console, bark], metrics, policy=NotificationPolicy(), retry_delays=(),
    )
    notifier.start()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        monitor = WatchdogMonitor(
            WatchdogSettings(_env_file=None, watchdog_startup_grace_seconds=0),
            client, notifier,
        )
        for _ in range(3):
            await monitor.check()
        async def wait_for_failure() -> None:
            while metrics.notification_failed.labels(
                channel="bark", priority="CRITICAL", category="INFRASTRUCTURE"
            )._value.get() != 1:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_failure(), 1)
        await monitor.check()
        await monitor.check()
    await notifier.stop(drain_seconds=1)
    assert [event.title for event in attempts] == ["🚨 Trading App Offline"]
    assert [event.title for event in console.events] == [
        "🚨 Trading App Offline", "✅ Trading App Recovered",
    ]
