from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest

from app.config import Settings
from app.models import (
    GovernorState,
    NotificationCategory,
    NotificationEvent,
    NotificationLevel,
    NotificationPriority,
    OrderState,
    PortfolioState,
)
from app.monitoring import Metrics, Notification
from app.notifications import BarkNotification, NotificationManager
from app.runtime import TradingRuntime
from app.storage import Store
from tests.test_production_safety import SYMBOL, runtime_with_entry
from tests.test_startup_partial_recovery import protective_algo


def event(
    title: str = "Test", *, level: NotificationLevel = NotificationLevel.INFO,
    category: NotificationCategory = NotificationCategory.SYSTEM,
    priority: NotificationPriority = NotificationPriority.ACTIVE,
    dedup_key: str | None = None,
    metadata: dict | None = None,
) -> NotificationEvent:
    return NotificationEvent(
        level=level, category=category, title=title, message="status",
        priority=priority, dedup_key=dedup_key, metadata=metadata or {},
    )


class Recorder(Notification):
    def __init__(self) -> None:
        self.events: list[NotificationEvent] = []

    async def send(self, item: NotificationEvent) -> None:
        self.events.append(item)

    async def send_alert(self, level: str, title: str, message: str) -> None:
        raise AssertionError("event send should be used")


def manager(channel: Notification, **kwargs) -> NotificationManager:
    return NotificationManager([channel], Metrics(), retry_delays=(), **kwargs)


def bark_settings(**kwargs) -> Settings:
    return Settings(
        _env_file=None, bark_enabled=True, bark_device_key="test-device-secret",
        **kwargs,
    )


@pytest.mark.asyncio
async def test_bark_post_payload():
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"code": 200})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        bark = BarkNotification(bark_settings(), client)
        await bark.send(event("Urgent", priority=NotificationPriority.CRITICAL))
    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert str(requests[0].url) == "https://api.day.app/push"
    payload = json.loads(requests[0].content)
    assert payload == {
        "device_key": "test-device-secret", "title": "🚨 系统严重异常",
        "body": "系统异常（reason_code: status）",
        "group": "OKX Quant", "level": "critical", "sound": "alarm", "volume": "5",
    }


@pytest.mark.asyncio
async def test_bark_timeout_does_not_affect_runtime():
    async def slow(_: httpx.Request) -> httpx.Response:
        await asyncio.sleep(1)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as client:
        bark = BarkNotification(bark_settings(bark_timeout_seconds=0.01), client)
        runtime = TradingRuntime(Settings(_env_file=None))
        runtime.notifications = manager(bark)
        runtime.notifications.start()
        await asyncio.wait_for(runtime.alert("ERROR", "Bark timeout", "observe"), 0.05)
        assert runtime.governor.state == GovernorState.HALT
        await runtime.notifications.stop(drain_seconds=0.2)
        assert runtime.notifications.metrics.notification_failed.labels(
            channel="bark", priority="TIME_SENSITIVE", category="SYSTEM"
        )._value.get() == 1
        await runtime.client.close()


@pytest.mark.asyncio
async def test_bark_500_retries():
    calls = 0

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500 if calls < 4 else 200, json={"code": 200})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        bark = BarkNotification(bark_settings(), client)
        notifier = NotificationManager([bark], Metrics(), retry_delays=(0, 0, 0))
        notifier.start()
        await notifier.publish(event())
        await notifier.stop(drain_seconds=1)
    assert calls == 4
    assert notifier.metrics.notification_sent.labels(
        channel="bark", priority="ACTIVE", category="SYSTEM"
    )._value.get() == 1


@pytest.mark.asyncio
async def test_bark_secret_not_logged(caplog):
    def fail(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
        bark = BarkNotification(bark_settings(), client)
        notifier = manager(bark)
        with caplog.at_level(logging.ERROR):
            notifier.start()
            await notifier.publish(event(priority=NotificationPriority.CRITICAL))
            await notifier.stop(drain_seconds=1)
    assert "test-device-secret" not in caplog.text
    assert "ConnectError" in caplog.text


@pytest.mark.asyncio
async def test_bark_dns_and_connection_refused_do_not_affect_governor(tmp_path):
    for error in (httpx.ConnectError("DNS failed"), httpx.ConnectError("refused")):
        async def fail(_: httpx.Request, failure: Exception = error) -> httpx.Response:
            raise failure

        async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
            runtime = TradingRuntime(Settings(
                _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/bark.db"
            ))
            runtime.governor.resume(synchronized=True, healthy=True)
            runtime.notifications = manager(BarkNotification(bark_settings(), client))
            runtime.notifications.start()
            await runtime.alert("INFO", "Event", "network failure")
            await runtime.notifications.stop(drain_seconds=1)
            assert runtime.governor.state == GovernorState.NORMAL
            await runtime.client.close()


@pytest.mark.asyncio
async def test_publish_does_not_wait_for_http():
    entered = asyncio.Event()
    release = asyncio.Event()

    class Slow(Recorder):
        async def send(self, item: NotificationEvent) -> None:
            entered.set()
            await release.wait()
            await super().send(item)

    channel = Slow()
    notifier = manager(channel)
    notifier.start()
    await asyncio.wait_for(notifier.publish(event()), timeout=0.05)
    await entered.wait()
    await asyncio.wait_for(notifier.publish(event("second")), timeout=0.05)
    assert notifier.queue_size == 1
    release.set()
    await notifier.stop(drain_seconds=1)
    assert [item.title for item in channel.events] == ["Test", "second"]


@pytest.mark.asyncio
async def test_notification_queue_does_not_block_trading():
    notifier = manager(Recorder(), max_queue=1)
    await notifier.publish(event("first"))
    await asyncio.wait_for(notifier.publish(event("second")), timeout=0.05)
    assert notifier.queue_size == 1


@pytest.mark.asyncio
async def test_low_priority_event_can_be_dropped_when_queue_full():
    notifier = manager(Recorder(), max_queue=1)
    await notifier.publish(event("important", priority=NotificationPriority.CRITICAL))
    await notifier.publish(event(
        "heartbeat", priority=NotificationPriority.PASSIVE,
        category=NotificationCategory.HEARTBEAT,
    ))
    assert notifier.queue_size == 1
    assert notifier._pending[NotificationPriority.CRITICAL][0].title == "important"
    assert notifier.metrics.notification_dropped.labels(
        priority="PASSIVE", category="HEARTBEAT"
    )._value.get() == 1


@pytest.mark.asyncio
async def test_critical_event_is_prioritized():
    channel = Recorder()
    notifier = manager(channel, max_queue=2)
    await notifier.publish(event("passive", priority=NotificationPriority.PASSIVE))
    await notifier.publish(event("active"))
    await notifier.publish(event("critical", priority=NotificationPriority.CRITICAL))
    notifier.start()
    await notifier.stop(drain_seconds=1)
    assert [item.title for item in channel.events] == ["critical", "active"]


@pytest.mark.asyncio
async def test_error_level_evictions_are_prioritized():
    notifier = manager(Recorder(), max_queue=1)
    await notifier.publish(event("info"))
    await notifier.publish(event("error", level=NotificationLevel.ERROR))
    pending = notifier._pending[NotificationPriority.TIME_SENSITIVE]
    assert [item.title for item in pending] == ["error"]


@pytest.mark.asyncio
async def test_duplicate_alert_is_suppressed():
    notifier = manager(Recorder())
    await notifier.publish(event(dedup_key="ws"))
    await notifier.publish(event(dedup_key="ws"))
    assert notifier.queue_size == 1


@pytest.mark.asyncio
async def test_recovery_event_is_not_suppressed():
    notifier = manager(Recorder())
    await notifier.publish(event("Disconnected", dedup_key="ws", metadata={"transition": True}))
    recovered = event("Recovered", dedup_key="ws", metadata={"recovery": True})
    await notifier.publish(recovered)
    await notifier.publish(recovered)
    assert notifier.queue_size == 2


@pytest.mark.asyncio
async def test_critical_transition_is_sent():
    notifier = manager(Recorder())
    critical = event(
        "EMERGENCY", level=NotificationLevel.CRITICAL,
        priority=NotificationPriority.CRITICAL, dedup_key="risk",
        metadata={"transition": True},
    )
    await notifier.publish(critical)
    await notifier.publish(event("Recovered", dedup_key="risk", metadata={"recovery": True}))
    await notifier.publish(critical)
    assert notifier.queue_size == 3


@pytest.mark.asyncio
async def test_notification_worker_crash_restarts():
    channel = Recorder()
    notifier = manager(channel)
    original = notifier._deliver
    crashed = False

    async def once(item: NotificationEvent) -> None:
        nonlocal crashed
        if not crashed:
            crashed = True
            raise RuntimeError("worker crashed")
        await original(item)

    notifier._deliver = once  # type: ignore[method-assign]
    notifier.start()
    await notifier.publish(event("crash"))
    for _ in range(20):
        if crashed and notifier._worker and not notifier._worker.done():
            break
        await asyncio.sleep(0.01)
    await notifier.publish(event("after restart"))
    await notifier.stop(drain_seconds=1)
    assert [item.title for item in channel.events] == ["after restart"]


@pytest.mark.asyncio
async def test_notification_audit_database_failure_isolated(caplog):
    class BrokenStore:
        async def append(self, *args, **kwargs) -> None:
            raise OSError("database unavailable")

    channel = Recorder()
    notifier = manager(channel, store=BrokenStore())
    notifier.start()
    await notifier.publish(event())
    await notifier.stop(drain_seconds=1)
    assert len(channel.events) == 1
    assert "notification audit unavailable" in caplog.text


@pytest.mark.asyncio
async def test_notification_audit_excludes_bark_secret(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/notifications.db")
    await store.initialize()

    def respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 200})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        notifier = NotificationManager(
            [BarkNotification(bark_settings(), client)], Metrics(), store=store,
            retry_delays=(),
        )
        notifier.start()
        await notifier.publish(event())
        await notifier.stop(drain_seconds=1)
    rows = await store.latest("notification_events")
    assert rows[0]["status"] == "SENT"
    assert "test-device-secret" not in json.dumps(rows)
    assert "api.day.app" not in json.dumps(rows)
    await store.close()


@pytest.mark.asyncio
async def test_notification_shutdown_drain_is_bounded():
    class Stuck(Recorder):
        async def send(self, _: NotificationEvent) -> None:
            await asyncio.Event().wait()

    notifier = manager(Stuck())
    notifier.start()
    await notifier.publish(event())
    await asyncio.wait_for(notifier.stop(drain_seconds=0.01), timeout=0.2)


@pytest.mark.asyncio
async def test_bad_bark_configuration_does_not_block_runtime(tmp_path):
    for configuration in (
        {"bark_enabled": True, "bark_device_key": ""},
        {"bark_enabled": True, "bark_device_key": "secret", "bark_server": "invalid"},
    ):
        runtime = TradingRuntime(Settings(
            _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/bad-bark.db",
            **configuration,
        ))
        assert len(runtime.notifications.channels) == 1
        await runtime.client.close()


@pytest.mark.asyncio
async def test_notification_format_error_does_not_change_order_handling(tmp_path):
    runtime, _, request = await runtime_with_entry(tmp_path)
    runtime.governor.resume(synchronized=True, healthy=True)
    runtime._notify_entry_filled = AsyncMock(side_effect=ValueError("notification format"))
    runtime.reconcile = AsyncMock()
    await runtime._on_private({"arg": {"channel": "orders"}, "data": [{
        "clOrdId": request.client_order_id, "instId": SYMBOL, "state": "filled",
        "accFillSz": "1", "fillPx": "invalid",
    }]})
    assert runtime.order_manager.orders[request.client_order_id]["state"] == "FILLED"
    assert runtime.governor.state == GovernorState.NORMAL
    runtime.reconcile.assert_awaited_once()
    await runtime.store.close()


@pytest.mark.asyncio
async def test_bark_levels_are_mapped():
    received: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        received.append(json.loads(request.content)["level"])
        return httpx.Response(200, json={"code": 200})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        bark = BarkNotification(bark_settings(), client)
        for priority in NotificationPriority:
            await bark.send(event(priority=priority))
    assert received == ["passive", "active", "timeSensitive", "critical"]


@pytest.mark.asyncio
async def test_entry_fill_sends_notification(tmp_path):
    runtime, _, request = await runtime_with_entry(tmp_path)
    runtime.notifications = manager(Recorder())
    runtime.reconcile = AsyncMock()
    await runtime._on_private({"arg": {"channel": "orders"}, "data": [{
        "clOrdId": request.client_order_id, "instId": SYMBOL, "state": "filled",
        "accFillSz": "1", "fillPx": "50000",
    }]})
    assert any("Filled" in item.title for item in runtime.notifications._pending[NotificationPriority.TIME_SENSITIVE])
    await runtime.store.close()


@pytest.mark.asyncio
async def test_partial_fill_sends_notification(tmp_path):
    runtime, _, request = await runtime_with_entry(tmp_path)
    runtime.notifications = manager(Recorder())
    runtime._handle_partial_fill = AsyncMock(return_value=True)
    await runtime._on_private({"arg": {"channel": "orders"}, "data": [{
        "clOrdId": request.client_order_id, "instId": SYMBOL, "state": "partially_filled",
        "accFillSz": "0.5",
    }]})
    pending = runtime.notifications._pending[NotificationPriority.TIME_SENSITIVE]
    assert any("Partial Fill" in item.title and "confirmed" in item.message for item in pending)
    await runtime.store.close()


@pytest.mark.asyncio
async def test_startup_partial_fill_reports_verified_protection(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    runtime.notifications = manager(Recorder())
    exchange.pending[0]["accFillSz"] = "0.5"
    exchange.position = Decimal("0.5")
    exchange.algos = [protective_algo(runtime, request)]
    await runtime.reconcile()
    pending = runtime.notifications._pending[NotificationPriority.TIME_SENSITIVE]
    titles = [item.title for item in pending]
    assert any("Partial Fill" in title for title in titles)
    assert any("Protection Active" in title for title in titles)
    await runtime.store.close()


@pytest.mark.asyncio
async def test_protective_stop_confirmed_sends_notification(tmp_path):
    runtime, _, request = await runtime_with_entry(tmp_path)
    runtime.notifications = manager(Recorder())
    await runtime.order_manager.transition(request.client_order_id, OrderState.FILLED, filled="1")
    entry = runtime.order_manager.orders[request.client_order_id]
    algo = {
        "algoClOrdId": entry["protective_algo_id"], "instId": SYMBOL,
        "side": "sell", "sz": "1", "slTriggerPx": entry["stop_price"],
        "slTriggerPxType": "mark", "slOrdPx": "-1", "posSide": "net",
        "state": "live", "failCode": "", "reduceOnly": "true",
    }
    assert await runtime._verify_protection(SYMBOL, Decimal("1"), [algo])
    pending = runtime.notifications._pending[NotificationPriority.TIME_SENSITIVE]
    assert any("Protection Active" in item.title for item in pending)
    await runtime.store.close()


@pytest.mark.asyncio
async def test_position_close_sends_notification(tmp_path):
    runtime = TradingRuntime(Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/close.db"
    ))
    runtime.portfolio.daily_pnl = Decimal("12")
    runtime.notifications = manager(Recorder())
    await runtime._notify_position_closed(SYMBOL)
    pending = runtime.notifications._pending[NotificationPriority.TIME_SENSITIVE]
    assert len(pending) == 1
    assert "Position Closed" in pending[0].title
    assert "Net PnL" not in pending[0].message
    assert "R:" not in pending[0].message
    await runtime.client.close()


@pytest.mark.asyncio
async def test_halt_sends_notification(tmp_path):
    runtime, _, _ = await runtime_with_entry(tmp_path)
    runtime.notifications = manager(Recorder())
    await runtime.enter_halt("market stale")
    pending = runtime.notifications._pending[NotificationPriority.TIME_SENSITIVE]
    assert len(pending) == 1
    assert pending[0].title == "🚨 HALT"
    assert runtime.governor.state == GovernorState.HALT
    await runtime.store.close()


@pytest.mark.asyncio
async def test_emergency_sends_critical_notification(tmp_path):
    runtime, _, _ = await runtime_with_entry(tmp_path)
    runtime.notifications = manager(Recorder())
    runtime.emergency.target = AsyncMock()
    runtime.emergency.step = AsyncMock()
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    await runtime.enter_emergency(SYMBOL, "unprotected position")
    pending = runtime.notifications._pending[NotificationPriority.CRITICAL]
    assert pending[0].title == "🚨 EMERGENCY"
    assert runtime.governor.state == GovernorState.EMERGENCY
    await runtime.store.close()


@pytest.mark.asyncio
async def test_repeated_same_halt_deduplicated(tmp_path):
    runtime, _, _ = await runtime_with_entry(tmp_path)
    runtime.notifications = manager(Recorder())
    await runtime.enter_halt("market stale")
    await runtime.enter_halt("market stale")
    assert len(runtime.notifications._pending[NotificationPriority.TIME_SENSITIVE]) == 1
    await runtime.store.close()


@pytest.mark.asyncio
async def test_normal_recovery_notification(tmp_path):
    runtime, _, _ = await runtime_with_entry(tmp_path)
    runtime.notifications = manager(Recorder())
    await runtime.enter_halt("WS stale")
    runtime.governor.resume(synchronized=True, healthy=True)
    await runtime._notify_risk_state()
    pending = runtime.notifications._pending[NotificationPriority.ACTIVE]
    assert any(item.title == "✅ Risk State Recovered" for item in pending)
    await runtime.store.close()


@pytest.mark.asyncio
async def test_heartbeat_contains_portfolio_and_ws_health(tmp_path):
    runtime = TradingRuntime(Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/heartbeat.db"
    ))
    runtime.notifications = manager(Recorder())
    runtime.governor.resume(synchronized=True, healthy=True)
    runtime.portfolio = PortfolioState(
        equity=Decimal("5000"), available_balance=Decimal("4500"),
        daily_pnl=Decimal("12"), weekly_drawdown=Decimal("0.01"),
        open_risk=Decimal("4"), positions={SYMBOL: Decimal("1")},
    )
    await runtime._publish_heartbeat()
    item = runtime.notifications._pending[NotificationPriority.PASSIVE][0]
    assert "Equity: 5000 USDT" in item.message
    assert "WS: 0/0 healthy" in item.message
    assert "Positions: 1" in item.message
    runtime.governor.halt("Redis unavailable")
    await runtime._publish_heartbeat()
    assert runtime.notifications._pending[NotificationPriority.PASSIVE][1].title == "⚠️ Quant Heartbeat"
    await runtime.client.close()


@pytest.mark.asyncio
async def test_system_start_waits_for_reconciliation(tmp_path):
    runtime = TradingRuntime(Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/startup.db"
    ))
    await runtime.store.initialize()
    runtime.notifications = manager(Recorder())
    runtime.governor.resume(synchronized=True, healthy=True)
    runtime.portfolio.synchronized = True
    runtime.portfolio.equity = Decimal("5000")
    runtime.redis = object()
    runtime.dead_man_healthy = True

    class HealthySocket:
        private = False
        reconciliation_required = False

        def is_fresh(self) -> bool:
            return True

        is_data_fresh = is_fresh
        is_processing_healthy = is_fresh
        def is_transport_healthy(self):
            return getattr(self, "connected", True)

    runtime.sockets = [HealthySocket() for _ in range(4)]
    await runtime._publish_started_if_ready()
    assert runtime.notifications.queue_size == 0
    runtime.reconciliation_healthy = True
    await runtime._publish_started_if_ready()
    await runtime._publish_started_if_ready()
    pending = runtime.notifications._pending[NotificationPriority.ACTIVE]
    assert len(pending) == 1
    assert "WS: 4/4" in pending[0].message
    await runtime.store.close()
    await runtime.client.close()


@pytest.mark.asyncio
async def test_infrastructure_outage_and_recovery_notifications(tmp_path):
    runtime = TradingRuntime(Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/infra.db"
    ))
    runtime.notifications = manager(Recorder())
    await runtime._observe_component(
        "WebSocket", True, outage="Disconnected", recovery="Recovered", threshold=2
    )
    await runtime._observe_component(
        "WebSocket", False, outage="Disconnected", recovery="Recovered", threshold=2
    )
    assert runtime.notifications.queue_size == 0
    await runtime._observe_component(
        "WebSocket", False, outage="Disconnected", recovery="Recovered", threshold=2
    )
    await runtime._observe_component(
        "WebSocket", True, outage="Disconnected", recovery="Recovered", threshold=2
    )
    await runtime._observe_component(
        "WebSocket", True, outage="Disconnected", recovery="Recovered", threshold=2
    )
    pending = runtime.notifications._pending[NotificationPriority.ACTIVE]
    assert [item.title for item in pending] == ["Disconnected", "Recovered"]
    await runtime.client.close()


@pytest.mark.asyncio
async def test_daily_report_omits_unavailable_trade_statistics(tmp_path):
    runtime = TradingRuntime(Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/daily.db"
    ))
    runtime.notifications = manager(Recorder())
    await runtime._publish_daily_report({
        "date": "2026-09-29", "equity_end": "5000", "equity_change": "10",
        "orders": 2, "fills": 1, "fees": "-0.1", "max_drawdown": "0.001",
        "halt_count": 0, "emergency_count": 0,
    })
    item = runtime.notifications._pending[NotificationPriority.PASSIVE][0]
    assert "Daily PnL: 10" in item.message
    assert "HALT count: 0" in item.message
    assert "Wins:" not in item.message
    assert "Funding:" not in item.message
    await runtime.client.close()


@pytest.mark.asyncio
async def test_notification_failure_does_not_affect_emergency_or_reconciliation(tmp_path):
    runtime, exchange, _ = await runtime_with_entry(tmp_path)

    class BrokenManager:
        async def publish(self, _: NotificationEvent) -> None:
            raise RuntimeError("notification unavailable")

    runtime.notifications = BrokenManager()
    runtime.emergency.target = AsyncMock()
    runtime.emergency.step = AsyncMock()
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    await runtime.enter_emergency(SYMBOL, "protective stop missing")
    assert runtime.governor.state == GovernorState.EMERGENCY
    assert runtime.emergency.step.await_count == 1
    exchange.position = Decimal(0)
    await runtime.reconcile()
    assert runtime.reconciliation_healthy
    await runtime.store.close()
