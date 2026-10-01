import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.bark_formatter import localize_bark_event
from app.incidents import IncidentManager
from app.models import GovernorState
from app.okx import OkxError
from tests.test_production_safety import SYMBOL, invalid_stop_runtime
from tests.test_reconciliation_incident import exit_reconciliation_runtime, infrastructure_event


async def test_in_progress_reconciliation_does_not_report_outage(tmp_path):
    runtime, exchange = await invalid_stop_runtime(tmp_path, {})
    runtime.portfolio.synchronized = True
    runtime.portfolio.positions = {SYMBOL: Decimal(1)}
    runtime.order_manager.mark_reconciled()
    entered, release = asyncio.Event(), asyncio.Event()
    account = exchange.account

    async def slow_account():
        entered.set()
        await release.wait()
        return await account()

    try:
        await runtime.reconcile()
        await runtime._observe_infrastructure()
        assert runtime._component_health["Reconciliation"]
        exchange.account = slow_account
        task = asyncio.create_task(runtime.reconcile())
        await entered.wait()
        try:
            assert not runtime.reconciliation_healthy  # Existing safety gate remains conservative.
            await runtime._observe_infrastructure()
            await runtime._observe_infrastructure()
            assert runtime._component_health["Reconciliation"]
        finally:
            release.set()
            await task
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_completed_failed_reconciliation_reports_outage(tmp_path):
    runtime, exchange = await invalid_stop_runtime(tmp_path, {})
    try:
        await runtime.reconcile()
        await runtime._observe_infrastructure()
        exchange.account = AsyncMock(side_effect=OkxError("read failure"))
        await runtime.reconcile()
        await runtime._observe_infrastructure()
        await runtime._observe_infrastructure()
        assert not runtime._component_health["Reconciliation"]
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_stalled_reconciliation_eventually_reports_outage(tmp_path):
    runtime, _ = await invalid_stop_runtime(tmp_path, {})
    clock = [0.0]
    runtime._clock = lambda: clock[0]
    try:
        await runtime.reconcile()
        await runtime._observe_infrastructure()
        clock[0] = 1000
        await runtime._observe_infrastructure()
        await runtime._observe_infrastructure()
        assert not runtime._component_health["Reconciliation"]
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


def test_infra_recurrence_during_emergency_has_new_timer():
    clock = [0.0]
    incidents = IncidentManager(clock=lambda: clock[0])
    incidents.observe(infrastructure_event())
    first_id = incidents.active["infra:trading"].id
    clock[0] = 25
    assert incidents.observe(infrastructure_event(recovery=True)) == []
    clock[0] = 2057
    incidents.observe(infrastructure_event())
    assert incidents.due() == []
    current = incidents.active["infra:trading"]
    assert current.id != first_id
    assert current.opened_tick == 2057
    clock[0] += 60
    opened = incidents.due()[0]
    assert "Duration: 60s+" in opened.message


def test_component_recovery_during_emergency_is_not_trading_recovery():
    clock = [0.0]
    incidents = IncidentManager(clock=lambda: clock[0])
    incidents.observe(infrastructure_event())
    clock[0] = 60
    opened = incidents.due()[0]
    incidents.delivery_confirmed(opened)
    clock[0] = 70
    recovered = incidents.observe(infrastructure_event(recovery=True))
    assert len(recovered) == 1
    assert recovered[0].event_code == "INFRASTRUCTURE_RECOVERED"
    assert "Risk: EMERGENCY" in recovered[0].message
    assert "NORMAL" not in recovered[0].message
    localized = localize_bark_event(recovered[0])
    assert localized.title == "✅ 交易基础设施已恢复"
    assert "EMERGENCY" in localized.message
    assert incidents.active == {}


async def blocked_completed_entry(tmp_path, *, protected_position=False):
    runtime, exchange, exit_row, _ = await exit_reconciliation_runtime(tmp_path)
    entry = next(row for row in runtime.order_manager.orders.values() if not row["reduce_only"])
    entry.update(approved_contracts="1", order_id="entry-id")
    exchange.order_state[entry["clOrdId"]] = {
        "clOrdId": entry["clOrdId"], "ordId": "entry-id", "instId": SYMBOL,
        "side": "buy", "reduceOnly": "false", "sz": "1", "accFillSz": "1", "state": "filled",
    }
    if protected_position:
        exit_row.update(state="CANCELLED", filled="0", reconciled_filled="0")
        exchange.position = Decimal(1)
        exchange.algos = [{
            "algoClOrdId": entry["protective_algo_id"], "instId": SYMBOL,
            "side": "sell", "sz": "1", "slTriggerPx": "49500",
            "state": "live", "failCode": "0", "reduceOnly": "true",
        }]
    runtime.entry_controller.blocked.add(SYMBOL)
    runtime.governor.halt("partial fill before protective stop active", emergency=True)
    return runtime, exchange, entry


@pytest.mark.parametrize("protected_position", [False, True])
async def test_completed_entry_block_clears_only_after_verified_reconciliation(tmp_path, protected_position):
    runtime, _, _ = await blocked_completed_entry(tmp_path, protected_position=protected_position)
    try:
        await runtime.reconcile()
        assert runtime.reconciliation_healthy
        assert runtime._last_reconcile_safe
        assert SYMBOL not in runtime.entry_controller.blocked
        assert runtime.governor.state == GovernorState.EMERGENCY
        assert not runtime.auto_recovery_status()["eligible"]
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


@pytest.mark.parametrize("change", [
    {"clOrdId": "foreign"}, {"ordId": "foreign"}, {"side": "sell"},
    {"instId": "ETH-USDT-SWAP"}, {"reduceOnly": "true"}, {"accFillSz": "0.5"},
    {"sz": "2"}, {"state": "live"}, {"accFillSz": "NaN"},
])
async def test_entry_block_is_retained_for_unverified_terminal_detail(tmp_path, change):
    runtime, exchange, entry = await blocked_completed_entry(tmp_path)
    exchange.order_state[entry["clOrdId"]].update(change)
    try:
        await runtime.reconcile()
        assert SYMBOL in runtime.entry_controller.blocked
        assert runtime.governor.state == GovernorState.EMERGENCY
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_query_failure_retains_entry_block(tmp_path):
    runtime, exchange, _ = await blocked_completed_entry(tmp_path, protected_position=True)
    exchange.query_error = True
    try:
        await runtime.reconcile()
        assert SYMBOL in runtime.entry_controller.blocked
        assert runtime.governor.state == GovernorState.EMERGENCY
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_missing_protection_retains_entry_block(tmp_path):
    runtime, exchange, _ = await blocked_completed_entry(tmp_path, protected_position=True)
    exchange.algos.clear()
    try:
        await runtime.reconcile()
        assert SYMBOL in runtime.entry_controller.blocked
        assert runtime.governor.state == GovernorState.EMERGENCY
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_audit_failure_retains_entry_block(tmp_path):
    runtime, _, _ = await blocked_completed_entry(tmp_path, protected_position=True)
    append = runtime.store.append

    async def failing_audit(table, payload, **kwargs):
        if payload.get("event") == "entry block release verified":
            raise OSError("audit unavailable")
        await append(table, payload, **kwargs)

    runtime.store.append = failing_audit
    try:
        await runtime.reconcile()
        assert SYMBOL in runtime.entry_controller.blocked
        assert runtime.governor.state == GovernorState.EMERGENCY
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_protection_lost_during_terminal_query_retains_entry_block(tmp_path):
    runtime, exchange, entry = await blocked_completed_entry(tmp_path, protected_position=True)
    query = exchange.order

    async def cancel_protection_during_query(symbol, client_id):
        result = await query(symbol, client_id)
        await runtime.algo_manager.ingest(exchange.algos[0] | {"state": "canceled"})
        return result

    exchange.order = cancel_protection_during_query
    try:
        await runtime.reconcile()
        assert entry["state"] == "FILLED"
        assert SYMBOL in runtime.entry_controller.blocked
        assert runtime.governor.state == GovernorState.EMERGENCY
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_foreign_risk_order_retains_entry_block(tmp_path):
    runtime, exchange, _ = await blocked_completed_entry(tmp_path, protected_position=True)
    exchange.pending = [{"instId": SYMBOL, "clOrdId": "foreign", "reduceOnly": "false"}]
    try:
        await runtime.reconcile()
        assert SYMBOL in runtime.entry_controller.blocked
        assert not runtime._last_reconcile_safe
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_emergency_target_retains_entry_block(tmp_path):
    runtime, _, _ = await blocked_completed_entry(tmp_path, protected_position=True)
    await runtime.emergency.target(SYMBOL)
    try:
        await runtime.reconcile()
        assert SYMBOL in runtime.entry_controller.blocked
        assert not runtime._last_reconcile_safe
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


def test_recovery_then_new_outage_does_not_resend_old_queued_message():
    clock = [0.0]
    incidents = IncidentManager(clock=lambda: clock[0])
    incidents.observe(infrastructure_event())
    clock[0] = 60
    old = incidents.due()[0]
    clock[0] = 65
    incidents.observe(infrastructure_event(recovery=True))
    clock[0] = 66
    incidents.observe(infrastructure_event())
    assert incidents.should_supersede_open(old)
    incidents.supersede_open(old, attempts=0)
    clock[0] = 126
    events = incidents.due()
    current = next(event for event in events if event.metadata.get("incident_phase") == "open")
    assert current.metadata["incident_id"] != old.metadata["incident_id"]
    assert "Duration: 60s+" in current.message


async def test_manual_resume_requires_full_health_after_block_release(tmp_path):
    runtime, _, _ = await blocked_completed_entry(tmp_path, protected_position=True)
    try:
        await runtime.reconcile()
        assert not runtime.entry_controller.blocked
        assert not await runtime.resume()
        assert runtime.governor.state == GovernorState.EMERGENCY
        runtime.redis = AsyncMock()
        runtime.redis.get.return_value = "1"
        runtime.dead_man_healthy = True
        runtime.sockets = [SimpleNamespace(connected=True, is_fresh=lambda: True)]
        assert await runtime.resume()
        assert runtime.governor.state == GovernorState.NORMAL
    finally:
        await runtime.notifications.close()
        await runtime.store.close()
