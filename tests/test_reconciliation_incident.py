from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.incidents import IncidentManager
from app.models import GovernorState, NotificationCategory, NotificationEvent, NotificationLevel
from tests.test_incident_delivery import risk_recovered
from tests.test_production_safety import SYMBOL, invalid_stop_runtime


@pytest.mark.parametrize("fail_code", ["", "0", 0, None])
async def test_zero_algo_failure_code_keeps_verified_protection(tmp_path, fail_code):
    runtime, exchange = await invalid_stop_runtime(tmp_path, {"failCode": fail_code})
    try:
        assert await runtime._verify_protection(SYMBOL, Decimal(1), exchange.algos)
        assert runtime.governor.state != GovernorState.EMERGENCY
        assert not exchange.placed
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


@pytest.mark.parametrize("fail_code", ["51008", "unknown", False, "00"])
async def test_actual_or_malformed_algo_failure_code_still_blocks(tmp_path, fail_code):
    runtime, exchange = await invalid_stop_runtime(tmp_path, {"failCode": fail_code})
    try:
        assert not runtime.algo_manager.valid(
            exchange.algos[0], symbol=SYMBOL, position=Decimal(1),
            stop_price=Decimal("49500"), tolerance=Decimal("0.1"),
        )
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_live_algo_ws_zero_failure_code_does_not_trigger_recovery(tmp_path):
    runtime, exchange = await invalid_stop_runtime(tmp_path, {"failCode": "0"})
    runtime.portfolio.positions = {SYMBOL: Decimal(1)}
    runtime.reconcile = AsyncMock()
    try:
        await runtime._on_private({"arg": {"channel": "orders-algo"}, "data": exchange.algos})
        runtime.reconcile.assert_not_awaited()
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def exit_reconciliation_runtime(tmp_path):
    runtime, exchange = await invalid_stop_runtime(tmp_path, {})
    entry = next(iter(runtime.order_manager.orders.values()))
    entry["reconciled_filled"] = "1"
    exit_row = entry | {
        "clOrdId": "qownedexit", "state": "ACKNOWLEDGED", "order_id": "exit-id",
        "reduce_only": True, "protective_algo_id": "", "direction": "SHORT",
        "approved_contracts": "1", "filled": "0", "reconciled_filled": "0",
    }
    runtime.order_manager.orders[exit_row["clOrdId"]] = exit_row
    runtime.portfolio.synchronized = True
    runtime.portfolio.positions = {SYMBOL: Decimal(1)}
    exchange.position = Decimal(0)
    exchange.pending = []
    exchange.algos = []
    detail = {
        "clOrdId": exit_row["clOrdId"], "ordId": "exit-id", "instId": SYMBOL,
        "side": "sell", "reduceOnly": "true", "sz": "1", "accFillSz": "1",
        "state": "filled",
    }
    exchange.order_state[exit_row["clOrdId"]] = detail
    return runtime, exchange, exit_row, detail


async def test_rest_confirmed_exit_fill_before_ws_reconciles(tmp_path):
    runtime, exchange, exit_row, _ = await exit_reconciliation_runtime(tmp_path)
    reads = 0
    positions = exchange.positions

    async def stale_initial_positions():
        nonlocal reads
        reads += 1
        if reads == 1:
            exchange.position = Decimal(1)
            result = await positions()
            exchange.position = Decimal(0)
            return result
        return await positions()

    exchange.positions = stale_initial_positions
    try:
        await runtime.reconcile()
        assert runtime.reconciliation_healthy
        assert runtime.portfolio.positions == {}
        assert exit_row["state"] == "FILLED"
        assert exit_row["reconciled_filled"] == "1"
        assert reads == 2
        assert not exchange.placed
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_stale_pending_snapshot_with_ws_terminal_fill_is_rechecked(tmp_path):
    runtime, exchange, exit_row, detail = await exit_reconciliation_runtime(tmp_path)
    exit_row.update(state="FILLED", filled="1")
    reads = 0

    async def pending():
        nonlocal reads
        reads += 1
        return [detail | {"state": "live", "accFillSz": "0"}] if reads == 1 else []

    exchange.pending_orders = pending
    try:
        await runtime.reconcile()
        assert runtime.reconciliation_healthy
        assert runtime.portfolio.positions == {}
        assert reads == 2
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


@pytest.mark.parametrize("change", [
    {"instId": "ETH-USDT-SWAP"}, {"clOrdId": "foreign"}, {"side": "buy"},
    {"reduceOnly": "false"}, {"sz": "2"}, {"accFillSz": "0.5"},
    {"state": "live"}, {"ordId": "foreign-id"}, {"accFillSz": "NaN"},
])
async def test_order_detail_mismatch_cannot_authorize_position_change(tmp_path, change):
    runtime, exchange, exit_row, detail = await exit_reconciliation_runtime(tmp_path)
    exchange.order_state[exit_row["clOrdId"]] = detail | change
    try:
        await runtime.reconcile()
        assert not runtime.reconciliation_healthy
        assert runtime.governor.state != GovernorState.NORMAL
        assert exit_row["state"] == "ACKNOWLEDGED"
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_missing_order_detail_still_fails_closed(tmp_path):
    runtime, exchange, _, _ = await exit_reconciliation_runtime(tmp_path)
    exchange.order_state.clear()
    try:
        await runtime.reconcile()
        assert not runtime.reconciliation_healthy
        assert runtime.governor.state != GovernorState.NORMAL
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_second_pending_snapshot_mismatch_still_blocks(tmp_path):
    runtime, exchange, _, detail = await exit_reconciliation_runtime(tmp_path)
    reads = 0

    async def pending():
        nonlocal reads
        reads += 1
        return [] if reads == 1 else [detail | {"state": "live", "accFillSz": "0"}]

    exchange.pending_orders = pending
    try:
        await runtime.reconcile()
        assert reads == 2
        assert not runtime.reconciliation_healthy
        assert runtime.governor.reason == "order mismatch"
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_terminal_order_query_failure_still_blocks(tmp_path):
    runtime, exchange, _, _ = await exit_reconciliation_runtime(tmp_path)
    exchange.query_error = True
    try:
        await runtime.reconcile()
        assert not runtime.reconciliation_healthy
        assert runtime.governor.state != GovernorState.NORMAL
        assert not exchange.placed
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


def infrastructure_event(*, recovery=False):
    return NotificationEvent(
        level=NotificationLevel.INFO if recovery else NotificationLevel.ERROR,
        category=NotificationCategory.INFRASTRUCTURE, title="Reconciliation", message="",
        metadata={"component": "reconciliation", "recovery": recovery, "risk_state": "EMERGENCY"},
    )


def test_recovered_infrastructure_does_not_report_unavailable_while_emergency():
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0])
    incidents.observe(infrastructure_event())
    now[0] = 15
    incidents.observe(infrastructure_event(recovery=True))
    now[0] = 180
    incidents.observe(infrastructure_event(recovery=True))
    assert incidents.due() == []
    assert incidents.observe(risk_recovered()) == []
    assert incidents.due() == []
    assert incidents.history[-1].duration_seconds == 15


def test_queued_infrastructure_alert_is_superseded_after_component_recovers():
    now = [0.0]
    incidents = IncidentManager(clock=lambda: now[0])
    incidents.observe(infrastructure_event())
    now[0] = 60
    event = incidents.due()[0]
    incidents.observe(infrastructure_event(recovery=True))
    assert incidents.should_supersede_open(event)
