from decimal import Decimal

import pytest

from app.models import GovernorState
from app.okx import OkxError
from app.runtime import TradingRuntime
from tests.test_core import instrument
from tests.test_production_safety import SYMBOL, runtime_with_entry, settings


def protective_algo(runtime, request, size="0.5"):
    return {
        "algoClOrdId": runtime.order_manager.orders[request.client_order_id]["protective_algo_id"],
        "instId": SYMBOL,
        "side": "sell",
        "sz": size,
        "slTriggerPx": "49500",
        "state": "live",
        "failCode": "",
        "reduceOnly": "true",
    }


@pytest.mark.asyncio
async def test_startup_partially_filled_pending_entry_with_valid_stop(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    exchange.pending[0]["accFillSz"] = "0.5"
    exchange.position = Decimal("0.5")
    exchange.algos = [protective_algo(runtime, request)]
    await runtime.reconcile()
    assert runtime.reconciliation_healthy
    assert runtime.portfolio.positions[SYMBOL] == Decimal("0.5")
    assert runtime.order_manager.orders[request.client_order_id]["filled"] == "0.5"
    assert runtime.order_manager.orders[request.client_order_id]["state"] == "CANCELLED"
    assert not runtime.entry_controller.blocked
    assert not exchange.placed
    await runtime.store.close()


@pytest.mark.asyncio
async def test_startup_partially_filled_pending_entry_without_stop_flattens(tmp_path):
    runtime, exchange, _ = await runtime_with_entry(tmp_path)
    exchange.pending[0]["accFillSz"] = "0.5"
    exchange.position = Decimal("0.5")
    await runtime.reconcile()
    assert runtime.governor.state == GovernorState.EMERGENCY
    assert runtime.emergency.targets[SYMBOL] == 0
    assert exchange.placed and exchange.placed[-1]["reduceOnly"] is True
    assert exchange.placed[-1]["sz"] == "0.5"
    assert await runtime.store.latest("emergency_targets")
    await runtime.store.close()


@pytest.mark.asyncio
async def test_startup_partial_fill_cancel_unconfirmed_still_protects_position(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    exchange.pending[0]["accFillSz"] = "0.5"
    exchange.position = Decimal("0.5")
    exchange.algos = [protective_algo(runtime, request)]
    exchange.query_error = True
    await runtime.reconcile()
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.governor.reason == "startup entry cancellation unconfirmed"
    assert (
        exchange.algos and runtime.order_manager.orders[request.client_order_id]["filled"] == "0.5"
    )
    assert not exchange.placed
    await runtime.store.close()


@pytest.mark.asyncio
async def test_startup_partial_fill_cancel_timeout_still_enters_emergency(tmp_path):
    runtime, exchange, _ = await runtime_with_entry(tmp_path)
    exchange.pending[0]["accFillSz"] = "0.5"
    exchange.position = Decimal("0.5")

    async def timeout(*args, **kwargs):
        raise OkxError("cancel timeout")

    exchange.cancel_order = timeout
    await runtime.reconcile()
    assert runtime.governor.state == GovernorState.EMERGENCY
    assert runtime.emergency.targets[SYMBOL] == 0
    assert exchange.placed and exchange.placed[-1]["reduceOnly"] is True
    await runtime.store.close()


@pytest.mark.asyncio
async def test_restart_during_partial_fill_recovery(tmp_path):
    runtime, exchange, _ = await runtime_with_entry(tmp_path)
    exchange.pending[0]["accFillSz"] = "0.5"
    exchange.position = Decimal("0.5")
    await runtime.reconcile()
    assert len(exchange.placed) == 1
    await runtime.store.close()

    restarted = TradingRuntime(settings(tmp_path))
    await restarted.store.initialize()
    await restarted.order_manager.restore()
    await restarted.emergency.restore()
    restarted.instruments[SYMBOL] = instrument()
    restarted.client = exchange
    restarted.execution.client = exchange
    restarted.emergency.client = exchange
    await restarted.reconcile()
    assert restarted.emergency.targets[SYMBOL] == 0
    assert len(exchange.placed) == 1
    await restarted.store.close()


@pytest.mark.asyncio
async def test_startup_partial_fill_growth_during_cancel_rechecks_coverage(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    exchange.pending[0]["accFillSz"] = "0.5"
    exchange.position = Decimal("0.5")
    exchange.algos = [protective_algo(runtime, request)]

    async def late_fill(symbol, *, client_order_id="", order_id=""):
        exchange.position = Decimal(1)
        exchange.pending = []
        exchange.order_state[client_order_id] = {"state": "canceled", "accFillSz": "1"}
        return [{"sCode": "0"}]

    exchange.cancel_order = late_fill
    await runtime.reconcile()
    assert runtime.governor.state == GovernorState.EMERGENCY
    assert runtime.emergency.targets[SYMBOL] == 0
    assert exchange.placed and exchange.placed[-1]["sz"] == "1"
    await runtime.store.close()
