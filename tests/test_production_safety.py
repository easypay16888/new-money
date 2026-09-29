from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from app.backtest import EventDrivenBacktester
from app.config import Settings
from app.decision import DecisionPipeline
from app.execution import ExecutionEngine, OrderManager
from app.models import GovernorState, OrderState, PortfolioState, Regime, Side, Signal
from app.okx import OkxError, OkxOrderRejected, OkxRestClient
from app.risk import RiskEngine, RiskGovernor
from app.runtime import TradingRuntime
from app.safety import EmergencyController
from app.storage import Store
from tests.test_core import evaluate, instrument, ready_risk
from tests.test_runtime_backtest import candles, confirmation_context

SYMBOL = "BTC-USDT-SWAP"


class Exchange:
    def __init__(self) -> None:
        self.pending: list[dict] = []
        self.algos: list[dict] = []
        self.position = Decimal(0)
        self.cancelled: list[str] = []
        self.placed: list[dict] = []
        self.order_state: dict[str, dict] = {}
        self.place_error = False
        self.query_error = False
        self.algo_error = False

    async def pending_orders(self):
        return list(self.pending)

    async def pending_algos(self):
        return list(self.algos)

    async def cancel_order(self, symbol, *, client_order_id="", order_id=""):
        self.cancelled.append(client_order_id or order_id)
        self.pending = [row for row in self.pending if row.get("clOrdId") != client_order_id]
        self.order_state[client_order_id] = {"state": "canceled", "accFillSz": "0"}
        return [{"sCode": "0"}]

    async def order(self, symbol, client_order_id):
        if self.query_error:
            raise OkxError("query timeout")
        return [self.order_state[client_order_id]] if client_order_id in self.order_state else []

    async def positions(self):
        return (
            [
                {
                    "instId": SYMBOL,
                    "pos": str(self.position),
                    "mgnRatio": "5",
                    "lever": "1",
                    "mgnMode": "isolated",
                    "margin": "500",
                    "notionalUsd": "500",
                    "markPx": "50000",
                }
            ]
            if self.position
            else []
        )

    async def account(self):
        return [{"totalEq": "10000", "details": [{"ccy": "USDT", "availEq": "9500"}]}]

    async def account_config(self):
        return [{"posMode": "net_mode"}]

    async def set_leverage(self, symbol, leverage):
        return None

    async def place_order(self, body):
        self.placed.append(body)
        if self.place_error:
            raise OkxError("HTTP timeout")
        self.order_state[body["clOrdId"]] = {"state": "live", "accFillSz": "0", "ordId": "remote-1"}
        return [{"sCode": "0", "ordId": "remote-1"}]

    async def place_algo(self, body):
        if self.algo_error:
            raise RuntimeError("algo failed")
        self.algos.append(
            {
                "algoClOrdId": body["algoClOrdId"],
                "instId": SYMBOL,
                "side": body["side"],
                "sz": body["sz"],
                "slTriggerPx": body["slTriggerPx"],
                "state": "live",
                "failCode": "",
                "reduceOnly": "true",
            }
        )
        return [{"sCode": "0", "algoId": "new-stop"}]

    async def cancel_algo(self, symbol, *, algo_id="", client_algo_id=""):
        self.algos = [algo for algo in self.algos if algo.get("algoClOrdId") != client_algo_id]
        return [{"sCode": "0"}]


def settings(tmp_path):
    return Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/safety.db",
        okx_api_key="demo",
        okx_secret_key="demo",
        okx_passphrase="demo",
    )


async def runtime_with_entry(tmp_path):
    runtime = TradingRuntime(settings(tmp_path))
    await runtime.store.initialize()
    runtime.instruments[SYMBOL] = instrument()
    exchange = Exchange()
    runtime.client = exchange
    runtime.execution.client = exchange
    runtime.entry_controller.client = exchange
    runtime.emergency.client = exchange
    request = ExecutionEngine.from_risk(evaluate(ready_risk()), instrument())
    await runtime.order_manager.create(request)
    await runtime.order_manager.transition(request.client_order_id, OrderState.ACKNOWLEDGED)
    exchange.pending = [{"instId": SYMBOL, "clOrdId": request.client_order_id}]
    return runtime, exchange, request


@pytest.mark.asyncio
async def test_halt_cancels_pending_entries(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    await runtime.enter_halt("test")
    assert request.client_order_id in exchange.cancelled
    assert runtime.order_manager.orders[request.client_order_id]["state"] == "CANCELLED"
    assert runtime.governor.state == GovernorState.HALT
    await runtime.store.close()


@pytest.mark.asyncio
async def test_halt_waits_for_inflight_entry_then_cancels(tmp_path):
    runtime = TradingRuntime(settings(tmp_path))
    await runtime.store.initialize()
    exchange = Exchange()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_place(body):
        entered.set()
        await release.wait()
        exchange.pending.append({"instId": SYMBOL, "clOrdId": body["clOrdId"]})
        exchange.order_state[body["clOrdId"]] = {"state": "live", "accFillSz": "0"}
        return [{"sCode": "0", "ordId": "remote"}]

    exchange.place_order = slow_place
    runtime.client = exchange
    runtime.execution.client = exchange
    request = ExecutionEngine.from_risk(evaluate(ready_risk()), instrument())
    runtime.governor.resume(synchronized=True, healthy=True)
    submit_task = asyncio.create_task(runtime.execution.submit(request))
    await entered.wait()
    halt_task = asyncio.create_task(runtime.enter_halt("race"))
    await asyncio.sleep(0)
    assert runtime.governor.state == GovernorState.HALT
    release.set()
    await asyncio.gather(submit_task, halt_task)
    assert request.client_order_id in exchange.cancelled
    assert runtime.order_manager.orders[request.client_order_id]["state"] == "CANCELLED"
    await runtime.store.close()


@pytest.mark.asyncio
async def test_halt_preserves_protective_orders(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    exchange.algos = [
        {"algoClOrdId": runtime.order_manager.orders[request.client_order_id]["protective_algo_id"]}
    ]
    await runtime.enter_halt("test")
    assert len(exchange.algos) == 1
    assert all(
        item not in {algo["algoClOrdId"] for algo in exchange.algos} for item in exchange.cancelled
    )
    await runtime.store.close()


@pytest.mark.asyncio
async def test_halt_preserves_reduce_only_exit(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    await runtime.order_manager.transition(request.client_order_id, OrderState.FILLED, filled="1")
    exchange.pending = [{"instId": SYMBOL, "clOrdId": "exit-1", "reduceOnly": "true"}]
    await runtime.enter_halt("test")
    assert not exchange.cancelled
    assert exchange.pending[0]["clOrdId"] == "exit-1"
    await runtime.store.close()


@pytest.mark.asyncio
async def test_expired_signal_cancels_entry(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    row = runtime.order_manager.orders[request.client_order_id]
    row["signal_expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    await runtime.entry_controller.expire()
    assert request.client_order_id in exchange.cancelled
    assert row["state"] == "CANCELLED"
    await runtime.store.close()


@pytest.mark.asyncio
async def test_stale_pending_order_never_late_fills(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    exchange.query_error = True
    await runtime.enter_halt("expired")
    assert SYMBOL in runtime.entry_controller.blocked
    assert runtime.order_manager.orders[request.client_order_id]["state"] == "CANCEL_REQUESTED"
    exchange.position = Decimal("0.5")
    await runtime._on_private(
        {
            "arg": {"channel": "orders"},
            "data": [
                {
                    "clOrdId": request.client_order_id,
                    "instId": SYMBOL,
                    "state": "filled",
                    "accFillSz": "0.5",
                    "ordId": "late-fill",
                }
            ],
        }
    )
    assert runtime.governor.state == GovernorState.EMERGENCY
    assert exchange.placed and exchange.placed[-1]["reduceOnly"] is True
    assert runtime.emergency.targets[SYMBOL] == 0
    await runtime.store.close()


@pytest.mark.asyncio
async def test_shutdown_cancels_pending_entry(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    runtime.running = True
    await runtime.stop()
    assert request.client_order_id in exchange.cancelled
    assert not runtime.running
    await runtime.store.close()


@pytest.mark.asyncio
async def test_shutdown_refuses_unconfirmed_cancel(tmp_path):
    runtime, exchange, _ = await runtime_with_entry(tmp_path)
    exchange.query_error = True
    runtime.running = True
    runtime.settings.entry_cancel_confirm_seconds = 0.01
    with pytest.raises(RuntimeError, match="unconfirmed"):
        await runtime.stop()
    assert runtime.running
    await runtime.store.close()


@pytest.mark.asyncio
async def test_shutdown_preserves_open_position_stop(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    await runtime.order_manager.transition(request.client_order_id, OrderState.FILLED, filled="1")
    exchange.pending = []
    exchange.position = Decimal(1)
    exchange.algos = [
        {
            "algoClOrdId": runtime.order_manager.orders[request.client_order_id][
                "protective_algo_id"
            ],
            "instId": SYMBOL,
            "side": "sell",
            "sz": "1",
            "slTriggerPx": "49500",
            "state": "live",
            "failCode": "",
            "reduceOnly": "true",
        }
    ]
    runtime.running = True
    await runtime.stop()
    assert exchange.algos and not exchange.cancelled
    await runtime.store.close()


async def invalid_stop_runtime(tmp_path, modification):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    await runtime.order_manager.transition(request.client_order_id, OrderState.FILLED, filled="1")
    exchange.pending = []
    exchange.position = Decimal(1)
    exchange.algo_error = True
    algo = {
        "algoClOrdId": runtime.order_manager.orders[request.client_order_id]["protective_algo_id"],
        "instId": SYMBOL,
        "side": "sell",
        "sz": "1",
        "slTriggerPx": "49500",
        "state": "live",
        "failCode": "",
        "reduceOnly": "true",
    }
    exchange.algos = [] if modification is None else [algo | modification]
    return runtime, exchange


async def assert_invalid_protection(tmp_path, modification):
    runtime, exchange = await invalid_stop_runtime(tmp_path, modification)
    assert not await runtime._verify_protection(SYMBOL, Decimal(1), exchange.algos)
    assert runtime.governor.state == GovernorState.EMERGENCY
    assert exchange.placed and exchange.placed[0]["reduceOnly"] is True
    assert exchange.placed[0]["sz"] == "1"
    await runtime.store.close()


@pytest.mark.asyncio
async def test_protective_stop_missing_enters_emergency(tmp_path):
    await assert_invalid_protection(tmp_path, None)


@pytest.mark.asyncio
async def test_protective_stop_wrong_size_enters_emergency(tmp_path):
    await assert_invalid_protection(tmp_path, {"sz": "0.5"})


@pytest.mark.asyncio
async def test_protective_stop_wrong_symbol_enters_emergency(tmp_path):
    await assert_invalid_protection(tmp_path, {"instId": "ETH-USDT-SWAP"})


@pytest.mark.asyncio
async def test_protective_stop_wrong_side_enters_emergency(tmp_path):
    await assert_invalid_protection(tmp_path, {"side": "buy"})


@pytest.mark.asyncio
async def test_protective_algo_failcode_enters_emergency(tmp_path):
    await assert_invalid_protection(tmp_path, {"state": "order_failed", "failCode": "51008"})


@pytest.mark.asyncio
async def test_protective_replacement_confirms_before_old_cancel(tmp_path):
    runtime, exchange = await invalid_stop_runtime(tmp_path, {"sz": "0.5"})
    exchange.algo_error = False
    assert await runtime._verify_protection(SYMBOL, Decimal(1), exchange.algos)
    assert len(exchange.algos) == 1
    assert exchange.algos[0]["sz"] == "1"
    assert (
        runtime.order_manager.orders[next(iter(runtime.order_manager.orders))]["protective_algo_id"]
        == exchange.algos[0]["algoClOrdId"]
    )
    await runtime.store.close()


@pytest.mark.asyncio
async def test_orders_algo_websocket_updates_state(tmp_path):
    runtime = TradingRuntime(settings(tmp_path))
    await runtime.store.initialize()
    event = {
        "algoClOrdId": "algo-1",
        "algoId": "123",
        "instId": SYMBOL,
        "side": "sell",
        "sz": "1",
        "slTriggerPx": "49500",
        "state": "order_failed",
        "failCode": "51008",
        "failReason": "insufficient",
    }
    await runtime._on_private({"arg": {"channel": "orders-algo"}, "data": [event]})
    assert runtime.algo_manager.algos["algo-1"]["failCode"] == "51008"
    await runtime.store.close()


@pytest.mark.asyncio
async def test_startup_foreign_entry_halts_without_canceling_it(tmp_path):
    runtime = TradingRuntime(settings(tmp_path))
    await runtime.store.initialize()
    exchange = Exchange()
    exchange.pending = [
        {"instId": SYMBOL, "clOrdId": "foreign", "ordId": "outside", "reduceOnly": "false"}
    ]
    runtime.client = exchange
    await runtime.reconcile()
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.governor.reason == "foreign risk-increasing pending order"
    assert exchange.cancelled == []
    await runtime.store.close()


@pytest.mark.asyncio
async def test_startup_owned_stale_entry_cancels_and_reconciles(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    await runtime.reconcile()
    assert request.client_order_id in exchange.cancelled
    assert runtime.order_manager.orders[request.client_order_id]["state"] == "CANCELLED"
    assert runtime.reconciliation_healthy
    await runtime.store.close()


async def emergency_parts(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/emergency.db")
    await store.initialize()
    exchange = Exchange()
    exchange.position = Decimal(1)
    manager = OrderManager(store)
    engine = ExecutionEngine(exchange, manager)
    governor = RiskGovernor()
    governor.halt("emergency", emergency=True)
    controller = EmergencyController(
        exchange,
        engine,
        manager,
        store,
        {SYMBOL: instrument()},
        RiskEngine(settings(tmp_path), governor),
    )
    await controller.target(SYMBOL)
    return store, exchange, manager, controller


@pytest.mark.asyncio
async def test_emergency_reduce_timeout_reconciles(tmp_path):
    store, exchange, manager, controller = await emergency_parts(tmp_path)
    exchange.place_error = exchange.query_error = True
    assert not await controller.step(SYMBOL)
    assert controller.targets[SYMBOL] == 0
    assert manager.orders[controller.inflight[SYMBOL]]["state"] == "UNKNOWN"
    exchange.place_error = exchange.query_error = False
    exchange.order_state[controller.inflight[SYMBOL]] = {"state": "filled", "accFillSz": "1"}
    exchange.position = Decimal(0)
    assert await controller.step(SYMBOL)
    assert SYMBOL not in controller.targets
    await store.close()


@pytest.mark.asyncio
async def test_emergency_reduce_query_timeout(tmp_path):
    store, exchange, manager, controller = await emergency_parts(tmp_path)
    await controller.step(SYMBOL)
    exchange.query_error = True
    await controller.step(SYMBOL)
    assert len(exchange.placed) == 1
    assert controller.targets[SYMBOL] == 0
    await store.close()


@pytest.mark.asyncio
async def test_emergency_reduce_rejected(tmp_path):
    store, exchange, manager, controller = await emergency_parts(tmp_path)

    async def reject(body):
        exchange.placed.append(body)
        raise OkxOrderRejected("rejected")

    exchange.place_order = reject
    await controller.step(SYMBOL)
    assert manager.orders[controller.inflight[SYMBOL]]["state"] == "REJECTED"
    exchange.place_order = Exchange.place_order.__get__(exchange)
    await controller.step(SYMBOL)
    await controller.step(SYMBOL)
    assert len(exchange.placed) == 2
    assert all(order["reduceOnly"] is True for order in exchange.placed)
    await store.close()


@pytest.mark.asyncio
async def test_okx_batch_error_classifies_order_rejection(tmp_path):
    async def handler(request):
        return httpx.Response(
            200, json={"code": "1", "msg": "", "data": [{"sCode": "51008", "sMsg": "rejected"}]}
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://openapi.okx.com") as client:
        okx = OkxRestClient(settings(tmp_path), client)
        with pytest.raises(OkxOrderRejected):
            await okx.place_order({"instId": SYMBOL})


@pytest.mark.asyncio
async def test_emergency_reduce_partial_fill_continues(tmp_path):
    store, exchange, manager, controller = await emergency_parts(tmp_path)
    await controller.step(SYMBOL)
    first = controller.inflight[SYMBOL]
    exchange.order_state[first] = {"state": "partially_filled", "accFillSz": "0.4"}
    exchange.position = Decimal("0.6")
    await controller.step(SYMBOL)
    assert first in exchange.cancelled
    await controller.step(SYMBOL)
    await controller.step(SYMBOL)
    assert len(exchange.placed) == 2
    assert exchange.placed[-1]["sz"] == "0.6"
    exchange.position = Decimal(0)
    assert await controller.step(SYMBOL)
    await store.close()


@pytest.mark.asyncio
async def test_emergency_restart_recovers_target_position(tmp_path):
    store, exchange, manager, controller = await emergency_parts(tmp_path)
    await controller.step(SYMBOL)
    restored_manager = OrderManager(store)
    await restored_manager.restore()
    governor = RiskGovernor()
    governor.halt("restart", emergency=True)
    recovered = EmergencyController(
        exchange,
        ExecutionEngine(exchange, restored_manager),
        restored_manager,
        store,
        {SYMBOL: instrument()},
        RiskEngine(settings(tmp_path), governor),
    )
    await recovered.restore()
    assert recovered.targets[SYMBOL] == 0
    assert not await recovered.step(SYMBOL)
    assert len(exchange.placed) == 1
    exchange.position = Decimal(0)
    assert await recovered.step(SYMBOL)
    await store.close()


async def assert_global_limit_cancels(tmp_path, field, value, reason):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    runtime.portfolio = PortfolioState(
        equity=Decimal("10000"),
        available_balance=Decimal("10000"),
        synchronized=True,
        **{field: value},
    )
    await runtime._check_portfolio_limits([])
    assert runtime.governor.reason == reason
    assert request.client_order_id in exchange.cancelled
    await runtime.store.close()


@pytest.mark.asyncio
async def test_daily_loss_cancels_existing_pending_entry(tmp_path):
    await assert_global_limit_cancels(tmp_path, "daily_pnl", Decimal("-200"), "daily loss limit")


@pytest.mark.asyncio
async def test_weekly_drawdown_cancels_existing_pending_entry(tmp_path):
    await assert_global_limit_cancels(
        tmp_path, "weekly_drawdown", Decimal("0.05"), "weekly drawdown limit"
    )


@pytest.mark.asyncio
async def test_margin_usage_cancels_existing_pending_entry(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    runtime.portfolio = PortfolioState(
        equity=Decimal("10000"),
        available_balance=Decimal("7500"),
        margin_used=Decimal("2500"),
        synchronized=True,
    )
    await runtime._check_portfolio_limits([])
    assert runtime.governor.reason == "margin usage limit"
    assert request.client_order_id in exchange.cancelled
    await runtime.store.close()


@pytest.mark.asyncio
async def test_low_margin_ratio_enters_emergency(tmp_path):
    runtime, exchange, request = await runtime_with_entry(tmp_path)
    exchange.position = Decimal(1)
    await runtime._check_portfolio_limits([{"instId": SYMBOL, "pos": "1", "mgnRatio": "1"}])
    assert runtime.governor.state == GovernorState.EMERGENCY
    assert request.client_order_id in exchange.cancelled
    assert exchange.placed and exchange.placed[-1]["reduceOnly"] is True
    await runtime.store.close()


@pytest.mark.asyncio
async def test_missing_margin_ratio_fails_closed(tmp_path):
    runtime, exchange, _ = await runtime_with_entry(tmp_path)
    exchange.position = Decimal(1)
    await runtime._check_portfolio_limits([{"instId": SYMBOL, "pos": "1"}])
    assert runtime.governor.state == GovernorState.EMERGENCY
    await runtime.store.close()


@pytest.mark.asyncio
async def test_backtest_and_live_decision_pipeline_parity(tmp_path):
    runtime = TradingRuntime(settings(tmp_path))
    backtest = EventDrivenBacktester(settings(tmp_path), instrument())
    assert isinstance(runtime.decision_pipeline, DecisionPipeline)
    assert isinstance(backtest.decision_pipeline, DecisionPipeline)

    class ForcedBreakout:
        async def generate_signal(self, state, portfolio, regime):
            return Signal(
                symbol=state.symbol,
                strategy="breakout",
                side=Side.LONG,
                confidence=0.9,
                entry_reference=state.close,
                stop_price=Decimal("99"),
                expected_rr=2,
                regime=Regime.RANGE,
                timestamp=state.timestamp,
                expires_at=state.timestamp + timedelta(minutes=20),
            )

    runtime.decision_pipeline = DecisionPipeline(runtime.settings, (ForcedBreakout(),))
    backtest.decision_pipeline = DecisionPipeline(backtest.settings, (ForcedBreakout(),))
    bars = {"15m": candles(201), **confirmation_context()}
    state = PortfolioState(
        equity=Decimal("10000"), available_balance=Decimal("10000"), synchronized=True
    )
    live = await runtime.decision_pipeline.decide(bars, state)
    offline = await backtest.decision_pipeline.decide(bars, state)
    assert [signal.strategy for signal in live[0]] == [signal.strategy for signal in offline[0]]
    assert [signal.strategy for signal in live[0]] == ["breakout"]
    assert live[1] is not None and offline[1] is not None
    assert live[1].direction == offline[1].direction == Side.LONG
    await runtime.client.close()
    await runtime.store.close()
