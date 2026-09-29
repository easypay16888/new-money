from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

import app.backtest as backtest_module
from app.backtest import EventDrivenBacktester, run_walk_forward, walk_forward_indices
from app.config import Settings
from app.execution import ExecutionEngine
from app.models import Candle, GovernorState, OrderState, PortfolioState, Regime, Side, Signal
from app.runtime import TradingRuntime
from tests.test_core import evaluate, instrument, ready_risk


def candles(count: int) -> list[Candle]:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    rows = []
    for index in range(count):
        price = Decimal("100")
        low = Decimal("99.9")
        if index == 200:
            price = Decimal("101")
            low = Decimal("98")
        rows.append(
            Candle(
                symbol="BTC-USDT-SWAP",
                timeframe="15m",
                timestamp=start + timedelta(minutes=15 * index),
                open=price,
                high=price + Decimal("0.1"),
                low=low,
                close=price,
                volume=Decimal("10"),
                confirmed=True,
            )
        )
    return rows


@pytest.mark.asyncio
async def test_backtest_executes_after_signal_and_charges_costs(monkeypatch):
    class ForcedStrategy:
        async def generate_signal(self, state, portfolio, regime):
            if state.ema200 is None:
                return None
            return Signal(
                symbol=state.symbol,
                strategy="trend",
                side=Side.LONG,
                confidence=0.8,
                entry_reference=state.close,
                stop_price=Decimal("99"),
                take_profit_reference=Decimal("105"),
                expected_rr=2,
                regime=Regime.TREND_UP,
                timestamp=state.timestamp,
                expires_at=state.timestamp + timedelta(minutes=20),
            )

    monkeypatch.setattr(backtest_module, "build_strategies", lambda settings: (ForcedStrategy(),))
    engine = EventDrivenBacktester(Settings(_env_file=None), instrument())
    before = await engine.run(candles(200), Decimal("10000"))
    after = await engine.run(candles(201), Decimal("10000"))
    assert before.trades == []
    assert len(after.trades) == 1
    trade = after.trades[0]
    assert trade.exit < trade.entry
    assert trade.fees > 0 and trade.slippage_cost > 0
    assert after.metrics()["net_pnl"] < 0


@pytest.mark.asyncio
async def test_backtest_gap_through_stop_fills_at_worse_open(monkeypatch):
    class ForcedStrategy:
        async def generate_signal(self, state, portfolio, regime):
            if state.ema200 is None:
                return None
            return Signal(
                symbol=state.symbol,
                strategy="trend",
                side=Side.LONG,
                confidence=0.8,
                entry_reference=state.close,
                stop_price=Decimal("99"),
                take_profit_reference=Decimal("105"),
                expected_rr=2,
                regime=Regime.TREND_UP,
                timestamp=state.timestamp,
                expires_at=state.timestamp + timedelta(minutes=20),
            )

    monkeypatch.setattr(backtest_module, "build_strategies", lambda settings: (ForcedStrategy(),))
    rows = candles(202)
    rows[-2] = rows[-2].model_copy(update={"low": Decimal("100")})
    rows[-1] = rows[-1].model_copy(
        update={
            "open": Decimal("95"),
            "high": Decimal("96"),
            "low": Decimal("94"),
            "close": Decimal("95"),
        }
    )
    result = await EventDrivenBacktester(Settings(_env_file=None), instrument()).run(
        rows, Decimal("10000")
    )
    assert result.trades[0].exit < Decimal("95")


def test_walk_forward_windows_do_not_overlap_oos_with_training():
    windows = list(walk_forward_indices(100, 40, 10, 10))
    assert windows[0] == (slice(0, 40), slice(40, 50), slice(50, 60))
    assert windows[1] == (slice(10, 50), slice(50, 60), slice(60, 70))
    assert windows[0][2].stop == windows[1][2].start


@pytest.mark.asyncio
async def test_walk_forward_runs_separate_oos_window():
    folds = await run_walk_forward(
        candles(240), Settings(_env_file=None), instrument(), Decimal("10000"), 200, 20, 20
    )
    assert len(folds) == 1
    assert folds[0]["train"] == [0, 200]
    assert folds[0]["validation"] == [200, 220]
    assert folds[0]["out_of_sample"] == [220, 240]
    assert folds[0]["out_of_sample_metrics"]["trades"] >= 0


@pytest.mark.asyncio
async def test_reconciliation_halts_on_unexpected_position(tmp_path):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/runtime.db")
    runtime = TradingRuntime(settings)
    await runtime.store.initialize()
    runtime.portfolio = PortfolioState(
        equity=Decimal("10000"), available_balance=Decimal("10000"), synchronized=True
    )

    class FakeClient:
        async def account(self):
            return [{"totalEq": "10000", "details": [{"ccy": "USDT", "availEq": "10000"}]}]

        async def positions(self):
            return [{"instId": "BTC-USDT-SWAP", "pos": "1", "mgnMode": "isolated", "margin": "100"}]

        async def pending_orders(self):
            return []

        async def pending_algos(self):
            return []

        async def account_config(self):
            return [{"posMode": "net_mode"}]

    runtime.client = FakeClient()
    await runtime.reconcile()
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.governor.reason == "position mismatch"
    await runtime.store.close()


@pytest.mark.asyncio
async def test_reconciliation_halts_on_unexplained_position_disappearance(tmp_path):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/missing.db")
    runtime = TradingRuntime(settings)
    await runtime.store.initialize()
    runtime.portfolio = PortfolioState(
        equity=Decimal("10000"),
        available_balance=Decimal("10000"),
        positions={"BTC-USDT-SWAP": Decimal("1")},
        synchronized=True,
    )

    class FakeClient:
        async def account(self):
            return [{"totalEq": "10000", "details": [{"ccy": "USDT", "availEq": "10000"}]}]

        async def positions(self):
            return []

        async def pending_orders(self):
            return []

        async def pending_algos(self):
            return []

        async def account_config(self):
            return [{"posMode": "net_mode"}]

    runtime.client = FakeClient()
    await runtime.reconcile()
    assert runtime.governor.reason == "position mismatch"
    await runtime.store.close()


@pytest.mark.asyncio
async def test_restart_recovers_owned_position_with_protective_algo(tmp_path):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/restore.db")
    runtime = TradingRuntime(settings)
    await runtime.store.initialize()
    runtime.instruments["BTC-USDT-SWAP"] = instrument()
    entry = ExecutionEngine.from_risk(evaluate(ready_risk()), instrument())
    await runtime.order_manager.create(entry)
    await runtime.order_manager.transition(
        entry.client_order_id, OrderState.FILLED, filled="1", order_id="entry-1"
    )
    protective_id = runtime.order_manager.orders[entry.client_order_id]["protective_algo_id"]
    await runtime.order_manager.restore()

    class FakeClient:
        async def account(self):
            return [{"totalEq": "10000", "details": [{"ccy": "USDT", "availEq": "9500"}]}]

        async def positions(self):
            return [
                {
                    "instId": "BTC-USDT-SWAP",
                    "pos": "1",
                    "lever": "1",
                    "mgnMode": "isolated",
                    "margin": "500",
                    "mgnRatio": "5",
                    "notionalUsd": "500",
                    "markPx": "50000",
                }
            ]

        async def pending_orders(self):
            return []

        async def pending_algos(self):
            return [{"algoClOrdId": protective_id}]

        async def account_config(self):
            return [{"posMode": "net_mode"}]

    runtime.client = FakeClient()
    await runtime.reconcile()
    assert runtime.portfolio.synchronized
    assert runtime.portfolio.positions == {"BTC-USDT-SWAP": Decimal("1")}
    assert runtime.portfolio.open_risk == Decimal("5.00")
    await runtime.store.close()
