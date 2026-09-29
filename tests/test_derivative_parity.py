from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.backtest import EventDrivenBacktester
from app.config import Settings
from app.derivatives import DerivativeHistory
from app.market import MarketDataEngine
from app.models import Candle, DerivativeObservation, MarketStateVector, PortfolioState
from app.runtime import TradingRuntime
from app.storage import Store
from tests.test_core import instrument
from tests.test_runtime_backtest import candles


def observation(kind, value, timestamp):
    return DerivativeObservation(
        symbol="BTC-USDT-SWAP", timestamp=timestamp, kind=kind, value=Decimal(value)
    )


def rising_context() -> dict[str, list[Candle]]:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    result = {}
    for timeframe, minutes in (("1H", 60), ("4H", 240), ("5m", 5)):
        series = []
        for index in range(240):
            price = Decimal("90") + Decimal(index) / 10
            series.append(
                Candle(
                    symbol="BTC-USDT-SWAP",
                    timeframe=timeframe,
                    timestamp=start + timedelta(minutes=minutes * index),
                    open=price,
                    high=price + Decimal("0.2"),
                    low=price - Decimal("0.2"),
                    close=price,
                    volume=Decimal("10"),
                    confirmed=True,
                )
            )
        result[timeframe] = series
    return result


@pytest.mark.asyncio
async def test_backtest_uses_only_derivatives_known_by_each_decision(tmp_path):
    rows = candles(22)
    start = rows[0].timestamp
    future = rows[-1].timestamp + timedelta(minutes=16)
    events = [
        observation("oi", "100", start),
        observation("funding", "0.01", start),
        observation("mark", "101", start),
        observation("index", "100", start),
        observation("oi", "150", future),
        observation("funding", "0.04", future),
        observation("mark", "120", future),
    ]
    seen = []
    engine = EventDrivenBacktester(Settings(_env_file=None), instrument())

    async def capture(bars, portfolio, *, main_state=None):
        seen.append(main_state)
        return [], None

    engine.decision_pipeline.decide = capture
    await engine.run(rows, Decimal("10000"), derivatives=events)
    assert seen
    assert all(state.open_interest == 100 for state in seen)
    assert all(state.funding_rate == 0.01 for state in seen)
    assert all(state.mark_index_premium == pytest.approx(0.01) for state in seen)
    assert all(state.oi_change is None for state in seen)
    baseline = [state.model_dump() for state in seen]
    seen.clear()
    await engine.run(rows, Decimal("10000"), derivatives=events[:4])
    assert [state.model_dump() for state in seen] == baseline


def test_derivative_history_advances_at_timestamp_boundary():
    start = datetime(2025, 1, 1, tzinfo=UTC)
    history = DerivativeHistory(
        [observation("oi", "100", start), observation("oi", "125", start + timedelta(minutes=15))]
    )
    assert history.at(start + timedelta(minutes=14))["oi"] == 100
    assert history.at(start + timedelta(minutes=15))["oi_change"] == 0.25


@pytest.mark.asyncio
async def test_market_derivative_observations_are_persisted_with_timestamps(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/derivatives.db")
    await store.initialize()
    market = MarketDataEngine(store)
    start = datetime.now(UTC) - timedelta(seconds=2)
    first = str(int(start.timestamp() * 1000))
    second = str(int((start + timedelta(seconds=1)).timestamp() * 1000))
    for channel, symbol, payload in (
        ("open-interest", "BTC-USDT-SWAP", {"oi": "100", "ts": first}),
        ("funding-rate", "BTC-USDT-SWAP", {"fundingRate": "0.01", "ts": first}),
        ("mark-price", "BTC-USDT-SWAP", {"markPx": "101", "ts": first}),
        ("index-tickers", "BTC-USDT", {"idxPx": "100", "ts": first}),
        ("open-interest", "BTC-USDT-SWAP", {"oi": "125", "ts": second}),
    ):
        await market.handle({"arg": {"channel": channel, "instId": symbol}, "data": [payload]})
    rows = await store.all_for_symbol("market_derivatives", "BTC-USDT-SWAP")
    assert len(rows) == 5
    assert market.derivative_context("BTC-USDT-SWAP", start)["oi"] == 100
    latest = market.derivative_context("BTC-USDT-SWAP", start + timedelta(seconds=1))
    assert latest["oi_change"] == 0.25
    assert latest["funding"] == 0.01
    assert latest["premium"] == pytest.approx(0.01)
    await store.close()


@pytest.mark.asyncio
async def test_real_trend_and_breakout_live_backtest_parity(tmp_path):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/parity.db")
    runtime = TradingRuntime(settings)
    backtest = EventDrivenBacktester(settings, instrument())
    main = MarketStateVector(
        symbol="BTC-USDT-SWAP",
        timeframe="15m",
        timestamp=datetime(2025, 1, 1, tzinfo=UTC),
        close=Decimal("112"),
        ema20=110,
        ema50=105,
        ema200=100,
        ema_slope=0.01,
        adx=30,
        atr=1,
        rsi=70,
        volume_ratio=2,
        atr_contraction_ratio=0.5,
        bollinger_bandwidth=0.03,
        donchian_upper=111,
        oi_change=0.25,
        open_interest=125,
        funding_rate=0.01,
        mark_index_premium=0.01,
        trend_score=0.6,
        volatility_score=0.2,
    )
    portfolio = PortfolioState(
        equity=Decimal("10000"), available_balance=Decimal("10000"), synchronized=True
    )
    context = rising_context()
    live_signals, live_intent = await runtime.decision_pipeline.decide(
        context, portfolio, main_state=main
    )
    offline_signals, offline_intent = await backtest.decision_pipeline.decide(
        context, portfolio, main_state=main
    )
    assert {signal.strategy for signal in live_signals} == {"trend", "breakout"}
    assert [
        (
            signal.strategy,
            signal.side,
            signal.stop_price,
            signal.confidence,
            signal.features_snapshot,
        )
        for signal in live_signals
    ] == [
        (
            signal.strategy,
            signal.side,
            signal.stop_price,
            signal.confidence,
            signal.features_snapshot,
        )
        for signal in offline_signals
    ]
    assert live_intent is not None and offline_intent is not None
    assert live_intent.direction == offline_intent.direction
    await runtime.client.close()
    await runtime.store.close()
