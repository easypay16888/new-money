from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.features import compute_features
from app.models import Candle, MarketRegime, MarketStateVector, PortfolioState, Regime
from app.strategies import MeanReversionStrategy


def bar(index: int, high: str = "101", close: str = "100") -> Candle:
    return Candle(
        symbol="BTC-USDT-SWAP",
        timeframe="15m",
        timestamp=datetime(2025, 1, 1, tzinfo=UTC) + timedelta(minutes=index * 15),
        open=Decimal("100"),
        high=Decimal(high),
        low=Decimal("99"),
        close=Decimal(close),
        volume=Decimal("10"),
        confirmed=True,
    )


def test_donchian_uses_only_prior_bars():
    bars = [bar(index) for index in range(20)] + [bar(20, high="120", close="119")]
    state = compute_features(bars)
    assert state is not None
    assert state.donchian_upper == 101
    assert state.close == Decimal("119")
    assert state.high_low_range == pytest.approx(21 / 119)


@pytest.mark.asyncio
async def test_mean_reversion_is_disabled_in_trend_and_panic():
    state = MarketStateVector(
        symbol="BTC-USDT-SWAP",
        timeframe="15m",
        timestamp=datetime.now(UTC),
        close=Decimal("90"),
        atr=2,
        bollinger_lower=95,
        rsi=20,
        price_zscore=-2,
        vwap_deviation=-0.05,
    )
    portfolio = PortfolioState(equity=Decimal("10000"), available_balance=Decimal("10000"))
    strategy = MeanReversionStrategy()
    for regime in (Regime.TREND_UP, Regime.TREND_DOWN, Regime.PANIC):
        result = await strategy.generate_signal(
            state,
            portfolio,
            MarketRegime(
                symbol=state.symbol, timestamp=state.timestamp, regime=regime, confidence=0.9
            ),
        )
        assert result is None
    result = await strategy.generate_signal(
        state,
        portfolio,
        MarketRegime(
            symbol=state.symbol, timestamp=state.timestamp, regime=Regime.RANGE, confidence=0.7
        ),
    )
    assert result is not None
