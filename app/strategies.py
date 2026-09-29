from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from decimal import Decimal

from app.config import Settings
from app.models import MarketRegime, MarketStateVector, PortfolioState, Regime, Side, Signal


def make_signal(
    name: str, state: MarketStateVector, regime: MarketRegime, side: Side, confidence: float
) -> Signal | None:
    if state.atr is None or state.atr <= 0:
        return None
    distance = Decimal(str(state.atr * 1.5))
    entry = state.close
    stop = entry - distance if side == Side.LONG else entry + distance
    target = entry + distance * 2 if side == Side.LONG else entry - distance * 2
    return Signal(
        symbol=state.symbol,
        strategy=name,
        side=side,
        confidence=confidence,
        entry_reference=entry,
        stop_price=stop,
        take_profit_reference=target,
        expected_rr=2,
        regime=regime.regime,
        features_snapshot=state.model_dump(mode="json"),
        reason={"regime": regime.regime.value},
        timestamp=state.timestamp,
        expires_at=state.timestamp + timedelta(minutes=20),
    )


class BaseStrategy(ABC):
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings(_env_file=None)

    @abstractmethod
    async def generate_signal(
        self, market_state: MarketStateVector, portfolio_state: PortfolioState, regime: MarketRegime
    ) -> Signal | None: ...


class TrendStrategy(BaseStrategy):
    async def generate_signal(
        self, market_state: MarketStateVector, portfolio_state: PortfolioState, regime: MarketRegime
    ) -> Signal | None:
        if (
            (market_state.adx or 0) < self.settings.trend_min_adx
            or (market_state.volume_ratio or 0) < self.settings.trend_min_volume_ratio
            or (market_state.oi_change is not None and market_state.oi_change < 0)
        ):
            return None
        if regime.regime == Regime.TREND_UP and (market_state.rsi or 0) > 52:
            return make_signal("trend", market_state, regime, Side.LONG, regime.confidence)
        if regime.regime == Regime.TREND_DOWN and (market_state.rsi or 100) < 48:
            return make_signal("trend", market_state, regime, Side.SHORT, regime.confidence)
        return None


class BreakoutStrategy(BaseStrategy):
    async def generate_signal(
        self, market_state: MarketStateVector, portfolio_state: PortfolioState, regime: MarketRegime
    ) -> Signal | None:
        if (
            regime.regime in {Regime.PANIC, Regime.UNKNOWN, Regime.HIGH_VOL}
            or (market_state.volume_ratio or 0) < self.settings.breakout_min_volume_ratio
            or (market_state.atr_contraction_ratio or 99)
            > self.settings.breakout_max_atr_contraction
            or (market_state.bollinger_bandwidth or 99) > self.settings.breakout_max_bandwidth
            or (market_state.oi_change is not None and market_state.oi_change < 0)
        ):
            return None
        if (
            market_state.donchian_upper is not None
            and float(market_state.close) > market_state.donchian_upper
            and (market_state.rsi or 0) > 55
        ):
            return make_signal("breakout", market_state, regime, Side.LONG, 0.7)
        if (
            market_state.donchian_lower is not None
            and float(market_state.close) < market_state.donchian_lower
            and (market_state.rsi or 100) < 45
        ):
            return make_signal("breakout", market_state, regime, Side.SHORT, 0.7)
        return None


class MeanReversionStrategy(BaseStrategy):
    async def generate_signal(
        self, market_state: MarketStateVector, portfolio_state: PortfolioState, regime: MarketRegime
    ) -> Signal | None:
        if regime.regime not in {Regime.RANGE, Regime.LOW_VOL}:
            return None
        if (
            market_state.bollinger_lower is not None
            and float(market_state.close) < market_state.bollinger_lower
            and (market_state.rsi or 50) < self.settings.mean_reversion_rsi_low
            and (market_state.price_zscore or 0) < -self.settings.mean_reversion_min_zscore
            and (market_state.vwap_deviation or 0) < 0
        ):
            return make_signal("mean_reversion", market_state, regime, Side.LONG, 0.68)
        if (
            market_state.bollinger_upper is not None
            and float(market_state.close) > market_state.bollinger_upper
            and (market_state.rsi or 50) > self.settings.mean_reversion_rsi_high
            and (market_state.price_zscore or 0) > self.settings.mean_reversion_min_zscore
            and (market_state.vwap_deviation or 0) > 0
        ):
            return make_signal("mean_reversion", market_state, regime, Side.SHORT, 0.68)
        return None


def build_strategies(settings: Settings) -> tuple[BaseStrategy, ...]:
    return (TrendStrategy(settings), BreakoutStrategy(settings), MeanReversionStrategy(settings))
