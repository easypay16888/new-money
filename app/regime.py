from app.config import Settings
from app.models import MarketRegime, MarketStateVector, Regime


def classify(state: MarketStateVector, settings: Settings | None = None) -> MarketRegime:
    settings = settings or Settings(_env_file=None)
    if state.ema50 is None or state.ema200 is None or state.atr is None:
        regime, confidence = Regime.UNKNOWN, 0.0
    elif (
        state.volatility_score > settings.regime_panic_volatility
        and state.volume_score > settings.regime_panic_volume
    ):
        regime, confidence = Regime.PANIC, 0.8
    elif state.volatility_score > settings.regime_high_volatility:
        regime, confidence = Regime.HIGH_VOL, 0.7
    elif (
        state.ema20
        and state.ema20 > state.ema50 > state.ema200
        and (state.ema_slope or 0) > 0
        and (state.adx or 0) >= settings.trend_min_adx
    ):
        regime, confidence = Regime.TREND_UP, min(0.95, 0.6 + abs(state.trend_score) / 3)
    elif (
        state.ema20
        and state.ema20 < state.ema50 < state.ema200
        and (state.ema_slope or 0) < 0
        and (state.adx or 0) >= settings.trend_min_adx
    ):
        regime, confidence = Regime.TREND_DOWN, min(0.95, 0.6 + abs(state.trend_score) / 3)
    elif state.volatility_score < settings.regime_low_volatility:
        regime, confidence = Regime.LOW_VOL, 0.65
    else:
        regime, confidence = Regime.RANGE, 0.65
    return MarketRegime(
        symbol=state.symbol,
        timestamp=state.timestamp,
        regime=regime,
        confidence=confidence,
        reason={
            "trend_score": state.trend_score,
            "volatility_score": state.volatility_score,
            "adx": state.adx,
        },
    )
