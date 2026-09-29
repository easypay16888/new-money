from __future__ import annotations

from collections.abc import Mapping, Sequence

from app.config import Settings
from app.features import compute_features
from app.meta import fuse
from app.models import Candle, MarketStateVector, PortfolioState, Signal, TradeIntent
from app.regime import classify
from app.strategies import BaseStrategy


class DecisionPipeline:
    def __init__(self, settings: Settings, strategies: Sequence[BaseStrategy]) -> None:
        self.settings, self.strategies = settings, strategies

    async def decide(
        self,
        candles: Mapping[str, Sequence[Candle]],
        portfolio: PortfolioState,
        *,
        main_state: MarketStateVector | None = None,
    ) -> tuple[list[Signal], TradeIntent | None]:
        main = main_state or compute_features(candles.get("15m", ()))
        if main is None:
            return [], None
        regime = classify(main, self.settings)
        higher = []
        for timeframe in ("1H", "4H"):
            state = compute_features(candles.get(timeframe, ()))
            higher.append(classify(state, self.settings).regime if state else None)
        entry = compute_features(candles.get("5m", ()))
        signals = [
            signal
            for strategy in self.strategies
            if (signal := await strategy.generate_signal(main, portfolio, regime))
            and (signal.strategy != "trend" or all(item == regime.regime for item in higher))
            and (
                signal.strategy not in {"trend", "breakout"}
                or (
                    entry is not None
                    and entry.ema20 is not None
                    and entry.ema50 is not None
                    and (
                        (signal.side.value == "LONG" and entry.ema20 >= entry.ema50)
                        or (signal.side.value == "SHORT" and entry.ema20 <= entry.ema50)
                    )
                )
            )
        ]
        intent = fuse(
            signals,
            portfolio,
            self.settings.min_signal_confidence,
            as_of=main.timestamp,
            weights=self.settings.strategy_weights,
        )
        return signals, intent
