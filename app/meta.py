from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from app.models import PortfolioState, Regime, Side, Signal, TradeIntent, utcnow

ALLOWED: dict[str, set[Regime]] = {
    "trend": {Regime.TREND_UP, Regime.TREND_DOWN},
    "breakout": {Regime.TREND_UP, Regime.TREND_DOWN, Regime.RANGE, Regime.LOW_VOL},
    "mean_reversion": {Regime.RANGE, Regime.LOW_VOL},
}


def fuse(
    signals: list[Signal],
    portfolio: PortfolioState,
    min_confidence: float,
    *,
    as_of: datetime | None = None,
    weights: dict[str, float] | None = None,
) -> TradeIntent | None:
    as_of = as_of or utcnow()
    weights = weights or {}
    eligible = [
        s
        for s in signals
        if s.confidence >= min_confidence
        and s.expires_at > as_of
        and s.regime in ALLOWED.get(s.strategy, set())
        and s.side != Side.FLAT
        and not portfolio.positions.get(s.symbol)
    ]
    groups: dict[tuple[str, Side], list[Signal]] = defaultdict(list)
    for signal in eligible:
        groups[(signal.symbol, signal.side)].append(signal)
    if not groups:
        return None
    ranked = sorted(
        groups.items(),
        key=lambda item: sum(s.confidence * weights.get(s.strategy, 1.0) for s in item[1]),
        reverse=True,
    )
    (symbol, direction), winners = ranked[0]
    if any(key[0] == symbol and key[1] != direction for key in groups):
        return None
    strongest = max(winners, key=lambda s: s.confidence)
    return TradeIntent(
        symbol=symbol,
        direction=direction,
        confidence=sum(s.confidence for s in winners) / len(winners),
        signal_ids=tuple(s.id for s in winners),
        strategies=tuple(s.strategy for s in winners),
        entry_reference=strongest.entry_reference,
        stop_price=strongest.stop_price,
        take_profit_reference=strongest.take_profit_reference,
    )
