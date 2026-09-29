from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

from app.models import Candle, MarketStateVector, OrderBook, Trade


def ema(values: Sequence[float], period: int) -> float | None:
    if len(values) < period:
        return None
    current = sum(values[:period]) / period
    factor = 2 / (period + 1)
    for value in values[period:]:
        current = value * factor + current * (1 - factor)
    return current


def standard_deviation(values: Sequence[float]) -> float:
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))


def adx(
    highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int = 14
) -> float | None:
    if len(closes) < period * 2 + 1:
        return None
    true_ranges = []
    plus_dm = []
    minus_dm = []
    for index in range(1, len(closes)):
        up = highs[index] - highs[index - 1]
        down = lows[index - 1] - lows[index]
        plus_dm.append(up if up > down and up > 0 else 0)
        minus_dm.append(down if down > up and down > 0 else 0)
        true_ranges.append(
            max(
                highs[index] - lows[index],
                abs(highs[index] - closes[index - 1]),
                abs(lows[index] - closes[index - 1]),
            )
        )
    dx = []
    for end in range(period, len(true_ranges) + 1):
        tr = sum(true_ranges[end - period : end])
        if tr <= 0:
            dx.append(0.0)
            continue
        plus = sum(plus_dm[end - period : end]) / tr
        minus = sum(minus_dm[end - period : end]) / tr
        dx.append(100 * abs(plus - minus) / (plus + minus) if plus + minus else 0.0)
    return sum(dx[-period:]) / period


def compute_features(
    candles: Sequence[Candle],
    book: OrderBook | None = None,
    derivatives: Mapping[str, float] | None = None,
    trades: Sequence[Trade] = (),
) -> MarketStateVector | None:
    if len(candles) < 20 or not candles[-1].confirmed:
        return None
    closes = [float(c.close) for c in candles]
    highs = [float(c.high) for c in candles]
    lows = [float(c.low) for c in candles]
    volumes = [float(c.volume) for c in candles]
    e20, e50, e200 = (ema(closes, period) for period in (20, 50, 200))
    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = sum(max(change, 0) for change in changes[-14:]) / 14
    losses = sum(max(-change, 0) for change in changes[-14:]) / 14
    rsi = 100 if losses == 0 else 100 - 100 / (1 + gains / losses)
    true_ranges = [
        max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        for i in range(1, len(candles))
    ]
    atr = sum(true_ranges[-14:]) / 14
    log_returns = [
        math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0
    ]
    vol = standard_deviation(log_returns[-20:]) if len(log_returns) >= 20 else 0.0
    volume_base = sum(volumes[-21:-1]) / min(20, len(volumes) - 1)
    imbalance = spread = None
    if book and book.bids and book.asks:
        bid_qty = sum(float(level[1]) for level in book.bids)
        ask_qty = sum(float(level[1]) for level in book.asks)
        imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty) if bid_qty + ask_qty else 0
        spread = float((book.asks[0][0] - book.bids[0][0]) / book.asks[0][0])
    previous_ema = ema(closes[:-5], 20) if len(closes) >= 25 else None
    slope = (
        (e20 - previous_ema) / closes[-1] if e20 is not None and previous_ema is not None else None
    )
    bb_mean = sum(closes[-20:]) / 20
    bb_std = standard_deviation(closes[-20:])
    vwap_denominator = sum(volumes[-20:])
    vwap = (
        sum(
            ((highs[i] + lows[i] + closes[i]) / 3) * volumes[i]
            for i in range(len(candles) - 20, len(candles))
        )
        / vwap_denominator
        if vwap_denominator
        else bb_mean
    )
    contraction = None
    if len(true_ranges) >= 20:
        previous_atr = sum(true_ranges[-20:-6]) / 14
        contraction = (sum(true_ranges[-6:-1]) / 5) / previous_atr if previous_atr else None
    derivatives = derivatives or {}
    buy_volume = sum(float(t.size) for t in trades if t.side == "buy")
    sell_volume = sum(float(t.size) for t in trades if t.side == "sell")
    trade_imbalance = (
        (buy_volume - sell_volume) / (buy_volume + sell_volume)
        if buy_volume + sell_volume
        else None
    )
    return MarketStateVector(
        symbol=candles[-1].symbol,
        timestamp=candles[-1].timestamp,
        timeframe=candles[-1].timeframe,
        close=candles[-1].close,
        returns=closes[-1] / closes[-2] - 1,
        log_returns=math.log(closes[-1] / closes[-2]),
        high_low_range=(highs[-1] - lows[-1]) / closes[-1],
        ema20=e20,
        ema50=e50,
        ema200=e200,
        ema_slope=slope,
        rsi=rsi,
        roc=closes[-1] / closes[-15] - 1,
        atr=atr,
        adx=adx(highs, lows, closes),
        realized_vol=vol,
        volume_ma=volume_base,
        volume_ratio=volumes[-1] / volume_base if volume_base else 0,
        donchian_upper=max(highs[-21:-1]) if len(highs) >= 21 else None,
        donchian_lower=min(lows[-21:-1]) if len(lows) >= 21 else None,
        bollinger_upper=bb_mean + 2 * bb_std,
        bollinger_lower=bb_mean - 2 * bb_std,
        bollinger_bandwidth=4 * bb_std / bb_mean if bb_mean else None,
        price_zscore=(closes[-1] - bb_mean) / bb_std if bb_std else 0,
        vwap_deviation=(closes[-1] / vwap - 1) if vwap else 0,
        atr_contraction_ratio=contraction,
        spread=spread,
        orderbook_imbalance=imbalance,
        trade_imbalance=trade_imbalance,
        funding_rate=derivatives.get("funding"),
        funding_zscore=derivatives.get("funding_zscore"),
        open_interest=derivatives.get("oi"),
        oi_change=derivatives.get("oi_change"),
        mark_index_premium=derivatives.get("premium"),
        trend_score=0 if e50 is None else max(-1, min(1, (closes[-1] / e50 - 1) * 50)),
        momentum_score=(rsi - 50) / 50,
        volatility_score=min(1, vol * 100),
        volume_score=min(1, volumes[-1] / volume_base / 2) if volume_base else 0,
    )
