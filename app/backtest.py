from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Any

from app.config import Settings
from app.decision import DecisionPipeline
from app.derivatives import DerivativeHistory
from app.features import compute_features
from app.meta import fuse
from app.models import Candle, DerivativeObservation, Instrument, PortfolioState, Side, Signal
from app.risk import RiskEngine, RiskGovernor
from app.strategies import build_strategies


@dataclass(frozen=True)
class ClosedTrade:
    symbol: str
    strategy: str
    regime: str
    side: Side
    entry: Decimal
    exit: Decimal
    contracts: Decimal
    net_pnl: Decimal
    fees: Decimal
    slippage_cost: Decimal
    spread_cost: Decimal
    funding_cost: Decimal
    r_multiple: Decimal


@dataclass
class SimPosition:
    signal: Signal
    contracts: Decimal
    entry: Decimal
    entry_fee: Decimal
    entry_slippage: Decimal
    entry_spread: Decimal
    funding_cost: Decimal = Decimal(0)


@dataclass
class BacktestResult:
    trades: list[ClosedTrade] = field(default_factory=list)
    equity_curve: list[Decimal] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)

    def metrics(self) -> dict[str, Any]:
        initial = Decimal(str(self.params["initial_equity"]))
        final = self.equity_curve[-1] if self.equity_curve else initial
        pnl = [float(t.net_pnl) for t in self.trades]
        wins = [x for x in pnl if x > 0]
        losses = [x for x in pnl if x < 0]
        peak = initial
        max_drawdown = Decimal(0)
        for equity in self.equity_curve:
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, (peak - equity) / peak if peak else Decimal(0))
        returns = [
            float((b - a) / a)
            for a, b in zip(self.equity_curve, self.equity_curve[1:], strict=False)
            if a
        ]
        mean = sum(returns) / len(returns) if returns else 0
        variance = sum((x - mean) ** 2 for x in returns) / len(returns) if returns else 0
        downside = sum(min(0, x) ** 2 for x in returns) / len(returns) if returns else 0
        annualized = (
            float(final / initial) ** (365 * 24 * 4 / len(self.equity_curve)) - 1
            if len(self.equity_curve) > 1 and final > 0
            else 0
        )
        by_regime: dict[str, float] = {}
        for trade in self.trades:
            by_regime[trade.regime] = by_regime.get(trade.regime, 0) + float(trade.net_pnl)
        return {
            "net_pnl": float(final - initial),
            "total_return": float(final / initial - 1),
            "annualized_return": annualized,
            "max_drawdown": float(max_drawdown),
            "sharpe": mean / math.sqrt(variance) * math.sqrt(365 * 24 * 4) if variance else 0,
            "sortino": mean / math.sqrt(downside) * math.sqrt(365 * 24 * 4) if downside else 0,
            "calmar": annualized / float(max_drawdown) if max_drawdown else 0,
            "profit_factor": sum(wins) / abs(sum(losses)) if losses else 0,
            "expectancy": sum(pnl) / len(pnl) if pnl else 0,
            "win_rate": len(wins) / len(pnl) if pnl else 0,
            "average_win": sum(wins) / len(wins) if wins else 0,
            "average_loss": sum(losses) / len(losses) if losses else 0,
            "average_r": (
                float(sum((t.r_multiple for t in self.trades), Decimal(0)) / len(pnl)) if pnl else 0
            ),
            "trades": len(pnl),
            "fees": float(sum((t.fees for t in self.trades), Decimal(0))),
            "funding_cost": float(sum((t.funding_cost for t in self.trades), Decimal(0))),
            "spread_cost": float(sum((t.spread_cost for t in self.trades), Decimal(0))),
            "slippage_cost": float(sum((t.slippage_cost for t in self.trades), Decimal(0))),
            "long_pnl": sum(float(t.net_pnl) for t in self.trades if t.side == Side.LONG),
            "short_pnl": sum(float(t.net_pnl) for t in self.trades if t.side == Side.SHORT),
            "regime_pnl": by_regime,
        }


class EventDrivenBacktester:
    """Confirmed bar decisions execute at the next bar open; stop wins an ambiguous bar."""

    def __init__(self, settings: Settings, instrument: Instrument) -> None:
        self.settings = settings
        self.instrument = instrument
        self.strategies = build_strategies(settings)
        self.decision_pipeline = DecisionPipeline(settings, self.strategies)

    async def run(
        self,
        candles: list[Candle],
        initial_equity: Decimal,
        *,
        warmup_bars: int = 0,
        context: Mapping[str, list[Candle]] | None = None,
        derivatives: Sequence[DerivativeObservation] = (),
    ) -> BacktestResult:
        if initial_equity <= 0:
            raise ValueError("initial equity must be positive")
        if any(a.timestamp >= b.timestamp for a, b in zip(candles, candles[1:], strict=False)):
            raise ValueError("candles must be strictly ascending")
        if any(not candle.confirmed for candle in candles):
            raise ValueError("backtest requires confirmed candles")
        result = BacktestResult(
            params={
                "initial_equity": str(initial_equity),
                "maker_fee": self.settings.maker_fee,
                "taker_fee": self.settings.taker_fee,
                "slippage_bps": self.settings.slippage_bps,
                "spread_bps": self.settings.backtest_spread_bps,
                "funding_rate": self.settings.backtest_funding_rate,
                "fill_ratio": self.settings.backtest_fill_ratio,
                "derivative_events": len(derivatives),
            }
        )
        equity = initial_equity
        history: list[Candle] = []
        position: SimPosition | None = None
        pending: Signal | None = None
        governor = RiskGovernor()
        governor.resume(synchronized=True, healthy=True)
        risk = RiskEngine(self.settings, governor)
        derivative_history = DerivativeHistory(
            [item for item in derivatives if item.symbol == self.instrument.symbol]
        )
        for index, candle in enumerate(candles):
            # Process orders and exits before the just-closed bar enters the feature history.
            if pending and not position:
                slip_rate = Decimal(str(self.settings.slippage_bps / 10000))
                half_spread = Decimal(str(self.settings.backtest_spread_bps / 20000))
                entry = candle.open * (
                    Decimal(1)
                    + (slip_rate + half_spread) * (1 if pending.side == Side.LONG else -1)
                )
                portfolio = PortfolioState(
                    equity=equity, available_balance=equity, synchronized=True
                )
                intent = fuse(
                    [pending],
                    portfolio,
                    self.settings.min_signal_confidence,
                    as_of=candle.timestamp,
                    weights=self.settings.strategy_weights,
                )
                if intent:
                    intent.entry_reference = entry
                    decision = risk.evaluate(
                        intent,
                        portfolio,
                        self.instrument,
                        data_fresh=True,
                        infrastructure_healthy=True,
                    )
                    if decision.approved:
                        contracts = (
                            decision.approved_contracts
                            * Decimal(str(self.settings.backtest_fill_ratio))
                            / self.instrument.lot_size
                        ).to_integral_value(rounding=ROUND_DOWN) * self.instrument.lot_size
                        if contracts >= self.instrument.min_size:
                            notional = contracts * self.instrument.contract_value * entry
                            fee = notional * Decimal(str(self.settings.taker_fee))
                            slip = (
                                contracts * self.instrument.contract_value * candle.open * slip_rate
                            )
                            spread = (
                                contracts
                                * self.instrument.contract_value
                                * candle.open
                                * half_spread
                            )
                            position = SimPosition(pending, contracts, entry, fee, slip, spread)
                pending = None
            if position:
                if candle.timestamp.hour in (0, 8, 16) and candle.timestamp.minute == 0:
                    funding_direction = (
                        Decimal(1) if position.signal.side == Side.LONG else Decimal(-1)
                    )
                    position.funding_cost += (
                        position.contracts
                        * self.instrument.contract_value
                        * candle.open
                        * Decimal(str(self.settings.backtest_funding_rate))
                        * funding_direction
                    )
                side = position.signal.side
                stop = position.signal.stop_price
                target = position.signal.take_profit_reference
                stop_hit = candle.low <= stop if side == Side.LONG else candle.high >= stop
                target_hit = bool(
                    target
                    and (candle.high >= target if side == Side.LONG else candle.low <= target)
                )
                if stop_hit or target_hit:
                    base_exit = stop if stop_hit else target
                    assert base_exit is not None
                    # A gap through a stop fills at the worse open.
                    if stop_hit:
                        base_exit = (
                            min(base_exit, candle.open)
                            if side == Side.LONG
                            else max(base_exit, candle.open)
                        )
                    exit_price = base_exit * (
                        Decimal(1)
                        - (
                            Decimal(str(self.settings.slippage_bps / 10000))
                            + Decimal(str(self.settings.backtest_spread_bps / 20000))
                        )
                        * (1 if side == Side.LONG else -1)
                    )
                    multiplier = self.instrument.contract_value
                    direction = Decimal(1) if side == Side.LONG else Decimal(-1)
                    gross = (
                        (exit_price - position.entry) * position.contracts * multiplier * direction
                    )
                    fee = (
                        exit_price
                        * position.contracts
                        * multiplier
                        * Decimal(str(self.settings.taker_fee))
                    )
                    slip = (
                        position.contracts
                        * multiplier
                        * base_exit
                        * Decimal(str(self.settings.slippage_bps / 10000))
                    )
                    spread = (
                        position.contracts
                        * multiplier
                        * base_exit
                        * Decimal(str(self.settings.backtest_spread_bps / 20000))
                    )
                    net = gross - position.entry_fee - fee - position.funding_cost
                    equity += net
                    risk_amount = abs(position.entry - stop) * position.contracts * multiplier
                    result.trades.append(
                        ClosedTrade(
                            candle.symbol,
                            position.signal.strategy,
                            position.signal.regime.value,
                            side,
                            position.entry,
                            exit_price,
                            position.contracts,
                            net,
                            position.entry_fee + fee,
                            position.entry_slippage + slip,
                            position.entry_spread + spread,
                            position.funding_cost,
                            net / risk_amount if risk_amount else Decimal(0),
                        )
                    )
                    position = None
            history.append(candle)
            state = compute_features(
                history,
                derivatives=derivative_history.at(candle.timestamp + timedelta(minutes=15)),
            )
            if state and index >= warmup_bars and position is None and pending is None:
                portfolio = PortfolioState(
                    equity=equity, available_balance=equity, synchronized=True
                )
                bars = {"15m": history}
                for timeframe in ("1H", "4H", "5m"):
                    duration = {"1H": 60, "4H": 240, "5m": 5}[timeframe]
                    bars[timeframe] = [
                        bar
                        for bar in (context or {}).get(timeframe, [])
                        if bar.timestamp + timedelta(minutes=duration)
                        <= candle.timestamp + timedelta(minutes=15)
                    ]
                signals, intent = await self.decision_pipeline.decide(
                    bars, portfolio, main_state=state
                )
                if intent:
                    pending = max(
                        (s for s in signals if s.id in intent.signal_ids),
                        key=lambda s: s.confidence,
                    )
            result.equity_curve.append(equity)
        if position and candles:
            last = candles[-1]
            side = position.signal.side
            base_exit = last.close
            slip_rate = Decimal(str(self.settings.slippage_bps / 10000))
            half_spread = Decimal(str(self.settings.backtest_spread_bps / 20000))
            exit_price = base_exit * (
                Decimal(1) - (slip_rate + half_spread) * (1 if side == Side.LONG else -1)
            )
            multiplier = self.instrument.contract_value
            direction = Decimal(1) if side == Side.LONG else Decimal(-1)
            gross = (exit_price - position.entry) * position.contracts * multiplier * direction
            fee = (
                exit_price * position.contracts * multiplier * Decimal(str(self.settings.taker_fee))
            )
            slip = position.contracts * multiplier * base_exit * slip_rate
            spread = position.contracts * multiplier * base_exit * half_spread
            net = gross - position.entry_fee - fee - position.funding_cost
            equity += net
            risk_amount = (
                abs(position.entry - position.signal.stop_price) * position.contracts * multiplier
            )
            result.trades.append(
                ClosedTrade(
                    last.symbol,
                    position.signal.strategy,
                    position.signal.regime.value,
                    side,
                    position.entry,
                    exit_price,
                    position.contracts,
                    net,
                    position.entry_fee + fee,
                    position.entry_slippage + slip,
                    position.entry_spread + spread,
                    position.funding_cost,
                    net / risk_amount if risk_amount else Decimal(0),
                )
            )
            result.equity_curve[-1] = equity
        return result


def walk_forward_indices(length: int, train: int, validation: int, out_of_sample: int):
    if min(length, train, validation, out_of_sample) <= 0:
        raise ValueError("window sizes must be positive")
    start = 0
    while start + train + validation + out_of_sample <= length:
        yield (
            slice(start, start + train),
            slice(start + train, start + train + validation),
            slice(start + train + validation, start + train + validation + out_of_sample),
        )
        start += out_of_sample


async def run_walk_forward(
    candles: list[Candle],
    settings: Settings,
    instrument: Instrument,
    initial_equity: Decimal,
    train: int,
    validation: int,
    out_of_sample: int,
    context: Mapping[str, list[Candle]] | None = None,
    derivatives: Sequence[DerivativeObservation] = (),
) -> list[dict[str, Any]]:
    folds = []
    for train_slice, validation_slice, oos_slice in walk_forward_indices(
        len(candles), train, validation, out_of_sample
    ):
        engine = EventDrivenBacktester(settings, instrument)
        train_result = await engine.run(
            candles[train_slice],
            initial_equity,
            warmup_bars=min(200, train),
            context=context,
            derivatives=derivatives,
        )
        validation_start = max(train_slice.start, validation_slice.start - 200)
        validation_result = await engine.run(
            candles[validation_start : validation_slice.stop],
            initial_equity,
            warmup_bars=validation_slice.start - validation_start,
            context=context,
            derivatives=derivatives,
        )
        oos_start = max(train_slice.start, oos_slice.start - 200)
        oos_result = await engine.run(
            candles[oos_start : oos_slice.stop],
            initial_equity,
            warmup_bars=oos_slice.start - oos_start,
            context=context,
            derivatives=derivatives,
        )
        folds.append(
            {
                "train": [train_slice.start, train_slice.stop],
                "validation": [validation_slice.start, validation_slice.stop],
                "out_of_sample": [oos_slice.start, oos_slice.stop],
                "params": oos_result.params,
                "train_metrics": train_result.metrics(),
                "validation_metrics": validation_result.metrics(),
                "out_of_sample_metrics": oos_result.metrics(),
            }
        )
    return folds
