from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, Decimal

from app.config import Settings
from app.models import GovernorState, Instrument, PortfolioState, RiskDecision, Side, TradeIntent


class RiskGovernor:
    def __init__(self) -> None:
        self.state = GovernorState.HALT
        self.reason = "startup reconciliation pending"

    def halt(self, reason: str, emergency: bool = False) -> None:
        self.state = (
            GovernorState.EMERGENCY
            if emergency or self.state == GovernorState.EMERGENCY
            else GovernorState.HALT
        )
        self.reason = reason

    def resume(self, *, synchronized: bool, healthy: bool) -> bool:
        if not synchronized or not healthy:
            return False
        self.state = GovernorState.NORMAL
        self.reason = "healthy"
        return True


class RiskEngine:
    def __init__(self, settings: Settings, governor: RiskGovernor) -> None:
        self.settings = settings
        self.governor = governor

    def evaluate(
        self,
        intent: TradeIntent,
        portfolio: PortfolioState,
        instrument: Instrument,
        *,
        data_fresh: bool,
        infrastructure_healthy: bool,
        spread: Decimal = Decimal(0),
    ) -> RiskDecision:
        def reject(reason: str, status: str = "REJECTED") -> RiskDecision:
            return RiskDecision(
                approved=False,
                status=status,
                reason=reason,
                intent_id=intent.id,
                symbol=intent.symbol,
                direction=intent.direction,
            )

        if self.governor.state != GovernorState.NORMAL:
            return reject(self.governor.reason, "HALT")
        # Validate before Decimal comparisons/rounding; unknown numeric state fails closed.
        values = [
            portfolio.equity, portfolio.available_balance, portfolio.margin_used,
            portfolio.position_notional, portfolio.open_risk, portfolio.daily_pnl,
            portfolio.weekly_drawdown, intent.entry_reference, intent.stop_price,
            instrument.contract_value, instrument.lot_size, instrument.min_size,
            instrument.tick_size, spread, *portfolio.positions.values(),
        ]
        if intent.take_profit_reference is not None:
            values.append(intent.take_profit_reference)
        config_values = [Decimal(str(value)) for value in (
            self.settings.margin_usage_target, self.settings.max_margin_usage,
            self.settings.maker_fee, self.settings.taker_fee, self.settings.slippage_bps,
            self.settings.risk_per_trade, self.settings.max_total_open_risk,
            self.settings.max_daily_loss, self.settings.max_weekly_drawdown,
        )]
        if (
            not all(value.is_finite() for value in values + config_values)
            or any(value <= 0 for value in (
                intent.entry_reference, intent.stop_price, instrument.contract_value,
                instrument.lot_size, instrument.min_size, instrument.tick_size,
            ))
            or any(value < 0 for value in (
                portfolio.margin_used, portfolio.position_notional, portfolio.open_risk,
                portfolio.weekly_drawdown, spread, *config_values[2:5],
            ))
            or not Decimal(0) < config_values[0] < config_values[1]
        ):
            self.governor.halt("invalid risk inputs")
            return reject(self.governor.reason, "HALT")
        if not portfolio.synchronized or not data_fresh or not infrastructure_healthy:
            self.governor.halt("state or data unhealthy")
            return reject(self.governor.reason, "HALT")
        if portfolio.equity <= 0 or portfolio.available_balance <= 0:
            return reject("no available equity")
        if portfolio.daily_pnl <= -portfolio.equity * Decimal(str(self.settings.max_daily_loss)):
            self.governor.halt("daily loss limit")
            return reject(self.governor.reason, "HALT")
        if portfolio.weekly_drawdown >= Decimal(str(self.settings.max_weekly_drawdown)):
            self.governor.halt("weekly drawdown limit")
            return reject(self.governor.reason, "HALT")
        if len([v for v in portfolio.positions.values() if v]) >= self.settings.max_open_positions:
            return reject("max positions")
        if portfolio.positions.get(intent.symbol):
            return reject("position already open")
        if portfolio.margin_used / portfolio.equity >= Decimal(str(self.settings.max_margin_usage)):
            self.governor.halt("margin usage limit")
            return reject(self.governor.reason, "HALT")
        if spread > Decimal(str(self.settings.max_spread_ratio)):
            self.governor.halt("spread explosion")
            return reject(self.governor.reason, "HALT")
        entry = intent.entry_reference
        rounding = ROUND_CEILING if intent.direction == Side.LONG else ROUND_FLOOR
        stop = (intent.stop_price / instrument.tick_size).to_integral_value(
            rounding=rounding
        ) * instrument.tick_size
        distance = abs(entry - stop)
        if entry <= 0 or distance <= 0 or distance >= entry:
            return reject("invalid stop distance")
        if (intent.direction == Side.LONG and stop >= entry) or (
            intent.direction == Side.SHORT and stop <= entry
        ):
            return reject("stop on wrong side")
        risk = portfolio.equity * Decimal(str(self.settings.risk_per_trade))
        if portfolio.open_risk + risk > portfolio.equity * Decimal(
            str(self.settings.max_total_open_risk)
        ):
            return reject("total open risk limit")
        desired_notional = risk / (distance / entry)
        cap_leverage = max(
            Decimal(0),
            portfolio.equity * Decimal(self.settings.max_leverage) - portfolio.position_notional,
        )
        cap_margin = (
            portfolio.equity * Decimal(str(self.settings.max_margin_usage)) - portfolio.margin_used
        ) * Decimal(self.settings.leverage)
        # M + N/L <= target * (E - existing stop/cost reserves - N * adverse_rate).
        # Risk-per-trade still determines desired_notional; this cap can only reduce it.
        target_usage = Decimal(str(self.settings.margin_usage_target))
        costs = (Decimal(2) * max(Decimal(str(self.settings.maker_fee)),
                                 Decimal(str(self.settings.taker_fee)))
                 + Decimal(2) * Decimal(str(self.settings.slippage_bps)) / Decimal(10000))
        adverse_rate = distance / entry + costs
        stressed_equity = (portfolio.equity - portfolio.open_risk
                           - portfolio.position_notional * costs)
        cap_buffer = max(Decimal(0),
                         (target_usage * stressed_equity - portfolio.margin_used)
                         / (Decimal(1) / Decimal(self.settings.leverage)
                            + target_usage * adverse_rate))
        cap_available = portfolio.available_balance / (
            Decimal(1) / Decimal(self.settings.leverage) + adverse_rate
        )
        notional = max(Decimal(0), min(desired_notional, cap_leverage, cap_margin,
                                     cap_buffer, cap_available))
        if instrument.contract_currency == "USD":
            per_contract = instrument.contract_value
        elif instrument.contract_currency == intent.symbol.split("-")[0]:
            per_contract = instrument.contract_value * entry
        else:
            return reject("unsupported contract currency")
        contracts = (notional / per_contract / instrument.lot_size).to_integral_value(
            rounding=ROUND_DOWN
        ) * instrument.lot_size
        if contracts < instrument.min_size:
            return reject("below minimum contract size")
        actual_notional = contracts * per_contract
        if actual_notional / Decimal(self.settings.leverage) > portfolio.available_balance:
            return reject("insufficient available balance")
        target = None
        if intent.take_profit_reference is not None:
            target_rounding = ROUND_CEILING if intent.direction == Side.LONG else ROUND_FLOOR
            target = (intent.take_profit_reference / instrument.tick_size).to_integral_value(
                rounding=target_rounding
            ) * instrument.tick_size
        return RiskDecision(
            approved=True,
            status="REDUCED" if actual_notional < desired_notional else "APPROVED",
            reason=(
                "capped by portfolio limits or lot size"
                if actual_notional < desired_notional
                else "within configured limits"
            ),
            intent_id=intent.id,
            symbol=intent.symbol,
            direction=intent.direction,
            approved_contracts=contracts,
            approved_notional=actual_notional,
            leverage=self.settings.leverage,
            stop_price=stop,
            entry_reference=entry,
            take_profit_reference=target,
            signal_expires_at=intent.expires_at,
        )

    def emergency_reduce(
        self, symbol: str, open_direction: Side, contracts: Decimal, instrument: Instrument
    ) -> RiskDecision:
        if self.governor.state != GovernorState.EMERGENCY or contracts <= 0:
            raise ValueError("emergency reduce requires active EMERGENCY and positive size")
        size = (contracts / instrument.lot_size).to_integral_value(
            rounding=ROUND_DOWN
        ) * instrument.lot_size
        if size <= 0:
            raise ValueError("reduce size below lot precision")
        return RiskDecision(
            approved=True,
            status="REDUCED",
            reason="emergency reduce only",
            intent_id="emergency-" + symbol,
            symbol=symbol,
            direction=Side.SHORT if open_direction == Side.LONG else Side.LONG,
            approved_contracts=size,
            approved_notional=Decimal(0),
            leverage=1,
        )
