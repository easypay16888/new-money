from decimal import Decimal

import pytest

from app.config import Settings
from app.models import GovernorState, Side
from app.risk import RiskEngine, RiskGovernor
from tests.test_core import instrument, intent, portfolio


def size(*, state=None, spec=None, signal=None, **settings):
    governor = RiskGovernor()
    governor.resume(synchronized=True, healthy=True)
    engine = RiskEngine(Settings(_env_file=None, **settings), governor)
    decision = engine.evaluate(signal or intent(), state or portfolio(), spec or instrument(),
                               data_fresh=True, infrastructure_healthy=True)
    return decision, governor


def test_margin_buffer_reduces_old_2499_percent_sizing():
    decision, _ = size()
    assert decision.approved
    assert decision.approved_notional < Decimal("2499")
    assert decision.approved_contracts == Decimal("3.99")
    assert Settings(_env_file=None).max_margin_usage == 0.25


def test_margin_buffer_survives_stop_costs_and_small_equity_decline():
    decision, _ = size()
    n = decision.approved_notional
    stop_loss = n * Decimal("0.01")
    costs = n * Decimal("0.0014")  # both sides of fee and slippage
    assert n / (Decimal("10000") - stop_loss - costs) <= Decimal("0.20")
    assert n / (Decimal("10000") - stop_loss - costs - Decimal("10")) < Decimal("0.25")


def test_fees_and_slippage_only_reduce_size():
    no_cost, _ = size(maker_fee=0, taker_fee=0, slippage_bps=0)
    normal, _ = size()
    higher, _ = size(taker_fee=0.005, slippage_bps=20)
    assert higher.approved_contracts <= normal.approved_contracts <= no_cost.approved_contracts


def test_buffer_contract_rounding_is_down_only():
    spec = instrument().model_copy(update={"lot_size": Decimal("0.3")})
    decision, _ = size(spec=spec)
    assert decision.approved_contracts == Decimal("3.9")
    assert decision.approved_contracts % spec.lot_size == 0


def test_buffer_below_minimum_is_no_trade():
    decision, _ = size(spec=instrument().model_copy(update={"min_size": Decimal("4")}))
    assert not decision.approved and decision.reason == "below minimum contract size"


@pytest.mark.parametrize("margin", ["0", "500", "1500", "1999"])
def test_buffer_includes_existing_margin_and_stop_and_closure_cost_reserves(margin):
    state = portfolio().model_copy(update={
        "margin_used": Decimal(margin), "position_notional": Decimal(margin),
        "open_risk": Decimal("20"), "positions": {"ETH-USDT-SWAP": Decimal("1")},
    })
    decision, _ = size(state=state)
    if decision.approved:
        n = decision.approved_notional
        stressed = state.equity - state.open_risk - state.position_notional * Decimal("0.0014")
        stressed -= n * Decimal("0.0114")
        assert (state.margin_used + n) / stressed <= Decimal("0.20")
    else:
        assert decision.reason == "below minimum contract size"


def test_multi_symbol_portfolio_margin_aggregated_before_sizing():
    state = portfolio().model_copy(update={
        "margin_used": Decimal("1200"), "position_notional": Decimal("1200"),
        "open_risk": Decimal("10"),
        "positions": {"ETH-USDT-SWAP": Decimal("1"), "TEST-USDT-SWAP": Decimal("2")},
    })
    decision, _ = size(state=state)
    empty, _ = size()
    assert decision.approved and decision.approved_contracts < empty.approved_contracts
    assert decision.approved_notional + state.margin_used < Decimal("2000")


@pytest.mark.parametrize("field,value", [
    ("equity", "NaN"), ("available_balance", "Infinity"), ("margin_used", "NaN"),
    ("position_notional", "Infinity"), ("open_risk", "-1"), ("margin_used", "-1"),
])
def test_margin_invalid_portfolio_values_fail_closed(field, value):
    decision, governor = size(state=portfolio().model_copy(update={field: Decimal(value)}))
    assert not decision.approved and governor.state == GovernorState.HALT


@pytest.mark.parametrize("stop", ["NaN", "Infinity", "0", "-100", "150000"])
def test_extreme_or_invalid_stop_distance_fails_closed(stop):
    signal = intent().model_copy(update={"stop_price": Decimal(stop), "direction": Side.SHORT})
    decision, _ = size(signal=signal)
    assert not decision.approved


@pytest.mark.parametrize("field,value", [
    ("lot_size", "0"), ("tick_size", "NaN"), ("contract_value", "Infinity"),
])
def test_invalid_instrument_size_fails_closed(field, value):
    decision, governor = size(spec=instrument().model_copy(update={field: Decimal(value)}))
    assert not decision.approved and governor.state == GovernorState.HALT


@pytest.mark.parametrize("target", [0.25, 0.23, float("nan"), float("inf")])
def test_sizing_target_cannot_reach_hard_limit_or_exceed_22_percent(target):
    with pytest.raises(ValueError):
        Settings(_env_file=None, margin_usage_target=target)
