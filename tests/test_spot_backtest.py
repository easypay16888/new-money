from datetime import timedelta
from decimal import Decimal

import pytest

from app.backtest import EventDrivenBacktester
from app.models import Regime, Side, Signal
from tests.test_runtime_backtest import candles, confirmation_context
from tests.test_spot import SPOT, instrument, settings


@pytest.mark.parametrize('side', [Side.LONG, Side.SHORT])
async def test_cash_backtest_has_no_borrowing_or_funding(monkeypatch, side):
    class TestSignal:
        async def generate_signal(self, state, portfolio, regime):
            if state.ema200 is None:
                return None
            return Signal(symbol=SPOT, strategy='mean_reversion', side=side,
                confidence=.8, entry_reference=state.close,
                stop_price=Decimal(90) if side == Side.LONG else Decimal(110),
                take_profit_reference=Decimal(200) if side == Side.LONG else Decimal(50),
                expected_rr=2, regime=Regime.RANGE, timestamp=state.timestamp,
                expires_at=state.timestamp + timedelta(minutes=20))
    monkeypatch.setattr('app.backtest.build_strategies', lambda s: (TestSignal(),))
    s = settings(backtest_funding_rate=.1)
    rows = [c.model_copy(update={'symbol': SPOT}) for c in candles(230)]
    context = {tf: [c.model_copy(update={'symbol': SPOT}) for c in values]
               for tf, values in confirmation_context().items()}
    result = await EventDrivenBacktester(s, instrument()).run(rows, Decimal(5000), context=context)
    if side == Side.SHORT:
        assert not result.trades
    else:
        assert result.trades and all(t.funding_cost == 0 for t in result.trades)
        assert all(t.fees > 0 and t.side == Side.LONG for t in result.trades)
        assert result.params['taker_fee'] == s.spot_fee_rate
