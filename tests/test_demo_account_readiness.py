from decimal import Decimal

import pytest

from app.models import GovernorState
from app.runtime import TradingRuntime
from tests.test_production_safety import Exchange, settings


@pytest.mark.asyncio
async def test_reconcile_uses_usdt_equity_for_usdt_swaps(tmp_path):
    runtime = TradingRuntime(settings(tmp_path))
    await runtime.store.initialize()

    class DemoExchange(Exchange):
        async def account(self):
            return [
                {
                    "totalEq": "103000",
                    "details": [
                        {"ccy": "BTC", "eq": "1", "availEq": ""},
                        {"ccy": "USDT", "eq": "5000", "availEq": "5000"},
                    ],
                }
            ]

    runtime.client = DemoExchange()
    await runtime.reconcile()
    assert runtime.reconciliation_healthy
    assert runtime.portfolio.equity == Decimal("5000")
    assert runtime.portfolio.available_balance == Decimal("5000")
    assert runtime.portfolio.daily_pnl == Decimal(0)
    await runtime.store.close()


@pytest.mark.asyncio
async def test_reconcile_halts_when_demo_account_is_spot_only(tmp_path):
    runtime = TradingRuntime(settings(tmp_path))
    await runtime.store.initialize()

    class SpotExchange(Exchange):
        async def account_config(self):
            return [{"posMode": "net_mode", "acctLv": "1"}]

    runtime.client = SpotExchange()
    await runtime.reconcile()
    assert runtime.reconciliation_healthy
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.governor.reason == "derivatives account mode required"
    await runtime.store.close()
