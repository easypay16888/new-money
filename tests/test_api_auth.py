from decimal import Decimal

import httpx
import pytest

from app.api import create_app
from app.config import Mode, Settings
from app.models import PortfolioState


@pytest.mark.asyncio
async def test_live_sensitive_endpoints_require_token(tmp_path):
    settings = Settings(
        _env_file=None,
        mode=Mode.LIVE,
        live_trading_enabled=True,
        confirm_live_account_id="expected",
        api_token="test-token",
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auth.db",
    )
    app = create_app(settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        assert (await client.get("/health")).status_code == 200
        assert (await client.get("/positions")).status_code == 401
        assert (await client.get("/metrics")).status_code == 401
        assert (
            await client.get("/positions", headers={"Authorization": "Bearer test-token"})
        ).status_code == 200


@pytest.mark.asyncio
async def test_resume_rejects_failed_reconciliation_even_with_old_synced_state(monkeypatch):
    app = create_app(Settings(_env_file=None))
    runtime = app.state.runtime
    runtime.portfolio = PortfolioState(
        equity=Decimal("10000"), available_balance=Decimal("10000"), synchronized=True
    )
    runtime.store.healthy = True
    runtime.dead_man_healthy = True
    runtime.redis = object()

    async def failed_reconcile():
        runtime.reconciliation_healthy = False

    monkeypatch.setattr(runtime, "reconcile", failed_reconcile)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        response = await client.post("/system/resume")
    assert response.status_code == 409
    assert runtime.governor.state.value == "HALT"
    await runtime.client.close()
    await runtime.store.close()
