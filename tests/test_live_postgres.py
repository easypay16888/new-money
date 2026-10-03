"""Runs against a dedicated disposable CI database, never an operational ledger."""
import os
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import delete
from sqlalchemy.engine.url import make_url

from app.storage import ROW_TYPES, Store


async def test_postgres_live_binding_and_equity_aggregation():
    url = os.environ.get("TEST_POSTGRES_URL")
    if not url:
        pytest.skip("dedicated PostgreSQL CI service not configured")
    parsed = make_url(url)
    assert parsed.database == "quant_live_acceptance_test"
    assert parsed.username == "quant_test" and parsed.host in {"127.0.0.1", "localhost"}
    store = Store(url)
    try:
        await store.initialize()
        async with store.sessions.begin() as session:
            for table in ROW_TYPES.values():
                await session.execute(delete(table))
        await store.bind_live_account("test-account")
        await store.bind_live_account("test-account")
        with pytest.raises(ValueError):
            await store.bind_live_account("other-test-account")
        start = datetime.now(UTC)
        for equity in ("5000", "7000", "4900"):
            await store.append("portfolio_snapshots", {"equity": equity})
        assert await store.max_equity_since(start) == Decimal("7000")
        await store.append("system_events", {"event": "start", "mode": "PAPER"})
        with pytest.raises(ValueError):
            await store.bind_live_account("test-account")
    finally:
        await store.close()
