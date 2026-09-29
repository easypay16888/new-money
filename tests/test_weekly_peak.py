from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import insert

from app.storage import ROW_TYPES, Store


@pytest.mark.asyncio
async def test_weekly_peak_scans_more_than_20000_snapshots(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/weekly.db")
    await store.initialize()
    timestamp = datetime.now(UTC) - timedelta(days=1)
    row_type = ROW_TYPES["portfolio_snapshots"]
    low = {"timestamp": timestamp, "payload": {"equity": "8000"}}
    async with store.sessions.begin() as session:
        for _ in range(11):
            await session.execute(insert(row_type), [low.copy() for _ in range(1000)])
        await session.execute(
            insert(row_type), [{"timestamp": timestamp, "payload": {"equity": "10000"}}]
        )
        for _ in range(11):
            await session.execute(insert(row_type), [low.copy() for _ in range(1000)])
    peak = await store.max_equity_since(datetime.now(UTC) - timedelta(days=7))
    assert peak == Decimal("10000")
    assert (peak - Decimal("8000")) / peak == Decimal("0.2")
    assert await store.max_equity_since(datetime.now(UTC)) is None
    await store.close()


@pytest.mark.asyncio
async def test_latest_emergency_target_is_not_limited_by_event_count(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/targets.db")
    await store.initialize()
    await store.append(
        "emergency_targets",
        {"symbol": "BTC-USDT-SWAP", "target": "0", "completed": False},
        symbol="BTC-USDT-SWAP",
    )
    row_type = ROW_TYPES["emergency_targets"]
    async with store.sessions.begin() as session:
        for _ in range(11):
            await session.execute(
                insert(row_type),
                [
                    {
                        "symbol": "ETH-USDT-SWAP",
                        "payload": {
                            "symbol": "ETH-USDT-SWAP",
                            "target": "0",
                            "completed": True,
                        },
                    }
                    for _ in range(1000)
                ],
            )
    latest = await store.latest_per_symbol("emergency_targets")
    assert any(row["symbol"] == "BTC-USDT-SWAP" and not row["completed"] for row in latest)
    await store.close()
