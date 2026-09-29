from datetime import UTC, datetime, timedelta

import pytest

from app.daily_review import build_daily_review
from app.storage import Store


@pytest.mark.asyncio
async def test_daily_review_uses_only_requested_utc_day(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/review.db")
    await store.initialize()
    day = datetime.now(UTC).date()
    await store.append("portfolio_snapshots", {"equity": "10000"})
    await store.append("portfolio_snapshots", {"equity": "9900"})
    report = await build_daily_review(store, day)
    assert report["equity_change"] == "-100"
    assert report["realized_pnl"] is None
    previous = await build_daily_review(store, day - timedelta(days=1))
    assert previous["equity_change"] is None
    await store.close()


@pytest.mark.asyncio
async def test_daily_review_reports_complete_fill_pnl_and_drawdown(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/review-pnl.db")
    await store.initialize()
    day = datetime.now(UTC).date()
    for equity in ("10000", "9800", "9900"):
        await store.append("portfolio_snapshots", {"equity": equity})
    await store.append("fills", {"fillPnl": "125", "fillFee": "-2"})
    await store.append("fills", {"fillPnl": "-25", "fillFee": "-1"})
    report = await build_daily_review(store, day)
    assert report["realized_pnl"] == "100"
    assert report["realized_pnl_after_fees"] == "97"
    assert report["max_drawdown"] == "0.02"
    await store.close()
