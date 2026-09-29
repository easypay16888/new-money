from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from app.storage import Store


async def build_daily_review(store: Store, day: date) -> dict[str, Any]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    snapshots = await store.between("portfolio_snapshots", start, end)
    fills = await store.between("fills", start, end)
    risk_events = await store.between("risk_events", start, end)
    orders = await store.between("orders", start, end)
    equity_start = Decimal(snapshots[0]["equity"]) if snapshots else None
    equity_end = Decimal(snapshots[-1]["equity"]) if snapshots else None
    fees = sum((Decimal(row.get("fillFee") or row.get("fee") or "0") for row in fills), Decimal(0))
    pnl_values = [row.get("fillPnl") for row in fills]
    realized_pnl = (
        sum((Decimal(str(value)) for value in pnl_values), Decimal(0))
        if fills and all(value not in (None, "") for value in pnl_values)
        else None
    )
    peak = Decimal(0)
    max_drawdown = Decimal(0)
    for snapshot in snapshots:
        equity = Decimal(snapshot["equity"])
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)
    report: dict[str, Any] = {
        "date": day.isoformat(),
        "equity_start": str(equity_start) if equity_start is not None else None,
        "equity_end": str(equity_end) if equity_end is not None else None,
        "equity_change": (
            str(equity_end - equity_start)
            if equity_start is not None and equity_end is not None
            else None
        ),
        "orders": len(orders),
        "fills": len(fills),
        "fees": str(fees),
        "max_drawdown": str(max_drawdown) if snapshots else None,
        "risk_events": len(risk_events),
        "halt_count": sum(
            row.get("event") == "enter_halt" and not row.get("emergency")
            for row in risk_events
        ),
        "emergency_count": sum(
            row.get("event") == "enter_emergency"
            or (row.get("event") == "enter_halt" and row.get("emergency"))
            for row in risk_events
        ),
        "anomalies": [
            row.get("reason", row.get("event"))
            for row in risk_events
            if row.get("status") in {"HALT", "REJECTED"} or row.get("reason")
        ],
        "research_suggestions": [],
        "realized_pnl": str(realized_pnl) if realized_pnl is not None else None,
        "realized_pnl_after_fees": (str(realized_pnl + fees) if realized_pnl is not None else None),
        "win_rate": None,
        "profit_factor": None,
    }
    if risk_events:
        report["research_suggestions"].append("Review risk triggers and market conditions")
    if not fills:
        report["research_suggestions"].append("Investigate whether filters are too restrictive")
    return report


async def save_daily_review(store: Store, day: date) -> dict[str, Any]:
    report = await build_daily_review(store, day)
    await store.append("daily_reports", report, reference_id=day.isoformat())
    return report
