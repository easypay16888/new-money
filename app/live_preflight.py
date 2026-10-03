"""Read-only acceptance inventory. This tool never authorizes or enables LIVE."""
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from app.config import Mode, Settings


async def inspect_readiness(
    settings: Settings, client: httpx.AsyncClient, status_url: str
) -> dict[str, Any]:
    url = urlsplit(status_url)
    if (
        url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password
        or url.path != "/status" or url.query or url.fragment
    ):
        raise ValueError("preflight requires a status URL without credentials or query")
    base = status_url.removesuffix("/status")
    checks: list[dict[str, str]] = []

    def record(name: str, passed: bool, detail: str) -> None:
        checks.append({"check": name, "result": "PASS" if passed else "BLOCKED", "detail": detail})

    snapshots: dict[str, Any] = {}
    for path in ("/status", "/health", "/positions", "/orders"):
        token = settings.status_api_token if path == "/status" else settings.api_token
        token = token or settings.api_token
        headers = {"Authorization": "Bearer " + token} if token else {}
        try:
            response = await client.get(base + path, headers=headers, follow_redirects=False)
            if response.status_code != 200:
                record("HTTP" + path, False, "HTTP " + str(response.status_code))
                continue
            snapshots[path] = response.json()
            record("HTTP" + path, True, "Read-only response received")
        except Exception as exc:
            record("HTTP" + path, False, type(exc).__name__)
    status = snapshots.get("/status")
    status = status if isinstance(status, dict) else {}
    health = snapshots.get("/health")
    health = health if isinstance(health, dict) else {}
    positions = snapshots.get("/positions")
    positions = positions if isinstance(positions, dict) else {}
    record("RUNTIME_NORMAL", status.get("running") is True and status.get("risk_state") == "NORMAL",
           "Runtime must be running in NORMAL; no halt or resume is performed")
    record("RECONCILIATION_SYNC", status.get("synchronized") is True,
           "Status reports synchronized reconciliation")
    websockets = status.get("websockets")
    record("WEBSOCKETS", isinstance(websockets, list) and len(websockets) == 4 and all(
        isinstance(row, dict) and row.get("connected") is True and row.get("fresh") is True
        for row in websockets
    ), "Four connected and fresh feeds required")
    record("RECOVERY_SETTLED", status.get("blocked_symbols") == [] and status.get("emergency_targets") == {},
           "No blocked entry symbols or emergency targets")
    record("DATABASE_REDIS", health.get("database") is True and health.get("redis") is True,
           "Health endpoint reports database and Redis available")
    record("FLAT_ACCEPTANCE_BASELINE", positions.get("positions") == {} and
           snapshots.get("/orders") is not None and isinstance(snapshots["/orders"], list) and all(
        isinstance(order, dict) and order.get("state") in {"FILLED", "CANCELLED", "REJECTED"}
        for order in snapshots["/orders"]
    ), "A flat, settled ledger is required before controlled acceptance drills")
    try:
        equity = Decimal(str(positions["equity"]))
        margin = Decimal(str(positions["margin_used"]))
        drawdown = Decimal(str(positions["weekly_drawdown"]))
        daily_pnl = Decimal(str(positions["daily_pnl"]))
        within_limits = all(value.is_finite() for value in (equity, margin, drawdown, daily_pnl)) and (
            equity > 0 and margin >= 0 and margin / equity < Decimal(str(settings.max_margin_usage))
            and 0 <= drawdown < Decimal(str(settings.max_weekly_drawdown))
            and daily_pnl > -equity * Decimal(str(settings.max_daily_loss))
        )
    except (KeyError, ValueError, ArithmeticError):
        within_limits = False
    record("PORTFOLIO_LIMITS", within_limits, "Current equity and risk limits must be valid")
    record("POSTGRESQL_STORAGE", status.get("database_backend") == "postgresql",
           "Long-term acceptance requires the supported PostgreSQL deployment")
    record("LIVE_CONTROL_TOKENS", len(settings.api_token.strip()) >= 32 and
           len(settings.status_api_token.strip()) >= 32 and settings.api_token.strip() != settings.status_api_token.strip(),
           "Distinct control and read-only status tokens required for LIVE operations")
    record("LIVE_REMAINS_DISABLED", settings.mode == Mode.PAPER and not settings.live_trading_enabled
           and status.get("mode") == "PAPER", "This acceptance stage runs in Demo only")
    lease = status.get("live_writer_lease")
    if settings.mode == Mode.LIVE or status.get("mode") == "LIVE":
        record("LIVE_SINGLE_WRITER", isinstance(lease, dict) and
               lease.get("required") is True and lease.get("held") is True,
               "LIVE must hold the shared coordinator lease; no ownership keys exposed")
    else:
        checks.append({"check": "LIVE_SINGLE_WRITER", "result": "UNVERIFIED",
                       "detail": "PAPER does not acquire LIVE ownership; field contention/loss drills required"})
    for name, detail in (
        ("LIVE_ACCOUNT_PREFLIGHT", "Verify real account UID, permissions, IP binding and separate ledger"),
        ("CAA_AND_SHUTDOWN_DRILLS", "Confirm CAA expiry, retained exits and safe shutdown on exchange"),
        ("PARTIAL_FILL_RESTART_DRILLS", "Observe partial-fill protection and restart recovery on exchange"),
        ("EMERGENCY_FAULT_DRILLS", "Confirm bounded recovery and reduce-only flatten under controlled faults"),
        ("SOAK_AND_MARGIN_HEADROOM", "Review uninterrupted Demo evidence and margin-boundary behavior"),
        ("OPERATOR_LIVE_APPROVAL", "Explicit human approval is required before any LIVE activation"),
    ):
        checks.append({"check": name, "result": "UNVERIFIED", "detail": detail})
    return {"checked_at": datetime.now(UTC).isoformat(), "live_ready": False,
            "scope": "read-only preflight; no trading or control actions", "checks": checks}


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only LIVE acceptance inventory")
    parser.add_argument("--status-url", default="http://127.0.0.1:18000/status")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    async def run() -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=5) as client:
            return await inspect_readiness(Settings(), client, args.status_url)
    try:
        report = asyncio.run(run())
    except Exception as exc:
        print(json.dumps({"live_ready": False, "error_type": type(exc).__name__}))
        raise SystemExit(2) from None
    output = json.dumps(report, indent=2)
    if args.output:
        args.output.write_text(output + "\n")
    print(output)
    raise SystemExit(2)  # Field drills and activation approval cannot be inferred from HTTP health.


if __name__ == "__main__":
    main()
