import json

import httpx
import pytest

from app.config import Settings
from app.live_preflight import inspect_readiness


def responses():
    return {
        "/status": {"mode": "PAPER", "running": True, "risk_state": "NORMAL",
            "synchronized": True, "blocked_symbols": [], "emergency_targets": {},
            "websockets": [{"connected": True, "fresh": True}] * 4},
        "/health": {"database": True, "redis": True},
        "/positions": {"equity": "5000", "margin_used": "0", "positions": {},
            "daily_pnl": "0", "weekly_drawdown": "0"},
        "/orders": [],
    }


async def inspect(data):
    requests = []
    def respond(request):
        requests.append(request)
        return httpx.Response(200, json=data[request.url.path])
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        report = await inspect_readiness(Settings(_env_file=None), client, "http://app:8000/status")
    return report, requests


async def test_healthy_preflight_never_claims_live_acceptance_or_calls_control_endpoints():
    report, requests = await inspect(responses())
    assert report["live_ready"] is False
    assert any(row["result"] == "UNVERIFIED" for row in report["checks"])
    assert [(r.method, r.url.path) for r in requests] == [
        ("GET", "/status"), ("GET", "/health"), ("GET", "/positions"), ("GET", "/orders")
    ]


@pytest.mark.parametrize("path,field,value,check", [
    ("/status", "risk_state", "HALT", "RUNTIME_NORMAL"),
    ("/status", "running", False, "RUNTIME_NORMAL"),
    ("/status", "synchronized", False, "RECONCILIATION_SYNC"),
    ("/status", "websockets", [], "WEBSOCKETS"),
    ("/status", "blocked_symbols", ["BTC-USDT-SWAP"], "RECOVERY_SETTLED"),
    ("/status", "emergency_targets", {"BTC-USDT-SWAP": "0"}, "RECOVERY_SETTLED"),
    ("/positions", "margin_used", "1250", "PORTFOLIO_LIMITS"),
    ("/positions", "equity", "NaN", "PORTFOLIO_LIMITS"),
    ("/positions", "positions", {"BTC-USDT-SWAP": "1"}, "FLAT_ACCEPTANCE_BASELINE"),
])
async def test_preflight_reports_runtime_and_risk_blockers(path, field, value, check):
    data = responses()
    data[path][field] = value
    report, _ = await inspect(data)
    assert next(row for row in report["checks"] if row["check"] == check)["result"] == "BLOCKED"


async def test_preflight_failure_redacts_response_and_exception_secrets():
    marker = "SECRET_MARKER"
    def fail(request):
        raise httpx.ConnectError(marker + str(request.url))
    async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
        report = await inspect_readiness(Settings(_env_file=None, api_token=marker), client,
                                         "http://app:8000/status")
    assert marker not in json.dumps(report)
    assert all(row["result"] == "BLOCKED" for row in report["checks"] if row["check"].startswith("HTTP"))


@pytest.mark.parametrize("url", [
    "http://secret@app:8000/status", "http://app:8000/status?token=secret",
    "http://app:8000/system/resume", "http://app:8000/status#secret",
])
async def test_preflight_rejects_credentials_and_non_status_url(url):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: pytest.fail("unexpected HTTP"))) as client:
        with pytest.raises(ValueError):
            await inspect_readiness(Settings(_env_file=None), client, url)


async def test_paper_preflight_single_writer_is_unverified():
    report, _ = await inspect(responses())
    check = next(row for row in report["checks"] if row["check"] == "LIVE_SINGLE_WRITER")
    assert check["result"] == "UNVERIFIED" and not report["live_ready"]


@pytest.mark.parametrize("lease,expected", [
    ({"required": True, "held": True}, "PASS"),
    ({"required": True, "held": False}, "BLOCKED"), ({}, "BLOCKED"),
])
async def test_observed_live_preflight_requires_held_lease(lease, expected):
    data = responses()
    data["/status"]["mode"] = "LIVE"
    data["/status"]["live_writer_lease"] = lease
    report, _ = await inspect(data)
    assert next(row for row in report["checks"] if row["check"] == "LIVE_SINGLE_WRITER")["result"] == expected
    assert not report["live_ready"]
