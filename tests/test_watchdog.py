from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from app.monitoring import Metrics
from app.notifications import NotificationManager
from app.watchdog import WatchdogMonitor, WatchdogSettings
from tests.test_notifications import Recorder


def healthy(*, risk: str = "NORMAL", running: bool = True) -> httpx.Response:
    return httpx.Response(200, json={
        "running": running,
        "synchronized": True,
        "risk_state": risk,
        "websockets": [{"fresh": True} for _ in range(4)],
    })


def settings(**kwargs) -> WatchdogSettings:
    return WatchdogSettings(
        _env_file=None, watchdog_startup_grace_seconds=0,
        watchdog_failure_threshold=3, watchdog_recovery_threshold=2,
        **kwargs,
    )


def notifications() -> NotificationManager:
    return NotificationManager([Recorder()], Metrics(), retry_delays=())


@pytest.mark.asyncio
async def test_watchdog_single_failure_no_alert():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(503))
    ) as client:
        notifier = notifications()
        monitor = WatchdogMonitor(settings(), client, notifier)
        await monitor.check()
    assert notifier.queue_size == 0


@pytest.mark.asyncio
async def test_watchdog_threshold_sends_offline_alert():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(503))
    ) as client:
        notifier = notifications()
        monitor = WatchdogMonitor(settings(), client, notifier)
        for _ in range(5):
            await monitor.check()
    assert notifier.queue_size == 1
    item = notifier._pending[next(iter(notifier._pending))][0]
    assert item.title == "🚨 Trading App Offline"
    assert item.message.endswith("Last error: HTTP 503")


@pytest.mark.asyncio
async def test_watchdog_recovery_threshold_sends_recovered():
    responses = [httpx.Response(503)] * 3 + [healthy(), healthy()]

    def respond(_: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        notifier = notifications()
        monitor = WatchdogMonitor(settings(), client, notifier)
        for _ in range(4):
            await monitor.check()
        assert notifier.queue_size == 1
        await monitor.check()
    assert notifier.queue_size == 2
    assert any(
        item.title == "✅ Trading App Recovered"
        for queue in notifier._pending.values() for item in queue
    )


@pytest.mark.asyncio
async def test_watchdog_startup_grace_suppresses_alert():
    now = [0.0]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(503))
    ) as client:
        notifier = notifications()
        monitor = WatchdogMonitor(
            WatchdogSettings(_env_file=None, watchdog_startup_grace_seconds=120),
            client, notifier, clock=lambda: now[0],
        )
        for current in (0, 30, 60, 90):
            now[0] = current
            await monitor.check()
        assert notifier.queue_size == 0
        for current in (120, 180, 240):
            now[0] = current
            await monitor.check()
    assert notifier.queue_size == 1


@pytest.mark.asyncio
async def test_watchdog_http_500_counts_failure():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(500))
    ) as client:
        monitor = WatchdogMonitor(settings(), client, notifications())
        await monitor.check()
    assert monitor.failures == 1
    assert monitor.failure_kind == "APP_DOWN"


@pytest.mark.asyncio
async def test_watchdog_timeout_counts_failure():
    def timeout(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timeout")

    async with httpx.AsyncClient(transport=httpx.MockTransport(timeout)) as client:
        monitor = WatchdogMonitor(settings(), client, notifications())
        await monitor.check()
    assert monitor.failures == 1
    assert monitor.failure_kind == "APP_DOWN"


@pytest.mark.asyncio
async def test_watchdog_running_false_counts_unhealthy():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: healthy(running=False))
    ) as client:
        notifier = notifications()
        monitor = WatchdogMonitor(settings(), client, notifier)
        for _ in range(3):
            await monitor.check()
    assert monitor.failure_kind == "APP_ALIVE_BUT_UNHEALTHY"
    assert any(
        item.title == "⚠️ Trading App Unhealthy"
        for queue in notifier._pending.values() for item in queue
    )


@pytest.mark.asyncio
async def test_watchdog_halt_does_not_mean_app_offline():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: healthy(risk="HALT"))
    ) as client:
        notifier = notifications()
        monitor = WatchdogMonitor(settings(), client, notifier)
        await monitor.check()
    assert monitor.failures == 0
    assert notifier.queue_size == 0


@pytest.mark.asyncio
async def test_watchdog_does_not_call_trading_endpoints():
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return healthy()

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        monitor = WatchdogMonitor(settings(), client, notifications())
        for _ in range(3):
            await monitor.check()
    assert {(request.method, request.url.path) for request in requests} == {("GET", "/status")}


def test_watchdog_compose_has_no_trade_credentials_or_port():
    compose = (Path(__file__).resolve().parents[1] / "docker-compose.yml").read_text()
    watchdog_service = compose.split("  watchdog:\n", 1)[1].split("  postgres:\n", 1)[0]
    assert "OKX_" not in watchdog_service
    assert "env_file:" not in watchdog_service
    assert "ports:" not in watchdog_service
    assert "network_mode:" not in watchdog_service
