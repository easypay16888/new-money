from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest

from app.api import create_app
from app.config import Mode, Settings
from app.models import GovernorState, PortfolioState
from app.okx import (
    OkxError,
    OkxRestClient,
    is_retryable_okx_error,
    safe_reconciliation_diagnostics,
)
from app.runtime import TradingRuntime


@pytest.fixture
async def recovery_runtime(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/recovery.db",
        auto_recovery_check_seconds=30,
        auto_recovery_min_halt_seconds=30,
    )
    runtime = TradingRuntime(settings)
    original_client = runtime.client
    await runtime.store.initialize()
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    clock = [0.0]
    runtime._clock = lambda: clock[0]

    async def successful_reconcile():
        runtime.reconciliation_healthy = True
        runtime.portfolio = PortfolioState(
            equity=Decimal("5000"), available_balance=Decimal("5000"), synchronized=True
        )

    runtime._reconcile_impl = AsyncMock(side_effect=successful_reconcile)
    runtime._resume_health = AsyncMock(return_value=(True, []))
    yield runtime, clock
    await runtime.notifications.close()
    await original_client.close()
    await runtime.store.close()


async def three_checks(runtime: TradingRuntime, clock: list[float]) -> None:
    for expected in (1, 2, 3):
        clock[0] += 30
        await runtime._auto_recovery_check()
        if expected < 3:
            assert runtime.governor.state == GovernorState.HALT
            assert runtime.auto_recovery_status()["successes"] == expected


@pytest.mark.parametrize("reason", [
    "reconciliation failed", "WebSocket disconnected or stale",
    "Redis unavailable", "dead man switch unavailable",
])
async def test_transient_halt_auto_recovers_after_three_successes(recovery_runtime, reason):
    runtime, clock = recovery_runtime
    await runtime.enter_halt(reason)
    await three_checks(runtime, clock)
    assert runtime.governor.state == GovernorState.NORMAL
    assert runtime._reconcile_impl.await_count == 4  # Three checks plus final reconciliation.


@pytest.mark.parametrize("reason", [
    "position mismatch", "foreign risk-increasing pending order", "unexpected algo order",
    "protective stop cannot be verified", "margin ratio danger", "manual kill switch",
    "unrecognized fault", "reconciliation permanent failure", "LIVE writer lease lost",
])
async def test_safety_manual_and_unknown_halts_never_auto_recover(recovery_runtime, reason):
    runtime, clock = recovery_runtime
    await runtime.enter_halt(reason)
    clock[0] = 180
    await runtime._auto_recovery_check()
    assert runtime.governor.state == GovernorState.HALT
    assert not runtime.auto_recovery_status()["eligible"]
    runtime._reconcile_impl.assert_not_awaited()


async def test_emergency_never_auto_recovers(recovery_runtime):
    runtime, clock = recovery_runtime
    await runtime.enter_halt("reconciliation failed")
    runtime.governor.halt("protective stop missing", emergency=True)
    clock[0] = 180
    await runtime._auto_recovery_check()
    assert runtime.governor.state == GovernorState.EMERGENCY
    runtime._reconcile_impl.assert_not_awaited()


async def test_safety_halt_cannot_be_overwritten_by_transient_halt(recovery_runtime):
    runtime, clock = recovery_runtime
    await runtime.enter_halt("position mismatch")
    await runtime.enter_halt("WebSocket disconnected or stale")
    clock[0] = 180
    await runtime._auto_recovery_check()
    assert runtime.governor.reason == "position mismatch"
    assert not runtime.auto_recovery_status()["eligible"]


async def test_risk_engine_halt_cannot_be_overwritten_by_transient_halt(recovery_runtime):
    runtime, clock = recovery_runtime
    runtime.governor.halt("daily loss limit")
    await runtime.enter_halt("Redis unavailable")
    clock[0] = 180
    await runtime._auto_recovery_check()
    assert runtime.governor.reason == "daily loss limit"
    assert not runtime.auto_recovery_status()["eligible"]


async def test_auto_recovery_counter_resets_on_failed_check(recovery_runtime):
    runtime, clock = recovery_runtime
    await runtime.enter_halt("reconciliation failed")
    clock[0] = 30
    await runtime._auto_recovery_check()
    runtime._resume_health = AsyncMock(side_effect=[(False, ["websockets"]), (True, [])])
    clock[0] = 60
    await runtime._auto_recovery_check()
    assert runtime.auto_recovery_status()["successes"] == 0
    clock[0] = 90
    await runtime._auto_recovery_check()
    assert runtime.auto_recovery_status()["successes"] == 1


async def test_auto_recovery_requires_minimum_halt_duration_and_interval(recovery_runtime):
    runtime, clock = recovery_runtime
    await runtime.enter_halt("reconciliation failed")
    clock[0] = 29
    await runtime._auto_recovery_check()
    runtime._reconcile_impl.assert_not_awaited()
    clock[0] = 30
    await runtime._auto_recovery_check()
    await runtime._auto_recovery_check()
    assert runtime._reconcile_impl.await_count == 1


@pytest.mark.parametrize("issue", ["redis", "websockets", "entry_cancellation", "emergency_targets"])
async def test_auto_recovery_requires_all_dependencies(recovery_runtime, issue):
    runtime, clock = recovery_runtime
    await runtime.enter_halt("reconciliation failed")
    runtime._resume_health = AsyncMock(return_value=(False, [issue]))
    clock[0] = 30
    await runtime._auto_recovery_check()
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.auto_recovery_status()["successes"] == 0


async def test_final_reconciliation_failure_does_not_resume(recovery_runtime):
    runtime, clock = recovery_runtime
    await runtime.enter_halt("reconciliation failed")
    runtime._resume_health = AsyncMock(side_effect=[
        (True, []), (True, []), (True, []), (False, ["reconciliation"]),
    ])
    await three_checks_except_last(runtime, clock)
    clock[0] = 90
    await runtime._auto_recovery_check()
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.auto_recovery_status()["successes"] == 0


async def test_concurrent_reconciliations_are_serialized(recovery_runtime):
    runtime, _ = recovery_runtime
    entered = asyncio.Event()
    release = asyncio.Event()
    active = 0
    peak = 0

    async def reconcile_once():
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        entered.set()
        await release.wait()
        active -= 1

    runtime._reconcile_impl = AsyncMock(side_effect=reconcile_once)
    first = asyncio.create_task(runtime.reconcile())
    await asyncio.wait_for(entered.wait(), timeout=1)
    second = asyncio.create_task(runtime.reconcile())
    await asyncio.sleep(0)
    assert runtime._reconcile_impl.await_count == 1
    release.set()
    await asyncio.gather(first, second)
    assert peak == 1


async def three_checks_except_last(runtime: TradingRuntime, clock: list[float]) -> None:
    for moment in (30, 60):
        clock[0] = moment
        await runtime._auto_recovery_check()


async def test_repeated_auto_recovery_failures_trip_circuit_breaker(recovery_runtime):
    runtime, clock = recovery_runtime
    runtime.settings.auto_recovery_max_resumes_per_hour = 1
    await runtime.enter_halt("reconciliation failed")
    await three_checks(runtime, clock)
    assert runtime.governor.state == GovernorState.NORMAL
    await runtime.enter_halt("reconciliation failed")
    for moment in (120, 150, 180):
        clock[0] = moment
        await runtime._auto_recovery_check()
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.governor.reason == "auto recovery circuit breaker"
    assert runtime.auto_recovery_status()["circuit_breaker"]
    assert not runtime.auto_recovery_status()["eligible"]
    await runtime._auto_recovery_check()
    assert runtime.metrics.auto_recovery_circuit_breaker.labels(
        reason_class="TRANSIENT_INFRA"
    )._value.get() == 1


async def test_recovery_notification_waits_for_stability(recovery_runtime):
    runtime, clock = recovery_runtime
    runtime.alert = AsyncMock()
    await runtime.enter_halt("reconciliation failed")
    await three_checks(runtime, clock)
    assert not any(call.args[1] == "✅ Auto Recovery Completed" for call in runtime.alert.await_args_list)
    clock[0] = 150
    await runtime._auto_recovery_check()
    assert any(call.args[1] == "✅ Auto Recovery Completed" for call in runtime.alert.await_args_list)


async def test_rehalt_during_stability_suppresses_recovery_notification(recovery_runtime):
    runtime, clock = recovery_runtime
    runtime.alert = AsyncMock()
    await runtime.enter_halt("reconciliation failed")
    await three_checks(runtime, clock)
    clock[0] = 100
    await runtime.enter_halt("reconciliation failed")
    clock[0] = 160
    await runtime._auto_recovery_check()
    assert not any(call.args[1] == "✅ Auto Recovery Completed" for call in runtime.alert.await_args_list)


async def test_live_mode_never_auto_recovers(recovery_runtime):
    runtime, clock = recovery_runtime
    runtime.settings.mode = Mode.LIVE
    await runtime.enter_halt("reconciliation failed")
    clock[0] = 180
    await runtime._auto_recovery_check()
    assert runtime.governor.state == GovernorState.HALT
    assert not runtime.auto_recovery_status()["enabled"]


async def test_status_exposes_auto_recovery_without_removing_existing_fields(tmp_path):
    app = create_app(Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/status.db"
    ))
    runtime = app.state.runtime
    await runtime.store.initialize()
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    await runtime.enter_halt("reconciliation failed")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        response = await client.get("/status")
    assert response.status_code == 200
    body = response.json()
    assert body["auto_recovery"] == {
        "enabled": True, "eligible": True, "successes": 0,
        "required": 3, "circuit_breaker": False,
    }
    assert "websockets" in body and "protective_algos" in body
    await runtime.notifications.close()
    await runtime.client.close()
    await runtime.store.close()


def test_okx_diagnostic_fields_reject_untrusted_values():
    exc = OkxError(
        "ignored", code="KEY_MARKER", endpoint="/api/v5/account/balance?secret=1",
        operation="SECRET_MARKER", http_status=999, error_type="PASSPHRASE_MARKER",
    )
    assert safe_reconciliation_diagnostics(exc) == (
        "unknown", "unknown", "unknown", None, "OkxError"
    )


async def test_manual_resume_clears_circuit_breaker(recovery_runtime):
    runtime, _ = recovery_runtime
    await runtime.enter_halt("auto recovery circuit breaker")
    runtime._auto_recovery_circuit_breaker = True
    assert await runtime.resume()
    assert runtime.governor.state == GovernorState.NORMAL
    assert not runtime.auto_recovery_status()["circuit_breaker"]


async def test_full_resume_health_gate_checks_redis_ws_caa_and_blocked_symbols(
    recovery_runtime,
):
    runtime, _ = recovery_runtime

    class RedisProbe:
        value = ""

        async def ping(self):
            return True

        async def set(self, _key, value, ex):
            self.value = value

        async def get(self, _key):
            return self.value

    class Socket:
        _run_task = None

        def reconnect_stalled(self):
            return False

        connected = True

        private = False
        reconciliation_required = False

        def is_fresh(self):
            return True

        is_data_fresh = is_fresh
        is_processing_healthy = is_fresh
        def is_transport_healthy(self):
            return self.connected

    runtime.redis = RedisProbe()
    runtime.sockets = [Socket()]
    runtime.reconciliation_healthy = True
    runtime._last_reconcile_safe = True
    runtime.dead_man_healthy = True
    runtime.portfolio = PortfolioState(
        equity=Decimal("5000"), available_balance=Decimal("5000"), synchronized=True
    )
    del runtime._resume_health  # Use the real safety gate in this test.
    assert await runtime._resume_health() == (True, [])
    runtime.entry_controller.blocked.add("BTC-USDT-SWAP")
    assert "entry_cancellation" in (await runtime._resume_health())[1]
    runtime.entry_controller.blocked.clear()
    runtime.emergency.targets["BTC-USDT-SWAP"] = Decimal(0)
    assert "emergency_targets" in (await runtime._resume_health())[1]
    runtime.emergency.targets.clear()
    runtime.dead_man_healthy = False
    assert "cancel_all_after" in (await runtime._resume_health())[1]
    runtime.dead_man_healthy = True
    runtime.sockets[0].connected = False
    assert "websockets" in (await runtime._resume_health())[1]
    runtime.sockets[0].connected = True
    runtime.redis = None
    assert "redis" in (await runtime._resume_health())[1]


async def test_auto_recovery_requires_recent_caa_refresh(recovery_runtime):
    runtime, clock = recovery_runtime

    class HealthyRedis:
        async def ping(self):
            return True

        async def set(self, *_args, **_kwargs):
            return True

        async def get(self, _key):
            return "1"

    class FreshSocket:
        _run_task = None

        def reconnect_stalled(self):
            return False

        connected = True

        private = False
        reconciliation_required = False

        def is_fresh(self):
            return True

        is_data_fresh = is_fresh
        is_processing_healthy = is_fresh
        is_transport_healthy = is_fresh

    del runtime._resume_health
    runtime.running = True
    runtime.redis = HealthyRedis()
    runtime.sockets = [FreshSocket()]
    runtime.portfolio = PortfolioState(
        equity=Decimal("5000"), available_balance=Decimal("5000"), synchronized=True
    )
    runtime.reconciliation_healthy = True
    runtime._last_reconcile_safe = True
    runtime.dead_man_healthy = True
    runtime._last_caa_success_at = 0
    clock[0] = 45
    assert "cancel_all_after" in (await runtime._resume_health())[1]
    runtime._last_caa_success_at = 45
    assert await runtime._resume_health() == (True, [])


@pytest.mark.parametrize("operation", [
    "account", "positions", "pending_orders", "pending_algos", "account_config",
])
@pytest.mark.parametrize("code", ["50011", "50013"])
async def test_reconciliation_endpoint_transient_fault_then_auto_recovers(
    tmp_path, operation, code
):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/fault.db",
    )
    runtime = TradingRuntime(settings)
    await runtime.store.initialize()
    runtime.portfolio = PortfolioState(
        equity=Decimal("5000"), available_balance=Decimal("5000"), synchronized=True
    )
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    clock = [0.0]
    runtime._clock = lambda: clock[0]

    class HealthyRedis:
        async def ping(self):
            return True

        async def set(self, *_args, **_kwargs):
            return True

        async def get(self, _key):
            return "1"

    class FreshSocket:
        _run_task = None

        def reconnect_stalled(self):
            return False

        connected = True

        private = False
        reconciliation_required = False

        def is_fresh(self):
            return True

        is_data_fresh = is_fresh
        is_processing_healthy = is_fresh
        is_transport_healthy = is_fresh

    class Exchange:
        failed = False

        async def maybe_fail(self, name):
            if name == operation and not self.failed:
                self.failed = True
                endpoints = {
                    "account": "/api/v5/account/balance",
                    "positions": "/api/v5/account/positions",
                    "pending_orders": "/api/v5/trade/orders-pending",
                    "pending_algos": "/api/v5/trade/orders-algo-pending",
                    "account_config": "/api/v5/account/config",
                }
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(lambda _: httpx.Response(
                        200, json={"code": code, "msg": "temporary failure", "data": []}
                    )), base_url="https://openapi.okx.com",
                ) as http:
                    await OkxRestClient(settings, http).request("GET", endpoints[name])

        async def account(self):
            await self.maybe_fail("account")
            return [{"details": [{"ccy": "USDT", "eq": "5000", "availEq": "5000"}]}]

        async def positions(self):
            await self.maybe_fail("positions")
            return []

        async def pending_orders(self):
            await self.maybe_fail("pending_orders")
            return []

        async def pending_algos(self):
            await self.maybe_fail("pending_algos")
            return []

        async def account_config(self):
            await self.maybe_fail("account_config")
            return [{"posMode": "net_mode", "acctLv": "2"}]

    runtime.client = Exchange()
    runtime.redis = HealthyRedis()
    runtime.sockets = [FreshSocket()]
    runtime.dead_man_healthy = True
    await runtime.reconcile()
    assert runtime.governor.reason == "reconciliation failed"
    assert runtime.auto_recovery_status()["eligible"]
    await three_checks(runtime, clock)
    assert runtime.governor.state == GovernorState.NORMAL
    await runtime.store.close()


async def test_authentication_failure_remains_manual_only(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/auth.db",
    )
    runtime = TradingRuntime(settings)
    await runtime.store.initialize()
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)

    class Exchange:
        async def account(self):
            raise OkxError(
                "authentication failure", code="50105", retryable=False,
                operation="account", endpoint="/api/v5/account/balance", http_status=401,
            )

        async def positions(self):
            return []

        async def pending_orders(self):
            return []

        async def pending_algos(self):
            return []

    runtime.client = Exchange()
    await runtime.reconcile()
    assert runtime.governor.reason == "reconciliation permanent failure"
    assert not runtime.auto_recovery_status()["eligible"]
    await runtime.store.close()


async def test_account_read_timeout_enters_transient_recovery(recovery_runtime):
    runtime, _ = recovery_runtime
    del runtime._reconcile_impl

    class Exchange:
        async def account(self):
            raise TimeoutError("read timed out")

        async def positions(self):
            return []

        async def pending_orders(self):
            return []

        async def pending_algos(self):
            return []

    runtime.client = Exchange()
    await runtime.reconcile()
    assert runtime.governor.reason == "reconciliation failed"
    assert runtime.auto_recovery_status()["eligible"]


@pytest.mark.parametrize("status,body,expected", [
    (200, {"code": "50011", "msg": "rate limited", "data": []}, True),
    (200, {"code": "50013", "msg": "system busy", "data": []}, True),
    (503, {"code": "50026", "msg": "server error", "data": []}, True),
    (401, {"code": "50105", "msg": "bad passphrase", "data": []}, False),
    (200, {"code": "50113", "msg": "bad signature", "data": []}, False),
    (503, {"code": "50111", "msg": "invalid key", "data": []}, False),
    (400, {"code": "51000", "msg": "bad parameter", "data": []}, False),
])
async def test_okx_error_preserves_safe_diagnostics(status, body, expected):
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda _: httpx.Response(status, json=body)
    ), base_url="https://openapi.okx.com") as client:
        okx = OkxRestClient(Settings(
            _env_file=None, okx_api_key="test", okx_secret_key="test", okx_passphrase="test"
        ), client)
        with pytest.raises(OkxError) as caught:
            await okx.positions()
    exc = caught.value
    assert exc.code == body["code"]
    assert exc.http_status == status
    assert exc.operation == "positions"
    assert exc.endpoint == "/api/v5/account/positions"
    assert is_retryable_okx_error(exc) is expected


@pytest.mark.parametrize("method,path,status", [
    ("POST", "/api/v5/trade/order", 200),
    ("POST", "/api/v5/trade/cancel-order", 200),
    ("POST", "/api/v5/trade/order-algo", 200),
    ("POST", "/api/v5/trade/cancel-all-after", 200),
    ("POST", "/api/v5/account/positions", 200),
    ("GET", "/api/v5/market/candles", 200),
    ("GET", "/api/v5/account/positions", 401),
    ("GET", "/api/v5/account/positions", 403),
])
async def test_busy_code_does_not_expand_write_or_auth_recovery(method, path, status):
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(status, json={"code": "50013", "data": []})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="https://openapi.okx.com"
    ) as client:
        okx = OkxRestClient(Settings(_env_file=None), client)
        with pytest.raises(OkxError) as caught:
            await okx.request(method, path)
    assert not caught.value.retryable
    assert len(calls) == 1


@pytest.mark.parametrize("failure", [httpx.ConnectError("dns"), httpx.ReadTimeout("timeout")])
async def test_network_fault_is_retryable_for_read_reconciliation(failure):
    def fail(_: httpx.Request) -> httpx.Response:
        raise failure

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(fail), base_url="https://openapi.okx.com"
    ) as client:
        with pytest.raises(OkxError) as caught:
            await OkxRestClient(Settings(
                _env_file=None, okx_api_key="test", okx_secret_key="test", okx_passphrase="test"
            ), client).pending_orders()
    assert caught.value.operation == "pending_orders"
    assert caught.value.retryable
    assert caught.value.error_type == type(failure).__name__


async def test_okx_error_log_redacts_credentials_and_server_message(
    tmp_path, caplog
):
    markers = ("KEY_MARKER", "SECRET_MARKER", "PASSPHRASE_MARKER")
    settings = Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/redact.db",
        okx_api_key=markers[0], okx_secret_key=markers[1], okx_passphrase=markers[2],
    )
    runtime = TradingRuntime(settings)
    await runtime.store.initialize()
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={
            "code": "50011", "msg": " ".join(markers) + str(request.url), "data": [],
        })

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="https://openapi.okx.com"
    ) as client:
        runtime.client = OkxRestClient(settings, client)
        with caplog.at_level(logging.ERROR, logger="runtime"):
            await runtime.reconcile()
    assert runtime.governor.reason == "reconciliation failed"
    assert "operation=account" in caplog.text
    assert "code=50011" in caplog.text
    assert "http_status=429" in caplog.text
    assert all(marker not in caplog.text for marker in markers)
    assert "?" not in caplog.text
    await runtime.store.close()
