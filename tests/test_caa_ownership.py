"""Account-global CAA fencing uses real PostgreSQL leases and fake OKX HTTP."""
import asyncio
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from app.config import Mode, Settings
from app.live_lease import LiveLeaseError, LiveRuntimeLease
from app.models import GovernorState
from app.okx import OkxError, OkxRestClient
from app.runtime import TradingRuntime
from tests.test_live_acceptance import account_config, live_settings
from tests.test_live_lease import coordinator_url
from tests.test_live_lease import postgres_store as postgres_store


@pytest.fixture
async def live_owner(postgres_store):
    url = coordinator_url(postgres_store)
    runtime = TradingRuntime(live_settings(
        database_url=url, live_lease_database_url=url, cancel_all_after_refresh_seconds=1,
    ))
    requests = []
    pending = []
    refreshed = asyncio.Event()

    def respond(request):
        requests.append(request)
        data = []
        if request.url.path == "/api/v5/account/config":
            data = [account_config()]
        elif request.url.path == "/api/v5/trade/orders-pending":
            data = list(pending)
        elif request.url.path == "/api/v5/trade/cancel-all-after":
            refreshed.set()
        return httpx.Response(200, json={"code": "0", "data": data})

    await runtime.client.close()
    runtime.client.client = httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                            base_url="https://openapi.okx.com")
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    runtime.alert = AsyncMock()
    runtime.live_lease = LiveRuntimeLease(url, "account-A", runtime._lease_lost)
    try:
        await runtime.client.account_config()
        await runtime.live_lease.acquire()
        yield runtime, requests, pending, refreshed
    finally:
        runtime.running = False
        await runtime.close()


def caa_requests(requests):
    return [r for r in requests if r.method == "POST"
            and r.url.path == "/api/v5/trade/cancel-all-after"]


async def drop_lease(runtime):
    raw = await runtime.live_lease._connection.get_raw_connection()
    await raw.driver_connection.close()
    assert not await runtime.live_lease.verify()
    await runtime._lease_loss_task


@pytest.mark.parametrize("timeout", [60, 0])
async def test_live_owner_can_refresh_and_disable_caa_even_while_halted(live_owner, timeout):
    runtime, requests, _, _ = live_owner
    assert runtime.governor.state == GovernorState.HALT
    await runtime.client.cancel_all_after(timeout)
    writes = caa_requests(requests)
    assert len(writes) == 1
    assert json.loads(writes[0].content) == {"timeOut": str(timeout)}


@pytest.mark.parametrize("timeout", [60, 0])
async def test_live_lease_loss_fences_direct_caa_before_http(live_owner, timeout):
    runtime, requests, _, _ = live_owner
    await runtime.client.cancel_all_after(60)
    await drop_lease(runtime)
    requests.clear()
    with pytest.raises(OkxError):
        await runtime.client.cancel_all_after(timeout)
    assert caa_requests(requests) == []
    assert runtime.governor.reason == "LIVE writer lease lost"


async def test_dead_man_loop_exits_after_lease_loss_without_false_caa_incident(live_owner):
    runtime, requests, _, refreshed = live_owner
    runtime.running = True
    loop = asyncio.create_task(runtime._dead_man_loop())
    try:
        await asyncio.wait_for(refreshed.wait(), 3)
        assert len(caa_requests(requests)) == 1
        await drop_lease(runtime)
        # Exiting after the next one-second refresh interval must produce no further CAA.
        await asyncio.wait_for(asyncio.shield(loop), 3)
        assert len(caa_requests(requests)) == 1
        assert runtime.governor.reason == "LIVE writer lease lost"
        assert not runtime.dead_man_healthy
        assert not any("dead man switch unavailable" in str(call)
                       for call in runtime.alert.await_args_list)
    finally:
        runtime.running = False
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)


async def test_stale_owner_safe_stop_cannot_disable_caa_for_retained_exit(live_owner, caplog):
    runtime, requests, pending, _ = live_owner
    await drop_lease(runtime)
    pending.append({"instId": "BTC-USDT-SWAP", "reduceOnly": "true", "clOrdId": "q-exit"})
    runtime.running = True

    async def confirmed_reconciliation():
        runtime.reconciliation_healthy = True

    runtime.reconcile = AsyncMock(side_effect=confirmed_reconciliation)
    requests.clear()
    with pytest.raises(RuntimeError):
        await runtime._safe_stop()
    assert caa_requests(requests) == []
    assert runtime.running  # Preserve safety tasks when shutdown is refused.
    assert "CAA disable skipped: LIVE writer lease not owned" in caplog.text
    assert runtime.governor.reason == "LIVE writer lease lost"
    with pytest.raises(LiveLeaseError):
        await runtime.live_lease.acquire()


async def test_split_brain_only_new_owner_can_mutate_caa(live_owner, postgres_store):
    a, requests_a, pending_a, _ = live_owner
    url = coordinator_url(postgres_store)
    b = TradingRuntime(live_settings(database_url=url, live_lease_database_url=url))
    b.entry_controller.cancel_all = AsyncMock(return_value=True)
    b.alert = AsyncMock()
    requests_b = []

    def respond_b(request):
        requests_b.append(request)
        data = [account_config()] if request.method == "GET" else []
        return httpx.Response(200, json={"code": "0", "data": data})

    await b.client.close()
    b.client.client = httpx.AsyncClient(transport=httpx.MockTransport(respond_b),
                                      base_url="https://openapi.okx.com")
    b.live_lease = LiveRuntimeLease(url, "account-A", b._lease_lost)
    try:
        await a.client.cancel_all_after(60)
        await b.client.account_config()
        with pytest.raises(LiveLeaseError):
            await b.live_lease.acquire()
        with pytest.raises(OkxError):
            await b.client.cancel_all_after(60)
        assert caa_requests(requests_b) == []
        await drop_lease(a)
        with pytest.raises(LiveLeaseError):
            await b.live_lease.acquire()  # No automatic reuse/takeover of a failed contender.
        b.live_lease = LiveRuntimeLease(url, "account-A", b._lease_lost)
        await b.live_lease.acquire()  # Explicitly start a new ownership session.
        assert await b.live_lease.verify()
        requests_a.clear()
        for timeout in (60, 0):
            with pytest.raises(OkxError):
                await a.client.cancel_all_after(timeout)
        pending_a.append({"instId": "BTC-USDT-SWAP", "reduceOnly": "true"})
        a.running = True
        a.reconciliation_healthy = True
        a.reconcile = AsyncMock()
        with pytest.raises(RuntimeError, match="LIVE writer ownership"):
            await a._safe_stop()
        assert await b.live_lease.verify()
        await b.client.cancel_all_after(60)
        assert caa_requests(requests_a) == []
        assert len(caa_requests(requests_b)) == 1
        assert json.loads(caa_requests(requests_b)[0].content) == {"timeOut": "60"}
        assert await b.live_lease.verify()
    finally:
        await b.close()


@pytest.mark.parametrize("order_type", ["market", "limit"])
async def test_lease_loss_retains_emergency_protective_and_owned_cancels(live_owner, order_type):
    runtime, requests, _, _ = live_owner
    await drop_lease(runtime)
    requests.clear()
    await runtime.client.place_order({
        "instId": "BTC-USDT-SWAP", "side": "sell", "ordType": order_type,
        "sz": "1", "reduceOnly": True,
    })
    await runtime.client.place_algo({
        "instId": "BTC-USDT-SWAP", "side": "sell", "sz": "1",
        "reduceOnly": "true", "slTriggerPx": "49000", "slOrdPx": "-1",
    })
    await runtime.client.cancel_order("BTC-USDT-SWAP", client_order_id="q-owned-entry")
    await runtime.client.cancel_algo("BTC-USDT-SWAP", client_algo_id="a-owned-stop")
    assert len(requests) == 4
    for action in (
        lambda: runtime.client.place_order({"reduceOnly": False}),
        lambda: runtime.client.place_algo({"reduceOnly": False}),
        lambda: runtime.client.set_leverage("BTC-USDT-SWAP", 1),
        lambda: runtime.client.cancel_all_after(60), lambda: runtime.client.cancel_all_after(0),
    ):
        with pytest.raises(OkxError):
            await action()
    assert len(requests) == 4  # None of the prohibited calls reach HTTP.


@pytest.mark.parametrize("body", [{}, {"timeOut": "-1"}, {"timeOut": "secret"},
                                  {"timeOut": True}, {"timeOut": 0.5}])
async def test_live_caa_invalid_timeout_fails_before_http(live_owner, body):
    runtime, requests, _, _ = live_owner
    requests.clear()
    with pytest.raises(OkxError, match="invalid timeout"):
        await runtime.client.request("POST", "/api/v5/trade/cancel-all-after", body=body)
    assert requests == []


async def test_reduce_only_field_cannot_bypass_caa_ownership(live_owner):
    runtime, requests, _, _ = live_owner
    await drop_lease(runtime)
    requests.clear()
    with pytest.raises(OkxError):
        await runtime.client.request("POST", "/api/v5/trade/cancel-all-after",
                                     body={"timeOut": "0", "reduceOnly": True})
    assert requests == []


async def test_dead_man_lease_loss_between_precheck_and_rest_gate_is_not_endpoint_failure(
    live_owner, monkeypatch,
):
    runtime, requests, _, _ = live_owner
    verify = runtime.live_lease.verify
    probes = 0

    async def lose_at_rest_guard():
        nonlocal probes
        probes += 1
        if probes == 2:
            await drop_lease(runtime)
        return await verify()

    # drop_lease itself calls verify; at probe 3 it observes the closed real PG session.
    monkeypatch.setattr(runtime.live_lease, "verify", lose_at_rest_guard)
    runtime.running = True
    await runtime._dead_man_loop()
    assert caa_requests(requests) == []
    assert runtime.governor.reason == "LIVE writer lease lost"
    assert not any("dead man switch unavailable" in str(call)
                   for call in runtime.alert.await_args_list)


async def test_owner_caa_endpoint_failure_still_reports_dead_man_unavailable(live_owner):
    runtime, requests, _, _ = live_owner
    runtime.governor.resume(synchronized=True, healthy=True)

    def fail_caa(request):
        requests.append(request)
        runtime.running = False  # One failing heartbeat iteration, no retry/infinite loop.
        return httpx.Response(500, json={"code": "50001", "data": []})

    await runtime.client.close()
    runtime.client.client = httpx.AsyncClient(transport=httpx.MockTransport(fail_caa),
                                            base_url="https://openapi.okx.com")
    runtime.running = True
    await runtime._dead_man_loop()
    assert len(caa_requests(requests)) == 1
    assert runtime.governor.reason == "dead man switch unavailable"
    assert await runtime.live_lease.verify()
    runtime._component_health["Cancel-All-After"] = True
    await runtime._observe_infrastructure()
    assert any("CAA Unavailable" in str(call) for call in runtime.alert.await_args_list)


async def test_lost_owner_safe_stop_checks_pending_risk_and_emergency_before_exiting(live_owner):
    runtime, requests, pending, _ = live_owner
    await drop_lease(runtime)
    runtime.reconcile = AsyncMock()
    runtime.reconciliation_healthy = True
    runtime.running = True
    pending.append({"instId": "BTC-USDT-SWAP", "reduceOnly": "false"})
    with pytest.raises(RuntimeError, match="risk-increasing entry"):
        await runtime._safe_stop()
    assert runtime.running and not caa_requests(requests)
    pending.clear()
    runtime.emergency.targets["BTC-USDT-SWAP"] = 0
    with pytest.raises(RuntimeError, match="cancellation or protection"):
        await runtime._safe_stop()
    assert runtime.running and not caa_requests(requests)
    assert runtime.governor.reason == "LIVE writer lease lost"


@pytest.mark.parametrize("timeout", [60, 0])
async def test_paper_caa_does_not_require_live_lease(timeout):
    settings = Settings(_env_file=None, mode=Mode.PAPER, okx_api_key="demo-test",
                        okx_secret_key="demo-test", okx_passphrase="demo-test")
    requests = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: requests.append(r) or
        httpx.Response(200, json={"code": "0", "data": []})
    ), base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(settings, http)
        await client.cancel_all_after(timeout)
    assert len(caa_requests(requests)) == 1
    assert requests[0].headers["x-simulated-trading"] == "1"


async def test_paper_dead_man_loop_unchanged_without_lease():
    runtime = TradingRuntime(Settings(_env_file=None, database_url="sqlite+aiosqlite:///:memory:",
        okx_api_key="demo-test", okx_secret_key="demo-test", okx_passphrase="demo-test",
        cancel_all_after_refresh_seconds=1))
    requests = []
    refreshed = asyncio.Event()

    def respond(request):
        requests.append(request)
        refreshed.set()
        return httpx.Response(200, json={"code": "0", "data": []})

    await runtime.client.close()
    runtime.client.client = httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                            base_url="https://openapi.okx.com")
    runtime.running = True
    loop = asyncio.create_task(runtime._dead_man_loop())
    try:
        await asyncio.wait_for(refreshed.wait(), 3)
        runtime.running = False
        await asyncio.wait_for(asyncio.shield(loop), 3)
        assert len(caa_requests(requests)) == 1
        assert runtime.dead_man_healthy and runtime._last_caa_success_at is not None
        assert runtime.live_lease is None
    finally:
        runtime.running = False
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)
        await runtime.close()


async def test_ownership_loss_does_not_emit_caa_infrastructure_outage(live_owner):
    runtime, _, _, _ = live_owner
    runtime._component_health["Cancel-All-After"] = True
    await runtime.client.cancel_all_after(60)
    await drop_lease(runtime)
    runtime.alert.reset_mock()
    await runtime._observe_infrastructure()
    assert not any("CAA Unavailable" in str(call) for call in runtime.alert.await_args_list)
    assert runtime.governor.reason == "LIVE writer lease lost"


async def test_live_owner_safe_stop_can_disable_caa_and_release_lease(live_owner):
    runtime, requests, pending, _ = live_owner
    pending.append({"instId": "BTC-USDT-SWAP", "reduceOnly": "true"})
    runtime.reconciliation_healthy = True
    runtime.reconcile = AsyncMock()
    runtime.running = True
    await runtime._safe_stop()
    assert not runtime.running and not runtime.live_lease.held
    assert [json.loads(r.content) for r in caa_requests(requests)] == [{"timeOut": "0"}]


async def test_live_dead_man_missing_lease_fails_closed_without_http(live_owner):
    runtime, requests, _, _ = live_owner
    await runtime.live_lease.close()
    runtime.live_lease = None
    runtime.running = True
    await runtime._dead_man_loop()
    await runtime._lease_loss_task
    assert not caa_requests(requests)
    assert runtime.governor.reason == "LIVE writer lease lost"
    assert not runtime.auto_recovery_status()["eligible"]


async def test_shutdown_lease_loss_at_final_rest_gate_blocks_disable(live_owner, monkeypatch, caplog):
    runtime, requests, pending, _ = live_owner
    pending.append({"instId": "BTC-USDT-SWAP", "reduceOnly": "true"})
    runtime.reconciliation_healthy = True
    runtime.reconcile = AsyncMock()
    verify = runtime.live_lease.verify
    probes = 0

    async def lose_at_disable_guard():
        nonlocal probes
        probes += 1
        if probes == 3:  # initial stop check, CAA lock check, final REST ownership gate
            await drop_lease(runtime)
        return await verify()

    monkeypatch.setattr(runtime.live_lease, "verify", lose_at_disable_guard)
    runtime.running = True
    with pytest.raises(RuntimeError, match="LIVE writer ownership"):
        await runtime.stop()
    assert runtime.running and not runtime._caa_shutting_down
    assert caa_requests(requests) == []
    assert runtime.governor.reason == "LIVE writer lease lost"
    assert "CAA disable skipped: LIVE writer lease not owned" in caplog.text
