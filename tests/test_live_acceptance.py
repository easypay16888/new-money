from unittest.mock import AsyncMock

import httpx
import pytest

from app.api import create_app
from app.config import Mode, Settings
from app.monitoring import ConsoleNotification, Metrics
from app.notifications import NotificationManager
from app.okx import OkxError, OkxRestClient
from app.runtime import TradingRuntime
from app.storage import Store
from app.watchdog import WatchdogMonitor, WatchdogSettings


def live_settings(**changes):
    defaults = dict(mode=Mode.LIVE, live_trading_enabled=True,
                    confirm_live_account_id="account-A", okx_api_key="KEY_MARKER",
                    okx_secret_key="SECRET_MARKER", okx_passphrase="PASSPHRASE_MARKER",
                    api_token="t" * 32, status_api_token="s" * 32,
                    database_url="postgresql+asyncpg://quant_test@localhost/quant_live_acceptance_test",
                    live_lease_database_url="postgresql+asyncpg://quant_test@localhost/quant_live_acceptance_test")
    return Settings(_env_file=None, **(defaults | changes))


def account_config(**changes):
    return {"uid": "account-A", "posMode": "net_mode", "acctLv": "2",
            "perm": "read_only,trade", "ip": "192.0.2.1", **changes}


@pytest.mark.parametrize("field", ["okx_api_key", "okx_secret_key", "okx_passphrase", "api_token"])
def test_live_requires_complete_credentials_and_control_token(field):
    settings = live_settings().model_dump()
    settings[field] = ""
    with pytest.raises(ValueError):
        Settings(_env_file=None, **settings)


def test_live_validation_error_does_not_expose_credentials():
    settings = live_settings().model_dump()
    settings["api_token"] = ""
    with pytest.raises(ValueError) as caught:
        Settings(_env_file=None, **settings)
    assert all(marker not in str(caught.value) for marker in
               ("KEY_MARKER", "SECRET_MARKER", "PASSPHRASE_MARKER"))


@pytest.mark.parametrize("path", [
    "/api/v5/trade/order", "/api/v5/trade/order-algo", "/api/v5/trade/cancel-order",
    "/api/v5/trade/cancel-algos", "/api/v5/trade/cancel-all-after",
    "/api/v5/account/set-leverage",
])
async def test_all_live_writes_require_verified_account(path):
    requests = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda r: requests.append(r) or httpx.Response(200, json={"code": "0", "data": []})
    ), base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(live_settings(), http)
        with pytest.raises(OkxError):
            await client.request("POST", path, body={}, private=True)
    assert requests == []


@pytest.mark.parametrize("invalid", [
    {"uid": "other-account"}, {"uid": ""}, {"posMode": "long_short_mode"},
    {"acctLv": "1"}, {"perm": "read_only"}, {"perm": "read_only,trade,withdraw"},
    {"perm": ""}, {"ip": ""}, {"ip": "*"},
])
async def test_live_identity_and_permissions_fail_closed(invalid):
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={"code": "0", "data": [account_config(**invalid)]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                 base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(live_settings(), http)
        with pytest.raises(OkxError):
            await client.account_config()
        with pytest.raises(OkxError):
            await client.cancel_all_after(60)
    assert [(r.method, r.url.path) for r in requests] == [("GET", "/api/v5/account/config")]


async def test_verified_live_account_allows_write_without_simulation_header():
    requests = []

    def respond(request):
        requests.append(request)
        data = [account_config()] if request.method == "GET" else []
        return httpx.Response(200, json={"code": "0", "data": data})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                 base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(live_settings(), http)
        await client.account_config()
        client.live_writer_guard = AsyncMock(return_value=True)
        await client.cancel_all_after(60)
    assert [r.method for r in requests] == ["GET", "POST"]
    assert all("x-simulated-trading" not in r.headers for r in requests)


async def test_credential_change_invalidates_live_write_verification():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(
        200, json={"code": "0", "data": [account_config()]}
    )), base_url="https://openapi.okx.com") as http:
        settings = live_settings()
        client = OkxRestClient(settings, http)
        await client.account_config()
        settings.okx_api_key = "OTHER_KEY"
        with pytest.raises(OkxError):
            await client.cancel_all_after(60)


async def test_live_startup_verifies_identity_before_restoring_emergency(tmp_path):
    runtime = TradingRuntime(live_settings())
    runtime.store.initialize = AsyncMock()
    runtime.order_manager.restore = AsyncMock()
    runtime.emergency.restore = AsyncMock()
    runtime.client.account_config = AsyncMock(side_effect=OkxError("identity rejected"))
    try:
        with pytest.raises(OkxError):
            await runtime.initialize()
        runtime.order_manager.restore.assert_not_awaited()
        runtime.emergency.restore.assert_not_awaited()
        assert not runtime.running
    finally:
        await runtime.client.close()
        await runtime.store.close()


async def test_live_startup_requires_manual_resume_even_after_valid_identity(tmp_path, monkeypatch):
    runtime = TradingRuntime(live_settings())
    runtime.store.initialize = AsyncMock()
    runtime.store.bind_live_account = AsyncMock()
    monkeypatch.setattr("app.live_lease.LiveRuntimeLease.acquire", AsyncMock())
    monkeypatch.setattr("app.live_lease.LiveRuntimeLease.verify", AsyncMock(return_value=True))
    runtime.client.account_config = AsyncMock(return_value=[account_config()])
    runtime.order_manager.restore = AsyncMock(side_effect=RuntimeError("test stop after startup gates"))
    try:
        with pytest.raises(RuntimeError, match="test stop after startup gates"):
            await runtime.initialize()
        assert runtime.governor.state.value == "HALT"
        assert runtime.governor.reason == "LIVE startup requires manual resume"
        assert not runtime.auto_recovery_status()["eligible"]
    finally:
        await runtime.client.close()
        await runtime.store.close()


@pytest.mark.parametrize("table,payload", [
    ("system_events", {"event": "start", "mode": "PAPER"}),
    ("system_events", {"event": "start"}),
    ("orders", {"clOrdId": "demo-entry"}),
    ("emergency_targets", {"symbol": "BTC-USDT-SWAP", "target": "0"}),
    ("portfolio_snapshots", {"equity": "5000"}),
])
async def test_live_cannot_bind_demo_or_unbound_trading_history(tmp_path, table, payload):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/binding.db")
    try:
        await store.initialize()
        await store.append(table, payload)
        with pytest.raises(ValueError):
            await store.bind_live_account("account-A")
        assert await store.latest("account_bindings") == []
    finally:
        await store.close()


async def test_live_ledger_binding_is_stable_and_rejects_another_account(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/binding.db")
    try:
        await store.initialize()
        await store.bind_live_account("account-A")
        await store.append("orders", {"clOrdId": "live-entry"})
        await store.bind_live_account("account-A")
        rows = await store.latest("account_bindings")
        assert len(rows) == 1
        assert "account-A" not in str(rows)
        with pytest.raises(ValueError):
            await store.bind_live_account("account-B")
        await store.append("system_events", {"event": "start", "mode": "PAPER"})
        with pytest.raises(ValueError):
            await store.bind_live_account("account-A")
    finally:
        await store.close()


@pytest.mark.parametrize("path", [
    "/system/start", "/system/stop", "/system/halt", "/system/resume", "/orders/cancel-all",
])
async def test_readonly_status_token_cannot_control_live_runtime(path, tmp_path):
    app = create_app(live_settings(
        status_api_token="s" * 32
    ))
    runtime = app.state.runtime
    runtime.start = AsyncMock()
    runtime.stop = AsyncMock()
    runtime.enter_halt = AsyncMock()
    runtime.resume = AsyncMock()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://local") as http:
            headers = {"Authorization": "Bearer " + "s" * 32}
            assert (await http.get("/status", headers=headers)).status_code == 200
            assert (await http.post(path, headers=headers)).status_code == 401
            assert (await http.get("/orders", headers=headers)).status_code == 401
        for method in (runtime.start, runtime.stop, runtime.enter_halt, runtime.resume):
            method.assert_not_awaited()
    finally:
        await runtime.client.close()
        await runtime.store.close()


async def test_watchdog_uses_only_readonly_token_and_status_endpoint():
    seen = []
    def respond(request):
        seen.append(request)
        return httpx.Response(200, json={"running": True, "synchronized": True,
            "risk_state": "NORMAL", "websockets": [{"fresh": True, "connected": True}]})
    settings = WatchdogSettings(_env_file=None, watchdog_status_url="http://app:8000/status",
                                watchdog_status_token="s" * 32)
    notifier = NotificationManager([ConsoleNotification()], Metrics())
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        monitor = WatchdogMonitor(settings, http, notifier)
        assert (await monitor._probe())[0] is None
    assert [(r.method, r.url.path) for r in seen] == [("GET", "/status")]
    assert seen[0].headers["Authorization"] == "Bearer " + "s" * 32
    assert "s" * 32 not in repr(settings)


async def test_failed_reverification_revokes_live_write_permission():
    responses = [account_config(), account_config(uid="other-account")]
    requests = []
    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={"code": "0", "data": [responses.pop(0)]})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                 base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(live_settings(), http)
        await client.account_config()
        with pytest.raises(OkxError):
            await client.account_config()
        with pytest.raises(OkxError):
            await client.cancel_all_after(60)
    assert all(r.method == "GET" for r in requests)


async def test_ongoing_identity_refresh_does_not_block_verified_emergency_write():
    import asyncio
    entered, release = asyncio.Event(), asyncio.Event()
    refreshing = False
    requests = []
    async def respond(request):
        requests.append(request)
        if request.method == "GET" and refreshing:
            entered.set()
            await release.wait()
        data = [account_config()] if request.method == "GET" else []
        return httpx.Response(200, json={"code": "0", "data": data})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                 base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(live_settings(), http)
        await client.account_config()
        refreshing = True
        task = asyncio.create_task(client.account_config())
        await entered.wait()
        try:
            await client.request("POST", "/api/v5/trade/order", private=True,
                                 body={"reduceOnly": "true"})
        finally:
            release.set()
            await task
    assert requests[-1].method == "POST"


@pytest.mark.parametrize("field", ["api_token", "status_api_token"])
def test_live_tokens_reject_short_values(field):
    values = live_settings().model_dump()
    values[field] = "short"
    with pytest.raises(ValueError):
        Settings(_env_file=None, **values)


@pytest.mark.parametrize("status,code,can_reduce", [(503, "50001", True), (401, "50105", False)])
async def test_account_read_failure_preserves_only_previously_verified_emergency_access(
    status, code, can_reduce
):
    reads = 0
    writes = []
    def respond(request):
        nonlocal reads
        if request.method == "POST":
            writes.append(request)
            return httpx.Response(200, json={"code": "0", "data": []})
        reads += 1
        if reads == 1:
            return httpx.Response(200, json={"code": "0", "data": [account_config()]})
        return httpx.Response(status, json={"code": code, "data": []})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                 base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(live_settings(), http)
        await client.account_config()
        with pytest.raises(OkxError):
            await client.account_config()
        if can_reduce:
            await client.request("POST", "/api/v5/trade/order", private=True,
                                 body={"reduceOnly": "true"})
        else:
            with pytest.raises(OkxError):
                await client.request("POST", "/api/v5/trade/order", private=True,
                                     body={"reduceOnly": "true"})
    assert bool(writes) is can_reduce


@pytest.mark.parametrize("url", ["sqlite+aiosqlite:///:memory:", "sqlite:///secret.db",
                                 "postgresql://user:DB_SECRET@localhost/live",
                                 "mysql+aiomysql://user:DB_SECRET@localhost/live"])
def test_live_requires_postgresql_asyncpg_and_redacts_url(url):
    with pytest.raises(ValueError, match="LIVE requires PostgreSQL storage") as caught:
        live_settings(database_url=url)
    assert "DB_SECRET" not in str(caught.value) and url not in str(caught.value)


@pytest.mark.parametrize("status", ["", " " * 40, "t" * 32, "  " + "t" * 32 + "  "])
def test_live_requires_dedicated_status_token_after_trim(status):
    with pytest.raises(ValueError):
        live_settings(status_api_token=status)


def test_live_trims_tokens_and_requires_explicit_shared_coordinator():
    settings = live_settings(api_token=" " + "t" * 32 + " ", status_api_token=" " + "s" * 32)
    assert settings.api_token == "t" * 32 and settings.status_api_token == "s" * 32
    with pytest.raises(ValueError, match="shared PostgreSQL"):
        live_settings(live_lease_database_url="")


@pytest.mark.parametrize("field", ["okx_api_key", "okx_secret_key", "okx_passphrase",
                                 "okx_rest_url", "confirm_live_account_id"])
async def test_live_identity_mutations_block_all_writes_before_http(field):
    requests = []
    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={"code": "0", "data": [account_config()]})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                 base_url="https://openapi.okx.com") as http:
        settings = live_settings()
        client = OkxRestClient(settings, http)
        await client.account_config()
        setattr(settings, field, "changed")
        with pytest.raises(OkxError):
            await client.cancel_all_after(60)
    assert len(requests) == 1


@pytest.mark.parametrize("invalid", [{"uid": "account-B"}, {"perm": "read_only"},
    {"perm": "read_only,trade,withdraw"}, {"ip": ""}])
async def test_live_reverification_revokes_prior_grant(invalid):
    reads = [account_config(), account_config(**invalid)]
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(
        200, json={"code": "0", "data": [reads.pop(0)]}
    )), base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(live_settings(), http)
        await client.account_config()
        with pytest.raises(OkxError):
            await client.account_config()
        with pytest.raises(OkxError):
            await client.cancel_all_after(60)


@pytest.mark.parametrize("status", [401, 403])
async def test_live_auth_rejection_on_any_endpoint_revokes_grant(status):
    reads = 0
    def respond(request):
        nonlocal reads
        reads += 1
        if reads == 1:
            return httpx.Response(200, json={"code": "0", "data": [account_config()]})
        return httpx.Response(status, text="AUTH_SECRET_MARKER")
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                 base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(live_settings(), http)
        await client.account_config()
        with pytest.raises(OkxError):
            await client.account()
        with pytest.raises(OkxError):
            await client.cancel_all_after(60)
    assert reads == 2


async def test_verified_identity_entry_requires_writer_and_normal_but_reduction_does_not():
    writes = []
    def respond(request):
        if request.method == "POST":
            writes.append(request)
        return httpx.Response(200, json={"code": "0", "data":
                                      [account_config()] if request.method == "GET" else []})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                 base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(live_settings(), http)
        await client.account_config()
        with pytest.raises(OkxError):
            await client.request("POST", "/api/v5/trade/order", body={"reduceOnly": False})
        client.live_writer_guard = AsyncMock(return_value=True)
        await client.request("POST", "/api/v5/trade/order", body={"reduceOnly": False})
        client.live_writer_guard = AsyncMock(return_value=False)
        with pytest.raises(OkxError):
            await client.request("POST", "/api/v5/trade/order", body={"reduceOnly": False})
        await client.request("POST", "/api/v5/trade/order", body={"reduceOnly": True})
        await client.request("POST", "/api/v5/trade/order-algo", body={"reduceOnly": "true"})
        with pytest.raises(OkxError):
            await client.cancel_all_after(60)
    assert len(writes) == 3


async def test_identity_change_during_lease_probe_is_rechecked_before_http():
    writes = []
    def respond(request):
        if request.method == "POST":
            writes.append(request)
        return httpx.Response(200, json={"code": "0", "data": [account_config()]})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                 base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(live_settings(), http)
        await client.account_config()
        async def probe(entry):
            client.settings.okx_secret_key = "changed"
            return True
        client.live_writer_guard = probe
        with pytest.raises(OkxError):
            await client.request("POST", "/api/v5/trade/order", body={"reduceOnly": False})
    assert writes == []


async def test_status_live_lease_exposes_only_required_and_held():
    app = create_app(live_settings())
    runtime = app.state.runtime
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://local") as http:
            response = await http.get("/status", headers={"Authorization": "Bearer " + "s" * 32})
            assert response.json()["live_writer_lease"] == {"required": True, "held": False}
            assert "account-A" not in response.text and "account_digest" not in response.text
            assert "advisory" not in response.text and "postgresql+asyncpg" not in response.text
    finally:
        await runtime.close()


@pytest.mark.parametrize("field", ["okx_api_key", "okx_rest_url"])
async def test_reverting_identity_change_does_not_restore_cached_grant(field):
    requests = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: requests.append(r) or
        httpx.Response(200, json={"code": "0", "data": [account_config()]})
    ), base_url="https://openapi.okx.com") as http:
        settings = live_settings()
        client = OkxRestClient(settings, http)
        await client.account_config()
        original = getattr(settings, field)
        setattr(settings, field, "changed")
        with pytest.raises(OkxError):
            await client.cancel_all_after(60)
        setattr(settings, field, original)
        with pytest.raises(OkxError):
            await client.cancel_all_after(60)
    assert len(requests) == 1


async def test_new_uid_cannot_be_reverified_into_an_existing_live_runtime():
    reads = [account_config(), account_config(uid="account-B")]
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(
        200, json={"code": "0", "data": [reads.pop(0)]}
    )), base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(live_settings(), http)
        await client.account_config()
        client.settings.confirm_live_account_id = "account-B"
        with pytest.raises(OkxError):
            await client.account_config()
        with pytest.raises(OkxError):
            await client.cancel_all_after(60)


async def test_unknown_write_method_cannot_bypass_lease_with_reduce_only_field():
    requests = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: requests.append(r) or
        httpx.Response(200, json={"code": "0", "data": [account_config()]})
    ), base_url="https://openapi.okx.com") as http:
        client = OkxRestClient(live_settings(), http)
        await client.account_config()
        with pytest.raises(OkxError):
            await client.request("PUT", "/api/v5/trade/order", body={"reduceOnly": True})
    assert len(requests) == 1
