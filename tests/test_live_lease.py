"""Real PostgreSQL ownership and atomic ledger tests, on an isolated database only."""
import asyncio
import os
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import delete, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from app.live_lease import LiveLeaseError, LiveRuntimeLease, lease_key
from app.models import GovernorState, NotificationPriority
from app.runtime import TradingRuntime
from app.storage import ROW_TYPES, Store
from tests.test_live_acceptance import account_config, live_settings


@pytest.fixture
async def postgres_store():
    url = os.environ.get("TEST_POSTGRES_URL")
    assert url, "TEST_POSTGRES_URL is required: isolated PostgreSQL concurrency tests cannot skip"
    parsed = make_url(url)
    assert parsed.drivername == "postgresql+asyncpg"
    assert parsed.database == "quant_live_acceptance_test"
    assert parsed.username == "quant_test" and parsed.host in {"127.0.0.1", "localhost"}
    store = Store(url)
    await store.initialize()
    async with store.sessions.begin() as session:
        for table in ROW_TYPES.values():
            await session.execute(delete(table))
    try:
        yield store
    finally:
        await store.close()


def coordinator_url(store):
    return store.engine.url.render_as_string(hide_password=False)


def test_lease_key_stable_signed_and_namespaced():
    assert lease_key("test-account") == -5138848912247500684
    assert -(1 << 63) <= lease_key("other-account") < (1 << 63)


async def test_postgres_single_account_contends_and_graceful_release(postgres_store):
    url = coordinator_url(postgres_store)
    a = LiveRuntimeLease(url, "account-A", lambda: None)
    b = LiveRuntimeLease(url, "account-A", lambda: None)
    try:
        await a.acquire()
        assert await a.verify()
        with pytest.raises(LiveLeaseError):
            await b.acquire()
        assert not b.held and await a.verify()
        await a.close()
        # A failed contender never takes over by itself; the operator starts a new session.
        b = LiveRuntimeLease(url, "account-A", lambda: None)
        await b.acquire()
        assert await b.verify()
    finally:
        await a.close()
        await b.close()


async def test_postgres_connection_drop_releases_and_latches_loss(postgres_store):
    lost = asyncio.Event()
    url = coordinator_url(postgres_store)
    a = LiveRuntimeLease(url, "account-A", lost.set)
    b = LiveRuntimeLease(url, "account-A", lambda: None)
    try:
        await a.acquire()
        raw = await a._connection.get_raw_connection()
        await raw.driver_connection.close()
        await asyncio.wait_for(lost.wait(), 2)
        assert not await a.verify()
        with pytest.raises(LiveLeaseError, match="cannot be reacquired"):
            await a.acquire()
        await b.acquire()
        assert await b.verify()
    finally:
        await a.close()
        await b.close()


async def test_postgres_lock_removed_without_disconnect_detects_loss(postgres_store):
    lost = asyncio.Event()
    a = LiveRuntimeLease(coordinator_url(postgres_store), "account-A", lost.set)
    try:
        await a.acquire()
        await a._connection.execute(text("SELECT pg_advisory_unlock_all()"))
        await a._connection.commit()
        assert not await a.verify() and lost.is_set()
    finally:
        await a.close()


async def test_postgres_atomic_binding_race_exactly_one_winner(postgres_store):
    second = Store(coordinator_url(postgres_store))
    try:
        results = await asyncio.gather(postgres_store.bind_live_account("account-A"),
                                       second.bind_live_account("account-B"),
                                       return_exceptions=True)
        assert sum(result is None for result in results) == 1
        assert sum(isinstance(result, ValueError) for result in results) == 1
        assert len(await postgres_store.latest("account_bindings")) == 1
    finally:
        await second.close()


async def test_postgres_binding_unique_constraint_prevents_conflicting_rows(postgres_store):
    await postgres_store.bind_live_account("account-A")
    with pytest.raises(IntegrityError):
        await postgres_store.append("account_bindings", {"mode": "LIVE", "account_digest": "other"})
    assert len(await postgres_store.latest("account_bindings")) == 1


@pytest.mark.parametrize("table,payload", [
    ("system_events", {"event": "start", "mode": "PAPER"}),
    ("orders", {"clOrdId": "demo"}), ("fills", {"tradeId": "demo", "instId": "BTC-USDT-SWAP"}),
    ("emergency_targets", {"target": "0"}), ("portfolio_snapshots", {"equity": "5000"}),
])
async def test_postgres_demo_or_unbound_ledger_rejected(postgres_store, table, payload):
    await postgres_store.append(table, payload, **(
        {"symbol": payload["instId"], "reference_id": payload["tradeId"]} if table == "fills" else {}
    ))
    with pytest.raises(ValueError):
        await postgres_store.bind_live_account("account-A")
    assert not await postgres_store.latest("account_bindings")


async def test_postgres_same_live_binding_restart_still_requires_lease(postgres_store):
    url = coordinator_url(postgres_store)
    a = LiveRuntimeLease(url, "account-A", lambda: None)
    b = LiveRuntimeLease(url, "account-B", lambda: None)
    same = LiveRuntimeLease(url, "account-A", lambda: None)
    try:
        await a.acquire()
        await postgres_store.bind_live_account("account-A")
        await postgres_store.append("orders", {"clOrdId": "existing-live"})
        await postgres_store.bind_live_account("account-A")
        with pytest.raises(LiveLeaseError):
            await same.acquire()
        await b.acquire()  # UID-scoped coordination; ledger invariant still rejects second UID.
        with pytest.raises(ValueError):
            await postgres_store.bind_live_account("account-B")
    finally:
        await a.close()
        await b.close()
        await same.close()


async def test_postgres_shared_coordinator_across_distinct_ledger_sessions(postgres_store):
    # Independent engines/connections emulate deployments with different ledger storage.
    a = LiveRuntimeLease(coordinator_url(postgres_store), "account-A", lambda: None)
    b = LiveRuntimeLease(coordinator_url(postgres_store), "account-A", lambda: None)
    try:
        await a.acquire()
        with pytest.raises(LiveLeaseError):
            await b.acquire()
    finally:
        await a.close()
        await b.close()


async def test_live_startup_lease_contention_precedes_binding_and_restore(postgres_store):
    url = coordinator_url(postgres_store)
    owner = LiveRuntimeLease(url, "account-A", lambda: None)
    runtime = TradingRuntime(live_settings(database_url=url, live_lease_database_url=url))
    runtime.client.account_config = AsyncMock(return_value=[account_config()])
    runtime.store.bind_live_account = AsyncMock()
    runtime.order_manager.restore = AsyncMock()
    runtime.emergency.restore = AsyncMock()
    runtime.client.request = AsyncMock()
    try:
        await owner.acquire()
        with pytest.raises(LiveLeaseError):
            await runtime.initialize()
        runtime.store.bind_live_account.assert_not_awaited()
        runtime.order_manager.restore.assert_not_awaited()
        runtime.emergency.restore.assert_not_awaited()
        runtime.client.request.assert_not_awaited()
        assert not runtime.running and not runtime.tasks
    finally:
        await owner.close()
        await runtime.close()


async def test_live_lease_loss_halts_critical_and_cannot_resume(postgres_store):
    url = coordinator_url(postgres_store)
    runtime = TradingRuntime(live_settings(database_url=url, live_lease_database_url=url))
    runtime.alert = AsyncMock()
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    runtime.live_lease = LiveRuntimeLease(url, "account-A", runtime._lease_lost)
    try:
        await runtime.live_lease.acquire()
        runtime.governor.resume(synchronized=True, healthy=True)
        raw = await runtime.live_lease._connection.get_raw_connection()
        await raw.driver_connection.close()
        assert not await runtime._live_writer_guard(True)
        await runtime._lease_loss_task
        assert runtime.governor.state == GovernorState.HALT
        assert runtime.governor.reason == "LIVE writer lease lost"
        assert not runtime.auto_recovery_status()["eligible"]
        assert runtime.alert.await_args.kwargs["priority"] == NotificationPriority.CRITICAL
        assert "live_writer_lease" in (await runtime._resume_health())[1]
    finally:
        await runtime.close()


async def test_live_runtime_lease_loss_blocks_direct_and_execution_entries_but_emergency_allowed(
    postgres_store,
):
    import httpx

    from app.execution import ExecutionEngine
    from tests.test_core import instrument, intent, portfolio

    url = coordinator_url(postgres_store)
    runtime = TradingRuntime(live_settings(database_url=url, live_lease_database_url=url))
    runtime.alert = AsyncMock()
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    runtime.live_lease = LiveRuntimeLease(url, "account-A", runtime._lease_lost)
    writes = []
    def respond(request):
        if request.method == "POST":
            writes.append(request)
        return httpx.Response(200, json={"code": "0", "data":
                                       [account_config()] if request.method == "GET" else []})
    from app.okx import OkxError
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                     base_url="https://openapi.okx.com") as http:
            await runtime.client.client.aclose()
            runtime.client.client = http
            runtime.client.owns_client = False
            await runtime.client.account_config()
            await runtime.live_lease.acquire()
            runtime.governor.resume(synchronized=True, healthy=True)
            approved = runtime.risk.evaluate(intent(), portfolio(), instrument(),
                                              data_fresh=True, infrastructure_healthy=True)
            assert approved.approved
            raw = await runtime.live_lease._connection.get_raw_connection()
            await raw.driver_connection.close()
            with pytest.raises(OkxError):
                await runtime.client.request("POST", "/api/v5/trade/order", body={"reduceOnly": False})
            with pytest.raises(OkxError):
                await runtime.execution.submit(ExecutionEngine.from_risk(approved, instrument()))
            # Manual and automatic callers share both Execution and final REST gates.
            assert not runtime.execution.entry_allowed()
            assert not await runtime._live_writer_guard(True)
            assert not runtime.order_manager.orders
            runtime.governor.halt("LIVE writer lease lost", emergency=True)
            await runtime.client.request("POST", "/api/v5/trade/order", body={"reduceOnly": True})
            await runtime.client.request("POST", "/api/v5/trade/order-algo", body={"reduceOnly": "true"})
            with pytest.raises(OkxError):
                await runtime.client.cancel_all_after(60)
            assert len(writes) == 2
    finally:
        await runtime.close()


async def test_live_transient_account_read_halts_entry_keeps_verified_emergency(postgres_store):
    import httpx

    from app.okx import OkxError
    url = coordinator_url(postgres_store)
    runtime = TradingRuntime(live_settings(database_url=url, live_lease_database_url=url))
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    runtime.alert = AsyncMock()
    runtime.live_lease = LiveRuntimeLease(url, "account-A", runtime._lease_lost)
    config_reads = 0
    writes = []
    def respond(request):
        nonlocal config_reads
        if request.method == "POST":
            writes.append(request)
            return httpx.Response(200, json={"code": "0", "data": []})
        if request.url.path == "/api/v5/account/config":
            config_reads += 1
            if config_reads > 1:
                return httpx.Response(503, json={"code": "50001", "data": []})
            return httpx.Response(200, json={"code": "0", "data": [account_config()]})
        return httpx.Response(200, json={"code": "0", "data": []})
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                     base_url="https://openapi.okx.com") as http:
            await runtime.client.client.aclose()
            runtime.client.client = http
            runtime.client.owns_client = False
            await runtime.client.account_config()
            await runtime.live_lease.acquire()
            runtime.governor.resume(synchronized=True, healthy=True)
            await runtime.reconcile()
            assert runtime.governor.state == GovernorState.HALT
            assert runtime.governor.reason == "reconciliation failed"
            with pytest.raises(OkxError):
                await runtime.client.request("POST", "/api/v5/trade/order", body={"reduceOnly": False})
            await runtime.client.request("POST", "/api/v5/trade/order", body={"reduceOnly": True})
            assert len(writes) == 1
    finally:
        await runtime.close()


async def test_same_uid_concurrent_binding_is_idempotent(postgres_store):
    other = Store(coordinator_url(postgres_store))
    try:
        await asyncio.gather(postgres_store.bind_live_account("account-A"),
                             other.bind_live_account("account-A"))
        assert len(await postgres_store.latest("account_bindings")) == 1
    finally:
        await other.close()


async def test_shared_coordinator_blocks_same_uid_with_different_ledger_database(postgres_store):
    # Only create/drop a test database owned by THIS test; never reuse an existing one.
    from sqlalchemy.ext.asyncio import create_async_engine
    url = coordinator_url(postgres_store)
    admin = create_async_engine(url, isolation_level="AUTOCOMMIT")
    second_url = postgres_store.engine.url.set(database="quant_live_acceptance_second_test")
    assert second_url.database == "quant_live_acceptance_second_test"
    other_store = Store(second_url.render_as_string(hide_password=False))
    a = LiveRuntimeLease(url, "account-A", lambda: None)
    runtime_b = TradingRuntime(live_settings(
        database_url=second_url.render_as_string(hide_password=False),
        live_lease_database_url=url,
    ))
    runtime_b.client.account_config = AsyncMock(return_value=[account_config()])
    runtime_b.order_manager.restore = AsyncMock()
    created = False
    try:
        async with admin.connect() as connection:
            await connection.execute(text('CREATE DATABASE quant_live_acceptance_second_test'))
            created = True
        await other_store.initialize()
        await a.acquire()
        with pytest.raises(LiveLeaseError):
            await runtime_b.start()
        runtime_b.order_manager.restore.assert_not_awaited()
        assert not runtime_b.tasks and not runtime_b.running
        assert runtime_b.notifications._worker is None
        assert not await other_store.latest("account_bindings")
    finally:
        await runtime_b.close()
        await a.close()
        await other_store.close()
        if created:
            async with admin.connect() as connection:
                await connection.execute(text('DROP DATABASE quant_live_acceptance_second_test'))
        await admin.dispose()


async def test_lease_loss_cancels_even_when_audit_database_unavailable(postgres_store):
    runtime = TradingRuntime(live_settings())
    runtime.alert = AsyncMock()
    runtime.store.append = AsyncMock(side_effect=RuntimeError("DB_PRIVATE_MARKER"))
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    try:
        runtime._lease_lost()
        await runtime._lease_loss_task
        assert runtime.governor.state == GovernorState.HALT
        assert runtime.alert.await_args.kwargs["priority"] == NotificationPriority.CRITICAL
        runtime.entry_controller.cancel_all.assert_awaited_once()
    finally:
        await runtime.close()
