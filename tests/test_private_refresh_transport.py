"""Recovery under sustained private notifications and independently failed REST pools."""
import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import event

from app.config import Settings
from app.models import GovernorState
from app.okx import OkxError, OkxRestClient
from app.paper_maintenance import maintenance_status
from app.rest_pool import RestPool
from app.runtime import TradingRuntime


@pytest.fixture
async def runtime(tmp_path):
    r = TradingRuntime(Settings(_env_file=None, database_url=f'sqlite+aiosqlite:///{tmp_path}/test.db'))
    await r.store.initialize()
    r.entry_controller.cancel_all = AsyncMock(return_value=True)
    try:
        yield r
    finally:
        r.running = False
        await r.close()


async def test_thousand_account_notifications_do_not_wait_for_rest(runtime):
    runtime.reconcile = AsyncMock()
    for i in range(1000):
        await runtime._on_private({'arg': {'channel': 'account' if i % 2 else 'positions'}, 'data': []})
    runtime.reconcile.assert_not_awaited()
    assert runtime.account_refresh_pending
    runtime.governor.state = GovernorState.NORMAL
    assert not runtime.execution.entry_allowed()


async def successful(r):
    r.reconciliation_healthy = True
    r._last_reconcile_safe = True
    r.portfolio.synchronized = True


async def test_burst_reconciles_once_and_keeps_manual_halt(runtime):
    runtime._reconcile_impl = AsyncMock(side_effect=lambda: None)
    async def snapshot():
        await successful(runtime)
    runtime._reconcile_impl.side_effect = snapshot
    for _ in range(1000):
        runtime._request_account_refresh()
    await runtime.reconcile()
    assert not runtime.account_refresh_pending
    assert not runtime._account_refresh_requested.is_set()
    assert runtime.governor.state == GovernorState.HALT
    runtime._reconcile_impl.assert_awaited_once()


async def test_notification_during_snapshot_cannot_be_cleared_by_old_result(runtime):
    started, finish = asyncio.Event(), asyncio.Event()
    async def snapshot():
        started.set()
        await finish.wait()
        await successful(runtime)
    runtime._reconcile_impl = snapshot
    runtime._request_account_refresh()
    task = asyncio.create_task(runtime.reconcile())
    await started.wait()
    runtime._request_account_refresh()
    finish.set()
    await task
    assert runtime.account_refresh_pending
    assert runtime._account_refresh_requested.is_set()
    await runtime.reconcile()
    assert not runtime.account_refresh_pending


async def test_failed_reconciliation_retains_entry_fence(runtime):
    runtime._request_account_refresh()
    runtime._reconcile_impl = AsyncMock()
    runtime.governor.state = GovernorState.NORMAL
    await runtime.reconcile()
    assert runtime.account_refresh_pending
    assert not runtime.execution.entry_allowed()


async def test_notification_wakes_periodic_reconciliation_without_per_event_work(runtime):
    runtime.settings.okx_api_key = 'test'
    runtime.settings.okx_secret_key = 'test'
    runtime.settings.okx_passphrase = 'test'
    runtime.running = True
    done = asyncio.Event()
    async def snapshot():
        await successful(runtime)
        runtime.running = False
        done.set()
    runtime._reconcile_impl = AsyncMock(side_effect=snapshot)
    loop = asyncio.create_task(runtime._reconcile_loop())
    for _ in range(1000):
        await runtime._on_private({'arg': {'channel': 'account'}, 'data': []})
    await asyncio.wait_for(done.wait(), 1)
    await loop
    assert not runtime.account_refresh_pending
    runtime._reconcile_impl.assert_awaited_once()


async def test_failed_snapshot_cancels_and_joins_sibling_reads(runtime):
    entered, cancelled = asyncio.Event(), asyncio.Event()
    async def pending():
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
    async def fail():
        await entered.wait()
        raise OkxError('read unavailable', retryable=True)
    runtime.client.account = fail
    runtime.client.positions = pending
    runtime.client.pending_orders = AsyncMock(return_value=[])
    runtime.client.pending_algos = AsyncMock(return_value=[])
    await runtime.reconcile()
    assert cancelled.is_set()
    assert runtime.governor.state == GovernorState.HALT


async def test_maintenance_status_does_not_reserve_sqlite_writer(runtime):
    statements = []
    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement.upper())
    event.listen(runtime.store.engine.sync_engine, 'before_cursor_execute', record)
    # A concurrent writer must not block the monitoring reader.
    async with runtime.store.engine.connect() as writer:
        await writer.exec_driver_sql('BEGIN IMMEDIATE')
        statements.clear()
        assert await asyncio.wait_for(maintenance_status(runtime.store), 1) == {
            'active': False, 'type': None,
        }
        await writer.rollback()
    assert all(s.startswith('SELECT') for s in statements)


def http_client(handler):
    return httpx.AsyncClient(base_url='https://test.invalid', transport=httpx.MockTransport(handler))


async def test_pool_timeout_retires_client_without_retrying_request():
    attempts, clients = [], []
    def factory():
        number = len(clients)
        def handler(request):
            attempts.append(number)
            if number == 0:
                raise httpx.PoolTimeout('unavailable')
            return httpx.Response(200, json={'code': '0', 'data': []})
        client = http_client(handler)
        clients.append(client)
        return client
    pool = RestPool(factory)
    try:
        with pytest.raises(httpx.PoolTimeout):
            await pool.request('POST', '/order')
        assert attempts == [0] and clients[0].is_closed
        assert (await pool.request('GET', '/positions')).status_code == 200
        assert attempts == [0, 1]
    finally:
        await pool.close()


async def test_retirement_never_cancels_inflight_write():
    started, release = asyncio.Event(), asyncio.Event()
    requests = []
    async def handler(request):
        requests.append(request.url.path)
        if request.url.path == '/inflight':
            started.set()
            await release.wait()
            return httpx.Response(200)
        raise httpx.PoolTimeout('unavailable')
    old = http_client(handler)
    pool = RestPool(lambda: old)
    task = asyncio.create_task(pool.request('POST', '/inflight'))
    try:
        await started.wait()
        with pytest.raises(httpx.PoolTimeout):
            await pool.request('GET', '/fail')
        assert not old.is_closed and not task.done()
        with pytest.raises(httpx.PoolTimeout):
            await pool.request('POST', '/must-not-send')
        assert requests == ['/inflight', '/fail']
        release.set()
        assert (await task).status_code == 200
        assert old.is_closed and pool.active == 0
    finally:
        release.set()
        await task
        await pool.close()


@pytest.mark.parametrize('exception', [httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError])
async def test_ambiguous_write_is_never_retried(exception):
    requests = []
    def handler(request):
        requests.append(request)
        raise exception('unavailable')
    pool = RestPool(lambda: http_client(handler))
    try:
        with pytest.raises(exception):
            await pool.request('POST', '/order')
        assert len(requests) == 1
    finally:
        await pool.close()


async def test_saturated_reads_do_not_block_caa_or_reduce_only_or_cancel():
    client = OkxRestClient(Settings(_env_file=None, okx_api_key='test', okx_secret_key='test', okx_passphrase='test'))
    started, release = asyncio.Event(), asyncio.Event()
    calls = []
    async def read(request):
        started.set()
        await release.wait()
        raise httpx.PoolTimeout('unavailable')
    def write(request):
        calls.append(request.url.path)
        return httpx.Response(200, json={'code': '0', 'data': []})
    client._pools['read'] = RestPool(lambda: http_client(read))
    client._pools['caa'] = RestPool(lambda: http_client(write))
    client._pools['safety'] = RestPool(lambda: http_client(write))
    task = asyncio.create_task(client.positions())
    try:
        await started.wait()
        await asyncio.wait_for(client.cancel_all_after(60), 1)
        for path, body in [('/api/v5/trade/order', {'reduceOnly': True}),
                           ('/api/v5/trade/order-algo', {'reduceOnly': 'true'}),
                           ('/api/v5/trade/cancel-order', {})]:
            await asyncio.wait_for(client.request('POST', path, body=body, private=True), 1)
        assert len(calls) == 4 and not task.done()
        release.set()
        with pytest.raises(OkxError):
            await task
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await client.close()


async def test_injected_transport_remains_caller_owned():
    http = http_client(lambda request: httpx.Response(200, json={'code': '0', 'data': []}))
    client = OkxRestClient(Settings(_env_file=None), http)
    await client.request('GET', '/public')
    await client.close()
    assert not http.is_closed
    await http.aclose()


async def test_real_http_read_pool_saturation_cannot_consume_caa_capacity():
    started, release = asyncio.Event(), asyncio.Event()
    handlers = set()
    caa_calls = []
    async def serve(reader, writer):
        handlers.add(asyncio.current_task())
        try:
            request = await reader.readuntil(b'\r\n\r\n')
            if b'/cancel-all-after' in request:
                caa_calls.append(1)
            elif not release.is_set():
                started.set()
                await release.wait()
            body = b'{"code":"0","data":[]}'
            writer.write(b'HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: '
                         + str(len(body)).encode() + b'\r\n\r\n' + body)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(asyncio.current_task())
    server = await asyncio.start_server(serve, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    client = OkxRestClient(Settings(_env_file=None, okx_api_key='test', okx_secret_key='test', okx_passphrase='test'))
    def factory():
        return httpx.AsyncClient(base_url=f'http://127.0.0.1:{port}', trust_env=False,
                                limits=httpx.Limits(max_connections=1),
                                timeout=httpx.Timeout(2, pool=0.05))
    client._pools['read'] = RestPool(factory)
    client._pools['caa'] = RestPool(factory)
    first = asyncio.create_task(client.positions())
    try:
        await asyncio.wait_for(started.wait(), 2)
        with pytest.raises(OkxError) as caught:
            await client.positions()
        assert caught.value.error_type == 'PoolTimeout'
        await asyncio.wait_for(client.cancel_all_after(60), 2)
        assert caa_calls == [1] and not first.done()
        release.set()
        assert await first == []
        assert await client.positions() == []
    finally:
        release.set()
        await asyncio.gather(first, return_exceptions=True)
        await client.close()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers)


async def test_refresh_loop_unexpected_failure_halts_without_losing_pending(runtime, monkeypatch):
    runtime.settings.okx_api_key = 'test'
    runtime.settings.okx_secret_key = 'test'
    runtime.settings.okx_passphrase = 'test'
    runtime.running = True
    runtime.reconcile = AsyncMock(side_effect=RuntimeError('unexpected'))
    sleeping = asyncio.Event()
    original_sleep = asyncio.sleep
    async def pause(delay):
        if delay == runtime.settings.reconcile_interval_seconds:
            sleeping.set()
            runtime.running = False
        else:
            await original_sleep(0)
    monkeypatch.setattr('app.runtime.asyncio.sleep', pause)
    runtime._request_account_refresh()
    await runtime._reconcile_loop()
    assert sleeping.is_set()
    assert runtime.account_refresh_pending
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.governor.reason == 'reconciliation permanent failure'
    runtime.reconcile.assert_awaited_once()


async def test_refresh_cannot_reauthorize_entry_decision_from_previous_account_version(runtime):
    runtime.governor.state = GovernorState.NORMAL
    runtime._reconcile_impl = AsyncMock(side_effect=lambda: None)
    async def snapshot():
        await successful(runtime)
    runtime._reconcile_impl.side_effect = snapshot
    token = runtime._entry_account_version.set(runtime._account_refresh_version)
    try:
        assert runtime.execution.entry_allowed()
        runtime._request_account_refresh()
        assert not runtime.execution.entry_allowed()
        await runtime.reconcile()
        assert not runtime.account_refresh_pending
        assert not runtime.execution.entry_allowed()
    finally:
        runtime._entry_account_version.reset(token)
    assert runtime.execution.entry_allowed()
