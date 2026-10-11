import asyncio
import json
import sys
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import delete, select, update
from sqlalchemy import event as sqlalchemy_event

from app.accounting_correction import AccountingCorrectionService
from app.api import create_app
from app.config import Mode, Settings
from app.models import GovernorState
from app.paper_maintenance import (
    MaintenanceBusy,
    MaintenanceError,
    PaperMaintenanceFence,
    db_time,
    ensure_schema,
    maintenance_status,
)
from app.runtime import TradingRuntime
from app.storage import ROW_TYPES, MaintenanceLock, Store
from tests.test_accounting_correction import raw_fills, seed
from tests.test_live_lease import postgres_store as postgres_store


@pytest.fixture(params=['sqlite', 'postgres'])
async def fence_store(request, tmp_path, postgres_store):
    store = (postgres_store if request.param == 'postgres'
             else Store(f'sqlite+aiosqlite:///{tmp_path}/fence.db'))
    await store.initialize()
    try:
        yield store
    finally:
        async with store.engine.begin() as conn:
            await conn.execute(delete(MaintenanceLock))
        if store is not postgres_store:
            await store.close()


def settings(store):
    return Settings(_env_file=None, database_url=store.engine.url.render_as_string(hide_password=False))


def service(store, state='HALT'):
    client = AsyncMock()
    status = AsyncMock(return_value={'mode': 'PAPER', 'running': False, 'risk_state': state})
    return AccountingCorrectionService(settings(store), client, store, status)


@pytest.mark.parametrize('state', ['HALT', 'EMERGENCY'])
async def test_stopped_maintenance_acquires_and_releases(fence_store, state):
    local, detail, rows, _ = await seed(fence_store)
    s = service(fence_store, state)

    async def order(*_):
        assert (await maintenance_status(fence_store))['active']
        return [detail]

    s.client.order.side_effect = order
    s.client.fills_history.side_effect = [rows, []]
    original = await raw_fills(fence_store)
    result = await s.correct(local['symbol'], local['clOrdId'], apply=True)
    assert result['corrections_added'] == 1
    assert await fence_store.ledger_repair_hold()
    assert await raw_fills(fence_store) == original
    assert not (await maintenance_status(fence_store))['active']


@pytest.mark.parametrize('state', [
    {'mode': 'PAPER', 'running': True, 'risk_state': 'HALT'},
    {'mode': 'PAPER', 'running': False, 'risk_state': 'NORMAL'},
    {'mode': 'LIVE', 'running': False, 'risk_state': 'HALT'},
])
async def test_bad_state_rejected_before_acquisition_or_okx(fence_store, state):
    s = service(fence_store)
    s.status.return_value = state
    with pytest.raises(ValueError):
        await s.correct('ETH-USDT-SWAP', 'owned-entry')
    s.client.order.assert_not_called()
    assert not (await maintenance_status(fence_store))['active']


async def test_live_settings_rejected_before_status_or_okx(fence_store):
    s = service(fence_store)
    s.settings = s.settings.model_copy(update={'mode': Mode.LIVE})
    with pytest.raises(ValueError):
        await s.correct('ETH-USDT-SWAP', 'owned-entry')
    s.status.assert_not_called()
    s.client.order.assert_not_called()


async def test_start_and_resume_rejected_through_api_and_lifespan(fence_store):
    app = create_app(settings(fence_store))
    runtime = app.state.runtime
    runtime._start = AsyncMock()
    runtime._reconcile_and_assess = AsyncMock(return_value=(True, []))
    async with PaperMaintenanceFence(fence_store, 'maintenance').hold():
        async with app.router.lifespan_context(app):
            assert not runtime.running and runtime.governor.state == GovernorState.HALT
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as api:
                assert (await api.post('/system/start')).status_code == 409
                assert (await api.post('/system/resume')).status_code == 409
                status = (await api.get('/status')).json()
                assert status['maintenance'] == {'active': True, 'type': 'paper_accounting_correction'}
                assert 'owner' not in json.dumps(status['maintenance'])
            runtime._start.assert_not_called()
            runtime._reconcile_and_assess.assert_not_called()


async def test_start_wins_race_maintenance_fails_closed(fence_store):
    s = service(fence_store)
    runtime = TradingRuntime(settings(fence_store))
    started, finish = asyncio.Event(), asyncio.Event()

    async def start(_):
        started.set()
        await finish.wait()
        runtime.running = True

    runtime._start = start
    task = asyncio.create_task(runtime.start())
    try:
        await started.wait()
        with pytest.raises(MaintenanceBusy):
            await s.correct('ETH-USDT-SWAP', 'owned-entry')
        s.client.order.assert_not_called()
    finally:
        finish.set()
        await task
        runtime.running = False
        await runtime.close()


async def test_start_between_first_guard_and_acquire_rechecked(fence_store):
    s = service(fence_store)
    s.status.side_effect = [dict(mode='PAPER', running=False, risk_state='HALT'),
                            dict(mode='PAPER', running=True, risk_state='NORMAL')]
    with pytest.raises(ValueError):
        await s.correct('ETH-USDT-SWAP', 'owned-entry')
    s.client.order.assert_not_called()
    assert not (await maintenance_status(fence_store))['active']


async def test_second_guard_passes_concurrent_start_before_commit_blocked(fence_store, monkeypatch):
    local, detail, rows, _ = await seed(fence_store)
    s = service(fence_store)
    s.client.order.return_value = [detail]
    s.client.fills_history.side_effect = [rows, []]
    runtime = TradingRuntime(settings(fence_store))
    runtime._start = AsyncMock()
    append = fence_store.append_accounting_corrections

    async def during_commit(*args, **kwargs):
        assert s.status.await_count == 3
        with pytest.raises(MaintenanceBusy):
            await runtime.start()
        runtime._start.assert_not_called()
        assert not await runtime.resume()
        return await append(*args, **kwargs)

    monkeypatch.setattr(fence_store, 'append_accounting_corrections', during_commit)
    try:
        await s.correct(local['symbol'], local['clOrdId'], apply=True)
        assert await fence_store.ledger_repair_hold()
        assert not runtime.running
    finally:
        await runtime.close()


async def test_dry_run_and_repeat_apply_preserve_invariants(fence_store):
    local, detail, rows, _ = await seed(fence_store)
    s = service(fence_store)
    s.client.order.return_value = [detail]
    s.client.fills_history.side_effect = [rows, [], rows, [], rows, []]
    report = await s.correct(local['symbol'], local['clOrdId'])
    assert report['corrections_added'] == 0 and report['corrections_proposed'] == 1
    assert not await fence_store.latest('fill_accounting_corrections')
    assert not await fence_store.ledger_repair_hold()
    assert not (await maintenance_status(fence_store))['active']
    assert (await s.correct(local['symbol'], local['clOrdId'], apply=True))['corrections_added'] == 1
    assert (await s.correct(local['symbol'], local['clOrdId'], apply=True))['corrections_added'] == 0


async def test_fetch_exception_releases_fence(fence_store):
    s = service(fence_store)
    s.client.order.side_effect = RuntimeError('injected read failure')
    with pytest.raises(RuntimeError):
        await s.correct('ETH-USDT-SWAP', 'owned-entry')
    assert not (await maintenance_status(fence_store))['active']
    assert not await fence_store.ledger_repair_hold()


async def test_apply_commits_hold_before_release_even_after_prior_manual_release(fence_store, monkeypatch):
    local, detail, rows, _ = await seed(fence_store)
    s = service(fence_store)
    s.client.order.return_value = [detail]
    s.client.fills_history.side_effect = [rows, [], rows, []]
    release = PaperMaintenanceFence.release

    async def check_release(fence):
        assert await fence_store.ledger_repair_hold()
        return await release(fence)

    monkeypatch.setattr(PaperMaintenanceFence, 'release', check_release)
    await s.correct(local['symbol'], local['clOrdId'], apply=True)
    await fence_store.append('system_events', {'event': 'ledger_repair_manual_release'})
    assert not await fence_store.ledger_repair_hold()
    assert (await s.correct(local['symbol'], local['clOrdId'], apply=True))['corrections_added'] == 0


async def test_fenced_transaction_rollback_releases_without_partial_hold(fence_store):
    local, detail, rows, _ = await seed(fence_store)
    s = service(fence_store)
    s.client.order.return_value = [detail]
    s.client.fills_history.side_effect = [rows, []]

    def fail(*_):
        raise RuntimeError('injected audit failure')

    sqlalchemy_event.listen(ROW_TYPES['system_events'], 'before_insert', fail)
    try:
        with pytest.raises(RuntimeError):
            await s.correct(local['symbol'], local['clOrdId'], apply=True)
    finally:
        sqlalchemy_event.remove(ROW_TYPES['system_events'], 'before_insert', fail)
    assert not await fence_store.latest('fill_accounting_corrections')
    assert not await fence_store.ledger_repair_hold()
    assert not (await maintenance_status(fence_store))['active']


async def test_two_independent_stores_atomic_acquire_exactly_one(fence_store):
    other = Store(fence_store.engine.url.render_as_string(hide_password=False))
    a, b = PaperMaintenanceFence(fence_store, 'maintenance'), PaperMaintenanceFence(other, 'start')
    try:
        results = await asyncio.gather(a.acquire(), b.acquire(), return_exceptions=True)
        assert sum(result is None for result in results) == 1
        assert sum(isinstance(result, MaintenanceBusy) for result in results) == 1
    finally:
        await a.release()
        await b.release()
        await other.close()


async def test_periodic_heartbeat_during_slow_evidence_and_loss_observed(fence_store, monkeypatch):
    from app import paper_maintenance

    monkeypatch.setattr(paper_maintenance, 'HEARTBEAT_SECONDS', 0.001)
    a = PaperMaintenanceFence(fence_store, 'maintenance')
    renewed, lost = asyncio.Event(), asyncio.Event()
    renew = a.renew

    async def tracked():
        await renew()
        renewed.set()

    a.renew = tracked
    a.on_loss = lost.set
    async with a.hold():
        await asyncio.wait_for(renewed.wait(), 2)
        await expire(fence_store)
        await asyncio.wait_for(lost.wait(), 2)
        assert a.lost
        with pytest.raises(MaintenanceError):
            await a.renew()


async def test_active_lease_cannot_be_stolen_and_renewed(fence_store):
    a, b = [PaperMaintenanceFence(fence_store, 'maintenance') for _ in range(2)]
    async with a.hold():
        with pytest.raises(MaintenanceBusy):
            await b.acquire()
        await a.renew()
        assert (await maintenance_status(fence_store))['active']


async def expire(store):
    async with store.engine.begin() as conn:
        now = await db_time(conn)
        await conn.execute(update(MaintenanceLock).values(acquired_at=now-1800, expires_at=now-1))


async def test_expired_crash_lease_replaced_old_owner_cannot_commit_or_release(fence_store):
    _, detail, rows, _ = await seed(fence_store)
    a = PaperMaintenanceFence(fence_store, 'maintenance')
    await a.acquire()
    await expire(fence_store)
    b = PaperMaintenanceFence(fence_store, 'maintenance')
    async with b.hold():
        with pytest.raises(MaintenanceError):
            await a.renew()
        with pytest.raises(MaintenanceError):
            await fence_store.append_accounting_corrections(detail, rows, maintenance_owner=a.owner)
        assert not await fence_store.latest('fill_accounting_corrections')
        await a.release()
        await b.renew()


@pytest.mark.parametrize('change', [{'kind': 'unknown'}, {'owner_id': 'bad'},
    {'expires_at': float('inf')}, {'acquired_at': 1e15}, {'name': 'bad'}])
async def test_corrupt_fence_fails_closed_migration_and_acquisition(fence_store, change):
    a = PaperMaintenanceFence(fence_store, 'maintenance')
    await a.acquire()
    a._heartbeat.cancel()
    await asyncio.gather(a._heartbeat, return_exceptions=True)
    async with fence_store.engine.begin() as conn:
        await conn.execute(update(MaintenanceLock).values(**change))
    for operation in [fence_store.initialize, lambda: maintenance_status(fence_store),
                      lambda: PaperMaintenanceFence(fence_store, 'start').acquire()]:
        with pytest.raises(MaintenanceError):
            await operation()


async def test_schema_idempotent_preserves_ledger_and_active_lease(fence_store):
    await seed(fence_store)
    original = await raw_fills(fence_store)
    async with PaperMaintenanceFence(fence_store, 'maintenance').hold() as a:
        await asyncio.gather(*(ensure_schema(fence_store) for _ in range(3)))
        await fence_store.initialize()
        await a.renew()
        async with fence_store.engine.connect() as conn:
            assert len((await conn.execute(select(MaintenanceLock))).all()) == 1
        assert await raw_fills(fence_store) == original


async def test_sqlite_separate_process_crash_keeps_bounded_fence(tmp_path):
    url = f'sqlite+aiosqlite:///{tmp_path}/cross-process.db'
    store = Store(url)
    code = f'''import asyncio,os
from app.storage import Store
from app.paper_maintenance import PaperMaintenanceFence
async def main():
 s=Store({url!r}); f=PaperMaintenanceFence(s,'maintenance'); await f.acquire(); os._exit(0)
asyncio.run(main())'''
    process = await asyncio.create_subprocess_exec(sys.executable, '-c', code)
    assert await process.wait() == 0
    try:
        with pytest.raises(MaintenanceBusy):
            await PaperMaintenanceFence(store, 'start').acquire()
        await expire(store)
        async with PaperMaintenanceFence(store, 'start').hold():
            pass
    finally:
        await store.close()
