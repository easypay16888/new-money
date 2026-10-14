"""Manual recovery drains accepted work without bypassing any safety gate."""
import asyncio
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from app.api import create_app
from app.models import GovernorState, utcnow
from app.okx import OkxError
from app.public_continuity import HOLD, RELEASE
from tests.test_public_continuity import (
    continuity_runtime as continuity_runtime,
)
from tests.test_public_continuity import drain, gap, warm
from tests.test_ws_reliability import BTC


class RedisProbe:
    async def aclose(self):
        pass

    async def ping(self):
        return True

    async def set(self, *args, **kwargs):
        pass

    async def get(self, *args):
        return '1'


def healthy(runtime, now):
    del runtime._resume_health  # Real final health check for every drain regression.
    runtime.redis = RedisProbe()
    runtime.reconciliation_healthy = True
    runtime._last_reconcile_safe = True
    runtime.dead_man_healthy = True
    runtime._last_caa_success_at = now[0]


def ticker():
    return {'arg': {'channel': 'tickers', 'instId': BTC}, 'data': [{
        'ts': str(int(utcnow().timestamp() * 1000)), 'last': '100', 'bidPx': '99', 'askPx': '101',
    }]}


async def busy_batch(runtime, ws, now):
    started, finish = asyncio.Event(), asyncio.Event()
    original = ws.batch_handler

    async def handle(messages):
        started.set()
        await finish.wait()
        await original(messages)

    ws.batch_handler = handle
    ws._enqueue_message(ticker(), now[0])
    await asyncio.wait_for(started.wait(), 2)
    return finish


async def test_resume_health_waits_for_accepted_normal_market_batch(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    healthy(r, now)
    finish = await busy_batch(r, ws, now)
    waiting = asyncio.Event()
    join = ws.queue.join

    async def tracked():
        waiting.set()
        await join()

    ws.queue.join = tracked
    task = asyncio.create_task(r._resume_health())
    await asyncio.wait_for(waiting.wait(), 2)
    assert not task.done() and r.public_continuity.required
    finish.set()
    assert await asyncio.wait_for(task, 2) == (True, [])
    assert r.public_continuity.required  # A health check never releases a hold.


async def test_new_batch_during_audit_read_is_drained_before_manual_release(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    healthy(r, now)
    audit = r.store.public_audit_state
    calls = 0
    finish = None
    waiting = asyncio.Event()
    join = ws.queue.join

    async def tracked_join():
        waiting.set()
        await join()

    async def read(*args):
        nonlocal calls, finish
        result = await audit(*args)
        calls += 1
        if calls == 2:
            finish = await busy_batch(r, ws, now)
        return result

    r.store.public_audit_state = read
    ws.queue.join = tracked_join
    task = asyncio.create_task(r.resume())
    await asyncio.wait_for(waiting.wait(), 2)
    assert not r.execution.entry_allowed() and r.public_continuity.required
    finish.set()
    assert await asyncio.wait_for(task, 2)
    assert r.governor.state == GovernorState.NORMAL
    assert not r.public_continuity.required
    assert await audit(HOLD, RELEASE) is None
    assert not r._last_resume_issues


async def test_busy_timeout_keeps_hold_and_does_not_cancel_handler(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    healthy(r, now)
    finish = await busy_batch(r, ws, now)
    ws.queue.join = AsyncMock(side_effect=TimeoutError)
    assert not await r.resume()
    assert r._last_resume_issues == ['public market warm-up incomplete']
    assert r.public_continuity.required and r.governor.state == GovernorState.HALT
    assert not ws._worker_task.done()
    assert await r.store.public_audit_state(HOLD, RELEASE) is not None
    finish.set()


async def test_fault_during_drain_is_not_masked_by_earlier_health(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    healthy(r, now)
    finish = await busy_batch(r, ws, now)
    join = ws.queue.join

    async def fail_during_wait():
        r.reconciliation_healthy = False
        r._last_reconcile_safe = False
        ws.processing_unsafe = True
        finish.set()
        await join()

    ws.queue.join = fail_during_wait
    ok, issues = await r._resume_health()
    assert not ok
    assert {'reconciliation', 'reconciliation_safety', 'websockets'} <= set(issues)
    assert r.public_continuity.required


async def test_caa_loss_during_final_drain_still_blocks_resume(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    healthy(r, now)
    finish = await busy_batch(r, ws, now)
    join = ws.queue.join

    async def caa_lost():
        r.dead_man_healthy = False
        finish.set()
        await join()

    ws.queue.join = caa_lost
    assert not await r.resume()
    assert 'cancel_all_after' in r._last_resume_issues
    assert await r.store.public_audit_state(HOLD, RELEASE) is not None


async def test_resume_409_reports_safe_failure_fields(continuity_runtime, monkeypatch):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    r._reconcile_and_assess = AsyncMock(return_value=(False, ['cancel_all_after']))
    monkeypatch.setattr('app.api.TradingRuntime', lambda _: r)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(r.settings)),
                                 base_url='http://test') as api:
        response = await api.post('/system/resume')
        assert response.status_code == 409
        assert response.json()['detail']['issues'] == ['cancel_all_after']
        state = (await api.get('/status')).json()
    assert state['last_manual_resume']['attempted_at']
    assert state['last_manual_resume']['issues'] == ['cancel_all_after']
    assert not state['cancel_all_after']['healthy']
    assert state['public_market_continuity']['hold_active']


@pytest.mark.parametrize('error_type,code,status', [('ConnectTimeout', '', None),
                                                 ('APIError', '50011', 200),
                                                 ('InvalidJSON', '', 503)])
async def test_caa_failure_records_only_safe_diagnostics(
    continuity_runtime, caplog, error_type, code, status,
):
    r, _, _ = continuity_runtime
    r.running = True
    r.settings.cancel_all_after_refresh_seconds = .001
    secret = 'fixture-secret-in-exception'
    r.client.cancel_all_after = AsyncMock(side_effect=OkxError(
        secret, code=code, error_type=error_type, http_status=status,
    ))
    halt = r.enter_halt

    async def failed(reason):
        await halt(reason)
        r.running = False

    r.enter_halt = failed
    await r._dead_man_loop()
    diagnostic = r.caa_status()
    assert not diagnostic['healthy']
    assert diagnostic['last_failure']['error_type'] == error_type
    assert diagnostic['last_failure']['code'] == (code or 'unknown')
    assert diagnostic['last_failure']['http_status'] == status
    assert secret not in caplog.text + json.dumps(diagnostic)
    assert r.governor.reason == 'dead man switch unavailable'
    assert r.client.cancel_all_after.await_count == 1  # No new write retries.


async def test_caa_recovery_status_does_not_auto_resume_emergency(continuity_runtime):
    r, _, now = continuity_runtime
    r.running = True
    r.settings.cancel_all_after_refresh_seconds = .001
    r.governor.halt('Public market continuity lost', emergency=True)
    r._last_caa_failure = {'code': 'unknown', 'http_status': None, 'error_type': 'ReadTimeout'}

    async def success(seconds):
        r.running = False

    r.client.cancel_all_after = AsyncMock(side_effect=success)
    await r._dead_man_loop()
    assert r.caa_status()['healthy']
    assert r.caa_status()['last_failure']['error_type'] == 'ReadTimeout'
    assert r.governor.state == GovernorState.EMERGENCY
    r.running = True
    now[0] += r.settings.cancel_all_after_seconds + 1
    assert not r.caa_status()['healthy']
    r.running = False


async def test_maintenance_fence_lost_during_drain_cannot_be_resumed(
    continuity_runtime, monkeypatch,
):
    from app.paper_maintenance import PaperMaintenanceFence

    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    healthy(r, now)
    captured = []
    acquire = PaperMaintenanceFence.acquire

    async def capture(fence):
        await acquire(fence)
        captured.append(fence)

    monkeypatch.setattr(PaperMaintenanceFence, 'acquire', capture)
    audit = r.store.public_audit_state
    join = ws.queue.join
    calls = 0
    finish = None

    async def read(*args):
        nonlocal calls, finish
        result = await audit(*args)
        calls += 1
        if calls == 2:
            finish = await busy_batch(r, ws, now)
        return result

    async def lost_while_waiting():
        captured[0].lost = True
        captured[0].on_loss()
        finish.set()
        await join()

    r.store.public_audit_state = read
    ws.queue.join = lost_while_waiting
    assert not await r.resume()
    assert r.governor.reason == 'PAPER maintenance ownership unavailable'
    assert r._last_resume_issues == ['maintenance_ownership_lost']
    assert await audit(HOLD, RELEASE) is not None
    assert not r.execution.entry_allowed()


async def test_caa_loss_in_audit_read_without_busy_queue_still_blocks(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    healthy(r, now)
    audit = r.store.public_audit_state
    calls = 0

    async def read(*args):
        nonlocal calls
        result = await audit(*args)
        calls += 1
        if calls == 2:
            assert ws.business_idle
            r.dead_man_healthy = False
        return result

    r.store.public_audit_state = read
    assert not await r.resume()
    assert r._last_resume_issues == ['cancel_all_after']
    assert await audit(HOLD, RELEASE) is not None


async def test_new_manual_safety_halt_in_audit_read_is_not_overwritten(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    healthy(r, now)
    audit = r.store.public_audit_state
    calls = 0

    async def read(*args):
        nonlocal calls
        result = await audit(*args)
        calls += 1
        if calls == 2:
            r._halt_state('manual kill switch')
        return result

    r.store.public_audit_state = read
    assert not await r.resume()
    assert r.governor.reason == 'manual kill switch'
    assert r._last_resume_issues == ['risk_state_changed_during_resume']
    assert await audit(HOLD, RELEASE) is not None


@pytest.mark.parametrize('reason', ['manual kill switch', 'Public market continuity lost'])
async def test_new_halt_in_first_drain_is_not_part_of_resume_authorization(
    continuity_runtime, reason,
):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    healthy(r, now)
    r._reconcile_and_assess = r._resume_health  # Real initial gate, no mocked success.
    finish = await busy_batch(r, ws, now)
    join = ws.queue.join

    async def interrupt():
        r._halt_state(reason)
        finish.set()
        await join()

    ws.queue.join = interrupt
    assert not await r.resume()
    assert r.governor.reason == reason
    assert r._last_resume_issues == ['risk_state_changed_during_resume']
    assert await r.store.public_audit_state(HOLD, RELEASE) is not None
