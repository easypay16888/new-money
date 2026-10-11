import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from app.config import Settings
from app.models import GovernorState
from app.okx import WebSocketFault
from app.recovery import HaltClass, classify_halt_reason
from app.runtime import TradingRuntime
from tests.test_private_algo_recovery import (
    Connect,
    Session,
    close_error,
    make_socket,
)
from tests.test_ws_reliability import wait_until


@pytest.mark.parametrize("phase", ["BACKOFF", "CONNECTING", "AUTHENTICATING", "SUBSCRIBING"])
def test_legitimate_phase_budget_is_not_stalled(phase):
    now = [3600.0]
    ws = make_socket(now)
    ws._phase = phase
    ws.last_connect_attempt_at = 0
    ws.last_reconnect_progress_at = 0
    if phase == "BACKOFF":
        ws._next_retry_at = now[0] + 30
    elif phase == "CONNECTING":
        ws.last_connect_attempt_at = now[0]
    elif phase == "AUTHENTICATING":
        ws.login_started_at = now[0]
    else:
        ws.subscribe_sent_at = now[0]
    now[0] += 5
    assert not ws.reconnect_stalled()


def test_expired_backoff_without_any_progress_is_a_real_stall():
    now = [0.0]
    ws = make_socket(now)
    ws._phase = "BACKOFF"
    ws.last_disconnect_at = ws.last_reconnect_progress_at = 0
    ws._next_retry_at = 1
    now[0] = 69
    assert ws.reconnect_stalled()
    assert classify_halt_reason("WebSocket reconnect loop stalled") == HaltClass.SAFETY_OR_MANUAL


def test_status_reads_never_extend_reconnect_progress():
    now = [100.0]
    ws = make_socket(now)
    ws.last_reconnect_progress_at = 99
    ws.status()
    now[0] += 100
    ws.status()
    assert ws.last_reconnect_progress_at == 99 and ws.reconnect_stalled()


async def test_one_hour_healthy_session_close_backoff_watchdog_and_full_recovery(monkeypatch, tmp_path):
    now = [0.0]
    ws = make_socket(now)
    rt = TradingRuntime(Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/progress.db"))
    await rt.store.initialize()
    rt.sockets = [ws]
    rt._schedule_ws_reconciliation = lambda *args: None
    rt.notifications.publish = AsyncMock()
    ws.on_fault = rt._on_ws_fault
    sessions = []
    released = asyncio.Event()
    reached_backoff = asyncio.Event()

    class LongSession(Session):
        async def recv(self):
            if self.messages.empty() and self.stage == "subscribe":
                now[0] = 3600
                self_outer.last_rx_at = self_outer.last_pong_at = now[0]
                raise close_error(1001)
            return await super().recv()

    self_outer = ws

    def connect(*args, **kwargs):
        wire = LongSession() if not sessions else Session()
        sessions.append(wire)
        return Connect(wire)

    async def backoff():
        reached_backoff.set()
        await released.wait()
        now[0] += ws.current_backoff

    monkeypatch.setattr("app.okx.websockets.connect", connect)
    ws._sleep_backoff = backoff
    task = asyncio.create_task(ws.run())
    try:
        await asyncio.wait_for(reached_backoff.wait(), 1)
        assert ws.last_connect_attempt_at == 0
        assert ws.last_reconnect_progress_at == ws.last_disconnect_at == 3600
        assert 1 <= ws.current_backoff <= 1.5
        assert not ws.reconnect_stalled()
        await rt._check_ws_liveness()
        assert rt.governor.reason == "WebSocket transport unavailable"
        assert classify_halt_reason(rt.governor.reason) == HaltClass.TRANSIENT_INFRA
        assert not rt._auto_recovery_forbidden
        released.set()
        await wait_until(lambda: len(sessions) == 2 and ws.is_transport_healthy())
        assert [json.loads(x)["op"] for x in sessions[1].sent] == ["login", "subscribe"]
        assert ws.reason_code == "healthy" and ws.last_failure_reason_code == "ws_connection_closed"
        assert ws.last_close_code == 1001 and ws.last_disconnect_at == 3600
        rt.reconciliation_healthy = rt._last_reconcile_safe = True
        rt._reconcile_impl = AsyncMock()
        await rt.reconcile()
        assert ws.is_fresh() and not ws.reconciliation_required
        assert not rt._auto_recovery_forbidden
        assert rt.governor.state == GovernorState.HALT  # Recovery gates remain independent.
        detail = rt._ws_details(ws)
        assert "Reason: healthy\nReason code: healthy" in detail
        assert "Last failure reason code: ws_connection_closed" in detail
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await rt.close()


async def test_handshake_progress_updates_only_at_real_transitions():
    from tests.test_private_algo_recovery import TimedWire
    now = [100.0]
    ws = make_socket(now)
    ws._reset_transport()
    assert ws.last_reconnect_progress_at == 100
    wire = TimedWire(now, login_delay=7, ack_delay=2)
    await ws._handshake(wire)
    assert ws.last_reconnect_progress_at == 107
    ready = asyncio.Event()
    ws.on_ready = lambda _: ready.set()
    task = asyncio.create_task(ws._receive(wire))
    try:
        await asyncio.wait_for(ready.wait(), 1)
        assert ws.last_reconnect_progress_at == 109
        assert ws.reason_code == "healthy"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def long_session_heartbeat_expiry(now, phase, failure):
    ws = make_socket(now)
    ws.connected = ws.login_ok = True
    ws.subscribed = {ws.feed_key(arg) for arg in ws.subscriptions}
    ws._phase = phase
    ws.last_connect_attempt_at = ws.last_reconnect_progress_at = 1
    ws.last_rx_at = now[0] - ws.settings.ws_idle_ping_seconds - ws.settings.ws_pong_timeout_seconds
    if failure == "pong":
        ws.last_rx_at = now[0]  # Other frames must not extend an outstanding pong deadline.
        ws.ping_pending = True
        ws.last_ping_at = now[0] - ws.settings.ws_pong_timeout_seconds
    return ws


@pytest.mark.parametrize("phase", ["ACTIVE", "RECONCILING"])
@pytest.mark.parametrize("failure", ["idle", "pong"])
@pytest.mark.parametrize("elapsed", [0, 4, 10])
def test_long_session_heartbeat_expiry_is_not_an_immediate_reconnect_stall(phase, failure, elapsed):
    now = [3600.0]
    ws = long_session_heartbeat_expiry(now, phase, failure)
    now[0] += elapsed
    assert not ws.is_transport_healthy()
    assert not ws.reconnect_stalled()
    assert ws.last_reconnect_progress_at == 1  # Health reads do not fabricate progress.


@pytest.mark.parametrize("phase", ["ACTIVE", "RECONCILING"])
@pytest.mark.parametrize("failure", ["idle", "pong"])
def test_receive_loop_that_never_handles_expired_heartbeat_is_still_detected(phase, failure):
    now = [3600.0]
    ws = long_session_heartbeat_expiry(now, phase, failure)
    now[0] += 69
    if failure == "pong":
        ws.last_rx_at = now[0]
    assert not ws.is_transport_healthy()
    assert ws.reconnect_stalled()


async def test_heartbeat_timeout_close_wait_does_not_latch_manual_halt(monkeypatch, tmp_path):
    now = [100.0]
    ws = make_socket(now)
    rt = TradingRuntime(Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/heartbeat.db"))
    await rt.store.initialize()
    rt.sockets = [ws]
    rt._schedule_ws_reconciliation = lambda *args: None
    rt.notifications.publish = AsyncMock()
    ws.on_fault = rt._on_ws_fault
    sessions = []
    closing = asyncio.Event()
    release_close = asyncio.Event()
    recovered = asyncio.Event()
    ws.on_ready = lambda sock: recovered.set() if sock.generation == 2 else None

    class ExpiredHeartbeat(Session):
        async def recv(self):
            if self.messages.empty() and self.stage == "subscribe":
                now[0] = 3600
                ws.last_rx_at = 3585
                raise WebSocketFault("heartbeat timeout")
            return await super().recv()

    class ClosingConnect(Connect):
        async def __aexit__(self, *args):
            closing.set()
            await release_close.wait()
            now[0] += 4
            return False

    def connect(*args, **kwargs):
        wire = ExpiredHeartbeat() if not sessions else Session()
        sessions.append(wire)
        return ClosingConnect(wire) if len(sessions) == 1 else Connect(wire)

    async def backoff():
        now[0] += ws.current_backoff
        await asyncio.sleep(0)

    monkeypatch.setattr("app.okx.websockets.connect", connect)
    ws._sleep_backoff = backoff
    task = asyncio.create_task(ws.run())
    try:
        await asyncio.wait_for(closing.wait(), 1)
        await rt._check_ws_liveness()
        assert not ws.reconnect_stalled()
        assert rt._ws_health_reason() == "WebSocket transport unavailable"
        await rt.enter_halt(rt._ws_health_reason())
        assert rt.governor.state == GovernorState.HALT and not rt.execution.entry_allowed()
        assert not rt._auto_recovery_forbidden
        release_close.set()
        await asyncio.wait_for(recovered.wait(), 1)
        assert ws.last_failure_reason_code == "ws_heartbeat_timeout"
        assert ws.is_transport_healthy() and ws.reconciliation_required
        rt.reconciliation_healthy = rt._last_reconcile_safe = True
        rt._reconcile_impl = AsyncMock()
        await rt.reconcile()
        assert ws.is_fresh() and not ws.reconciliation_required
        assert classify_halt_reason(rt.governor.reason) == HaltClass.TRANSIENT_INFRA
        assert not rt._auto_recovery_forbidden
        assert rt.governor.state == GovernorState.HALT  # Full recovery gates still decide resume.
    finally:
        release_close.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await rt.close()
