"""Private/business protocol, bounded recovery and fail-closed diagnostics."""
import asyncio
import json
import logging
from unittest.mock import AsyncMock, Mock

import pytest
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from websockets.frames import Close

from app.bark_formatter import localize_bark_event
from app.config import Mode, Settings
from app.incidents import IncidentManager
from app.logging import JsonFormatter
from app.models import GovernorState, NotificationCategory, NotificationEvent, NotificationLevel
from app.monitoring import Metrics
from app.okx import OkxWebSocket, WebSocketFault
from app.recovery import HaltClass, classify_halt_reason
from app.runtime import TradingRuntime
from tests.test_ws_reliability import Wire, noop, wait_until

ALGO = {"channel": "orders-algo", "instType": "ANY"}
# Official /business acknowledgement; connId is deliberately not exposed in diagnostics.
ACK = {"event": "subscribe", "arg": ALGO, "connId": "fixture-connection"}
LOGIN = {"event": "login", "code": "0", "msg": "", "connId": "fixture-connection"}


def make_socket(now=None, **kw):
    settings = Settings(_env_file=None, okx_api_key="fixture-api-key",
                        okx_secret_key="fixture-secret", okx_passphrase="fixture-pass", **kw)
    ws = OkxWebSocket("wss://wspap.okx.com/ws/v5/business", [dict(ALGO)], noop,
                      settings, private=True, name="private-algo", metrics=Metrics(),
                      **({"clock": lambda: now[0]} if now else {}))
    return ws


class TimedWire(Wire):
    def __init__(self, now, login_delay=0, ack_delay=0, login=LOGIN):
        super().__init__([login, ACK])
        self.now, self.delays, self.calls = now, [login_delay, ack_delay], 0

    async def recv(self):
        if self.calls < len(self.delays):
            self.now[0] += self.delays[self.calls]
            self.calls += 1
        return await super().recv()


async def test_login_and_subscribe_have_independent_eight_second_deadlines():
    now = [100.0]
    ws = make_socket(now)
    ws._reset_transport()
    wire = TimedWire(now, login_delay=7, ack_delay=2)
    await ws._handshake(wire)
    ready = asyncio.Event()
    ws.on_ready = lambda _: ready.set()
    receive = asyncio.create_task(ws._receive(wire))
    try:
        await asyncio.wait_for(ready.wait(), 1)
        assert ws.login_completed_at == ws.subscribe_sent_at == 107
        assert ws.subscriptions_completed_at == 109
        assert ws.is_transport_healthy() and ws.reconciliation_required
        assert ws.phase == "RECONCILING"
    finally:
        receive.cancel()
        await asyncio.gather(receive, return_exceptions=True)


async def test_login_exceeds_deadline_fails_closed():
    now = [100.0]
    ws = make_socket(now)
    ws._reset_transport()
    with pytest.raises(WebSocketFault, match="login timeout"):
        await ws._handshake(TimedWire(now, login_delay=9))
    assert not ws.login_ok and not ws.subscribed


async def test_maintenance_during_login_is_not_an_authentication_or_network_error():
    ws = make_socket()
    ws._reset_transport()
    with pytest.raises(WebSocketFault) as result:
        await ws._handshake(Wire([{"event": "notice", "code": "64008"}]))
    assert result.value.kind == "maintenance"
    ws._fault(result.value.message, kind=result.value.kind)
    assert ws.reason_code == "ws_server_maintenance" and not ws.login_ok


async def test_subscription_deadline_starts_after_login_and_still_expires():
    now = [100.0]
    ws = make_socket(now)
    ws._reset_transport()
    wire = TimedWire(now, login_delay=2, ack_delay=9)
    await ws._handshake(wire)
    with pytest.raises(WebSocketFault, match="acknowledgement timeout"):
        await ws._receive(wire)
    assert ws.login_ok and not ws.is_transport_healthy()


@pytest.mark.parametrize("phase", ["login", "subscribe"])
async def test_handshake_send_and_receive_hangs_are_bounded(phase):
    ws = make_socket(request_timeout_seconds=.01)
    ws._reset_transport()
    wire = Wire([LOGIN] if phase == "subscribe" else [])
    if phase == "subscribe":
        wire.send = AsyncMock(side_effect=[None, TimeoutError()])
    with pytest.raises(WebSocketFault, match="timeout"):
        await ws._handshake(wire)


@pytest.mark.parametrize("code", ["60009", "60005", None])
async def test_rejected_login_is_not_reported_as_connection_closed(code):
    ws = make_socket()
    ws._reset_transport()
    with pytest.raises(WebSocketFault, match="login failed") as caught:
        await ws._handshake(Wire([{"event": "login", "code": code, "msg": "private secret"}]))
    ws._fault(caught.value.message, kind=caught.value.kind)
    assert ws.reason_code == "ws_login_failed"
    assert "private secret" not in json.dumps(ws.status())


@pytest.mark.parametrize("arg", [{"channel": "orders-algo", "instType": "SWAP"},
                                  {"channel": "orders-algo"}])
async def test_subscription_ack_requires_exact_requested_inst_type(arg):
    ws = make_socket()
    ws._reset_transport()
    ws.login_ok = True
    with pytest.raises(WebSocketFault, match="invalid subscription"):
        await ws._receive(Wire([{"event": "subscribe", "arg": arg}]))
    assert not ws.is_transport_healthy()


@pytest.mark.parametrize("event", ["error", "unsubscribe", "channel-conn-count-error"])
async def test_server_subscription_rejection_and_revocation_fail_closed(event):
    ws = make_socket()
    ws._reset_transport()
    with pytest.raises(WebSocketFault, match="subscription rejected") as caught:
        await ws._receive(Wire([{"event": event, "arg": ALGO, "code": "60012"}]))
    ws._fault(caught.value.message, kind=caught.value.kind)
    assert ws.reason_code == "ws_subscription_rejected"


class Session(Wire):
    def __init__(self, closed=None, close_at="active"):
        super().__init__()
        self.closed, self.close_at = closed, close_at if closed is not None else "active"
        self.stage = "connect"

    async def send(self, value):
        self.sent.append(value)
        if value == "ping":
            self.messages.put_nowait("pong")
        else:
            op = json.loads(value)["op"]
            self.stage = "login" if op == "login" else "subscribe"
            if self.close_at != self.stage:
                self.messages.put_nowait(json.dumps(LOGIN if op == "login" else ACK))

    async def recv(self):
        await asyncio.sleep(0)
        if self.messages.empty() and self.closed is not None:
            raise self.closed
        return await super().recv()


class Connect:
    def __init__(self, wire):
        self.wire = wire

    async def __aenter__(self):
        return self.wire

    async def __aexit__(self, *args):
        return False


def close_error(code):
    if code is None:
        return ConnectionClosedError(None, None, None)
    cls = ConnectionClosedOK if code in (1000, 1001) else ConnectionClosedError
    return cls(Close(code, "going away"), Close(code, "going away"), True)


def install_sessions(monkeypatch, ws, failures=1, *, code=1001, close_at="active", now=None):
    sessions, delays, kwargs_seen = [], [], []

    def connect(_, **kwargs):
        kwargs_seen.append(kwargs)
        wire = Session(close_error(code) if len(sessions) < failures else None, close_at)
        sessions.append(wire)
        return Connect(wire)

    async def backoff():
        delays.append(ws.current_backoff)
        if now is not None:
            now[0] += ws.current_backoff
        await asyncio.sleep(0)

    monkeypatch.setattr("app.okx.websockets.connect", connect)
    ws._sleep_backoff = backoff
    return sessions, delays, kwargs_seen


@pytest.mark.parametrize("code,close_at", [(1000, "active"), (1001, "active"),
                                           (None, "active"), (1001, "login"),
                                           (1001, "subscribe")])
async def test_server_close_reconnects_authenticates_and_resubscribes(monkeypatch, code, close_at):
    ws = make_socket()
    ready = asyncio.Event()
    ws.on_ready = lambda sock: ready.set() if sock.generation == 2 else None
    sessions, delays, seen = install_sessions(monkeypatch, ws, code=code, close_at=close_at)
    task = asyncio.create_task(ws.run())
    try:
        await asyncio.wait_for(ready.wait(), 2)
        assert ws.reconnects == 1 and ws.worker_alive
        assert ws.last_failure_reason_code == "ws_connection_closed"
        assert ws.last_close_code == code
        assert ws.last_close_side == ("server" if code else "unavailable")
        assert ws.is_transport_healthy() and ws.reconciliation_required and not ws.is_fresh()
        assert ws.phase == "RECONCILING"
        assert all(k["open_timeout"] == 8 for k in seen)
        assert delays and max(delays) <= ws.settings.ws_backoff_max_seconds
        for wire in sessions:
            assert json.loads(wire.sent[0])["op"] == "login"
        assert json.loads(sessions[-1].sent[1]) == {"op": "subscribe", "args": [ALGO]}
        ws.reconciliation_required = False
        assert ws.phase == "ACTIVE" and ws.is_fresh()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_three_failed_sessions_then_fourth_recovers(monkeypatch):
    now = [100.0]
    ws = make_socket(now)
    sessions, delays, _ = install_sessions(monkeypatch, ws, failures=3, now=now)
    ready = asyncio.Event()
    ws.on_ready = lambda sock: ready.set() if sock.generation == 4 else None
    task = asyncio.create_task(ws.run())
    try:
        await asyncio.wait_for(ready.wait(), 2)
        assert len(sessions) == 4 and ws.reconnects == 3
        assert ws.consecutive_failures == 3 and ws.worker_alive
        assert ws.reconciliation_required
        assert not ws._queued_at and ws.queue.empty() and ws.queue._unfinished_tasks == 0
        assert 1 <= delays[0] <= 1.5 and 2 <= delays[1] <= 2.5 and 4 <= delays[2] <= 4.5
        ws.reconciliation_required = False
        now[0] += 31
        ws.last_rx_at = ws.last_pong_at = now[0]
        assert ws.status()["consecutive_failures"] == 0 and ws.is_fresh()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_one_virtual_hour_reconnects_continue_and_entries_remain_blocked(monkeypatch, tmp_path):
    now = [100.0]
    ws = make_socket(now)
    rt = TradingRuntime(Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/hour.db"))
    rt.sockets = [ws]
    rt._schedule_ws_reconciliation = lambda *args: None
    ws.on_fault = rt._on_ws_fault
    _, delays, seen = install_sessions(monkeypatch, ws, failures=130, now=now)
    task = asyncio.create_task(ws.run())
    try:
        await wait_until(lambda: now[0] >= 3700)
        assert len(seen) > 100 and ws.reconnects > 100 and not task.done()
        assert max(delays) <= 30 and ws.worker_alive
        assert ws.queue.empty() and not ws._queued_at and ws.queue._unfinished_tasks == 0
        assert rt.governor.state == GovernorState.HALT
        assert not rt._websockets_healthy()
        assert not rt.execution.entry_allowed()
        assert ws.last_failure_reason_code == "ws_connection_closed"
        assert ws.status()["last_disconnect_phase"] in {"RECONCILING", "ACTIVE"}
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await rt.close()


async def test_private_accepted_handler_does_not_block_reconnect(monkeypatch):
    ws = make_socket()
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(_):
        entered.set()
        await release.wait()

    ws.handler = handler
    ws._enqueue_message({"arg": ALGO, "data": [{"state": "effective"}]}, ws.clock())
    sessions, _, _ = install_sessions(monkeypatch, ws)
    task = asyncio.create_task(ws.run())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await wait_until(lambda: len(sessions) == 2 and ws.is_transport_healthy())
        assert not release.is_set() and ws.reconciliation_required and not ws.business_idle
        assert not ws.is_fresh()
        release.set()
        await asyncio.wait_for(ws.queue.join(), 1)
        assert ws.worker_alive and ws.queue._unfinished_tasks == 0
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_handler_exception_always_completes_queue_accounting():
    ws = make_socket()
    ws.handler = AsyncMock(side_effect=ValueError("not logged"))
    ws._enqueue_message({"arg": ALGO, "data": [{}]}, ws.clock())
    worker = asyncio.create_task(ws._business_worker())
    try:
        await asyncio.wait_for(ws.queue.join(), 1)
        assert not worker.done() and ws.processing_unsafe
        assert ws.queue._unfinished_tasks == 0 and ws.reconciliation_required
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_metrics_failure_after_get_still_calls_task_done():
    ws = make_socket()
    ws._enqueue_message({"arg": ALGO, "data": [{}]}, ws.clock())
    ws._queue_metric = lambda: (_ for _ in ()).throw(RuntimeError("observer failed"))
    worker = asyncio.create_task(ws._business_worker())
    try:
        await asyncio.wait_for(ws.queue.join(), 1)
        assert ws.queue._unfinished_tasks == 0 and ws.processing_unsafe
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_worker_unexpected_exit_is_fenced_and_independently_restarted(monkeypatch):
    ws = make_socket()
    original = ws._business_worker
    calls = 0
    faults = []

    async def worker():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("worker crash")
        await original()

    ws._business_worker = worker
    ws.on_fault = lambda _, reason: faults.append(reason)
    install_sessions(monkeypatch, ws, failures=0)
    task = asyncio.create_task(ws.run())
    try:
        await wait_until(lambda: calls >= 2 and ws.is_transport_healthy())
        assert ws.worker_alive and not task.done()
        assert ws.worker_exception_type == "RuntimeError"
        assert ws.processing_unsafe and ws.reconciliation_required and not ws.is_fresh()
        assert "WebSocket processing failed" in faults
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("reason,kind,code", [
    ("heartbeat timeout", "transport", "ws_heartbeat_timeout"),
    ("connection closed", "transport", "ws_connection_closed"),
    ("server maintenance notice 64008", "maintenance", "ws_server_maintenance"),
    ("subscription acknowledgement timeout", "transport", "ws_subscription_timeout"),
    ("processing queue full", "backlog", "ws_processing_backlog"),
    ("invalid frame", "protocol", "ws_protocol_error"),
])
def test_machine_reason_classes_remain_distinct(reason, kind, code):
    ws = make_socket()
    ws._fault(reason, kind=kind)
    assert ws.reason_code == code


async def test_sanitized_close_diagnostics_include_real_code_and_phase(monkeypatch, caplog):
    ws = make_socket()
    error = ConnectionClosedError(Close(4007, "fixture-api-key token=bad wss://host?secret=bad\n"),
                                  Close(4007, ""), True)
    sessions = []

    def connect(_, **kwargs):
        wire = Session(error if not sessions else None)
        sessions.append(wire)
        return Connect(wire)

    monkeypatch.setattr("app.okx.websockets.connect", connect)
    ws._sleep_backoff = AsyncMock()
    task = asyncio.create_task(ws.run())
    try:
        await wait_until(lambda: len(sessions) == 2 and ws.is_transport_healthy())
        status = ws.status()
        assert status["close_code"] == 4007 and status["close_side"] == "server"
        record = next(r for r in caplog.records if r.getMessage() == "websocket session ended")
        assert record.phase == "RECONCILING" and record.login_ok
        assert record.subscriptions_acked == 1 and record.reconciliation_required
        output = JsonFormatter().format(record) + json.dumps(status)
        assert "fixture-api-key" not in output and "token=bad" not in output and "secret=bad" not in output
        event = NotificationEvent(level=NotificationLevel.WARNING, category=NotificationCategory.INFRASTRUCTURE,
                                  event_code="WS_DISCONNECTED", title="outage",
                                  message=TradingRuntime._ws_details(ws))
        bark = localize_bark_event(event)
        assert "阶段：" in bark.message and "关闭代码：4007" in bark.message
        assert "reason_code：ws_connection_closed" in bark.message
        assert "系统异常" not in bark.message
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("port", [443, 8443])
def test_endpoint_logging_is_safe_and_legacy_port_warns(caplog, port):
    ws = make_socket()
    ws.url = f"wss://fixture-api-key:fixture-pass@wspap.okx.com:{port}/ws/v5/business?token=fixture-secret"
    with caplog.at_level(logging.INFO):
        ws.endpoint_log()
    output = " ".join(JsonFormatter().format(r) for r in caplog.records)
    assert "wspap.okx.com" in output and "business" in output and str(port) in output
    assert "fixture-api-key" not in output and "fixture-pass" not in output and "fixture-secret" not in output
    assert ("legacy OKX WebSocket port configured" in output) == (port == 8443)


@pytest.mark.parametrize("endpoint", ["wss://example.com:fixture-secret/business", "wss://[fixture-secret/business"])
def test_invalid_endpoint_diagnostics_do_not_leak_parsing_exception(caplog, endpoint):
    ws = make_socket()
    ws.url = endpoint
    ws.endpoint_log()
    output = " ".join(JsonFormatter().format(r) for r in caplog.records)
    assert "invalid WebSocket endpoint configured" in output
    assert "fixture-secret" not in output and endpoint not in output


@pytest.mark.parametrize("mode,host", [(Mode.PAPER, "wspap.okx.com"), (Mode.LIVE, "ws.okx.com")])
def test_default_business_path_is_standard_tls(mode, host):
    s = Settings(_env_file=None)
    base = s.okx_paper_ws_url if mode == Mode.PAPER else s.okx_live_ws_url
    assert base + "/business" == f"wss://{host}/ws/v5/business"
    assert not s.live_trading_enabled


def test_reconnect_stall_detection_is_bounded_and_not_an_auto_recovery_reason():
    now = [100.0]
    ws = make_socket(now)
    ws.last_connect_attempt_at = ws.last_reconnect_progress_at = 100
    now[0] += 69
    assert ws.reconnect_stalled()
    assert ws.status()["reconnect_stalled"]
    assert classify_halt_reason("WebSocket reconnect loop stalled") == HaltClass.SAFETY_OR_MANUAL
    assert classify_halt_reason("WebSocket run task stopped") == HaltClass.SAFETY_OR_MANUAL


def ws_incident(ws, *, recovery=False, pending=False):
    return NotificationEvent(level=NotificationLevel.WARNING,
                             category=NotificationCategory.INFRASTRUCTURE,
                             title="transport event", message=TradingRuntime._ws_details(ws),
                             metadata={"component": "ws_recovery" if pending else "websocket",
                                       "ws_incident_key": "ws:recovery:private-algo" if pending else "ws:transport:private-algo",
                                       "ws_details": TradingRuntime._ws_details(ws),
                                       "risk_state": "HALT", "recovery": recovery})


def test_transport_incident_closes_while_reconciliation_remains_pending():
    now = [100.0]
    ws = make_socket(now)
    ws._worker_task = Mock(spec=asyncio.Task)
    ws._worker_task.done.return_value = False
    ws._fault("connection closed")
    manager = IncidentManager(delay_seconds=60, clock=lambda: now[0])
    manager.observe(ws_incident(ws))
    now[0] += 61
    outage = manager.due()[0]
    manager.delivery_confirmed(outage)
    ws._reset_transport()
    ws.login_ok = True
    ws.subscribed = {ws.feed_key(ALGO)}
    recovery = manager.observe(ws_incident(ws, recovery=True))
    assert len(recovery) == 1 and recovery[0].event_code == "WS_TRANSPORT_RECOVERED"
    assert "ws:transport:private-algo" not in manager.active
    manager.observe(ws_incident(ws, pending=True))
    now[0] += 61
    ws.last_rx_at = now[0]
    event = manager.due()[0]
    assert event.event_code == "WS_RECOVERY_PENDING"
    manager.delivery_confirmed(event)
    assert ws.reconciliation_required and not ws.is_fresh()
    ws.reconciliation_required = False
    completed = manager.observe(ws_incident(ws, pending=True, recovery=True))
    assert completed[0].event_code == "WS_RECOVERED"
    assert "Reconciliation: healthy" in completed[0].message
    assert "Risk: HALT" in completed[0].message  # Socket recovered doesn't grant trading resume.


def test_repeated_reconnects_aggregate_one_outage_and_one_full_recovery():
    now = [100.0]
    ws = make_socket(now)
    ws._worker_task = Mock(spec=asyncio.Task)
    ws._worker_task.done.return_value = False
    manager = IncidentManager(delay_seconds=60, clock=lambda: now[0])
    for n in range(3):
        ws._fault("connection closed")
        ws.reconnects = n + 1
        manager.observe(ws_incident(ws))
        now[0] += 20
    outage = manager.due()
    assert len(outage) == 1 and "Reconnects: 3" in outage[0].message
    manager.delivery_confirmed(outage[0])
    assert manager.due() == []
    ws._reset_transport()
    ws.login_ok = True
    ws.subscribed = {ws.feed_key(ALGO)}
    ws.reconciliation_required = False
    recovery = manager.observe(ws_incident(ws, recovery=True))
    assert len(recovery) == 1 and recovery[0].event_code == "WS_RECOVERED"
    manager.delivery_confirmed(recovery[0])
    assert manager.observe(ws_incident(ws, recovery=True)) == []


async def test_full_reconciliation_clears_only_current_idle_generation(tmp_path):
    rt = TradingRuntime(Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/epoch.db"))
    ws = make_socket()
    ws._reset_transport()
    ws.login_ok = True
    ws.subscribed = {ws.feed_key(ALGO)}
    rt.sockets = [ws]

    async def reconcile():
        ws.generation += 1
        rt.reconciliation_healthy = rt._last_reconcile_safe = True

    rt._reconcile_impl = reconcile
    try:
        await rt.reconcile()
        assert ws.reconciliation_required
        rt._reconcile_impl = AsyncMock()
        await rt.reconcile()
        assert not ws.reconciliation_required
    finally:
        await rt.close()


@pytest.mark.parametrize("code", [1000, 1001, None])
async def test_real_server_close_after_healthy_pong_reconnects(code):
    sessions = 0
    ready = asyncio.Event()

    async def server(socket):
        nonlocal sessions
        sessions += 1
        first = sessions == 1
        assert json.loads(await socket.recv())["op"] == "login"
        await socket.send(json.dumps(LOGIN))
        assert json.loads(await socket.recv()) == {"op": "subscribe", "args": [ALGO]}
        await socket.send(json.dumps(ACK))
        try:
            async for raw in socket:
                if raw == "ping":
                    await socket.send("pong")
                    if first:
                        if code is None:
                            socket.transport.abort()
                        else:
                            await socket.close(code=code, reason="going away")
                        return
        except (ConnectionClosedError, ConnectionClosedOK):
            pass

    async with websockets.serve(server, "127.0.0.1", 0) as srv:
        ws = make_socket(ws_idle_ping_seconds=.01, ws_pong_timeout_seconds=.1)
        ws.url = f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}/ws/v5/business"
        ws._sleep_backoff = AsyncMock()
        ws.on_ready = lambda sock: ready.set() if sock.generation >= 2 else None
        task = asyncio.create_task(ws.run())
        try:
            await asyncio.wait_for(ready.wait(), 2)
            assert ws.reconnects == 1 and ws.last_failure_reason_code == "ws_connection_closed"
            assert ws.last_close_code == code
            assert ws.worker_alive and ws.is_transport_healthy() and ws.reconciliation_required
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("failed", ["run", "worker", "stall"])
async def test_liveness_failure_halts_and_emits_critical_without_auto_recovery(failed, tmp_path):
    now = [100.0]
    ws = make_socket(now)
    rt = TradingRuntime(Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/liveness.db"))
    rt.running = True
    rt.sockets = [ws]
    rt._schedule_ws_reconciliation = lambda *args: None
    rt.notifications.publish = AsyncMock()
    done = asyncio.create_task(noop(None))
    await done
    if failed == "run":
        ws._run_task = done
        rt._on_ws_run_done(ws, done)
        await asyncio.gather(*rt.tasks)
    elif failed == "worker":
        ws._run_task = asyncio.current_task()
        ws._worker_task = done
        await rt._check_ws_liveness()
    else:
        ws.last_connect_attempt_at = ws.last_reconnect_progress_at = 100
        now[0] += 69
        await rt._check_ws_liveness()
    try:
        assert rt.governor.state == GovernorState.HALT and not rt.execution.entry_allowed()
        assert rt._auto_recovery_forbidden
        events = [call.args[0] for call in rt.notifications.publish.await_args_list]
        assert events[-1].level.value == "CRITICAL"
        assert events[-1].metadata["transition"]
        assert ws.reason_code in {"ws_run_task_failed", "ws_worker_failed", "ws_reconnect_stalled"}
        assert not rt._websockets_healthy()
    finally:
        rt.running = False
        await rt.close()


async def test_run_task_done_during_planned_shutdown_does_not_alert(tmp_path):
    rt = TradingRuntime(Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/stopped.db"))
    ws = make_socket()
    rt.notifications.publish = AsyncMock()
    task = asyncio.create_task(noop(None))
    await task
    rt._on_ws_run_done(ws, task)
    assert not rt.tasks
    rt.notifications.publish.assert_not_awaited()
    await rt.close()


async def test_connect_timeout_enters_backoff_and_continues_attempting(monkeypatch):
    ws = make_socket()
    attempts = 0
    ready = asyncio.Event()

    class HangingConnect(Connect):
        async def __aenter__(self):
            raise TimeoutError("must not leak endpoint or secret")

    def connect(_, **kwargs):
        nonlocal attempts
        attempts += 1
        assert kwargs["open_timeout"] == 8
        return HangingConnect(None) if attempts < 3 else Connect(Session())

    monkeypatch.setattr("app.okx.websockets.connect", connect)
    ws._sleep_backoff = AsyncMock()
    ws.on_ready = lambda _: ready.set()
    task = asyncio.create_task(ws.run())
    try:
        await asyncio.wait_for(ready.wait(), 1)
        assert attempts == 3 and ws.reconnects == 2
        assert ws.reason_code == "healthy"
        assert ws.last_failure_reason_code == "ws_connect_timeout"
        assert ws.worker_alive and ws.reconciliation_required
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_idle_private_algo_remains_healthy_without_business_events():
    now = [100.0]
    ws = make_socket(now)
    ws._reset_transport()
    ws.login_ok = True
    ws.subscribed = {ws.feed_key(ALGO)}
    ws.reconciliation_required = False
    ws._ensure_worker()
    try:
        now[0] += 3600
        ws.last_rx_at = ws.last_pong_at = now[0]
        assert ws.is_fresh() and ws.worker_alive and not ws.last_data_at
        assert ws.phase == "ACTIVE"
    finally:
        ws._stopping = True
        ws._worker_task.cancel()
        await asyncio.gather(ws._worker_task, return_exceptions=True)


async def test_same_generation_reconciliation_cannot_clear_busy_worker(tmp_path):
    rt = TradingRuntime(Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/busy.db"))
    ws = make_socket()
    ws._reset_transport()
    ws.login_ok = True
    ws.subscribed = {ws.feed_key(ALGO)}
    ws._enqueue_message({"arg": ALGO, "data": [{}]}, ws.clock())
    rt.sockets = [ws]
    rt._reconcile_impl = AsyncMock()
    rt.reconciliation_healthy = rt._last_reconcile_safe = True
    try:
        await rt.reconcile()
        assert ws.reconciliation_required
    finally:
        await rt.close()


def test_status_and_metrics_use_bounded_reason_labels_and_no_secrets():
    now = [100.0]
    ws = make_socket(now)
    ws._fault("connection closed")
    ws.last_connect_attempt_at = 99
    ws.status()
    rendered = ws.metrics.render().decode()
    for name in ("quant_ws_connect_attempts", "quant_ws_connect_failures",
                 "quant_ws_consecutive_failures", "quant_ws_session_duration_seconds",
                 "quant_ws_reconnect_backoff_seconds", "quant_ws_last_connect_attempt_age_seconds",
                 "quant_ws_worker_alive"):
        assert name in rendered
    assert "close_reason=" not in rendered
    assert "fixture-api-key" not in rendered + json.dumps(ws.status())
    assert "fixture-secret" not in rendered + json.dumps(ws.status())
