import asyncio
import json
import time
from contextlib import suppress
from unittest.mock import AsyncMock

import httpx
import pytest
import websockets

from app.config import Settings
from app.incidents import IncidentManager
from app.logging import JsonFormatter
from app.models import GovernorState, NotificationCategory, NotificationEvent, NotificationLevel
from app.monitoring import Metrics
from app.notification_policy import NotificationPolicy
from app.notifications import NotificationManager
from app.okx import OkxWebSocket, WebSocketFault
from app.recovery import HaltClass, classify_halt_reason
from app.runtime import TradingRuntime
from app.watchdog import WatchdogMonitor, WatchdogSettings
from tests.test_notifications import Recorder


@pytest.mark.parametrize("field", ["okx_paper_ws_url", "okx_live_ws_url"])
def test_default_endpoint_uses_443(field):
    assert ":8443" not in getattr(Settings(_env_file=None), field)


async def test_slow_handler_does_not_block_pong_receive():
    entered = asyncio.Event()
    release = asyncio.Event()
    pong_sent = asyncio.Event()

    async def server_handler(socket):
        await socket.recv()
        await socket.send(json.dumps({"event": "subscribe", "arg": {"channel": "orders"}}))
        await socket.send(json.dumps({"arg": {"channel": "orders"}, "data": [{"state": "live"}]}))
        await entered.wait()
        await asyncio.sleep(0.01)
        await socket.send("pong")
        pong_sent.set()
        await release.wait()

    async def handler(_):
        entered.set()
        await release.wait()

    async with websockets.serve(server_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        ws = OkxWebSocket(
            f"ws://127.0.0.1:{port}", [{"channel": "orders"}], handler, Settings(_env_file=None)
        )
        task = asyncio.create_task(ws.run())
        try:
            await asyncio.wait_for(entered.wait(), 2)
            before = ws.last_message_at
            await asyncio.wait_for(pong_sent.wait(), 2)
            async with asyncio.timeout(0.5):
                while ws.last_message_at <= before:
                    await asyncio.sleep(0.001)
            assert not release.is_set()
        finally:
            release.set()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


BTC = "BTC-USDT-SWAP"


async def noop(_):
    pass


def ready_socket(*, private=False, channels=("orders",), now=None, **kwargs):
    now = now or [100.0]
    ws = OkxWebSocket(
        "ws://test.invalid",
        [dict(channel=c, instId=BTC) for c in channels],
        noop,
        Settings(_env_file=None, **kwargs),
        private=private,
        name="private-account" if private else "public-market",
        clock=lambda: now[0],
    )
    ws._reset_transport()
    ws.login_ok = True
    ws.subscribed = {ws.feed_key(arg) for arg in ws.subscriptions}
    ws.reconciliation_required = False
    return ws, now


@pytest.mark.parametrize("field", ["okx_paper_ws_url", "okx_live_ws_url"])
def test_custom_endpoint_is_not_rewritten(field):
    custom = "wss://custom.example:8443/custom/path"
    assert getattr(Settings(_env_file=None, **{field: custom}), field) == custom


def test_private_business_inactivity_with_pong_is_healthy():
    ws, now = ready_socket(private=True)
    now[0] += 300
    ws.last_rx_at = ws.last_pong_at = now[0]
    assert not ws.last_data_at
    assert ws.is_transport_healthy()
    assert ws.is_data_fresh() and ws.is_fresh()


@pytest.mark.parametrize("channel", ["books5", "tickers", "mark-price", "index-tickers"])
def test_each_critical_subscription_requires_its_own_data(channel):
    ws, now = ready_socket(channels=(channel, "funding-rate"))
    ws.last_data_at[ws.feed_key(ws.subscriptions[0])] = now[0] - 21
    ws.last_data_at[ws.feed_key(ws.subscriptions[1])] = now[0]
    assert ws.is_transport_healthy()
    assert not ws.is_data_fresh()
    assert ws.stale_feeds()[0]["feed"] == channel
    assert ws.stale_feeds()[0]["symbol"] == BTC


def test_other_symbol_or_pong_cannot_refresh_stale_book():
    ws, now = ready_socket(channels=("books5",))
    ws.subscriptions.append(dict(channel="books5", instId="ETH-USDT-SWAP"))
    ws.subscribed.add("books5:ETH-USDT-SWAP")
    ws.last_data_at["books5:ETH-USDT-SWAP"] = now[0]
    ws.last_pong_at = now[0]
    assert not ws.is_data_fresh()
    assert [r["symbol"] for r in ws.stale_feeds()] == [BTC]


@pytest.mark.parametrize(
    "channel", ["funding-rate", "open-interest", "orders", "positions", "account", "orders-algo"]
)
def test_event_or_low_frequency_channel_inactivity_is_not_market_stale(channel):
    ws, _ = ready_socket(
        private=channel in ("orders", "positions", "account", "orders-algo"), channels=(channel,)
    )
    assert ws.is_data_fresh()


def test_candle_freshness_is_semantic_not_twenty_seconds():
    ws, now = ready_socket(channels=("candle15m",))
    ws.last_data_at["candle15m:" + BTC] = now[0]
    now[0] += 60
    ws.last_rx_at = now[0]
    assert ws.is_fresh()
    now[0] += 1800
    assert not ws.is_data_fresh()


def test_transport_requires_login_and_each_subscription_ack():
    ws, _ = ready_socket(private=True, channels=("orders", "account"))
    ws.login_ok = False
    assert not ws.is_transport_healthy()
    ws.login_ok = True
    ws.subscribed.remove("account:" + BTC)
    assert not ws.is_transport_healthy()
    ws.subscribed.add("account:" + BTC)
    assert ws.is_transport_healthy()


def test_reconnect_resets_heartbeat_sequence_and_freshness():
    ws, now = ready_socket(channels=("books5",))
    ws.ping_pending = True
    ws.last_pong_at = 90
    ws.last_ping_at = 95
    ws.ping_rtt = 0.1
    ws.sequences["books5:" + BTC] = 100
    ws.last_data_at["books5:" + BTC] = now[0]
    generation = ws.generation
    ws._reset_transport()
    assert ws.generation == generation + 1
    assert not ws.ping_pending and ws.last_pong_at == ws.last_ping_at == 0
    assert ws.ping_rtt is None
    assert not ws.sequences and not ws.last_data_at and not ws.subscribed
    assert not ws.is_transport_healthy()


class Wire:
    def __init__(self, messages=(), *, pong=False):
        self.messages = asyncio.Queue()
        for m in messages:
            self.messages.put_nowait(m if isinstance(m, str) else json.dumps(m))
        self.sent = []
        self.pong = pong
        self.ping_sent = asyncio.Event()

    async def recv(self):
        return await self.messages.get()

    async def send(self, data):
        self.sent.append(data)
        if data == "ping":
            self.ping_sent.set()
            if self.pong:
                self.messages.put_nowait("pong")


async def wait_until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.001)


async def test_application_ping_pong_keeps_idle_private_transport_alive():
    ws, _ = ready_socket(private=True, ws_idle_ping_seconds=0.05, ws_pong_timeout_seconds=0.2)
    ws.clock = time.monotonic
    ws.last_rx_at = ws._connected_at = ws.clock()
    wire = Wire(pong=True)
    task = asyncio.create_task(ws._receive(wire))
    try:
        await wait_until(lambda: ws.last_pong_at > 0)
        assert ws.is_transport_healthy() and ws.is_data_fresh()
        assert ws.ping_rtt is not None
        assert ws.last_data_at == {}
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_pong_timeout_reconnects_without_second_ping():
    ws, _ = ready_socket(private=True, ws_idle_ping_seconds=0.01, ws_pong_timeout_seconds=0.01)
    ws.clock = time.monotonic
    ws.last_rx_at = ws._connected_at = ws.clock()
    wire = Wire()
    with pytest.raises(WebSocketFault, match="heartbeat timeout"):
        async with asyncio.timeout(2):
            await ws._receive(wire)
    assert wire.sent == ["ping"]
    assert not ws.is_transport_healthy()


async def test_other_frames_do_not_extend_pending_pong_deadline():
    ws, now = ready_socket(private=True)
    ws.ping_pending = True
    ws.last_ping_at = now[0] - ws.settings.ws_pong_timeout_seconds
    wire = Wire(["pong"])
    with pytest.raises(WebSocketFault, match="heartbeat timeout"):
        await ws._receive(wire)
    assert wire.messages.qsize() == 1


async def test_subscription_timeout_is_bounded_even_with_regular_pong():
    ws, now = ready_socket(private=True)
    ws.subscribed.clear()
    now[0] += ws.settings.request_timeout_seconds + 1
    ws.last_rx_at = now[0]
    with pytest.raises(WebSocketFault, match="acknowledgement timeout"):
        await ws._receive(Wire(["pong"]))


@pytest.mark.parametrize("private,channel", [(False, "candle1m"), (True, "orders")])
async def test_requested_snapshot_before_ack_is_buffered_without_early_health(private, channel):
    ws, now = ready_socket(private=private, channels=(channel,))
    ws.subscribed.clear()
    message = dict(arg=ws.subscriptions[0], data=[dict(n=1)])
    wire = Wire([message])
    task = asyncio.create_task(ws._receive(wire))
    try:
        await wait_until(lambda: wire.messages.empty())
        assert ws.queue.empty() and not ws.last_data_at
        assert not ws.is_transport_healthy()
        now[0] += 1
        wire.messages.put_nowait(json.dumps(dict(event="subscribe", arg=ws.subscriptions[0])))
        await wait_until(lambda: ws.queue.qsize() == 1)
        queued, arrival = ws.queue.get_nowait()
        assert queued == message and arrival == 100.0
        assert ws.is_transport_healthy()
        assert not ws.last_data_at  # The business handler must still apply the frame.
        ws.queue.task_done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_pre_ack_buffer_preserves_interleaved_subscription_order():
    ws, _ = ready_socket(channels=("candle1m", "candle15m"))
    ws.subscribed.clear()
    wire = Wire([
        dict(arg=ws.subscriptions[1], data=[dict(n=1)]),
        dict(event="subscribe", arg=ws.subscriptions[0]),
        dict(arg=ws.subscriptions[0], data=[dict(n=2)]),
        dict(event="subscribe", arg=ws.subscriptions[1]),
        dict(arg=ws.subscriptions[1], data=[dict(n=3)]),
    ])
    task = asyncio.create_task(ws._receive(wire))
    try:
        await wait_until(lambda: ws.queue.qsize() == 3)
        assert [ws.queue.get_nowait()[0]["data"][0]["n"] for _ in range(3)] == [1, 2, 3]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_pre_ack_data_does_not_bypass_ack_timeout():
    ws, now = ready_socket(channels=("candle1m",))
    ws.subscribed.clear()
    wire = Wire([dict(arg=ws.subscriptions[0], data=[dict(n=1)])])
    task = asyncio.create_task(ws._receive(wire))
    await wait_until(lambda: wire.messages.empty())
    now[0] += ws.settings.request_timeout_seconds + 1
    wire.messages.put_nowait("pong")
    with pytest.raises(WebSocketFault, match="acknowledgement timeout"):
        await asyncio.wait_for(task, 2)
    assert ws.queue.empty() and not ws.last_data_at and not ws.is_transport_healthy()


async def test_pre_ack_buffer_overflow_fails_closed():
    ws, _ = ready_socket(private=True, channels=("orders",), ws_queue_maxsize=1)
    ws.subscribed.clear()
    faults = []
    ws.on_fault = lambda _, reason: faults.append(reason)
    wire = Wire([dict(arg=ws.subscriptions[0], data=[dict(n=n)]) for n in (1, 2)])
    with pytest.raises(WebSocketFault, match="queue full"):
        await ws._receive(wire)
    assert faults == ["WebSocket processing backlog"]
    assert not ws.is_processing_healthy() and ws.queue.empty()


async def test_unsolicited_pre_ack_data_is_still_rejected():
    ws, _ = ready_socket(channels=("candle1m",))
    ws.subscribed.clear()
    with pytest.raises(WebSocketFault, match="unacknowledged subscription data"):
        await ws._receive(Wire([dict(arg=dict(channel="orders", instId=BTC), data=[{}])]))
    assert ws.queue.empty() and not ws.subscribed


async def test_slow_private_reconcile_does_not_block_transport_pong(tmp_path):
    runtime = TradingRuntime(
        Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/slow.db")
    )
    entered, release = asyncio.Event(), asyncio.Event()

    async def slow_reconcile():
        entered.set()
        await release.wait()

    runtime.reconcile = slow_reconcile
    ws, now = ready_socket(private=True, channels=("positions",))
    ws.handler = runtime._on_private
    wire = Wire([dict(arg=ws.subscriptions[0], data=[{"pos": "0"}])])
    worker = asyncio.create_task(ws._business_worker())
    receive = asyncio.create_task(ws._receive(wire))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        now[0] += 30  # Simulate a 30-second reconcile, without wall-clock sleeps.
        wire.messages.put_nowait("pong")
        await wait_until(lambda: ws.last_pong_at == now[0])
        assert ws.is_transport_healthy()
        assert ws.is_data_fresh()
        assert not ws.is_processing_healthy()  # A processing delay is reported accurately.
    finally:
        release.set()
        receive.cancel()
        worker.cancel()
        await asyncio.gather(receive, worker, return_exceptions=True)
        await runtime.close()


async def test_private_queue_overflow_immediately_fences_and_keeps_accepted_events(tmp_path):
    runtime = TradingRuntime(
        Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/overflow.db")
    )
    await runtime.store.initialize()
    runtime.governor.resume(synchronized=True, healthy=True)
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    ws, _ = ready_socket(private=True, ws_queue_maxsize=2)
    ws.on_fault = runtime._on_ws_fault
    runtime.sockets = [ws]
    seen = []

    async def handler(message):
        seen.append(message["data"][0]["n"])

    ws.handler = handler
    wire = Wire([dict(arg=ws.subscriptions[0], data=[dict(n=n)]) for n in range(3)])
    with pytest.raises(WebSocketFault, match="queue full"):
        await ws._receive(wire)
    assert ws.queue.maxsize == ws.queue.qsize() == 2
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.governor.reason == "WebSocket processing backlog"
    assert ws.reconciliation_required and not runtime.portfolio.synchronized
    assert not runtime._websockets_healthy()
    worker = asyncio.create_task(ws._business_worker())
    try:
        await asyncio.wait_for(ws.queue.join(), 2)
        assert seen == [0, 1]  # The rejected frame triggers recovery; accepted frames aren't lost.
    finally:
        worker.cancel()
        await asyncio.gather(worker, *runtime._ws_safety_tasks.values(), return_exceptions=True)
        await runtime.close()


async def test_market_overflow_fails_closed_without_snapshot_coalescing():
    ws, _ = ready_socket(channels=("books5",), ws_queue_maxsize=1)
    faults = []
    ws.on_fault = lambda _, reason: faults.append(reason)
    messages = [
        dict(arg=ws.subscriptions[0], data=[{"seqId": n, "prevSeqId": n - 1}]) for n in (1, 2)
    ]
    with pytest.raises(WebSocketFault, match="queue full"):
        await ws._receive(Wire(messages))
    assert faults == ["WebSocket processing backlog"]
    assert ws.queue.qsize() == 1 and not ws.is_processing_healthy()


async def test_worker_credits_arrival_time_not_completion_time():
    ws, now = ready_socket(channels=("books5",))
    message = dict(arg=ws.subscriptions[0], data=[{"bids": []}])
    ws.queue.put_nowait((message, now[0]))
    now[0] += 30
    worker = asyncio.create_task(ws._business_worker())
    try:
        await asyncio.wait_for(ws.queue.join(), 2)
        assert ws.last_data_at["books5:" + BTC] == 100
        assert not ws.is_data_fresh()
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.parametrize("notice,kind", [("64008", "maintenance"), ("unknown", "protocol")])
async def test_notice_has_explicit_reconnect_classification(notice, kind):
    ws, _ = ready_socket()
    with pytest.raises(WebSocketFault) as caught:
        await ws._receive(
            Wire([dict(event="notice", code=notice, msg="secret must not be logged")])
        )
    assert caught.value.kind == kind
    if notice == "64008":
        assert "server maintenance" in caught.value.message
    assert "secret" not in caught.value.message


async def test_private_reconnect_relogs_resubscribes_and_requests_reconciliation():
    connections, logins = [], []
    ready = asyncio.Event()
    faults = []

    async def server_handler(socket):
        login = json.loads(await socket.recv())
        logins.append(login["op"])
        await socket.send(json.dumps(dict(event="login", code="0")))
        sub = json.loads(await socket.recv())
        connections.append(sub["args"])
        for arg in sub["args"]:
            await socket.send(json.dumps(dict(event="subscribe", arg=arg)))
        if len(connections) == 1:
            await socket.send(json.dumps(dict(event="notice", code="64008")))
        else:
            await socket.wait_closed()

    async with websockets.serve(server_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        ws = OkxWebSocket(
            f"ws://127.0.0.1:{port}",
            [dict(channel="orders", instType="SWAP")],
            noop,
            Settings(
                _env_file=None,
                okx_api_key="fake-key",
                okx_secret_key="fake-secret",
                okx_passphrase="fake-pass",
                ws_backoff_max_seconds=0.01,
            ),
            private=True,
            name="private-account",
            on_fault=lambda _, reason: faults.append(reason),
            on_ready=lambda sock: ready.set() if sock.generation >= 2 else None,
        )
        task = asyncio.create_task(ws.run())
        try:
            await asyncio.wait_for(ready.wait(), 3)
            assert logins == ["login", "login"]
            assert connections[0] == connections[1]
            assert ws.reconnects == 1 and ws.reconciliation_required
            assert ws.is_transport_healthy()
            assert ws.disconnect_kind == "maintenance"
            assert faults == ["WebSocket transport unavailable"]
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


def test_sequence_gap_has_structured_details_and_blocks_transport(caplog):
    ws, _ = ready_socket(channels=("books5",))
    ws._check_sequence(dict(arg=ws.subscriptions[0], data=[dict(seqId=5, prevSeqId=4)]))
    with pytest.raises(WebSocketFault, match="sequence gap"):
        ws._check_sequence(dict(arg=ws.subscriptions[0], data=[dict(seqId=7, prevSeqId=6)]))
    assert not ws.connected
    record = caplog.records[-1]
    assert record.socket_name == "public-market"
    assert record.expected == 5 and record.actual == 6
    assert record.channel == "books5" and record.symbol == BTC


def test_incremental_feed_requires_initial_snapshot():
    ws, _ = ready_socket(channels=("books",))
    with pytest.raises(WebSocketFault, match="requires snapshot"):
        ws._check_sequence(dict(arg=ws.subscriptions[0], action="update", data=[dict(seqId=1)]))


def test_close_reason_and_structured_log_never_include_secrets():
    import logging

    ws = OkxWebSocket(
        "wss://custom.invalid?token=hidden",
        [],
        noop,
        Settings(_env_file=None, okx_api_key="private-value"),
    )
    text = ws._safe_close_reason("private-value wss://custom.invalid?token=hidden token=abc")
    record = logging.makeLogRecord(
        dict(
            msg="websocket session ended",
            levelname="ERROR",
            socket_name="private-account",
            close_reason=text,
            close_code=1008,
        )
    )
    formatted = JsonFormatter().format(record)
    assert "private-value" not in formatted and "hidden" not in formatted and "abc" not in formatted
    assert json.loads(formatted)["socket_name"] == "private-account"
    assert json.loads(formatted)["close_code"] == 1008


@pytest.mark.parametrize(
    "reason",
    ["WebSocket transport unavailable", "Market data stale", "WebSocket processing backlog"],
)
def test_only_explicit_ws_transient_reasons_can_auto_recover(reason):
    assert classify_halt_reason(reason) == HaltClass.TRANSIENT_INFRA
    for other in (
        "unknown WS failure",
        "WebSocket protocol error",
        "WebSocket processing failed",
        "LIVE writer lease lost",
    ):
        assert classify_halt_reason(other) == HaltClass.SAFETY_OR_MANUAL


async def test_fault_fence_preserves_manual_halt_and_emergency(tmp_path):
    runtime = TradingRuntime(
        Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/fence.db")
    )
    runtime._schedule_ws_reconciliation = lambda *args: None
    ws, _ = ready_socket(private=True)
    runtime._halt_state("LIVE writer lease lost")
    runtime._on_ws_fault(ws, "WebSocket transport unavailable")
    assert runtime.governor.reason == "LIVE writer lease lost"
    assert runtime._auto_recovery_forbidden
    runtime.governor.halt("unprotected position", emergency=True)
    runtime._on_ws_fault(ws, "WebSocket processing backlog")
    assert runtime.governor.state == GovernorState.EMERGENCY
    assert not runtime.auto_recovery_status()["eligible"]
    await runtime.close()


def ws_event(component, *, recovery=False):
    titles = dict(
        websocket="🚨 WebSocket 连接中断",
        market_data="⚠️ 市场数据过期",
        ws_backlog="🚨 WebSocket 消息处理积压",
    )
    return NotificationEvent(
        level=NotificationLevel.WARNING,
        category=NotificationCategory.INFRASTRUCTURE,
        title=titles[component],
        message="component changed",
        metadata=dict(
            component=component,
            recovery=recovery,
            risk_state="NORMAL",
            ws_details="Socket: public-market\nFeed: books5\nSymbol: " + BTC + "\nAge: 25s",
        ),
    )


@pytest.mark.parametrize("component", ["websocket", "market_data", "ws_backlog"])
def test_prolonged_ws_incident_alert_once_then_recovery_once(component):
    now = [0.0]
    manager = IncidentManager(delay_seconds=60, clock=lambda: now[0])
    manager.observe(ws_event(component))
    now[0] = 61
    events = manager.due()
    assert len(events) == 1
    assert "Socket: public-market" in events[0].message
    if component == "market_data":
        assert events[0].title == "⚠️ 市场数据过期"
        assert "Feed: books5" in events[0].message and BTC in events[0].message
    assert NotificationPolicy().evaluate(events[0], "bark").send
    manager.delivery_confirmed(events[0])
    assert manager.due() == []
    recovery = manager.observe(ws_event(component, recovery=True))
    assert len(recovery) == 1
    assert recovery[0].title in ("✅ WebSocket 已恢复", "✅ 市场数据已恢复", "✅ WS 消息处理恢复")
    manager.delivery_confirmed(recovery[0])
    assert manager.observe(ws_event(component, recovery=True)) == []


def test_short_ws_reconnect_suppresses_phone_noise():
    now = [0.0]
    manager = IncidentManager(delay_seconds=60, clock=lambda: now[0])
    manager.observe(ws_event("websocket"))
    now[0] = 2
    assert manager.observe(ws_event("websocket", recovery=True)) == []
    assert manager.due() == []


def test_transport_stale_and_backlog_have_different_phone_titles():
    titles = []
    for component in ("websocket", "market_data", "ws_backlog"):
        manager = IncidentManager(delay_seconds=0)
        manager.observe(ws_event(component))
        titles.append(manager.due()[0].title)
    assert len(set(titles)) == 3


@pytest.mark.parametrize(
    "bad_field,error",
    [
        ("transport_healthy", "WebSocketTransportUnavailable"),
        ("critical_data_fresh", "MarketDataStale"),
        ("processing_healthy", "WebSocketProcessingBacklog"),
        ("reconciliation_required", "WebSocketReconciliationPending"),
    ],
)
async def test_external_watchdog_uses_layered_health(bad_field, error):
    socket = dict(
        name="private-account",
        connected=True,
        transport_healthy=True,
        critical_data_fresh=True,
        processing_healthy=True,
        reconciliation_required=False,
        last_rx_age_seconds=1,
        last_business_age_seconds=300,
    )
    socket[bad_field] = bad_field == "reconciliation_required"
    status = dict(running=True, synchronized=True, risk_state="HALT", websockets=[socket])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=status))
    ) as client:
        notifier = NotificationManager([Recorder()], Metrics())
        monitor = WatchdogMonitor(WatchdogSettings(_env_file=None), client, notifier)
        assert (await monitor._probe())[1] == error
        socket[bad_field] = bad_field != "reconciliation_required"
        assert (await monitor._probe())[0] is None
        await notifier.close()


def test_status_and_metrics_are_bounded_and_safe():
    ws, _ = ready_socket(private=True)
    ws.metrics = Metrics()
    status = ws.status()
    assert status["name"] == "private-account"
    assert status["transport_healthy"] and status["critical_data_fresh"]
    assert status["queue_capacity"] == 1000 and status["queue_depth"] == 0
    assert "url" not in status and "credentials" not in json.dumps(status)
    ws._queue_metric()
    metrics = ws.metrics.render().decode()
    for metric in (
        "quant_ws_queue_depth",
        "quant_ws_queue_wait_seconds",
        "quant_ws_handler_latency_seconds",
        "quant_ws_ping_rtt_seconds",
        "quant_ws_transport_disconnects_total",
        "quant_ws_market_stale_events_total",
    ):
        assert metric in metrics
    assert BTC not in metrics


def test_short_reconnect_with_immediate_halt_stays_phone_silent_until_gates_pass():
    now = [0.0]
    incidents = IncidentManager(delay_seconds=60, clock=lambda: now[0])
    incidents.observe(ws_event("websocket"))
    incidents.observe(
        NotificationEvent(
            level=NotificationLevel.ERROR,
            category=NotificationCategory.RISK,
            title="🚨 HALT",
            message="Reason: WebSocket transport unavailable",
            metadata={"risk_state": "HALT"},
        )
    )
    now[0] = 2
    assert incidents.observe(ws_event("websocket", recovery=True)) == []
    now[0] = 65
    assert incidents.due() == []  # Waiting for risk gates is not an ongoing disconnect.
    now[0] = 90
    resolved = incidents.observe(
        NotificationEvent(
            level=NotificationLevel.INFO,
            category=NotificationCategory.RISK,
            title="✅ Risk State Recovered",
            message="Current: NORMAL",
            metadata={"recovery": True},
        )
    )
    assert resolved == []
    assert incidents.history[-1].duration_seconds == 2


def test_maintenance_incident_phone_title_is_not_local_network_failure():
    incidents = IncidentManager(delay_seconds=0)
    event = ws_event("websocket")
    event.metadata["ws_details"] = "Socket: private-algo\nReason: server maintenance notice 64008"
    incidents.observe(event)
    alert = incidents.due()[0]
    assert alert.title == "⚠️ OKX WebSocket 服务端要求重连"


def test_active_handler_age_is_not_hidden_by_newer_queued_messages():
    ws, now = ready_socket(private=True)
    ws._handler_started_at = now[0]
    now[0] += 30
    ws._queued_at.append(now[0])
    assert not ws.is_processing_healthy()


async def test_unknown_handler_error_is_manual_and_never_auto_recovers(tmp_path):
    runtime = TradingRuntime(
        Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/badhandler.db")
    )
    runtime._schedule_ws_reconciliation = lambda *args: None
    ws, _ = ready_socket(private=True)
    ws.on_fault = runtime._on_ws_fault

    async def broken(_):
        raise ValueError("fake-secret-must-not-be-logged")

    ws.handler = broken
    ws.queue.put_nowait((dict(arg=ws.subscriptions[0], data=[{}]), 100))
    worker = asyncio.create_task(ws._business_worker())
    try:
        await asyncio.wait_for(ws.queue.join(), 2)
        assert runtime.governor.state == GovernorState.HALT
        assert runtime.governor.reason == "WebSocket processing failed"
        assert runtime._auto_recovery_forbidden and not ws.is_processing_healthy()
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        await runtime.close()


@pytest.fixture
async def ws_runtime(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/health.db",
        okx_api_key="fake-key",
        okx_secret_key="fake-secret",
        okx_passphrase="fake-pass",
    )
    runtime = TradingRuntime(settings)
    await runtime.store.initialize()
    now = [100.0]
    runtime._clock = lambda: now[0]
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    runtime.redis = AsyncMock()
    runtime.redis.get.return_value = "1"
    runtime.dead_man_healthy = True
    runtime._last_caa_success_at = now[0]
    from decimal import Decimal

    from app.models import PortfolioState

    runtime.portfolio = PortfolioState(
        equity=Decimal(5000), available_balance=Decimal(5000), synchronized=True
    )

    async def reconcile():
        runtime.reconciliation_healthy = True
        runtime._last_reconcile_safe = True
        runtime.portfolio.synchronized = True

    runtime._reconcile_impl = AsyncMock(side_effect=reconcile)
    runtime.governor.resume(synchronized=True, healthy=True)
    yield runtime, now
    for task in runtime._ws_safety_tasks.values():
        task.cancel()
    await asyncio.gather(*runtime._ws_safety_tasks.values(), return_exceptions=True)
    await runtime.close()


async def test_private_disconnect_requires_reconciliation_then_three_health_checks(ws_runtime):
    runtime, now = ws_runtime
    ws, _ = ready_socket(private=True, now=now)
    runtime.sockets = [ws]
    ws.connected = False
    ws.reconciliation_required = True
    runtime._on_ws_fault(ws, "WebSocket transport unavailable")
    assert runtime.governor.state == GovernorState.HALT  # Before any await/cancellation/HTTP.
    assert not runtime.portfolio.synchronized
    await asyncio.gather(*runtime._ws_safety_tasks.values())
    assert ws.reconciliation_required
    assert not (await runtime._resume_health())[0]
    ws._reset_transport()
    ws.login_ok = True
    ws.subscribed = {ws.feed_key(a) for a in ws.subscriptions}
    assert not runtime._websockets_healthy()  # Connection established alone isn't recovery.
    runtime._on_ws_ready(ws)
    await asyncio.gather(*runtime._ws_safety_tasks.values())
    assert not ws.reconciliation_required
    assert runtime.governor.state == GovernorState.HALT
    for index in range(3):
        now[0] += 30
        ws.last_rx_at = ws.last_pong_at = now[0]
        await runtime._auto_recovery_check()
        assert runtime.governor.state == (
            GovernorState.NORMAL if index == 2 else GovernorState.HALT
        )
    assert runtime._reconcile_impl.await_count >= 5


async def test_market_stale_blocks_risk_and_recovers_only_after_all_gates(ws_runtime):
    from tests.test_core import instrument, intent, portfolio, ready_risk

    runtime, now = ws_runtime
    ws, _ = ready_socket(channels=("books5", "tickers", "mark-price"), now=now)
    runtime.sockets = [ws]
    ws.last_data_at = {ws.feed_key(a): now[0] - 21 for a in ws.subscriptions}
    assert runtime._ws_health_reason() == "Market data stale"
    assert (
        not ready_risk()
        .evaluate(
            intent(),
            portfolio(),
            instrument(),
            data_fresh=runtime._websockets_healthy(),
            infrastructure_healthy=True,
        )
        .approved
    )
    await runtime.enter_halt(runtime._ws_health_reason())
    now[0] += 30
    ws.last_rx_at = now[0]
    await runtime._auto_recovery_check()
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.auto_recovery_status()["successes"] == 0
    for index in range(3):
        now[0] += 30
        ws.last_rx_at = now[0]
        ws.last_data_at = {ws.feed_key(a): now[0] for a in ws.subscriptions}
        await runtime._auto_recovery_check()
        assert runtime.governor.state == (
            GovernorState.NORMAL if index == 2 else GovernorState.HALT
        )


@pytest.mark.parametrize(
    "bad_gate",
    ["redis", "cancel_all_after", "database", "blocked_entries", "emergency_targets", "margin"],
)
async def test_ws_recovery_does_not_bypass_other_safety_gates(ws_runtime, bad_gate):
    from decimal import Decimal

    runtime, now = ws_runtime
    ws, _ = ready_socket(private=True, now=now)
    runtime.sockets = [ws]
    await runtime.enter_halt("WebSocket transport unavailable")
    if bad_gate == "redis":
        runtime.redis.get.return_value = "unknown"
    elif bad_gate == "cancel_all_after":
        runtime.dead_man_healthy = False
    elif bad_gate == "database":
        runtime.store.healthy = False
    elif bad_gate == "blocked_entries":
        runtime.entry_controller.blocked.add(BTC)
    elif bad_gate == "emergency_targets":
        runtime.emergency.targets[BTC] = Decimal(0)
    elif bad_gate == "margin":
        runtime.portfolio.margin_used = Decimal(2000)
    now[0] += 30
    ws.last_rx_at = now[0]
    await runtime._auto_recovery_check()
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.auto_recovery_status()["successes"] == 0


@pytest.mark.parametrize("race", ["new_connection", "queued_event", "failed_reconciliation"])
async def test_private_reconciliation_epoch_cannot_clear_unknown_state(ws_runtime, race):
    runtime, now = ws_runtime
    ws, _ = ready_socket(private=True, now=now)
    ws.reconciliation_required = True
    ws.processing_unsafe = True
    runtime.sockets = [ws]
    original = runtime._reconcile_impl.side_effect

    async def race_during_reconcile():
        await original()
        if race == "new_connection":
            ws._reset_transport()
            ws.login_ok = True
            ws.subscribed = {ws.feed_key(a) for a in ws.subscriptions}
        elif race == "queued_event":
            ws.queue.put_nowait((dict(arg=ws.subscriptions[0], data=[]), now[0]))
        else:
            runtime.reconciliation_healthy = False

    runtime._reconcile_impl.side_effect = race_during_reconcile
    await runtime.reconcile()
    assert ws.reconciliation_required
    assert not runtime._websockets_healthy()


async def test_reconciled_backlog_does_not_resume_governor_by_itself(ws_runtime):
    runtime, now = ws_runtime
    ws, _ = ready_socket(private=True, now=now)
    ws.processing_unsafe = True
    ws.reconciliation_required = True
    runtime.sockets = [ws]
    await runtime.enter_halt("WebSocket processing backlog")
    await runtime.reconcile()
    assert not ws.processing_unsafe and not ws.reconciliation_required
    assert runtime.governor.state == GovernorState.HALT
    assert (await runtime._resume_health())[0]


async def test_regular_private_pong_allows_risk_with_no_order_activity(ws_runtime):
    from tests.test_core import instrument, intent, portfolio, ready_risk

    runtime, now = ws_runtime
    ws, _ = ready_socket(private=True, now=now)
    runtime.sockets = [ws]
    now[0] += 300
    ws.last_rx_at = ws.last_pong_at = now[0]
    assert runtime._websockets_healthy()
    assert (
        ready_risk()
        .evaluate(
            intent(),
            portfolio(),
            instrument(),
            data_fresh=runtime._websockets_healthy(),
            infrastructure_healthy=True,
        )
        .approved
    )


async def test_shutdown_unsettled_private_queue_fails_closed_without_cancelling_handler(ws_runtime):
    runtime, now = ws_runtime
    runtime.settings.entry_cancel_confirm_seconds = 0.01
    runtime.running = True
    ws, _ = ready_socket(private=True, now=now)
    runtime.sockets = [ws]
    entered, release = asyncio.Event(), asyncio.Event()

    async def slow_handler(_):
        entered.set()
        await release.wait()

    ws.handler = slow_handler
    ws.queue.put_nowait((dict(arg=ws.subscriptions[0], data=[{}]), now[0]))
    worker = asyncio.create_task(ws._business_worker())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        with pytest.raises(RuntimeError, match="processing unconfirmed"):
            await runtime._safe_stop()
        assert runtime.running and runtime.governor.state == GovernorState.HALT
        assert not worker.done()
        release.set()
        await asyncio.wait_for(ws.queue.join(), 2)
    finally:
        release.set()
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        runtime.running = False


async def test_maintenance_reconnect_logs_are_structured_and_not_network_errors(caplog):
    import logging

    caplog.set_level(logging.INFO, logger="okx")
    ready = asyncio.Event()
    connections = [0]

    async def server_handler(socket):
        await socket.recv()
        connections[0] += 1
        await socket.send(json.dumps(dict(event="subscribe", arg=dict(channel="orders"))))
        if connections[0] == 1:
            await socket.send(json.dumps(dict(event="notice", code="64008")))
        else:
            ready.set()
            await socket.wait_closed()

    async with websockets.serve(server_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        ws = OkxWebSocket(
            f"ws://127.0.0.1:{port}",
            [dict(channel="orders")],
            noop,
            Settings(_env_file=None, ws_backoff_max_seconds=0.01),
            name="private-algo",
        )
        task = asyncio.create_task(ws.run())
        try:
            await asyncio.wait_for(ready.wait(), 3)
            record = next(r for r in caplog.records if r.getMessage() == "websocket session ended")
            assert (
                record.levelname == "INFO" and record.ws_reason == "server maintenance notice 64008"
            )
            assert record.socket_name == "private-algo"
            assert ws.reconnects == 1
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_connection_closed_logs_safe_code_reason_and_fences_immediately(caplog):
    ready = asyncio.Event()
    connections = [0]
    faults = []

    async def server_handler(socket):
        await socket.recv()
        connections[0] += 1
        await socket.send(json.dumps(dict(event="subscribe", arg=dict(channel="orders"))))
        if connections[0] == 1:
            await socket.close(code=1012, reason="fake-secret wss://example.invalid?token=hidden")
        else:
            ready.set()
            await socket.wait_closed()

    async with websockets.serve(server_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        ws = OkxWebSocket(
            f"ws://127.0.0.1:{port}",
            [dict(channel="orders")],
            noop,
            Settings(_env_file=None, okx_secret_key="fake-secret", ws_backoff_max_seconds=0.01),
            name="public-market",
            on_fault=lambda _, reason: faults.append(reason),
        )
        task = asyncio.create_task(ws.run())
        try:
            await asyncio.wait_for(ready.wait(), 3)
            record = next(r for r in caplog.records if r.getMessage() == "websocket session ended")
            assert record.close_code == 1012 and record.ws_reason == "connection closed"
            formatted = JsonFormatter().format(record)
            assert "fake-secret" not in formatted and "token=hidden" not in formatted
            assert "last_rx_age" in formatted and "queue_depth" in formatted
            assert faults == ["WebSocket transport unavailable"]
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_partial_modern_watchdog_status_is_not_accepted_as_legacy():
    status = dict(
        running=True,
        synchronized=True,
        risk_state="NORMAL",
        websockets=[dict(fresh=True, critical_data_fresh=False)],
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=status))
    ) as client:
        notifier = NotificationManager([Recorder()], Metrics())
        monitor = WatchdogMonitor(WatchdogSettings(_env_file=None), client, notifier)
        assert (await monitor._probe())[1] == "InvalidWebSocketStatus"
        await notifier.close()


async def test_server_maintenance_does_not_count_as_network_flapping(ws_runtime):
    runtime, now = ws_runtime
    ws, _ = ready_socket(private=True, now=now)
    ws.reconnects = 4
    ws.maintenance_reconnects = 4
    runtime.sockets = [ws]
    await runtime._observe_infrastructure()
    assert runtime._last_reconnect_total == 0
    assert not runtime._reconnect_times


def test_compatibility_fresh_requires_private_reconnect_reconciliation():
    ws, _ = ready_socket(private=True)
    assert ws.is_fresh()
    ws.reconciliation_required = True
    assert ws.is_transport_healthy() and ws.is_data_fresh()
    assert not ws.is_fresh() and not ws.status()["fresh"]


async def test_late_pong_returned_after_scheduler_delay_is_not_healthy():
    ws, now = ready_socket(private=True)
    ws.ping_pending = True
    ws.last_ping_at = now[0]

    class LateWire(Wire):
        async def recv(self):
            now[0] += 6
            return "pong"

    with pytest.raises(WebSocketFault, match="heartbeat timeout"):
        await ws._receive(LateWire())
    assert ws.last_pong_at == 0 and not ws.is_transport_healthy()
