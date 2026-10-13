"""Public burst admission is lossless and bounded; private safety remains fail closed."""
import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from app.models import utcnow
from app.okx import WebSocketFault
from tests.test_live_lease import postgres_store as postgres_store
from tests.test_ws_reliability import BTC, ready_socket


class BurstWire:
    def __init__(self, subscriptions, count=6000):
        self.frames = iter(
            [json.dumps(dict(event="subscribe", arg=a)) for a in subscriptions]
            + [json.dumps(dict(arg=subscriptions[0], data=[dict(n=n)])) for n in range(count)]
            + ["pong"]
        )
        self.pong_read = asyncio.Event()
        self.send = AsyncMock()

    async def recv(self):
        try:
            frame = next(self.frames)
        except StopIteration:
            await asyncio.Future()
            raise AssertionError("unreachable") from None
        if frame == "pong":
            self.pong_read.set()
        return frame


async def test_public_burst_waits_for_consumer_without_loss_or_reconnect():
    ws, _ = ready_socket(channels=("trades",), ws_queue_maxsize=16)
    entered, release = asyncio.Event(), asyncio.Event()
    seen = []

    async def handle(messages):
        entered.set()
        await release.wait()
        seen.extend(m["data"][0]["n"] for m in messages)

    ws.batch_handler = handle
    ws._ensure_worker()
    wire = BurstWire(ws.subscriptions)
    receiver = asyncio.create_task(ws._receive(wire))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        # Fill the bounded queue while a durable batch is still in flight.
        async with asyncio.timeout(2):
            while ws.queue.qsize() < 12 and not receiver.done():
                await asyncio.sleep(0)
        assert not receiver.done() and not ws.processing_unsafe
        assert ws.queue.qsize() <= 16
        release.set()
        await asyncio.wait_for(wire.pong_read.wait(), 10)
        await asyncio.wait_for(ws.queue.join(), 10)
        assert seen == list(range(6000))
        assert ws.last_pong_at and not ws.processing_unsafe
        assert ws.queue._unfinished_tasks == 0
    finally:
        release.set()
        receiver.cancel()
        ws._stopping = True
        ws._worker_task.cancel()
        await asyncio.gather(receiver, ws._worker_task, return_exceptions=True)


async def test_public_stalled_consumer_still_fails_closed():
    ws, _ = ready_socket(channels=("trades",), ws_queue_maxsize=16)
    ws.batch_handler = AsyncMock()
    faults = []
    ws.on_fault = lambda _, reason: faults.append(reason)
    with pytest.raises(WebSocketFault):
        await asyncio.wait_for(ws._receive(BurstWire(ws.subscriptions)), 2)
    assert faults == ["WebSocket processing backlog"]
    assert ws.processing_unsafe
    assert ws.queue.qsize() <= 16


@pytest.mark.parametrize('private,channel', [(True, 'orders'), (False, 'candle15m')])
async def test_private_and_decision_overflow_remains_immediate(private, channel):
    ws, _ = ready_socket(private=private, channels=(channel,), ws_queue_maxsize=1)
    message = dict(arg=ws.subscriptions[0], data=[{}])
    ws._enqueue_message(message, 100)
    with pytest.raises(WebSocketFault, match='processing queue full'):
        await ws._admit_message(message, 100)
    assert ws.processing_unsafe and ws.queue.qsize() == 1


async def test_cancelled_public_admission_has_no_orphan_put_task():
    ws, _ = ready_socket(channels=('trades',), ws_queue_maxsize=4)
    ws.batch_handler = AsyncMock()
    message = dict(arg=ws.subscriptions[0], data=[{}])
    for _ in range(3):
        ws._enqueue_message(message, 100)
    task = asyncio.create_task(ws._admit_message(message, 100))
    await asyncio.sleep(0)
    assert not task.done() and ws.status()['backpressure_active']
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert ws.queue.qsize() == 3 and ws.queue._unfinished_tasks == 3
    assert not ws.status()['backpressure_active']


async def durable_burst(store):
    import websockets

    from app.config import Settings
    from app.market import MarketDataEngine
    from app.monitoring import Metrics
    from app.okx import OkxWebSocket

    market = MarketDataEngine(store)
    metrics = Metrics()
    market.batch_latency_observer = lambda stage, elapsed: metrics.market_batch_latency.labels(stage=stage).observe(elapsed)
    subscriptions = [dict(channel=c, instId=BTC) for c in ('trades', 'tickers')]
    ts = str(int(utcnow().timestamp() * 1000))
    complete = asyncio.Event()
    batches = []

    async def handle(messages):
        await market.handle_batch(messages)
        batches.append(len(messages))
        if messages[-1]['arg']['channel'] == 'tickers':
            complete.set()

    async def server_handler(socket):
        await socket.recv()
        for arg in subscriptions:
            await socket.send(json.dumps(dict(event='subscribe', arg=arg)))
        for n in range(6000):
            await socket.send(json.dumps(dict(arg=subscriptions[0], data=[dict(
                ts=ts, tradeId=str(n), px='100', sz='1', side='buy'
            )])))
        await socket.send(json.dumps(dict(arg=subscriptions[1], data=[dict(
            ts=ts, last='101', bidPx='100', askPx='102'
        )])))
        await socket.send('pong')
        await socket.wait_closed()

    faults = []
    async with websockets.serve(server_handler, '127.0.0.1', 0) as server:
        ws = OkxWebSocket(
            f'ws://127.0.0.1:{server.sockets[0].getsockname()[1]}', subscriptions, AsyncMock(),
            Settings(_env_file=None, ws_queue_maxsize=64), batch_handler=handle,
            metrics=metrics, name='public-market', on_fault=lambda _, reason: faults.append(reason),
        )
        task = asyncio.create_task(ws.run())
        try:
            await asyncio.wait_for(complete.wait(), 20)
            await asyncio.wait_for(ws.queue.join(), 2)
            assert not faults and ws.reconnects == 0 and ws.is_fresh()
            rows = await store.latest('market_trades', limit=6001)
            assert [r['trade_id'] for r in reversed(rows)] == [str(n) for n in range(6000)]
            assert [t.trade_id for t in market.latest_trades[BTC]] == [str(n) for n in range(5900, 6000)]
            assert market.latest_ticks[BTC].last == 101
            assert sum(batches) == 6001 and max(batches) <= 64
            assert metrics.registry.get_sample_value('quant_ws_received_messages_total', {'socket': 'public-market'}) == 6001
            assert metrics.registry.get_sample_value('quant_ws_backpressure_seconds_count', {'socket': 'public-market'}) > 0
            assert metrics.registry.get_sample_value('quant_market_batch_stage_seconds_count', {'stage': 'database'}) == len(batches)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_sqlite_real_socket_burst_is_complete_and_fresh(tmp_path):
    from app.storage import Store

    store = Store(f'sqlite+aiosqlite:///{tmp_path}/burst.db')
    await store.initialize()
    try:
        await durable_burst(store)
    finally:
        await store.close()


async def test_postgres_real_socket_burst_is_complete_and_fresh(postgres_store):
    await durable_burst(postgres_store)


async def test_backpressure_never_extends_pending_pong_deadline():
    ws, now = ready_socket(channels=('trades',), ws_queue_maxsize=4)
    ws.batch_handler = AsyncMock()
    ws.ping_pending = True
    ws.last_ping_at = now[0]
    message = dict(arg=ws.subscriptions[0], data=[{}])
    for _ in range(3):
        ws._enqueue_message(message, now[0])
    task = asyncio.create_task(ws._admit_message(message, now[0]))
    await asyncio.sleep(0)
    now[0] += ws.settings.ws_pong_timeout_seconds
    ws.queue.get_nowait()
    ws.queue.task_done()
    ws._queue_metric()
    with pytest.raises(WebSocketFault, match='heartbeat timeout'):
        await task
    assert ws.queue.qsize() == 2 and not ws.status()['backpressure_active']


async def test_worker_failure_during_backpressure_is_not_transient_backlog():
    ws, _ = ready_socket(channels=('trades',), ws_queue_maxsize=4)
    ws.batch_handler = AsyncMock()
    message = dict(arg=ws.subscriptions[0], data=[{}])
    for _ in range(3):
        ws._enqueue_message(message, 100)
    task = asyncio.create_task(ws._admit_message(message, 100))
    await asyncio.sleep(0)
    ws.processing_unsafe = True
    ws._queue_progress.set()
    with pytest.raises(WebSocketFault) as result:
        await task
    assert result.value.kind == 'handler'
    assert ws.queue.qsize() == 3


async def test_dead_worker_wakes_public_admission_and_preserves_failure_class():
    ws, _ = ready_socket(channels=('trades',), ws_queue_maxsize=4)
    ws.batch_handler = AsyncMock()
    message = dict(arg=ws.subscriptions[0], data=[{}])
    for _ in range(3):
        ws._enqueue_message(message, 100)
    admission = asyncio.create_task(ws._admit_message(message, 100))
    await asyncio.sleep(0)

    async def broken_worker():
        raise RuntimeError('worker crashed')

    ws._worker_task = asyncio.create_task(broken_worker())
    ws._worker_task.add_done_callback(ws._worker_done)
    await asyncio.gather(ws._worker_task, return_exceptions=True)
    with pytest.raises(WebSocketFault) as result:
        await admission
    assert result.value.kind == 'handler'
    assert ws.last_failure_reason_code == 'ws_worker_failed'
    assert ws.processing_unsafe and ws.queue.qsize() == 3
