"""Upgrade failures and scheduler races never bypass continuity/heartbeat gates."""
import asyncio
import json
import logging
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest
import websockets
from sqlalchemy import insert
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

from app.bark_formatter import localize_bark_event
from app.logging import JsonFormatter
from app.models import GovernorState, NotificationEvent, utcnow
from app.okx import WebSocketFault
from app.runtime import TradingRuntime
from tests.test_production_safety import Exchange, settings
from tests.test_public_continuity import continuity_runtime as continuity_runtime
from tests.test_ws_reliability import ready_socket


@pytest.mark.parametrize('status', [500, 502, 503, 504, 401, 403, 404, 429, 200])
async def test_upgrade_status_is_classified_without_logging_server_payload(
    continuity_runtime, monkeypatch, caplog, status,
):
    runtime, ws, _ = continuity_runtime
    secret = 'fixture-private-http-body'
    ws.settings.okx_api_key = secret
    response = Response(status, secret, Headers({'X-Secret': secret}), body=secret.encode())

    class Rejected:
        async def __aenter__(self):
            raise InvalidStatus(response)

        async def __aexit__(self, *_):
            return False

    monkeypatch.setattr(websockets, 'connect', lambda *a, **k: Rejected())
    waiting = asyncio.Event()

    async def backoff():
        waiting.set()
        await asyncio.Future()

    ws._sleep_backoff = backoff
    caplog.set_level(logging.ERROR, logger='okx')
    task = asyncio.create_task(ws.run())
    try:
        await asyncio.wait_for(waiting.wait(), 2)
        assert runtime.governor.state == GovernorState.HALT
        temporary = status in {500, 502, 503, 504}
        assert ws.disconnect_kind == ('transport' if temporary else 'protocol')
        assert ws.reason_code == ('ws_handshake_unavailable' if temporary else 'ws_protocol_error')
        assert runtime.public_continuity.required is (not temporary)
        assert runtime._auto_recovery_forbidden is (not temporary)
        assert ws.status()['last_handshake_http_status'] == status
        assert ws.reconnects == 1 and ws.current_backoff <= ws.settings.ws_backoff_max_seconds
        record = next(r for r in caplog.records if r.getMessage() == 'websocket session ended')
        formatted = JsonFormatter().format(record)
        assert json.loads(formatted)['handshake_http_status'] == status
        event = NotificationEvent(level='ERROR', category='INFRASTRUCTURE',
                                  title='transport event', message=runtime._ws_details(ws))
        bark = localize_bark_event(event)
        assert f'最近握手 HTTP 状态码：{status}' in bark.message
        assert secret not in formatted + bark.message + json.dumps(ws.status())
        assert not ws.is_transport_healthy()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_real_http_503_upgrade_then_reconnect_resubscribes():
    attempts = 0
    active = asyncio.Event()
    faults = []

    async def process_request(connection, request):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return connection.respond(503, 'temporary maintenance')
        return None

    async def handler(socket):
        request = json.loads(await socket.recv())
        for arg in request['args']:
            await socket.send(json.dumps(dict(event='subscribe', arg=arg)))
        await socket.wait_closed()

    ws, _ = ready_socket(channels=('trades',), ws_backoff_max_seconds=.01)
    ws.batch_handler = AsyncMock()
    ws.on_fault = lambda _, reason: faults.append(reason)
    ws.on_ready = lambda _: active.set()
    async with websockets.serve(handler, '127.0.0.1', 0, process_request=process_request) as server:
        ws.url = f'ws://127.0.0.1:{server.sockets[0].getsockname()[1]}'
        task = asyncio.create_task(ws.run())
        try:
            await asyncio.wait_for(active.wait(), 2)
            assert attempts == 2 and ws.reconnects == 1
            assert faults == ['WebSocket transport unavailable']
            assert ws.reason_code == 'healthy'
            assert ws.last_failure_reason_code == 'ws_handshake_unavailable'
            assert ws.status()['last_handshake_http_status'] == 503
            assert ws.worker_alive and ws.is_transport_healthy()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize('relief_delay,timeout_wins,expected', [
    (.20, True, None), (.30, True, 'processing backpressure timeout'),
    (.20, False, None), (.30, False, 'processing backpressure timeout'),
])
async def test_backpressure_uses_consumer_relief_timestamp_not_callback_order(
    monkeypatch, relief_delay, timeout_wins, expected,
):
    ws, now = ready_socket(channels=('trades',), ws_queue_maxsize=4)
    ws.batch_handler = AsyncMock()
    message = dict(arg=ws.subscriptions[0], data=[{}])
    for _ in range(3):
        ws._enqueue_message(message, now[0])

    class Race:
        async def __aenter__(self):
            now[0] += relief_delay
            ws.queue.get_nowait()
            ws.queue.task_done()
            ws._queue_metric()
            # Admission resumes after the original deadline, but only timely
            # recorded relief can prove the bounded wait succeeded.
            now[0] = 100.4
            if timeout_wins:
                raise TimeoutError()

        async def __aexit__(self, *_):
            return False

    monkeypatch.setattr(asyncio, 'timeout', lambda _: Race())
    if expected:
        with pytest.raises(WebSocketFault, match=expected):
            await ws._admit_message(message, 100)
        assert ws.processing_unsafe and ws.queue.qsize() == 2
    else:
        await ws._admit_message(message, 100)
        assert not ws.processing_unsafe and ws.queue.qsize() == 3
    assert not ws.status()['backpressure_active']


@pytest.mark.parametrize('failure', ['heartbeat', 'worker'])
async def test_timely_backpressure_relief_cannot_hide_safety_failure(monkeypatch, failure):
    ws, now = ready_socket(channels=('trades',), ws_queue_maxsize=4)
    ws.batch_handler = AsyncMock()
    message = dict(arg=ws.subscriptions[0], data=[{}])
    for _ in range(3):
        ws._enqueue_message(message, 100)

    class Race:
        async def __aenter__(self):
            now[0] += .1
            ws.queue.get_nowait()
            ws.queue.task_done()
            ws._queue_metric()
            if failure == 'heartbeat':
                ws.ping_pending = True
                ws.last_ping_at = 90
            else:
                ws.processing_unsafe = True
            raise TimeoutError()

        async def __aexit__(self, *_):
            return False

    monkeypatch.setattr(asyncio, 'timeout', lambda _: Race())
    with pytest.raises(WebSocketFault, match='heartbeat timeout' if failure == 'heartbeat' else 'business handler failed'):
        await ws._admit_message(message, 100)
    assert ws.queue.qsize() == 2


async def test_daily_baseline_reconcile_reads_one_snapshot_preserving_full_weekly_peak(tmp_path):
    from app.storage import ROW_TYPES

    runtime = TradingRuntime(settings(tmp_path))
    await runtime.store.initialize()
    runtime.client = Exchange()
    runtime.client.position = 0
    runtime._schedule_ws_reconciliation = Mock()
    midnight = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        async with runtime.store.sessions.begin() as session:
            await session.execute(insert(ROW_TYPES['portfolio_snapshots']), [
                dict(timestamp=midnight, payload=dict(equity='9990')),
                *[dict(timestamp=midnight, payload=dict(equity='10000')) for _ in range(20001)],
                dict(timestamp=midnight, payload=dict(equity='10010')),
            ])
        original = runtime.store.since
        runtime.store.since = AsyncMock(wraps=original)
        await runtime.reconcile()
        assert runtime.reconciliation_healthy
        assert runtime.portfolio.daily_pnl == 10
        assert runtime.portfolio.weekly_drawdown == Decimal(10) / Decimal(10010)
        args = runtime.store.since.call_args
        assert args.args[0] == 'portfolio_snapshots' and args.kwargs['limit'] == 1
    finally:
        await runtime.notifications.close()
        await runtime.store.close()
