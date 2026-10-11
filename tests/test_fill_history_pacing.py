import asyncio
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import text

from app.accounting_correction import AccountingCorrectionService
from app.config import Settings
from app.okx import FillHistoryLimiter, OkxError, OkxRestClient
from app.storage import Store
from app.terminal_fills import TerminalFillError, read_terminal_fills
from tests.test_accounting_correction import evidence, seed
from tests.test_live_lease import postgres_store as postgres_store


class Clock:
    def __init__(self):
        self.now = 0.0
        self.waits = []

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        self.waits.append(seconds)
        self.now += seconds
        await asyncio.sleep(0)


def paced_client(http, clock):
    client = OkxRestClient(Settings(_env_file=None, okx_api_key='test',
        okx_secret_key='test', okx_passphrase='test'), http)
    client._fills_history_limiter = FillHistoryLimiter(clock=clock, sleep=clock.sleep)
    return client


@pytest.mark.parametrize('concurrent', [False, True])
async def test_more_than_eleven_history_calls_obey_uid_window(concurrent):
    clock, times = Clock(), []

    def respond(request):
        times.append(clock())
        return httpx.Response(200, json={'code': '0', 'data': []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url='https://test') as http:
        client = paced_client(http, clock)
        if concurrent:
            await asyncio.gather(*(client.fills_history('BTC-USDT-SWAP' if i % 2 else 'ETH-USDT-SWAP') for i in range(30)))
        else:
            for _ in range(30):
                await client.fills_history('BTC-USDT-SWAP')
    assert len(times) == 30 and times[0] == 0
    assert all(b-a >= 0.25 for a, b in zip(times, times[1:], strict=False))
    assert max(sum(start <= t < start + 2 for t in times) for start in times) <= 10
    assert len(clock.waits) == 29


async def test_direct_history_request_shares_limiter_with_page_method():
    clock, times = Clock(), []
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: times.append(clock()) or
            httpx.Response(200, json={'code': '0', 'data': []})), base_url='https://test') as http:
        client = paced_client(http, clock)
        await client.request('GET', '/api/v5/trade/fills-history', private=True)
        await client.fills_history('BTC-USDT-SWAP')
    assert times == [0, 0.25]


async def test_non_history_get_and_post_not_blocked_by_history_sleep_or_retried():
    waiting = asyncio.Event()
    requests = []

    async def sleep(_):
        waiting.set()
        await asyncio.Event().wait()

    def respond(request):
        requests.append((request.method, request.url.path))
        return httpx.Response(200, json={'code': '50011' if request.method == 'POST' else '0', 'data': []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url='https://test') as http:
        client = paced_client(http, Clock())
        client._fills_history_limiter.sleep = sleep
        await client.fills_history('BTC-USDT-SWAP')
        delayed = asyncio.create_task(client.fills_history('BTC-USDT-SWAP'))
        try:
            await waiting.wait()
            assert await client.positions() == []
            assert await client.fills('ETH-USDT-SWAP') == []
            with pytest.raises(OkxError):
                await client.request('POST', '/api/v5/trade/order', body={'reduceOnly': True})
            assert requests.count(('POST', '/api/v5/trade/order')) == 1
        finally:
            delayed.cancel()
            await asyncio.gather(delayed, return_exceptions=True)


def order_fills(n):
    _, detail, old, _ = evidence()
    rows = [{**old[0], 'tradeId': f'fill-{i}', 'billId': str(i), 'fillSz': '1', 'fee': '-0.1'}
            for i in range(n, 0, -1)]
    detail.update(sz=str(n), accFillSz=str(n), fillSz='1', tradeId=f'fill-{n}',
                  fee=str(Decimal('-0.1') * n))
    return detail, rows


@pytest.mark.parametrize('n', [1, 101, 5000, 5001])
async def test_exact_order_pagination_exhausts_or_fails_at_boundary_under_pacing(n):
    detail, rows = order_fills(n)
    clock, requests = Clock(), []

    def respond(request):
        requests.append(request)
        q = dict(request.url.params)
        assert q['instId'] == detail['instId'] and q['ordId'] == detail['ordId']
        assert q['instType'] == 'SWAP' and q['limit'] == '100'
        after = int(q.get('after', n + 1))
        page = [r for r in rows if int(r['billId']) < after][:100]
        return httpx.Response(200, json={'code': '0', 'data': page})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url='https://test') as http:
        if n > 5000:
            with pytest.raises(TerminalFillError, match='evidence_incomplete'):
                await read_terminal_fills(paced_client(http, clock), detail)
            assert len(requests) == 51
        else:
            result = await read_terminal_fills(paced_client(http, clock), detail)
            assert len(result) == n and len(requests) == (n + 99) // 100 + 1
            assert sum(Decimal(r['fillSz']) for r in result) == Decimal(detail['accFillSz'])
            assert requests[-1].url.params['after'] == '1'
    assert clock.now == 0.25 * (len(requests) - 1)


@pytest.mark.parametrize('status,code', [(200, '50011'), (429, '50011'), (401, '50113'), (403, '50113')])
async def test_server_rate_or_auth_error_is_single_read_and_incomplete(status, code):
    detail, _ = order_fills(1)
    calls = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: calls.append(r) or
            httpx.Response(status, json={'code': code, 'data': []})), base_url='https://test') as http:
        with pytest.raises(TerminalFillError, match='history_unavailable'):
            await read_terminal_fills(paced_client(http, Clock()), detail)
    assert len(calls) == 1 and calls[0].method == 'GET'


async def test_server_50011_after_full_size_still_rejects_partial_evidence():
    detail, rows = order_fills(1)
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, json={'code': '0' if len(calls) == 1 else '50011',
                                        'data': rows if len(calls) == 1 else []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url='https://test') as http:
        with pytest.raises(TerminalFillError, match='history_unavailable'):
            await read_terminal_fills(paced_client(http, Clock()), detail)
    assert len(calls) == 2


async def test_pacing_wait_consumes_existing_deadline_and_does_not_accept_partial(monkeypatch):
    from app import terminal_fills

    monkeypatch.setattr(terminal_fills, 'HISTORY_TIMEOUT_SECONDS', 0.01)
    detail, rows = order_fills(1)
    requests, waited, clock = [], asyncio.Event(), Clock()

    async def stuck_sleep(_):
        waited.set()
        await asyncio.Event().wait()

    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: requests.append(r) or
            httpx.Response(200, json={'code': '0', 'data': rows})), base_url='https://test') as http:
        client = paced_client(http, clock)
        client._fills_history_limiter.sleep = stuck_sleep
        with pytest.raises(TerminalFillError, match='history_unavailable'):
            await read_terminal_fills(client, detail)
        assert waited.is_set() and len(requests) == 1
        client._fills_history_limiter.sleep = clock.sleep
        await client.fills_history(detail['instId'])


@pytest.mark.parametrize('case', ['repeat_cursor', 'size', 'last_fill'])
async def test_paced_history_preserves_fatal_evidence_checks(case):
    detail, rows = order_fills(1)
    if case == 'size':
        detail['accFillSz'] = '2'
    if case == 'last_fill':
        detail['fillPx'] = '1'
    calls = []

    def respond(request):
        calls.append(request)
        page = rows if len(calls) == 1 or case == 'repeat_cursor' else []
        return httpx.Response(200, json={'code': '0', 'data': page})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url='https://test') as http:
        with pytest.raises(TerminalFillError):
            await read_terminal_fills(paced_client(http, Clock()), detail)
    assert len(calls) == 2


async def test_correction_uses_pacing_without_holding_sqlite_ledger_transaction(tmp_path):
    store = Store(f'sqlite+aiosqlite:///{tmp_path}/paced-correction.db')
    await store.initialize()
    local, detail, rows, _ = await seed(store)
    clock, calls = Clock(), []

    async def assert_unlocked():
        async with store.engine.begin() as connection:
            await connection.execute(text('BEGIN IMMEDIATE'))

    async def respond(request):
        await assert_unlocked()
        calls.append(request)
        data = ([detail] if request.url.path.endswith('/order') else
                rows if 'after' not in request.url.params else [])
        return httpx.Response(200, json={'code': '0', 'data': data})

    async def sleep(seconds):
        await assert_unlocked()
        await clock.sleep(seconds)

    async def status():
        return dict(mode='PAPER', running=False, risk_state='HALT')

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url='https://test') as http:
            client = paced_client(http, clock)
            client._fills_history_limiter.sleep = sleep
            result = await AccountingCorrectionService(client.settings, client, store, status).correct(
                local['symbol'], local['clOrdId'], apply=True,
            )
            assert result['corrections_added'] == 1 and clock.waits == [0.25]
        assert len(calls) == 3 and all(r.method == 'GET' for r in calls)
    finally:
        await store.close()


async def test_postgres_correction_http_and_pacing_do_not_hold_ledger_locks(postgres_store):
    store = postgres_store
    local, detail, rows, _ = await seed(store)
    clock = Clock()

    async def assert_unlocked():
        async with store.engine.begin() as conn:
            await conn.execute(text('LOCK TABLE fills, orders, order_events, fill_accounting_corrections IN ACCESS EXCLUSIVE MODE NOWAIT'))

    async def respond(request):
        await assert_unlocked()
        data = [detail] if request.url.path.endswith('/order') else rows if 'after' not in request.url.params else []
        return httpx.Response(200, json={'code': '0', 'data': data})

    async def sleep(seconds):
        await assert_unlocked()
        await clock.sleep(seconds)

    async def status():
        return dict(mode='PAPER', running=False, risk_state='HALT')

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url='https://test') as http:
        client = paced_client(http, clock)
        client._fills_history_limiter.sleep = sleep
        report = await AccountingCorrectionService(client.settings, client, store, status).correct(
            local['symbol'], local['clOrdId'], apply=True,
        )
    assert report['corrections_added'] == 1 and clock.waits == [0.25]
