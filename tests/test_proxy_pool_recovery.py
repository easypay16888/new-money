"""Failed proxy TLS handshakes must not strand a bounded REST pool."""
import asyncio

import httpx
import pytest

from app.rest_pool import RestPool


async def test_proxy_tls_timeout_retires_pool_before_next_independent_request():
    handlers = set()
    connects = []

    async def proxy(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        try:
            header = await reader.readuntil(b'\r\n\r\n')
            assert header.startswith(b'CONNECT exchange.invalid:443 ')
            connects.append(header)
            writer.write(b'HTTP/1.1 200 Connection established\r\n\r\n')
            await writer.drain()
            # Accept CONNECT, then withhold the TLS handshake response.
            while await reader.read(8192):
                pass
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_server(proxy, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    clients = []

    def factory():
        client = httpx.AsyncClient(
            base_url='https://exchange.invalid', proxy=f'http://127.0.0.1:{port}',
            trust_env=False, limits=httpx.Limits(max_connections=1),
            timeout=httpx.Timeout(2, connect=0.1, pool=0.1),
        )
        clients.append(client)
        return client

    pool = RestPool(factory)
    try:
        for expected in (1, 2):
            with pytest.raises(httpx.ConnectTimeout):
                await pool.request('POST', '/caa')
            assert len(connects) == expected  # No implicit retry of this write.
            assert clients[-1].is_closed
            assert pool.active == 0 and pool.client is None
    finally:
        await pool.close()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers)


@pytest.mark.parametrize('failure', [
    httpx.ConnectTimeout, httpx.ConnectError, httpx.ProxyError,
    httpx.ReadTimeout, httpx.WriteTimeout, httpx.RemoteProtocolError,
])
async def test_transport_failure_drains_without_cancelling_or_replaying_writes(failure):
    started, release = asyncio.Event(), asyncio.Event()
    calls, clients = [], []

    def factory():
        generation = len(clients)

        async def handler(request):
            calls.append((generation, request.url.path))
            if request.url.path == '/inflight':
                started.set()
                await release.wait()
            elif request.url.path == '/failed':
                raise failure('transport unavailable')
            return httpx.Response(200)

        client = httpx.AsyncClient(base_url='https://exchange.invalid',
                                   transport=httpx.MockTransport(handler))
        clients.append(client)
        return client

    pool = RestPool(factory)
    inflight = asyncio.create_task(pool.request('POST', '/inflight'))
    try:
        await started.wait()
        with pytest.raises(failure):
            await pool.request('POST', '/failed')
        assert not clients[0].is_closed and not inflight.done()
        with pytest.raises(httpx.PoolTimeout):
            await pool.request('POST', '/must-not-send')
        assert calls == [(0, '/inflight'), (0, '/failed')]
        release.set()
        assert (await inflight).status_code == 200
        assert clients[0].is_closed
        assert (await pool.request('POST', '/new-action')).status_code == 200
        assert calls[-1] == (1, '/new-action')
    finally:
        release.set()
        await inflight
        await pool.close()


async def test_cancelled_handshake_retires_pool_without_retry():
    started = asyncio.Event()
    calls = []

    async def handler(request):
        calls.append(request.url.path)
        started.set()
        await asyncio.Event().wait()

    client = httpx.AsyncClient(base_url='https://exchange.invalid',
                               transport=httpx.MockTransport(handler))
    pool = RestPool(lambda: client)
    task = asyncio.create_task(pool.request('POST', '/order'))
    try:
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert calls == ['/order']
        assert client.is_closed and pool.active == 0 and pool.client is None
    finally:
        await pool.close()
