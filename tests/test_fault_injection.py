import asyncio
import json
import time
from decimal import Decimal

import pytest
import websockets

import app.runtime as runtime_module
from app.config import Settings
from app.execution import ExecutionEngine, OrderManager
from app.models import GovernorState
from app.okx import OkxError, OkxWebSocket
from app.runtime import TradingRuntime
from tests.test_core import evaluate, instrument, intent, portfolio, ready_risk


@pytest.mark.asyncio
async def test_websocket_reconnects_after_disconnect():
    connections = 0
    received = asyncio.Event()

    async def server_handler(socket):
        nonlocal connections
        connections += 1
        await socket.recv()
        if connections == 1:
            await socket.close()
        else:
            await socket.send(
                json.dumps(
                    {"arg": {"channel": "tickers", "instId": "BTC"}, "data": [{"last": "100"}]}
                )
            )
            await asyncio.sleep(1)

    async def client_handler(message):
        assert message["data"][0]["last"] == "100"
        received.set()

    async with websockets.serve(server_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        ws = OkxWebSocket(
            f"ws://127.0.0.1:{port}",
            [{"channel": "tickers", "instId": "BTC"}],
            client_handler,
            Settings(_env_file=None, ws_backoff_max_seconds=1),
        )
        task = asyncio.create_task(ws.run())
        try:
            await asyncio.wait_for(received.wait(), timeout=5)
            assert ws.reconnects >= 1
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


def test_stale_websocket_blocks_data_freshness():
    async def handler(_):
        pass

    settings = Settings(_env_file=None, stale_timeout_seconds=1)
    ws = OkxWebSocket("ws://example.invalid", [], handler, settings)
    ws.connected = True
    ws.last_message_at = time.monotonic() - 2
    assert not ws.is_fresh()


def test_spread_explosion_triggers_halt():
    engine = ready_risk()
    decision = engine.evaluate(
        intent(),
        portfolio(),
        instrument(),
        data_fresh=True,
        infrastructure_healthy=True,
        spread=Decimal("0.01"),
    )
    assert not decision.approved
    assert engine.governor.state == GovernorState.HALT


@pytest.mark.asyncio
async def test_database_failure_prevents_order_submission():
    class FailingStore:
        async def append(self, *args, **kwargs):
            raise OSError("database unavailable")

    class FakeClient:
        placements = 0

        async def place_order(self, body):
            self.placements += 1
            return [{"ordId": "1"}]

    client = FakeClient()
    engine = ExecutionEngine(client, OrderManager(FailingStore()))
    request = ExecutionEngine.from_risk(evaluate(ready_risk()), instrument())
    with pytest.raises(OSError):
        await engine.submit(request)
    assert client.placements == 0


@pytest.mark.asyncio
async def test_unconfirmed_timeout_never_replaces_order(tmp_path):
    from app.storage import Store

    store = Store(f"sqlite+aiosqlite:///{tmp_path}/unknown.db")
    await store.initialize()

    class FakeClient:
        placements = 0

        async def place_order(self, body):
            self.placements += 1
            raise OkxError("response timeout")

        async def order(self, symbol, client_order_id):
            raise OkxError("query timeout")

    client = FakeClient()
    manager = OrderManager(store)
    engine = ExecutionEngine(client, manager)
    request = ExecutionEngine.from_risk(evaluate(ready_risk()), instrument())
    with pytest.raises(OkxError):
        await engine.submit(request)
    assert client.placements == 1
    assert manager.orders[request.client_order_id]["state"] == "UNKNOWN"
    await store.close()


@pytest.mark.asyncio
async def test_partial_fill_triggers_reduce_only_emergency(tmp_path):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/partial.db")
    runtime = TradingRuntime(settings)
    await runtime.store.initialize()
    runtime.instruments["BTC-USDT-SWAP"] = instrument()
    entry = ExecutionEngine.from_risk(evaluate(ready_risk()), instrument())
    await runtime.order_manager.create(entry)

    class FakeClient:
        reduce_orders = []
        cancelled = []

        async def pending_orders(self):
            return [{"instId": entry.symbol, "clOrdId": entry.client_order_id}]

        async def pending_algos(self):
            return []

        async def cancel_order(self, symbol, **identifiers):
            self.cancelled.append((symbol, identifiers))

        async def place_order(self, body):
            self.reduce_orders.append(body)
            return [{"ordId": "reduce-1", "sCode": "0"}]

    client = FakeClient()
    runtime.client = client
    runtime.execution.client = client
    event = {
        "clOrdId": entry.client_order_id,
        "instId": entry.symbol,
        "state": "partially_filled",
        "accFillSz": "0.5",
        "ordId": "entry-1",
    }
    await runtime._on_private({"arg": {"channel": "orders"}, "data": [event]})
    assert runtime.governor.state == GovernorState.EMERGENCY
    assert len(client.cancelled) == 1
    assert len(client.reduce_orders) == 1
    assert client.reduce_orders[0]["reduceOnly"] is True
    assert "attachAlgoOrds" not in client.reduce_orders[0]
    await runtime._on_private({"arg": {"channel": "orders"}, "data": [event]})
    assert len(client.reduce_orders) == 1
    await runtime.store.close()


@pytest.mark.asyncio
async def test_redis_restart_stays_halted_until_reconciliation(monkeypatch):
    runtime = TradingRuntime(Settings(_env_file=None))

    class FakeRedis:
        def __init__(self, available):
            self.available = available
            self.closed = False

        async def ping(self):
            if not self.available:
                raise ConnectionError("Redis down")

        async def set(self, *args, **kwargs):
            pass

        async def aclose(self):
            self.closed = True

    failed = FakeRedis(False)
    recovered = FakeRedis(True)
    runtime.redis = failed
    runtime.market.redis = failed
    monkeypatch.setattr(runtime_module.Redis, "from_url", lambda *args, **kwargs: recovered)
    await runtime._refresh_redis()
    assert failed.closed
    assert runtime.redis is None
    assert runtime.governor.state == GovernorState.HALT
    await runtime._refresh_redis()
    assert runtime.redis is recovered
    assert runtime.market.redis is recovered
    assert runtime.governor.state == GovernorState.HALT
    await runtime.client.close()
    await runtime.store.close()
