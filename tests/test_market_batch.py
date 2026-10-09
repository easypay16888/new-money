import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event

from app.config import Settings
from app.models import utcnow
from app.okx import OkxWebSocket
from app.runtime import TradingRuntime
from app.storage import Store
from tests.test_live_lease import postgres_store as postgres_store
from tests.test_ws_reliability import BTC, noop, ready_socket


async def test_market_batch_retains_all_rows_order_and_references_with_one_commit(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/batch.db")
    await store.initialize()
    commits = []
    inserts = []
    event.listen(store.engine.sync_engine, "commit", lambda _: commits.append(True))
    event.listen(store.engine.sync_engine, "before_cursor_execute", lambda c, cursor, sql,
                 params, context, many: inserts.append(many) if sql.startswith("INSERT") else None)
    try:
        async with store.market_batch():
            for n in range(64):
                await store.append("market_trades", {"n": n}, symbol=BTC, reference_id=str(n))
        assert len(commits) == 1
        assert inserts == [True]  # Actual executemany, not 64 ORM INSERT round trips.
        assert await store.latest("market_trades") == [{"n": n} for n in reversed(range(64))]
        async with store.sessions() as session:
            from sqlalchemy import text
            rows = (await session.execute(text(
                "SELECT reference_id FROM market_trades ORDER BY id"
            ))).scalars().all()
        assert rows == [str(n) for n in range(64)]
    finally:
        await store.close()


async def test_postgres_market_bulk_preserves_all_payloads_and_order(postgres_store):
    async with postgres_store.market_batch():
        for n in range(64):
            await postgres_store.append("market_trades", {"n": n}, reference_id=str(n))
            await postgres_store.append("market_derivatives", {"n": n})
    expected = [{"n": n} for n in reversed(range(64))]
    assert await postgres_store.latest("market_trades") == expected
    assert await postgres_store.latest("market_derivatives") == expected


@pytest.mark.parametrize("table", ["orders", "fills", "emergency_targets", "market_candles"])
async def test_market_batch_cannot_group_trading_or_decision_writes(tmp_path, table):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/batch.db")
    await store.initialize()
    try:
        with pytest.raises(ValueError, match="non-market"):
            async with store.market_batch():
                await store.append("market_trades", {"n": 1})
                await store.append(table, {"n": 2})
        assert await store.latest("market_trades") == []
        assert await store.latest(table) == []
        await store.append(table, {"n": 3})
        assert await store.latest(table) == [{"n": 3}]
    finally:
        await store.close()


async def test_market_batch_rolls_back_and_resets_context_on_failure(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/batch.db")
    await store.initialize()
    try:
        with pytest.raises(RuntimeError, match="injected"):
            async with store.market_batch():
                await store.append("market_derivatives", {"n": 1})
                raise RuntimeError("injected")
        assert await store.latest("market_derivatives") == []
        await store.append("orders", {"n": 2})
        assert await store.latest("orders") == [{"n": 2}]
    finally:
        await store.close()


async def test_market_transaction_does_not_capture_concurrent_order_worker(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/batch.db")
    await store.initialize()
    ready = asyncio.Event()

    async def order_worker():
        await ready.wait()
        await store.append("orders", {"order": "independent"})

    task = asyncio.create_task(order_worker())
    try:
        with pytest.raises(RuntimeError, match="rollback"):
            async with store.market_batch():
                await store.append("market_trades", {"n": 1})
                ready.set()
                await task
                raise RuntimeError("rollback")
        assert await store.latest("market_trades") == []
        assert await store.latest("orders") == [{"order": "independent"}]
    finally:
        ready.set()
        await asyncio.gather(task, return_exceptions=True)
        await store.close()


async def test_batch_worker_is_bounded_ordered_and_preserves_arrival_freshness():
    ws, _ = ready_socket(channels=("trades",))
    seen, sizes = [], []

    async def handle(messages):
        sizes.append(len(messages))
        seen.extend(m["data"][0]["n"] for m in messages)

    ws.batch_handler = handle
    for n in range(130):
        ws._enqueue_message(dict(arg=ws.subscriptions[0], data=[dict(n=n)]), 90 + n / 100)
    worker = asyncio.create_task(ws._business_worker())
    try:
        await asyncio.wait_for(ws.queue.join(), 2)
        assert sizes == [64, 64, 2] and seen == list(range(130))
        assert ws.last_data_at["trades:" + BTC] == 91.29
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_batch_does_not_grant_freshness_until_commit_returns():
    ws, _ = ready_socket(channels=("mark-price",))
    entered, release = asyncio.Event(), asyncio.Event()

    async def handle(messages):
        entered.set()
        await release.wait()

    ws.batch_handler = handle
    ws._enqueue_message(dict(arg=ws.subscriptions[0], data=[{}]), 100)
    worker = asyncio.create_task(ws._business_worker())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert not ws.last_data_at and not ws.is_data_fresh()
        release.set()
        await asyncio.wait_for(ws.queue.join(), 2)
        assert ws.is_data_fresh()
    finally:
        release.set()
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_batch_commit_failure_fences_and_does_not_credit_market_health():
    ws, _ = ready_socket(channels=("mark-price",))
    faults = []
    ws.on_fault = lambda _, reason: faults.append(reason)
    ws.batch_handler = AsyncMock(side_effect=RuntimeError("commit failed"))
    ws._enqueue_message(dict(arg=ws.subscriptions[0], data=[{}]), 100)
    worker = asyncio.create_task(ws._business_worker())
    try:
        await asyncio.wait_for(ws.queue.join(), 2)
        assert faults == ["WebSocket processing failed"]
        assert not ws.last_data_at and not ws.is_processing_healthy()
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.parametrize("private,channel", [(True, "orders"), (False, "candle15m")])
def test_batch_handler_rejected_for_private_and_decision_streams(private, channel):
    with pytest.raises(ValueError, match="public market"):
        OkxWebSocket(
            "ws://test.invalid", [dict(channel=channel, instId=BTC)], noop,
            Settings(_env_file=None), private=private, batch_handler=AsyncMock(),
        )


async def test_runtime_market_batch_preserves_trade_data_and_fails_atomic_on_invalid_row(tmp_path):
    runtime = TradingRuntime(Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/market.db"
    ))
    await runtime.store.initialize()
    ts = str(int(utcnow().timestamp() * 1000))
    messages = [dict(arg=dict(channel="trades", instId=BTC), data=[dict(
        ts=ts, tradeId=str(n), px="100", sz="1", side="buy"
    )]) for n in range(3)]
    try:
        await runtime._on_public_market_batch(messages)
        assert [t.trade_id for t in runtime.market.latest_trades[BTC]] == ["0", "1", "2"]
        assert [r["trade_id"] for r in await runtime.store.latest("market_trades")] == ["2", "1", "0"]
        with pytest.raises(KeyError):
            await runtime._on_public_market_batch([messages[0], dict(
                arg=dict(channel="trades", instId=BTC), data=[{}]
            )])
        assert len(await runtime.store.latest("market_trades")) == 3
    finally:
        await runtime.close()


class CachePipeline:
    def __init__(self):
        self.writes = []
        self.execute = AsyncMock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def set(self, key, value, *, ex):
        self.writes.append((key, value, ex))


def ticker_message(n=0):
    return dict(arg=dict(channel="tickers", instId=BTC), data=[dict(
        ts=str(int(utcnow().timestamp() * 1000)), last=str(100 + n), bidPx="99", askPx="101"
    )])


async def test_public_batch_pipelines_latest_cache_once_after_durable_commit(tmp_path):
    from types import SimpleNamespace

    runtime = TradingRuntime(Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/cache.db"
    ))
    await runtime.store.initialize()
    pipeline = CachePipeline()
    cache = SimpleNamespace(set=AsyncMock(), pipeline=lambda **kw: pipeline)
    runtime.market.redis = cache
    commits = []
    event.listen(runtime.store.engine.sync_engine, "commit", lambda _: commits.append(True))

    async def execute():
        assert commits  # Cache I/O never holds the durable database transaction.

    pipeline.execute.side_effect = execute
    try:
        trade = dict(arg=dict(channel="trades", instId=BTC), data=[dict(
            ts=str(int(utcnow().timestamp() * 1000)), tradeId="durable", px="100", sz="1", side="buy"
        )])
        await runtime._on_public_market_batch([ticker_message(n) for n in range(1, 64)] + [trade])
        cache.set.assert_not_awaited()
        pipeline.execute.assert_awaited_once()
        assert len(pipeline.writes) == 1 and pipeline.writes[0][2] == 120
        import json
        assert json.loads(pipeline.writes[0][1])["last"] == "163"
        assert runtime.market.latest_ticks[BTC].last == 163
    finally:
        await runtime.close()


async def test_derivative_context_rebuilt_once_per_symbol_per_public_batch(tmp_path):
    from unittest.mock import Mock

    runtime = TradingRuntime(Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/derivative-batch.db"
    ))
    await runtime.store.initialize()
    ts = str(int(utcnow().timestamp() * 1000))
    context = Mock(wraps=runtime.market.derivative_context)
    runtime.market.derivative_context = context
    messages = [dict(arg=dict(channel="open-interest", instId=BTC), data=[dict(
        ts=ts, oi=str(100 + n)
    )]) for n in range(64)]
    try:
        await runtime._on_public_market_batch(messages)
        assert context.call_count == 1
        assert len(await runtime.store.latest("market_derivatives")) == 64
        assert runtime.market.latest_derivatives[BTC]["oi"] == 163
        assert float(runtime.market.latest_derivatives[BTC]["oi_change"]) == pytest.approx(1 / 162)
    finally:
        await runtime.close()


async def test_invalid_batch_does_not_publish_cache_and_context_resets(tmp_path):
    from types import SimpleNamespace

    runtime = TradingRuntime(Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/bad-batch.db"
    ))
    await runtime.store.initialize()
    pipeline = CachePipeline()
    cache = SimpleNamespace(set=AsyncMock(), pipeline=lambda **kw: pipeline)
    runtime.market.redis = cache
    try:
        with pytest.raises(KeyError):
            await runtime._on_public_market_batch([ticker_message(), dict(
                arg=dict(channel="trades", instId=BTC), data=[{}]
            )])
        pipeline.execute.assert_not_awaited()
        assert not pipeline.writes
        await runtime.market.handle(ticker_message())
        cache.set.assert_awaited_once()  # Other workers retain their independent cache path.
    finally:
        await runtime.close()


async def test_public_cache_failure_fences_worker_without_losing_committed_trades(tmp_path):
    from types import SimpleNamespace

    runtime = TradingRuntime(Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/cache-fault.db"
    ))
    await runtime.store.initialize()
    pipeline = CachePipeline()
    pipeline.execute.side_effect = ConnectionError("cache unavailable")
    runtime.market.redis = SimpleNamespace(pipeline=lambda **kw: pipeline)
    ws, _ = ready_socket(channels=("tickers", "trades"))
    faults = []
    ws.on_fault = lambda _, reason: faults.append(reason)
    ws.batch_handler = runtime._on_public_market_batch
    trade = dict(arg=dict(channel="trades", instId=BTC), data=[dict(
        ts=str(int(utcnow().timestamp() * 1000)), tradeId="batch-trade", px="100", sz="1", side="buy"
    )])
    for message in (ticker_message(), trade):
        ws._enqueue_message(message, 100)
    worker = asyncio.create_task(ws._business_worker())
    try:
        await asyncio.wait_for(ws.queue.join(), 2)
        assert faults == ["WebSocket processing failed"]
        assert not ws.is_processing_healthy() and not ws.last_data_at
        assert len(await runtime.store.latest("market_trades")) == 1
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        await runtime.close()


async def test_batched_derivatives_preserve_sequential_decision_context(tmp_path):
    from app.market import MarketDataEngine

    stores = [Store(f"sqlite+aiosqlite:///{tmp_path}/{name}.db") for name in ("batch", "single")]
    for store in stores:
        await store.initialize()
    batch, single = [MarketDataEngine(store) for store in stores]
    ts = int(utcnow().timestamp() * 1000) - 1000
    messages = []
    for n in range(16):
        for channel, field, symbol, value in (
            ("mark-price", "markPx", BTC, str(101 + n)),
            ("index-tickers", "idxPx", "BTC-USDT", str(100 + n)),
            ("funding-rate", "fundingRate", BTC, str(n / 10000)),
            ("open-interest", "oi", BTC, str(100 + n)),
        ):
            messages.append(dict(arg=dict(channel=channel, instId=symbol), data=[{
                "ts": str(ts + n), field: value
            }]))
    try:
        for message in messages:
            await single.handle(message)
        await batch.handle_batch(messages)
        assert batch.latest_derivatives == single.latest_derivatives
        for n in (0, 7, 15):
            from datetime import UTC, datetime
            at = datetime.fromtimestamp((ts + n) / 1000, UTC)
            assert batch.derivative_context(BTC, at) == single.derivative_context(BTC, at)
        assert await stores[0].all_for_symbol("market_derivatives", BTC) == await stores[1].all_for_symbol("market_derivatives", BTC)
    finally:
        for store in stores:
            await store.close()


@pytest.mark.parametrize("channel", ["orders", "positions", "candle15m", "books"])
async def test_public_cache_batch_cannot_capture_private_decision_or_sequence_feed(tmp_path, channel):
    from app.market import MarketDataEngine

    store = Store(f"sqlite+aiosqlite:///{tmp_path}/restricted.db")
    market = MarketDataEngine(store)
    try:
        with pytest.raises(ValueError, match="bounded public"):
            await market.handle_batch([dict(arg=dict(channel=channel, instId=BTC), data=[])])
        with pytest.raises(ValueError, match="bounded public"):
            await market.handle_batch([ticker_message()] * 65)
    finally:
        await store.close()


async def test_cancelled_public_cache_batch_does_not_capture_concurrent_worker(tmp_path):
    from types import SimpleNamespace

    from app.market import MarketDataEngine

    store = Store(f"sqlite+aiosqlite:///{tmp_path}/cancel.db")
    await store.initialize()
    entered, release = asyncio.Event(), asyncio.Event()
    pipeline = CachePipeline()

    async def stalled_cache():
        entered.set()
        await release.wait()

    pipeline.execute.side_effect = stalled_cache
    cache = SimpleNamespace(set=AsyncMock(), pipeline=lambda **kw: pipeline)
    market = MarketDataEngine(store, cache)
    task = asyncio.create_task(market.handle_batch([ticker_message()]))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        await market.handle(ticker_message(1))
        cache.set.assert_awaited_once()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        await market.handle_batch([ticker_message(2)])
        assert pipeline.execute.await_count == 2
        assert market._batch_cache.get() is None and market._batch_derivatives.get() is None
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await store.close()
