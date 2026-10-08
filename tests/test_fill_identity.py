import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import delete, insert, inspect, text
from sqlalchemy.exc import IntegrityError

from app.config import Settings
from app.execution import OrderManager
from app.fill_identity import fill_key
from app.models import GovernorState
from app.okx import OkxError
from app.runtime import TradingRuntime
from app.storage import ROW_TYPES, Store
from tests.test_ledger_repair import SYMBOL, entry, fill
from tests.test_ledger_repair import repair as repair
from tests.test_live_lease import postgres_store as postgres_store

ETH = "ETH-USDT-SWAP"


@pytest.mark.parametrize("inst,trade", [("", "1"), ("BTC", ""), (None, "1"), ("BTC", None),
                                      (" BTC", "1"), ("BTC", "1 "), (123, "1")])
def test_fill_key_rejects_invalid_identifiers(inst, trade):
    with pytest.raises(ValueError, match="invalid fill identity"):
        fill_key(inst, trade)


@pytest.fixture(params=["sqlite", "postgresql"])
async def identity_store(request, tmp_path, postgres_store):
    # PostgreSQL fixture asserts the isolated test DB/user before any cleanup.
    store = postgres_store if request.param == "postgresql" else Store(
        f"sqlite+aiosqlite:///{tmp_path}/identity.db"
    )
    if request.param == "sqlite":
        await store.initialize()
    try:
        yield store
    finally:
        # Restore intentionally corrupted test schemas; never a production DB.
        async with store.engine.begin() as conn:
            await conn.execute(delete(ROW_TYPES["fills"]))
            await conn.execute(text("DROP INDEX IF EXISTS fills_reference_unique"))
            await conn.execute(text("DROP INDEX IF EXISTS fills_instrument_reference_unique"))
        await store.initialize()
        if request.param == "sqlite":
            await store.close()


async def raw_insert(store, symbol=SYMBOL, trade="123", **changes):
    payload = {**fill(trade), "instId": symbol, **changes}
    async with store.engine.begin() as conn:
        await conn.execute(insert(ROW_TYPES["fills"]).values(
            symbol=symbol, reference_id=trade, payload=payload
        ))


async def index_schema(store):
    async with store.engine.connect() as conn:
        indexes = await conn.run_sync(lambda c: inspect(c).get_indexes("fills"))
    return [{**i, "dialect_options": {k: str(v) for k, v in i.get("dialect_options", {}).items()}}
            for i in indexes]


async def test_database_constraint_accepts_cross_symbol_but_rejects_same_symbol(identity_store):
    store = identity_store
    await raw_insert(store)
    await raw_insert(store, ETH)
    with pytest.raises(IntegrityError):
        await raw_insert(store, fillSz="2")
    assert len(await store.latest("fills")) == 2
    assert {key for key, _ in await store.fill_records()} == {(SYMBOL, "123"), (ETH, "123")}


async def test_old_global_index_is_migrated_atomically_and_restart_safe(identity_store):
    store = identity_store
    await raw_insert(store)
    before = await store.latest("fills")
    async with store.engine.begin() as conn:
        await conn.execute(text("DROP INDEX fills_instrument_reference_unique"))
        await conn.execute(text("CREATE UNIQUE INDEX fills_reference_unique ON fills (reference_id) WHERE reference_id IS NOT NULL"))
    await store.initialize()
    schema = await index_schema(store)
    assert not any(i["name"] == "fills_reference_unique" for i in schema)
    index = [i for i in schema if i["name"] == "fills_instrument_reference_unique"]
    assert len(index) == 1 and index[0]["unique"] and index[0]["column_names"] == ["symbol", "reference_id"]
    assert await store.latest("fills") == before
    await store.initialize()
    assert await index_schema(store) == schema
    assert await store.latest("fills") == before
    await raw_insert(store, ETH)
    assert len(await store.latest("fills")) == 2


async def test_migration_preserves_existing_cross_symbol_duplicate_trade_ids(identity_store):
    store = identity_store
    async with store.engine.begin() as conn:
        await conn.execute(text("DROP INDEX fills_instrument_reference_unique"))
    await raw_insert(store)
    await raw_insert(store, ETH)
    before = await store.latest("fills")
    await store.initialize()
    await store.initialize()
    assert await store.latest("fills") == before


@pytest.mark.parametrize("size", ["1", "2"])
async def test_migration_never_cleans_same_symbol_duplicates(identity_store, size):
    store = identity_store
    async with store.engine.begin() as conn:
        await conn.execute(text("DROP INDEX fills_instrument_reference_unique"))
    await raw_insert(store)
    await raw_insert(store, fillSz=size)
    before = await store.latest("fills")
    for _ in range(2):
        with pytest.raises(ValueError, match="ledger fill conflict"):
            await store.initialize()
        assert not store.healthy
        assert await store.latest("fills") == before
        assert not any(i["name"] == "fills_instrument_reference_unique" for i in await index_schema(store))


async def test_invalid_legacy_payload_preserves_global_index_on_failed_migration(identity_store):
    store = identity_store
    async with store.engine.begin() as conn:
        await conn.execute(text("DROP INDEX fills_instrument_reference_unique"))
        await conn.execute(text("CREATE UNIQUE INDEX fills_reference_unique ON fills (reference_id) WHERE reference_id IS NOT NULL"))
    await raw_insert(store, instId=ETH)
    before = await store.latest("fills")
    with pytest.raises(ValueError, match="ledger fill conflict"):
        await store.initialize()
    assert await store.latest("fills") == before and not store.healthy
    assert any(i["name"] == "fills_reference_unique" for i in await index_schema(store))


async def test_migration_validates_beyond_first_batch_without_modifying_rows(identity_store):
    store = identity_store
    rows = [{"symbol": SYMBOL, "reference_id": str(i),
             "payload": {**fill(str(i)), "instId": ETH if i == 5000 else SYMBOL}}
            for i in range(5001)]
    async with store.engine.begin() as conn:
        await conn.execute(insert(ROW_TYPES["fills"]), rows)
    with pytest.raises(ValueError, match="ledger fill conflict"):
        await store.initialize()
    assert len(await store.fill_records()) == 5001
    assert (await store.latest("fills", 1))[0]["instId"] == ETH


async def test_migration_rejects_wrong_existing_index_definition(identity_store):
    store = identity_store
    async with store.engine.begin() as conn:
        await conn.execute(text("DROP INDEX fills_instrument_reference_unique"))
        await conn.execute(text("CREATE UNIQUE INDEX fills_instrument_reference_unique ON fills (reference_id) WHERE reference_id IS NOT NULL"))
    with pytest.raises(ValueError, match="invalid fill identity index"):
        await store.initialize()
    assert not store.healthy


async def test_concurrent_initialization_converges_without_data_change(identity_store):
    store = identity_store
    await raw_insert(store)
    other = Store(store.engine.url.render_as_string(hide_password=False))
    before = await store.latest("fills")
    try:
        await asyncio.gather(store.initialize(), other.initialize())
        assert store.healthy and other.healthy
        assert await store.latest("fills") == before
        assert sum(i["name"] == "fills_instrument_reference_unique" for i in await index_schema(store)) == 1
    finally:
        await other.close()


async def seeded_manager(store):
    manager = OrderManager(store)
    for symbol, client in ((SYMBOL, "btc-entry"), (ETH, "eth-entry")):
        row = {**entry(), "symbol": symbol, "clOrdId": client, "filled": "0", "state": "ACKNOWLEDGED"}
        await store.append("orders", row, symbol=symbol, reference_id=client)
    await manager.restore()
    return manager


def ws_fill(symbol=SYMBOL, trade="123", client="btc-entry", size="0.5", **kw):
    return {**fill(trade, size, order=client + "-order", side="buy", client=client),
            "instId": symbol, "state": "partially_filled", "accFillSz": size, **kw}


async def test_ws_cross_symbol_same_trade_id_ingest_restore_and_duplicate(identity_store):
    store = identity_store
    manager = await seeded_manager(store)
    btc, eth = ws_fill(), ws_fill(ETH, client="eth-entry")
    await manager.ingest(btc)
    await manager.ingest(eth)
    await manager.ingest(btc)
    assert len(await store.latest("fills")) == 2
    assert manager.seen_trade_ids == {(SYMBOL, "123"), (ETH, "123")}
    await manager.restore()
    assert manager.seen_trade_ids == {(SYMBOL, "123"), (ETH, "123")}
    assert len(manager.seen_trade_ids) == 2  # Existing metrics count still counts fills.
    await manager.ingest(btc)
    assert len(await store.latest("fills")) == 2


@pytest.mark.parametrize("changes", [{"fillSz": "0.6"}, {"fillPx": "111"},
                                    {"ordId": "other"}, {"side": "sell"}])
async def test_ws_same_symbol_conflict_does_not_mutate_ledger(identity_store, changes):
    store = identity_store
    manager = await seeded_manager(store)
    event = ws_fill()
    await manager.ingest(event)
    before = await store.latest("fills")
    events = await store.latest("order_events")
    with pytest.raises(OkxError, match="ledger fill conflict"):
        await manager.ingest({**event, **changes})
    assert await store.latest("fills") == before
    assert await store.latest("order_events") == events
    assert manager.orders["btc-entry"]["filled"] == "0.5"


async def test_ws_partial_fills_have_separate_instrument_scoped_identities(identity_store):
    store = identity_store
    manager = await seeded_manager(store)
    for symbol, client in ((SYMBOL, "btc-entry"), (ETH, "eth-entry")):
        await manager.ingest(ws_fill(symbol, "A", client, "0.3"))
        await manager.ingest(ws_fill(symbol, "B", client, "0.2", accFillSz="0.5"))
    assert len(await store.latest("fills")) == len(manager.seen_trade_ids) == 4
    await manager.restore()
    assert manager.orders["btc-entry"]["filled"] == manager.orders["eth-entry"]["filled"] == "0.5"


async def test_ws_conflict_uses_existing_runtime_fail_closed_path(identity_store):
    store = identity_store
    manager = await seeded_manager(store)
    await manager.ingest(ws_fill())
    before = await store.latest("fills")
    runtime = TradingRuntime(Settings(_env_file=None, database_url=store.engine.url.render_as_string(hide_password=False)))
    runtime.order_manager = manager
    runtime.entry_controller.cancel_all = AsyncMock(return_value=True)
    runtime.reconcile = AsyncMock()
    try:
        await runtime._on_private({"arg": {"channel": "orders"}, "data": [ws_fill(fillPx="120")]})
        assert runtime.governor.state == GovernorState.EMERGENCY
        assert not runtime.execution.entry_allowed()
        assert await store.latest("fills") == before
        runtime.reconcile.assert_awaited_once()
    finally:
        await runtime.close()


async def test_btc_protective_repair_while_eth_same_trade_id_already_exists(repair):
    service, client, manager, store = repair
    eth = {**fill("555"), "instId": ETH, "ordId": "eth-exit"}
    await store.append("fills", eth, symbol=ETH, reference_id="555")
    client.fills_history_window.return_value = [fill("555")]
    await manager.restore()
    first = await service.repair({SYMBOL})
    assert first.evidence_complete and first.fills_added == 1
    assert await store.fill_for_key((ETH, "555")) == eth
    assert (await store.fill_for_key((SYMBOL, "555")))["recovered"]
    assert manager.orders["protective-exit-order"]["reduce_only"]
    assert manager.seen_trade_ids == {(SYMBOL, "555"), (ETH, "555")}
    second = await service.repair({SYMBOL})
    assert second.evidence_complete and second.fills_added == second.orders_added == 0


async def test_repair_remote_fill_map_and_existing_map_are_instrument_scoped(repair):
    service, client, manager, store = repair
    eth_entry = {**entry(), "symbol": ETH, "clOrdId": "eth-entry", "order_id": "eth-entry-order",
                 "protective_algo_id": "eth-stop"}
    await store.append("orders", eth_entry, symbol=ETH, reference_id="eth-entry")
    await manager.restore()
    client.fills_history_window.side_effect = lambda symbol, **kw: [
        fill("555") if symbol == SYMBOL else {**fill("555"), "instId": ETH, "ordId": "eth-exit"}
    ]
    detail = client.order_by_id.return_value[0]
    client.order_by_id.side_effect = lambda symbol, oid: [
        {**detail, "instId": symbol, "ordId": oid,
         "algoClOrdId": "known-stop" if symbol == SYMBOL else "eth-stop"}
    ]
    algo = client.algo_order.return_value[0]
    client.algo_order.side_effect = lambda aid: [algo if aid == "known-stop" else {
        **algo, "instId": ETH, "algoClOrdId": "eth-stop", "ordIdList": ["eth-exit"]
    }]
    first = await service.repair({SYMBOL, ETH})
    assert first.evidence_complete and first.fills_added == first.orders_added == 2
    assert manager.seen_trade_ids == {(SYMBOL, "555"), (ETH, "555")}
    assert sum(Decimal(r["filled"]) * (1 if r["direction"] == "LONG" else -1)
               for r in manager.orders.values()) == 0
    second = await service.repair({SYMBOL, ETH})
    assert second.evidence_complete and second.fills_added == second.orders_added == 0
