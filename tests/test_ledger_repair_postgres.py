import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import IntegrityError

from app.execution import OrderManager
from app.ledger_repair import LedgerRepairService
from tests.test_ledger_repair import SYMBOL, entry, fill
from tests.test_live_lease import postgres_store as postgres_store


async def seeded_service(store):
    await store.append("orders", entry(), symbol=SYMBOL, reference_id="entry")
    manager = OrderManager(store)
    await manager.restore()
    client = AsyncMock()
    client.fills_history_window.return_value = [fill()]
    client.order_by_id.return_value = [
        {
            "instId": SYMBOL,
            "ordId": "exit-order",
            "clOrdId": "",
            "side": "sell",
            "posSide": "net",
            "reduceOnly": "true",
            "state": "filled",
            "accFillSz": "1",
            "algoClOrdId": "known-stop",
        }
    ]
    client.algo_order.return_value = [
        {
            "instId": SYMBOL,
            "algoClOrdId": "known-stop",
            "ordIdList": ["exit-order"],
            "side": "sell",
            "posSide": "net",
            "state": "effective",
            "failCode": "0",
        }
    ]
    return LedgerRepairService(client, manager, store), manager


async def test_postgres_recovery_atomic_and_idempotent(postgres_store):
    service, manager = await seeded_service(postgres_store)
    first = await service.repair({SYMBOL})
    second = await service.repair({SYMBOL})
    assert first.evidence_complete and first.fills_added == first.orders_added == 1
    assert second.evidence_complete and second.fills_added == second.orders_added == 0
    assert len(await postgres_store.latest("fills")) == 1
    assert len(await postgres_store.latest("orders")) == 2
    assert (
        sum(
            Decimal(r["filled"]) * (1 if r["direction"] == "LONG" else -1)
            for r in manager.orders.values()
        )
        == 0
    )


async def test_postgres_two_repair_transactions_cannot_duplicate_fill(postgres_store):
    await postgres_store.append("orders", entry(), symbol=SYMBOL, reference_id="entry")
    snapshot = {
        t: await postgres_store.ledger_snapshot(t, {SYMBOL})
        for t in ("orders", "order_events", "fills")
    }
    records = [("fills", fill(), "exit-001")]
    results = await asyncio.gather(
        postgres_store.append_ledger_recovery(records, snapshot, {SYMBOL}),
        postgres_store.append_ledger_recovery(records, snapshot, {SYMBOL}),
        return_exceptions=True,
    )
    assert sum(r is None for r in results) == 1
    assert sum(isinstance(r, ValueError) for r in results) == 1
    assert len(await postgres_store.latest("fills")) == 1


async def test_postgres_recovery_rolls_back_all_records_on_failure(postgres_store):
    snapshot = {t: [] for t in ("orders", "order_events", "fills")}
    with pytest.raises(ValueError):
        await postgres_store.append_ledger_recovery(
            [("orders", entry(), "entry"), ("fills", fill(), "exit-001"), ("bad", {}, "bad")],
            snapshot,
            {SYMBOL},
        )
    assert not await postgres_store.latest("orders")
    assert not await postgres_store.latest("fills")


async def test_postgres_trade_id_constraint_rejects_duplicate_without_overwrite(postgres_store):
    await postgres_store.append("fills", fill(), symbol=SYMBOL, reference_id="exit-001")
    with pytest.raises(IntegrityError):
        await postgres_store.append(
            "fills", {**fill(), "fillSz": "2"}, symbol=SYMBOL, reference_id="exit-001"
        )
    assert (await postgres_store.latest("fills"))[0]["fillSz"] == "1"
