from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError

from app.execution import ExecutionEngine, OrderManager
from app.storage import Store
from tests.test_live_lease import postgres_store as postgres_store
from tests.test_spot import SPOT, SWAP, decision, fill, instrument


async def test_postgres_spot_and_swap_identical_trade_id_are_distinct(postgres_store: Store):
    spot = fill(trade='123')
    swap = fill(trade='123', instId=SWAP, feeCcy='USDT', fee='-.1', fillPnl='0')
    for event in [spot, swap]:
        await postgres_store.append('fills', event, symbol=event['instId'], reference_id='123')
    assert len(await postgres_store.fill_records()) == 2
    with pytest.raises(IntegrityError):
        await postgres_store.append('fills', spot | {'fillSz': '1'}, symbol=SPOT, reference_id='123')
    assert len(await postgres_store.fill_records()) == 2


async def test_postgres_cash_fee_inventory_restores_exactly(postgres_store: Store):
    manager = OrderManager(postgres_store)
    request = ExecutionEngine.from_risk(decision(), instrument())
    await manager.create(request)
    event = fill(cid=request.client_order_id)
    await manager.ingest(event)
    await manager.ingest(event)
    restored = OrderManager(postgres_store)
    await restored.restore()
    assert restored.spot_fees[SPOT] == Decimal('-.00001')
    assert restored.seen_trade_ids == {(SPOT, 'trade')}
    assert restored.orders[request.client_order_id]['risk_decision']['leverage'] == 1
