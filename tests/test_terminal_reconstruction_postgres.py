from decimal import Decimal

import pytest

from tests.test_live_lease import postgres_store as postgres_store
from tests.test_terminal_fill_evidence import snapshot
from tests.test_terminal_fill_evidence import snapshot_runtime as snapshot_runtime


async def adopt_postgres(runtime, store):
    row = runtime.order_manager.orders['entry'].copy()
    runtime.store = store
    runtime.order_manager.store = store
    await store.append('orders', row, symbol=row['symbol'], reference_id=row['clOrdId'])


async def test_postgres_terminal_reconstruction_atomic_complete_and_restore(snapshot_runtime, postgres_store):
    runtime, _, _, _ = snapshot_runtime
    original_store = runtime.store
    try:
        await adopt_postgres(runtime, postgres_store)
        await snapshot(runtime)
        fills = await postgres_store.latest('fills')
        assert len(fills) == 2
        assert sum(Decimal(r['fillSz']) for r in fills) == Decimal('3.93')
        assert len(await postgres_store.latest('order_events')) == 1
        await runtime.order_manager.restore()
        assert runtime.order_manager.orders['entry']['filled'] == '3.93'
        assert len(runtime.order_manager.seen_trade_ids) == 2
    finally:
        await original_store.close()


async def test_postgres_terminal_transaction_failure_never_advances_order(snapshot_runtime, postgres_store):
    runtime, _, _, _ = snapshot_runtime
    original_store = runtime.store
    try:
        await adopt_postgres(runtime, postgres_store)
        original = postgres_store.append_ledger_recovery

        async def fail(records, snapshots, symbols):
            await original([*records, ('invalid', {}, 'bad')], snapshots, symbols)

        postgres_store.append_ledger_recovery = fail
        with pytest.raises(ValueError, match='invalid recovery record'):
            await snapshot(runtime)
        assert not await postgres_store.latest('fills')
        assert not await postgres_store.latest('order_events')
        assert runtime.order_manager.orders['entry']['filled'] == '0'
        assert not runtime.order_manager.seen_trade_ids
    finally:
        await original_store.close()
