import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import delete, insert, select, update
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy.exc import IntegrityError

from app.daily_review import build_daily_review
from app.execution import OrderManager
from app.fill_identity import equivalent_fill
from app.okx import OkxError
from app.storage import ROW_TYPES, Store
from tests.test_live_lease import postgres_store as postgres_store


def evidence(symbol='ETH-USDT-SWAP'):
    now = str(int(datetime.now(UTC).timestamp() * 1000))
    detail = dict(instId=symbol, ordId='owned-order', clOrdId='owned-entry', side='buy',
                  reduceOnly='false', posSide='net', state='filled', sz='3.93', accFillSz='3.93',
                  fee='-0.200696454', feeCcy='USDT', tradeId='last', fillSz='3.79',
                  fillPx='2553.39', fillTime=now)
    last = dict(instId=symbol, ordId='owned-order', clOrdId='owned-entry', side='buy',
                posSide='net', tradeId='last', fillSz='3.79', fillPx='2553.39', fillTime=now,
                ts=now, billId='200', fee='-0.193546962', feeCcy='USDT', fillPnl='0')
    first = {**last, 'tradeId': 'first', 'fillSz': '0.14', 'billId': '100', 'fee': '-0.007149492'}
    legacy = {**detail}  # REST terminal snapshot, cumulative fee and no fillPnl.
    local = dict(clOrdId='owned-entry', symbol=symbol, direction='LONG', reduce_only=False,
                 state='FILLED', filled='3.93', approved_contracts='3.93', order_id='owned-order',
                 protective_algo_id='owned-stop', intent_id='owned-intent',
                 created_at=(datetime.now(UTC) - timedelta(hours=1)).isoformat())
    return local, detail, [last, first], legacy


async def seed(store, symbol='ETH-USDT-SWAP'):
    local, detail, rows, legacy = evidence(symbol)
    await store.append('orders', local, symbol=symbol, reference_id=local['clOrdId'])
    for row in [rows[1], legacy]:
        await store.append('fills', row, symbol=symbol, reference_id=row['tradeId'])
    return local, detail, rows, legacy


@pytest.fixture
async def correction_store(tmp_path):
    store = Store(f'sqlite+aiosqlite:///{tmp_path}/correction.db')
    await store.initialize()
    try:
        yield store
    finally:
        await store.close()


async def raw_fills(store):
    async with store.sessions() as session:
        return [r.payload for r in await session.scalars(select(ROW_TYPES['fills']).order_by(ROW_TYPES['fills'].id))]


async def test_correction_append_only_effective_reads_and_restart(correction_store):
    store = correction_store
    _, detail, rows, legacy = await seed(store)
    original = await raw_fills(store)
    assert await store.append_accounting_corrections(detail, rows) == 1
    assert await raw_fills(store) == original
    assert (await raw_fills(store))[1] == legacy
    assert len(await store.latest('fills')) == 2
    effective = await store.fill_for_key((detail['instId'], 'last'))
    assert equivalent_fill(effective, rows[0])
    assert effective['fee'] == '-0.193546962' and effective['fillPnl'] == '0'
    assert len(await store.latest('fill_accounting_corrections')) == 1
    assert await store.ledger_repair_hold()
    await store.initialize()
    manager = OrderManager(store)
    await manager.restore()
    assert len(manager.seen_trade_ids) == 2 and manager.orders['owned-entry']['filled'] == '3.93'
    assert equivalent_fill((await store.fill_records())[1][1], rows[0])


async def test_repeat_correction_idempotent(correction_store):
    _, detail, rows, _ = await seed(correction_store)
    assert await correction_store.append_accounting_corrections(detail, rows) == 1
    before = await correction_store.latest('system_events')
    assert await correction_store.append_accounting_corrections(detail, rows) == 0
    assert await correction_store.latest('system_events') == before
    assert len(await correction_store.latest('fill_accounting_corrections')) == 1


async def test_daily_review_uses_corrected_fee_once(correction_store):
    _, detail, rows, _ = await seed(correction_store)
    await correction_store.append_accounting_corrections(detail, rows)
    day = datetime.now(UTC).date()
    report = await build_daily_review(correction_store, day)
    assert report['fills'] == 2 and report['realized_pnl'] == '0'
    assert Decimal(report['fees']) == Decimal(detail['fee'])
    start = datetime.combine(day, datetime.min.time(), UTC)
    assert len(await correction_store.since('fills', start)) == 2
    assert all(r.get('fillPnl') == '0' for r in await correction_store.between('fills', start, start + timedelta(days=1)))


async def test_corrected_fill_late_ws_duplicate_is_idempotent(correction_store):
    _, detail, rows, _ = await seed(correction_store)
    await correction_store.append_accounting_corrections(detail, rows)
    manager = OrderManager(correction_store)
    await manager.restore()
    await manager.ingest({**detail, **rows[0], 'fillFee': rows[0]['fee']})
    assert len(await raw_fills(correction_store)) == 2
    assert manager.orders['owned-entry']['filled'] == '3.93'
    with pytest.raises(OkxError, match='ledger fill conflict'):
        await manager.ingest({**detail, **rows[0], 'fillPx': '2554'})


@pytest.mark.parametrize('change', [
    {'instId': 'BTC-USDT-SWAP'}, {'ordId': 'foreign'}, {'clOrdId': 'foreign'},
    {'side': 'sell'}, {'fillSz': '3.78'}, {'fillPx': '1'}, {'fillTime': '1'},
    {'feeCcy': 'BTC'}, {'fee': 'NaN'}, {'fillPnl': ''}, {'billId': ''},
])
async def test_unproven_evidence_never_corrects(correction_store, change):
    _, detail, rows, _ = await seed(correction_store)
    original = await raw_fills(correction_store)
    rows[0].update(change)
    with pytest.raises(ValueError):
        await correction_store.append_accounting_corrections(detail, rows)
    assert await raw_fills(correction_store) == original
    assert not await correction_store.latest('fill_accounting_corrections')
    assert not await correction_store.ledger_repair_hold()


@pytest.mark.parametrize('change', [
    {'fee': '-123'}, {'fillPnl': '123'}, {'accFillSz': '3.79'}, {'fillFee': '-123'},
])
async def test_unrecognized_legacy_conflict_rejected(correction_store, change):
    local, detail, rows, legacy = evidence()
    legacy.update(change)
    await correction_store.append('orders', local, symbol=local['symbol'])
    for row in [rows[1], legacy]:
        await correction_store.append('fills', row, symbol=row['instId'], reference_id=row['tradeId'])
    with pytest.raises(ValueError):
        await correction_store.append_accounting_corrections(detail, rows)
    assert not await correction_store.latest('fill_accounting_corrections')


@pytest.mark.parametrize('case', ['unknown_order', 'missing_fill', 'extra_fill', 'wrong_side', 'size', 'state', 'duplicate'])
async def test_order_and_complete_set_required(correction_store, case):
    local, detail, rows, legacy = evidence()
    if case != 'unknown_order':
        if case == 'wrong_side':
            local['direction'] = 'SHORT'
        if case == 'size':
            local['approved_contracts'] = '4'
        if case == 'state':
            local['state'] = 'ACKNOWLEDGED'
        await correction_store.append('orders', local, symbol=local['symbol'])
    await correction_store.append('fills', legacy, symbol=legacy['instId'], reference_id='last')
    if case != 'missing_fill':
        await correction_store.append('fills', rows[1], symbol=rows[1]['instId'], reference_id='first')
    if case == 'extra_fill':
        await correction_store.append('fills', {**rows[1], 'tradeId': 'extra'}, symbol=rows[1]['instId'], reference_id='extra')
    if case == 'duplicate':
        rows.append(rows[0])
    with pytest.raises(ValueError):
        await correction_store.append_accounting_corrections(detail, rows)
    assert not await correction_store.latest('fill_accounting_corrections')


async def test_tampered_original_or_receipt_fails_closed(correction_store):
    _, detail, rows, _ = await seed(correction_store)
    await correction_store.append_accounting_corrections(detail, rows)
    table = ROW_TYPES['fills']
    async with correction_store.sessions.begin() as session:
        await session.execute(update(table).where(table.reference_id == 'last').values(payload={**rows[0], 'fee': '-99'}))
    with pytest.raises(ValueError):
        await correction_store.fill_for_key((detail['instId'], 'last'))


async def test_receipt_injection_through_generic_append_forbidden(correction_store):
    with pytest.raises(ValueError):
        await correction_store.append('fill_accounting_corrections', {})


async def test_postgres_concurrent_correction_exactly_once(postgres_store):
    _, detail, rows, _ = await seed(postgres_store)
    result = await asyncio.gather(*(postgres_store.append_accounting_corrections(detail, rows) for _ in range(2)))
    assert sorted(result) == [0, 1]
    assert len(await postgres_store.latest('fill_accounting_corrections')) == 1
    assert len(await raw_fills(postgres_store)) == 2
    with pytest.raises(IntegrityError):
        async with postgres_store.sessions.begin() as session:
            table = ROW_TYPES['fill_accounting_corrections']
            row = await session.scalar(select(table))
            await session.execute(insert(table).values(symbol=row.symbol, reference_id=row.reference_id, payload=row.payload))


async def test_service_is_read_only_and_requires_stopped_paper(correction_store):
    from app.accounting_correction import AccountingCorrectionService
    from app.config import Settings

    local, detail, rows, _ = await seed(correction_store)
    client = AsyncMock()
    client.order.return_value = [detail]
    client.fills_history.side_effect = [rows, []]
    guard = AsyncMock(return_value={'mode': 'PAPER', 'running': False, 'risk_state': 'EMERGENCY'})
    service = AccountingCorrectionService(Settings(_env_file=None), client, correction_store, guard)
    report = await service.correct(local['symbol'], local['clOrdId'], apply=True)
    assert report['corrections_added'] == 1
    client.place_order.assert_not_called()
    client.cancel_order.assert_not_called()
    client.cancel_all_after.assert_not_called()
    assert guard.await_count >= 2


@pytest.mark.parametrize('status', [
    {'mode': 'LIVE', 'running': False, 'risk_state': 'HALT'},
    {'mode': 'PAPER', 'running': True, 'risk_state': 'EMERGENCY'},
    {'mode': 'PAPER', 'running': False, 'risk_state': 'NORMAL'}, {},
])
async def test_service_safety_gate_before_http(correction_store, status):
    from app.accounting_correction import AccountingCorrectionService
    from app.config import Settings

    client = AsyncMock()
    service = AccountingCorrectionService(Settings(_env_file=None), client, correction_store, AsyncMock(return_value=status))
    with pytest.raises(ValueError):
        await service.correct('ETH-USDT-SWAP', 'owned-entry', apply=True)
    client.order.assert_not_called()


async def test_dry_run_does_not_append_or_release_hold(correction_store):
    _, detail, rows, _ = await seed(correction_store)
    original = await raw_fills(correction_store)
    assert await correction_store.append_accounting_corrections(detail, rows, apply=False) == 1
    assert await raw_fills(correction_store) == original
    assert not await correction_store.latest('fill_accounting_corrections')
    assert not await correction_store.latest('system_events')


async def test_receipt_and_hold_transaction_rollback_together(correction_store):
    _, detail, rows, _ = await seed(correction_store)

    def fail(*args):
        raise RuntimeError('injected transaction failure')

    table = ROW_TYPES['system_events']
    sqlalchemy_event.listen(table, 'before_insert', fail)
    try:
        with pytest.raises(RuntimeError):
            await correction_store.append_accounting_corrections(detail, rows)
    finally:
        sqlalchemy_event.remove(table, 'before_insert', fail)
    assert not await correction_store.latest('fill_accounting_corrections')
    assert not await correction_store.latest('system_events')
    assert await correction_store.append_accounting_corrections(detail, rows) == 1


@pytest.mark.parametrize('change', [{'version': True}, {'original_fill_id': True},
                                   {'original_digest': 'tampered'}, {'reason': 'override'}])
async def test_corrupt_receipt_fails_startup(correction_store, change):
    _, detail, rows, _ = await seed(correction_store)
    await correction_store.append_accounting_corrections(detail, rows)
    receipt = (await correction_store.latest('fill_accounting_corrections'))[0]
    async with correction_store.sessions.begin() as session:
        await session.execute(update(ROW_TYPES['fill_accounting_corrections']).values(payload={**receipt, **change}))
    with pytest.raises(ValueError):
        await correction_store.initialize()


async def test_orphan_receipt_fails_startup(correction_store):
    _, detail, rows, _ = await seed(correction_store)
    await correction_store.append_accounting_corrections(detail, rows)
    async with correction_store.sessions.begin() as session:
        await session.execute(delete(ROW_TYPES['fills']).where(ROW_TYPES['fills'].reference_id == 'last'))
    with pytest.raises(ValueError):
        await correction_store.initialize()


async def test_cross_instrument_same_trade_id_not_corrected(correction_store):
    _, detail, rows, _ = await seed(correction_store)
    btc = {**rows[0], 'instId': 'BTC-USDT-SWAP', 'ordId': 'btc-order', 'clOrdId': 'btc-entry'}
    await correction_store.append('fills', btc, symbol=btc['instId'], reference_id='last')
    await correction_store.append_accounting_corrections(detail, rows)
    assert await correction_store.fill_for_key(('BTC-USDT-SWAP', 'last')) == btc
    assert len(await correction_store.fill_records()) == 3


async def test_receipt_never_stores_unrelated_response_fields(correction_store):
    _, detail, rows, _ = await seed(correction_store)
    detail['unrelated_secret'] = 'must-not-store'
    rows[0]['unrelated_secret'] = 'must-not-store'
    await correction_store.append_accounting_corrections(detail, rows)
    assert 'must-not-store' not in str(await correction_store.latest('fill_accounting_corrections'))


async def test_snapshot_with_altered_accounting_cannot_acquire_receipt(correction_store):
    _, detail, rows, legacy = await seed(correction_store)
    await correction_store.append_accounting_corrections(detail, rows)
    with pytest.raises(ValueError):
        await correction_store.effective_fill_payloads([{**legacy, 'fee': '-100'}])


async def test_ledger_repair_compares_effective_evidence_but_keeps_raw_snapshot(correction_store):
    from app.ledger_repair import LedgerRepairService

    local, detail, rows, _ = await seed(correction_store)
    original = await raw_fills(correction_store)
    await correction_store.append_accounting_corrections(detail, rows)
    manager = OrderManager(correction_store)
    await manager.restore()
    client = AsyncMock()
    client.fills_history_window.return_value = rows
    result = await LedgerRepairService(client, manager, correction_store).repair({local['symbol']})
    assert result.evidence_complete and not result.conflicts and not result.unresolved
    assert result.fills_added == result.orders_added == 0
    assert await raw_fills(correction_store) == original


async def test_postgres_transaction_failure_rolls_back_receipt_and_hold(postgres_store):
    _, detail, rows, _ = await seed(postgres_store)

    def fail(*args):
        raise RuntimeError('injected failure')

    sqlalchemy_event.listen(ROW_TYPES['system_events'], 'before_insert', fail)
    try:
        with pytest.raises(RuntimeError):
            await postgres_store.append_accounting_corrections(detail, rows)
    finally:
        sqlalchemy_event.remove(ROW_TYPES['system_events'], 'before_insert', fail)
    assert not await postgres_store.latest('fill_accounting_corrections')
    assert not await postgres_store.latest('system_events')
    assert len(await raw_fills(postgres_store)) == 2


async def test_service_rechecks_safety_after_history_before_commit(correction_store):
    from app.accounting_correction import AccountingCorrectionService
    from app.config import Settings

    local, detail, rows, _ = await seed(correction_store)
    client = AsyncMock()
    client.order.return_value = [detail]
    client.fills_history.side_effect = [rows, []]
    guard = AsyncMock(side_effect=[{'mode': 'PAPER', 'running': False, 'risk_state': 'EMERGENCY'},
                                  {'mode': 'PAPER', 'running': True, 'risk_state': 'NORMAL'}])
    service = AccountingCorrectionService(Settings(_env_file=None), client, correction_store, guard)
    with pytest.raises(ValueError):
        await service.correct(local['symbol'], local['clOrdId'], apply=True)
    assert not await correction_store.latest('fill_accounting_corrections')
