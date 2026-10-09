import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.daily_review import build_daily_review
from app.models import GovernorState
from app.terminal_fills import TerminalFillError, read_terminal_fills
from tests.test_terminal_fill_evidence import earlier_fill, snapshot
from tests.test_terminal_fill_evidence import snapshot_runtime as snapshot_runtime


def history(client, rows):
    client.fills_history.side_effect = [rows, []]


def ws_event(detail, canonical, accumulated):
    return {**detail, **canonical, 'state': 'partially_filled', 'accFillSz': accumulated,
            'fillFee': canonical['fee']}


async def test_existing_first_fill_only_missing_last_is_appended(snapshot_runtime):
    runtime, client, detail, canonical = snapshot_runtime
    first = earlier_fill(canonical)
    await runtime.order_manager.ingest(ws_event(detail, first, '0.14'))
    before = await runtime.store.fill_for_key((first['instId'], first['tradeId']))
    await snapshot(runtime)
    assert len(await runtime.store.latest('fills')) == 2
    assert await runtime.store.fill_for_key((first['instId'], first['tradeId'])) == before
    assert runtime.order_manager.orders['entry']['filled'] == '3.93'


@pytest.mark.parametrize("terminal_ws", [False, True])
async def test_ws_during_history_read_is_exactly_once(snapshot_runtime, terminal_ws):
    runtime, client, detail, canonical = snapshot_runtime
    requested, released = asyncio.Event(), asyncio.Event()
    calls = 0

    async def page(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls > 1:
            return []
        requested.set()
        await released.wait()
        return [canonical, earlier_fill(canonical)]

    client.fills_history.side_effect = page
    task = asyncio.create_task(snapshot(runtime))
    await requested.wait()
    try:
        # Network GET holds no ledger lock, so this cannot deadlock.
        event = ws_event(detail, canonical, '3.93' if terminal_ws else '3.79')
        if terminal_ws:
            event['state'] = 'filled'
        await asyncio.wait_for(runtime.order_manager.ingest(event), 2)
        released.set()
        await task
    finally:
        released.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(await runtime.store.latest('fills')) == 2
    assert runtime.order_manager.orders['entry']['filled'] == '3.93'


@pytest.mark.parametrize('size', ['3.79', '4'])
async def test_size_mismatch_never_advances_order(snapshot_runtime, size):
    runtime, client, _, canonical = snapshot_runtime
    history(client, [{**canonical, 'fillSz': size}])
    with pytest.raises(TerminalFillError, match='terminal_fill_size_mismatch'):
        await snapshot(runtime)
    assert not await runtime.store.latest('fills')
    assert not await runtime.store.latest('order_events')
    assert runtime.order_manager.orders['entry']['filled'] == '0'


@pytest.mark.parametrize('change', [
    {'instId': ''}, {'tradeId': ''}, {'instId': 'ETH-USDT-SWAP'}, {'ordId': 'wrong'},
    {'side': 'sell'}, {'clOrdId': 'foreign'}, {'posSide': 'short'},
    {'fillSz': 'NaN'}, {'fillSz': 'Infinity'}, {'fillSz': '-1'}, {'fillSz': '0'},
    {'fillPx': 'NaN'}, {'fillPx': 'Infinity'}, {'fillPx': '0'},
    {'fillTime': ''}, {'fillTime': 'NaN'}, {'fillTime': '-1'},
    {'fillTime': '9999999999999999'}, {'ts': '9999999999999999'},
    {'feeCcy': 'BTC'}, {'billId': '0200'}, {'clOrdId': 0}, {'fillFee': '-999'},
    {'fee': ''}, {'fee': 'Infinity'}, {'fillPnl': ''}, {'fillPnl': 'NaN'}, {'feeCcy': ''},
    {'billId': ''}, {'billId': '-1'}, {'billId': 'bad'}, {'billId': ' 200'},
])
async def test_malformed_or_foreign_evidence_is_atomic_fail_closed(snapshot_runtime, change):
    runtime, client, _, canonical = snapshot_runtime
    history(client, [{**canonical, **change}, earlier_fill(canonical)])
    with pytest.raises(TerminalFillError):
        await snapshot(runtime)
    assert not await runtime.store.latest('fills')
    assert not await runtime.store.latest('order_events')
    assert runtime.order_manager.orders['entry']['filled'] == '0'


@pytest.mark.parametrize('change', [
    {'tradeId': 'absent'}, {'fillSz': '3.78'}, {'fillPx': '111'}, {'fillTime': '1'},
])
async def test_terminal_last_fill_still_cross_checked(snapshot_runtime, change):
    runtime, _, detail, _ = snapshot_runtime
    detail.update(change)
    with pytest.raises(TerminalFillError):
        await snapshot(runtime)
    assert not await runtime.store.latest('fills')


async def test_cross_symbol_same_trade_id_remains_valid(snapshot_runtime):
    runtime, _, detail, canonical = snapshot_runtime
    eth = {**canonical, 'instId': 'ETH-USDT-SWAP', 'ordId': 'eth-order', 'clOrdId': 'eth-entry'}
    await runtime.store.append('fills', eth, symbol=eth['instId'], reference_id=eth['tradeId'])
    runtime.order_manager.seen_trade_ids.add((eth['instId'], eth['tradeId']))
    await snapshot(runtime)
    assert len(await runtime.store.latest('fills')) == 3
    assert await runtime.store.fill_for_key((eth['instId'], eth['tradeId'])) == eth
    assert (detail['instId'], canonical['tradeId']) in runtime.order_manager.seen_trade_ids


async def test_partial_cancelled_reconstruction_uses_accumulated_not_requested_size(snapshot_runtime):
    runtime, client, detail, canonical = snapshot_runtime
    runtime.order_manager.orders['entry']['approved_contracts'] = '5'
    detail.update(state='canceled', sz='5', accFillSz='2', fillSz='1.3')
    last = {**canonical, 'fillSz': '1.3'}
    first = {**earlier_fill(canonical), 'fillSz': '0.7'}
    history(client, [last, first])
    await snapshot(runtime)
    row = runtime.order_manager.orders['entry']
    assert row['state'] == 'CANCELLED' and row['filled'] == '2'
    assert sum(Decimal(r['fillSz']) for r in await runtime.store.latest('fills')) == 2


async def test_zero_fill_cancelled_has_no_invented_fill(snapshot_runtime):
    runtime, client, detail, _ = snapshot_runtime
    detail.update(state='canceled', accFillSz='0', tradeId='', fillSz='0', fillPx='', fillTime='')
    await snapshot(runtime)
    assert runtime.order_manager.orders['entry']['state'] == 'CANCELLED'
    assert runtime.order_manager.orders['entry']['filled'] == '0'
    assert not await runtime.store.latest('fills')
    client.fills_history.assert_not_awaited()


async def test_repeated_reconstruction_has_no_extra_fill_or_order_event(snapshot_runtime):
    runtime, client, detail, canonical = snapshot_runtime
    await snapshot(runtime)
    before = {t: await runtime.store.latest(t) for t in ('fills', 'order_events')}
    history(client, [canonical, earlier_fill(canonical)])
    evidence = await read_terminal_fills(client, detail)
    await runtime._append_terminal_fills(detail, evidence)
    assert {t: await runtime.store.latest(t) for t in before} == before


async def test_full_fee_pnl_and_trade_day_in_daily_review(snapshot_runtime):
    runtime, client, detail, canonical = snapshot_runtime
    first = {**earlier_fill(canonical), 'fee': '-0.01', 'fillPnl': '2'}
    last = {**canonical, 'fee': '-0.09', 'fillPnl': '3'}
    history(client, [last, first])
    await snapshot(runtime)
    day = datetime.fromtimestamp(int(last['fillTime']) / 1000, UTC).date()
    report = await build_daily_review(runtime.store, day)
    assert report['fills'] == 2
    assert Decimal(report['fees']) == Decimal('-0.10')
    assert Decimal(report['realized_pnl']) == 5
    assert Decimal(report['realized_pnl_after_fees']) == Decimal('4.90')
    assert runtime.order_manager.orders['entry']['order_fee'] == detail['fee']


async def test_order_positive_fill_without_last_fields_still_needs_complete_history(snapshot_runtime):
    runtime, client, detail, canonical = snapshot_runtime
    for key in ('tradeId', 'fillSz', 'fillPx', 'fillTime'):
        detail.pop(key)
    history(client, [canonical])
    with pytest.raises(TerminalFillError, match='terminal_fill_size_mismatch'):
        await snapshot(runtime)
    assert runtime.order_manager.orders['entry']['filled'] == '0'


async def test_exhaustion_is_required_even_when_sum_already_matches(snapshot_runtime):
    runtime, client, _, canonical = snapshot_runtime
    extra = {**canonical, 'tradeId': 'unexpected-extra', 'billId': '50', 'fillSz': '0.1'}
    client.fills_history.side_effect = [[canonical, earlier_fill(canonical)], [extra], []]
    with pytest.raises(TerminalFillError, match='terminal_fill_size_mismatch'):
        await snapshot(runtime)
    assert client.fills_history.await_count == 3
    assert not await runtime.store.latest('fills')


async def test_many_pages_reconstruct_every_fill(snapshot_runtime, monkeypatch):
    import app.terminal_fills as terminal

    runtime, client, detail, canonical = snapshot_runtime
    monkeypatch.setattr(terminal, 'PAGE_SIZE', 2)
    rows = [{**canonical, 'tradeId': str(i), 'billId': str(i + 1), 'fillSz': '1'} for i in range(5)]
    detail.update(sz='5', accFillSz='5', tradeId='4', fillSz='1')
    runtime.order_manager.orders['entry']['approved_contracts'] = '5'
    client.fills_history.side_effect = [[rows[4], rows[3]], [rows[2], rows[1]], [rows[0]], []]
    await snapshot(runtime)
    assert len(await runtime.store.latest('fills')) == 5
    assert client.fills_history.await_count == 4
    assert [c.kwargs['after'] for c in client.fills_history.await_args_list] == ['', '4', '2', '1']


@pytest.mark.parametrize('case', ['pages', 'records', 'cursor-repeat', 'cursor-duplicate', 'key-repeat'])
async def test_pagination_bounds_and_repetition_do_not_commit_partial_evidence(snapshot_runtime, monkeypatch, case):
    import app.terminal_fills as terminal

    runtime, client, _, canonical = snapshot_runtime
    monkeypatch.setattr(terminal, 'PAGE_SIZE', 1 if case in ('pages', 'records') else 100)
    if case == 'pages':
        monkeypatch.setattr(terminal, 'MAX_PAGES', 1)
        client.fills_history.side_effect = [[canonical]]
    elif case == 'records':
        monkeypatch.setattr(terminal, 'MAX_RECORDS', 1)
        client.fills_history.side_effect = [[canonical], [earlier_fill(canonical)]]
    elif case == 'cursor-repeat':
        client.fills_history.side_effect = [[canonical], [{**earlier_fill(canonical), 'billId': '200'}]]
    elif case == 'cursor-duplicate':
        client.fills_history.side_effect = [[canonical, {**earlier_fill(canonical), 'billId': '200'}]]
    else:
        client.fills_history.side_effect = [[canonical], [{**canonical, 'billId': '100'}]]
    with pytest.raises(TerminalFillError):
        await snapshot(runtime)
    assert not await runtime.store.latest('fills')
    assert runtime.order_manager.orders['entry']['filled'] == '0'


async def test_history_deadline_is_bounded_and_leaves_ledger_unchanged(snapshot_runtime, monkeypatch):
    import app.terminal_fills as terminal

    runtime, client, _, _ = snapshot_runtime
    monkeypatch.setattr(terminal, 'HISTORY_TIMEOUT_SECONDS', 0)

    async def hangs(*args, **kwargs):
        await asyncio.Event().wait()

    client.fills_history.side_effect = hangs
    with pytest.raises(TerminalFillError, match='terminal_fill_history_unavailable'):
        await snapshot(runtime)
    assert not await runtime.store.latest('fills')


async def test_cancellation_during_read_preserves_lock_and_ledger(snapshot_runtime):
    runtime, client, _, _ = snapshot_runtime
    began = asyncio.Event()

    async def hangs(*args, **kwargs):
        began.set()
        await asyncio.Event().wait()

    client.fills_history.side_effect = hangs
    task = asyncio.create_task(snapshot(runtime))
    await began.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not runtime.order_manager.ledger_lock.locked()
    assert not await runtime.store.latest('fills')


async def test_local_order_changes_during_get_fail_closed(snapshot_runtime):
    runtime, client, _, canonical = snapshot_runtime

    async def changes(*args, **kwargs):
        runtime.order_manager.orders['entry']['approved_contracts'] = '9'
        return [canonical, earlier_fill(canonical)]

    calls = 0

    async def callback(*args, **kwargs):
        nonlocal calls
        calls += 1
        return await changes() if calls == 1 else []

    client.fills_history.side_effect = callback
    with pytest.raises(TerminalFillError):
        await snapshot(runtime)
    assert not await runtime.store.latest('fills')
    assert runtime.order_manager.orders['entry']['filled'] == '0'


async def test_order_ledger_cannot_hide_incomplete_fills_even_when_remote_position_matches(tmp_path):
    from tests.test_ledger_repair import fill
    from tests.test_reconciliation_incident import exit_reconciliation_runtime

    runtime, exchange, row, detail = await exit_reconciliation_runtime(tmp_path)
    detail.update(sz='3.93', accFillSz='3.93', tradeId='last', fillSz='3.79',
                  fillPx='110', fillTime=fill()['fillTime'])
    row['approved_contracts'] = '3.93'
    runtime.portfolio.positions[row['symbol']] = Decimal('3.93')
    evidence = {**fill('last', '3.79', 'exit-id', 'sell', row['clOrdId']), 'billId': '100'}
    exchange.fills_history = AsyncMock(side_effect=[[evidence], []])
    try:
        await runtime.reconcile()
        assert not runtime.reconciliation_healthy
        assert not runtime._last_reconcile_safe
        assert runtime.governor.state != GovernorState.NORMAL
        assert row['filled'] == '0' and row['state'] == 'ACKNOWLEDGED'
        assert not await runtime.store.latest('fills')
        assert not exchange.placed
    finally:
        await runtime.notifications.close()
        await runtime.store.close()


async def test_missing_owned_database_order_cannot_be_reconstructed(snapshot_runtime):
    from sqlalchemy import delete

    from app.storage import ROW_TYPES

    runtime, _, _, _ = snapshot_runtime
    async with runtime.store.sessions.begin() as session:
        await session.execute(delete(ROW_TYPES['orders']))
    with pytest.raises(TerminalFillError, match='terminal_fill_identity_mismatch'):
        await snapshot(runtime)
    assert not await runtime.store.latest('fills')
    assert not await runtime.store.latest('order_events')


async def test_local_extra_fill_not_in_authoritative_set_is_conflict(snapshot_runtime):
    runtime, _, detail, canonical = snapshot_runtime
    extra = {**canonical, 'tradeId': 'old-unexplained', 'fillSz': '0.1'}
    await runtime.order_manager.ingest(ws_event(detail, extra, '0.1'))
    before = await runtime.store.latest('fills')
    with pytest.raises(TerminalFillError, match='terminal_fill_evidence_conflict'):
        await snapshot(runtime)
    assert await runtime.store.latest('fills') == before
    assert runtime.order_manager.orders['entry']['filled'] == '0.1'


async def test_complete_evidence_conflicts_with_old_cumulative_fee_and_is_not_rewritten(snapshot_runtime):
    runtime, _, detail, canonical = snapshot_runtime
    await runtime.order_manager.ingest(ws_event(detail, {**canonical, 'fee': detail['fee']}, '3.79'))
    before = await runtime.store.latest('fills')
    with pytest.raises(TerminalFillError, match='terminal_fill_evidence_conflict'):
        await snapshot(runtime)
    assert await runtime.store.latest('fills') == before
    assert runtime.order_manager.orders['entry']['filled'] == '3.79'


async def test_transaction_failure_rolls_back_fills_and_terminal_state(snapshot_runtime):
    runtime, _, _, _ = snapshot_runtime
    original = runtime.store.append_ledger_recovery

    async def fail(records, snapshots, symbols):
        await original([*records, ('bad-table', {}, 'bad')], snapshots, symbols)

    runtime.store.append_ledger_recovery = fail
    with pytest.raises(ValueError, match='invalid recovery record'):
        await snapshot(runtime)
    assert not await runtime.store.latest('fills')
    assert not await runtime.store.latest('order_events')
    assert runtime.order_manager.orders['entry']['filled'] == '0'
    assert not runtime.order_manager.seen_trade_ids


async def test_restart_after_atomic_reconstruction_has_all_fills(snapshot_runtime):
    from app.execution import OrderManager

    runtime, _, _, _ = snapshot_runtime
    await snapshot(runtime)
    restored = OrderManager(runtime.store)
    await restored.restore()
    assert restored.orders['entry']['filled'] == '3.93'
    assert len(restored.seen_trade_ids) == 2
    assert sum(Decimal(r['fillSz']) for r in await runtime.store.latest('fills')) == Decimal('3.93')


async def test_last_fields_absent_with_complete_set_is_supported(snapshot_runtime):
    runtime, _, detail, _ = snapshot_runtime
    for field in ('tradeId', 'fillSz', 'fillPx', 'fillTime'):
        detail.pop(field)
    await snapshot(runtime)
    assert len(await runtime.store.latest('fills')) == 2
    assert runtime.order_manager.orders['entry']['filled'] == '3.93'


async def test_more_than_one_hundred_fills_with_real_page_limit(snapshot_runtime):
    runtime, client, detail, canonical = snapshot_runtime
    rows = [{**canonical, 'tradeId': str(i), 'billId': str(i + 1), 'fillSz': '0.01'} for i in range(101)]
    detail.update(sz='1.01', accFillSz='1.01', tradeId='100', fillSz='0.01')
    runtime.order_manager.orders['entry']['approved_contracts'] = '1.01'
    client.fills_history.side_effect = [list(reversed(rows[1:])), [rows[0]], []]
    await snapshot(runtime)
    saved = await runtime.store.latest('fills', limit=200)
    assert len(saved) == 101
    assert sum(Decimal(r['fillSz']) for r in saved) == Decimal('1.01')
    assert client.fills_history.await_count == 3
    assert all(call.kwargs['limit'] == 100 for call in client.fills_history.await_args_list)


async def test_healthy_transport_or_position_cannot_resume_incomplete_accounting(snapshot_runtime):
    runtime, client, _, canonical = snapshot_runtime
    history(client, [canonical])
    runtime.governor.halt('manual kill switch')
    with pytest.raises(TerminalFillError):
        await snapshot(runtime)
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.order_manager.orders['entry']['filled'] == '0'


@pytest.mark.parametrize('terminal_state', ['canceled', 'mmp_canceled'])
async def test_zero_fill_decimal_string_is_accepted(snapshot_runtime, terminal_state):
    runtime, client, detail, _ = snapshot_runtime
    detail.update(state=terminal_state, accFillSz='0.0', tradeId='', fillSz='0.000', fillPx='', fillTime='')
    await snapshot(runtime)
    assert runtime.order_manager.orders['entry']['state'] == 'CANCELLED'
    assert not await runtime.store.latest('fills')
    client.fills_history.assert_not_awaited()


async def test_history_failure_diagnostics_never_log_remote_secret(snapshot_runtime, caplog):
    runtime, client, _, _ = snapshot_runtime
    from app.okx import OkxError

    client.fills_history.side_effect = OkxError('secret-api-key-and-auth-header', code='secret')
    with pytest.raises(TerminalFillError, match='terminal_fill_history_unavailable') as caught:
        await snapshot(runtime)
    assert str(caught.value) == 'terminal_fill_history_unavailable'
    assert 'secret-api-key-and-auth-header' not in caplog.text
    assert not await runtime.store.latest('fills')


async def test_exact_remote_position_cannot_hide_missing_earlier_entry_fill(snapshot_runtime, caplog):
    runtime, client, detail, canonical = snapshot_runtime
    client.positions.return_value = [{'instId': detail['instId'], 'pos': '3.93'}]
    client.account_config.return_value = [{'posMode': 'net_mode', 'acctLv': '2'}]
    history(client, [canonical])
    await runtime.reconcile()
    assert not runtime.reconciliation_healthy
    assert not runtime._last_reconcile_safe
    assert runtime.governor.state != GovernorState.NORMAL
    assert runtime.order_manager.orders['entry']['filled'] == '0'
    assert not await runtime.store.latest('fills')
    client.place_order.assert_not_awaited()
    client.place_algo.assert_not_awaited()
    assert any(getattr(record, 'error_code', '') == 'terminal_fill_size_mismatch' for record in caplog.records)


async def test_halt_cancel_confirmation_cannot_bypass_incomplete_terminal_fills(snapshot_runtime):
    runtime, client, _, canonical = snapshot_runtime
    runtime.entry_controller.client = client
    history(client, [canonical])
    assert not await runtime.entry_controller.confirm(runtime.order_manager.orders['entry'])
    assert runtime.order_manager.orders['entry']['filled'] == '0'
    assert runtime.order_manager.orders['entry']['symbol'] in runtime.entry_controller.blocked
    assert not await runtime.store.latest('fills')


async def test_halt_cancel_confirmation_reuses_complete_reconstruction(snapshot_runtime):
    runtime, client, _, _ = snapshot_runtime
    runtime.entry_controller.client = client
    assert not await runtime.entry_controller.confirm(runtime.order_manager.orders['entry'])
    assert runtime.order_manager.orders['entry']['filled'] == '3.93'
    assert len(await runtime.store.latest('fills')) == 2


@pytest.mark.parametrize('complete', [False, True])
async def test_ambiguous_placement_terminal_get_requires_all_fills_without_write_retry(snapshot_runtime, complete):
    from app.execution import ExecutionEngine
    from app.okx import OkxError
    from tests.test_core import evaluate, instrument, ready_risk

    runtime, client, detail, canonical = snapshot_runtime
    decision = evaluate(ready_risk())
    decision.approved_contracts = Decimal('3.93')
    request = ExecutionEngine.from_risk(decision, instrument())
    runtime.execution.client = client
    assert runtime.governor.resume(synchronized=True, healthy=True)
    client.place_order.side_effect = OkxError('ambiguous HTTP timeout')
    terminal = {**detail, 'clOrdId': request.client_order_id, 'ordId': 'ambiguous-order'}
    last = {**canonical, 'clOrdId': request.client_order_id, 'ordId': 'ambiguous-order'}
    client.order.return_value = [terminal]
    history(client, [last, earlier_fill(last)] if complete else [last])
    if complete:
        await runtime.execution.submit(request)
        row = runtime.order_manager.orders[request.client_order_id]
        assert row['state'] == 'FILLED' and row['filled'] == '3.93'
        assert len(await runtime.store.latest('fills')) == 2
    else:
        with pytest.raises(TerminalFillError, match='terminal_fill_size_mismatch'):
            await runtime.execution.submit(request)
        assert runtime.order_manager.orders[request.client_order_id]['filled'] == '0'
        assert not await runtime.store.latest('fills')
    client.place_order.assert_awaited_once()
    client.order.assert_awaited_once()
