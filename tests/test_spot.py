from __future__ import annotations

from datetime import timedelta
from decimal import Decimal as D
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from app.config import Mode, Settings
from app.daily_review import build_daily_review
from app.execution import ExecutionEngine, OrderManager
from app.models import (
    GovernorState,
    Instrument,
    MarketTick,
    OrderBook,
    OrderState,
    PortfolioState,
    Side,
    utcnow,
)
from app.okx import OkxError, OkxRestClient, public_subscriptions
from app.products import base_fee_delta, spot_balance_positions
from app.risk import RiskEngine, RiskGovernor
from app.runtime import TradingRuntime
from app.safety import AlgoOrderManager, PortfolioRiskMonitor
from app.spot import normalize_positions
from app.storage import Store
from tests.test_core import instrument as swap_instrument
from tests.test_core import intent as swap_intent

SPOT = 'BTC-USDT'
SWAP = 'BTC-USDT-SWAP'


def settings(**kwargs):
    return Settings(_env_file=None, spot_symbols=(SPOT,), **kwargs)


def instrument():
    return Instrument(symbol=SPOT, contract_value=D(1), contract_currency='BTC',
                      lot_size=D('.00001'), min_size=D('.0001'), tick_size=D('.1'), market_type='SPOT')


def intent():
    return swap_intent().model_copy(update={'symbol': SPOT, 'expires_at': utcnow() + timedelta(minutes=10)})


def portfolio():
    return PortfolioState(equity=D(5000), available_balance=D(5000), synchronized=True)


def decision(s=None, p=None, i=None):
    g = RiskGovernor()
    g.resume(synchronized=True, healthy=True)
    return RiskEngine(s or settings(), g).evaluate(i or intent(), p or portfolio(), instrument(),
                                                 data_fresh=True, infrastructure_healthy=True)


def account(quantity='0', cash='5000'):
    return [{'details': [
        {'ccy': 'USDT', 'eq': cash, 'eqUsd': cash, 'availEq': cash, 'availBal': cash, 'cashBal': cash,
         'coinUsdPrice': '1', 'liab': '0'},
        {'ccy': 'BTC', 'cashBal': quantity, 'availBal': quantity,
         'eqUsd': str(D(quantity) * 50000), 'liab': '0'},
    ]}]


def fill(cid='entry', oid='order', trade='trade', size='.01', side='buy', **changes):
    ts = str(int(utcnow().timestamp() * 1000))
    return {'instId': SPOT, 'clOrdId': cid, 'ordId': oid, 'tradeId': trade,
            'fillSz': size, 'accFillSz': size, 'sz': size, 'fillPx': '50000', 'fillTime': ts, 'ts': ts,
            'side': side, 'posSide': '', 'state': 'filled', 'fee': '-.00001',
            'feeCcy': 'BTC', 'fillPnl': '', 'tdMode': 'cash', **changes}


def stop(aid, size='.00999'):
    return {'instId': SPOT, 'tdMode': 'cash', 'side': 'sell', 'ordType': 'conditional',
            'algoClOrdId': aid, 'algoId': 'stop-id', 'sz': size, 'slTriggerPx': '49500',
            'slTriggerPxType': 'last', 'slOrdPx': '-1', 'state': 'live', 'failCode': '0'}


def test_spot_opt_in_and_default_swap_unchanged():
    s = Settings(_env_file=None)
    assert not s.spot_symbols and s.trading_symbols == s.symbols
    both = settings()
    assert both.trading_symbols == (*both.symbols, SPOT)
    assert s.max_margin_usage == both.max_margin_usage == .25
    assert s.margin_usage_target == both.margin_usage_target == .2
    assert not both.live_trading_enabled


@pytest.mark.parametrize('symbols', [('BTC-USDT', 'BTC-USDT'), ('BTC-USDC',), ('BTC-USDT-SWAP',)])
def test_invalid_spot_symbols_rejected(symbols):
    with pytest.raises(ValueError):
        Settings(_env_file=None, spot_symbols=symbols)


def test_live_spot_is_hard_blocked():
    with pytest.raises(ValueError, match='spot support requires PAPER'):
        settings(mode=Mode.LIVE, live_trading_enabled=True)


def test_spot_sizing_uses_base_units_and_one_times_leverage():
    d = decision(settings(leverage=3))
    assert d.approved and d.leverage == 1
    assert d.approved_notional == d.approved_contracts * 50000
    assert d.approved_contracts % instrument().lot_size == 0
    assert d.approved_notional < 1000


def test_spot_cannot_open_short():
    assert not decision(i=intent().model_copy(update={'direction': Side.SHORT})).approved


def test_spot_and_swap_share_risk_budget_and_funded_capital():
    base = decision()
    used = portfolio().model_copy(update={'spot_notional': D(700), 'margin_used': D(100),
                                        'position_notional': D(800)})
    assert decision(p=used).approved_contracts < base.approved_contracts
    assert not decision(p=portfolio().model_copy(update={'open_risk': D(50)})).approved
    g = RiskGovernor()
    g.resume(synchronized=True, healthy=True)
    engine = RiskEngine(settings(), g)
    free = engine.evaluate(swap_intent(), portfolio(), swap_instrument(), data_fresh=True, infrastructure_healthy=True)
    shared = engine.evaluate(swap_intent(), used, swap_instrument(), data_fresh=True, infrastructure_healthy=True)
    assert shared.approved_contracts < free.approved_contracts


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '-1'])
def test_spot_invalid_inventory_notional_fails_closed(value):
    assert not decision(p=portfolio().model_copy(update={'spot_notional': D(value)})).approved


def test_spot_minimum_net_exit_size_and_zero_cash():
    assert not decision(p=portfolio().model_copy(update={'available_balance': D(0)})).approved
    tiny = settings(risk_per_trade=.0000001)
    assert not decision(s=tiny).approved


def test_cash_balance_position_and_valuation():
    rows = spot_balance_positions(account('.00999', '4500'), (SPOT,))
    assert D(rows[0]['pos']) == D('.00999') and D(rows[0]['notionalUsd']) == D('499.5')
    assert rows[0]['mgnMode'] == 'cash'


@pytest.mark.parametrize('change', [
    {'cashBal': '-1'}, {'liab': '1'}, {'cashBal': 'NaN'}, {'eqUsd': ''},
])
def test_cash_balance_ambiguous_or_borrowed_fails_closed(change):
    a = account('.01')
    a[0]['details'][1].update(change)
    with pytest.raises((ValueError, ArithmeticError)):
        spot_balance_positions(a, (SPOT,))


def test_normalized_snapshot_uses_same_balance_read():
    stale = [{'instId': SPOT, 'pos': '9'}, {'instId': SWAP, 'pos': '1'}]
    rows = normalize_positions(account('.00999'), stale, (SPOT,))
    assert rows[0]['instId'] == SWAP and D(rows[1]['pos']) == D('.00999')


def test_spot_market_subscriptions_do_not_require_derivatives():
    subs = public_subscriptions((SWAP, SPOT))
    assert {s['channel'] for s in subs if s['instId'] == SPOT and s['channel'] != 'index-tickers'} == {'books5', 'trades', 'tickers'}
    assert any(s['channel'] == 'mark-price' and s['instId'] == SWAP for s in subs)


@pytest.mark.parametrize('change', [
    {'tdMode': 'cross'}, {'side': 'buy'}, {'sz': '.01'}, {'sz': '.009'},
    {'slTriggerPxType': 'mark'}, {'failCode': '51000'}, {'state': 'canceled'},
    {'slOrdPx': '49000'},
])
def test_spot_protection_requires_exact_cash_sell_coverage(change):
    algo = stop('known')
    algo.update(change)
    assert not AlgoOrderManager(Store('sqlite+aiosqlite:///:memory:')).valid(
        algo, symbol=SPOT, position=D('.00999'), stop_price=D(49500), tolerance=D('.1'))


def test_spot_protection_accepts_fail_code_zero_without_reduce_only():
    assert AlgoOrderManager(Store('sqlite+aiosqlite:///:memory:')).valid(
        stop('known'), symbol=SPOT, position=D('.00999'), stop_price=D(49500), tolerance=D('.1'))


async def test_spot_and_swap_instrument_and_history_routing():
    calls = []
    def handler(req):
        calls.append((req.url.path, dict(req.url.params)))
        if req.url.path.endswith('instruments'):
            row = {'instId': SPOT, 'baseCcy': 'BTC', 'lotSz': '.00001', 'minSz': '.0001',
                   'tickSz': '.1', 'state': 'live'} if req.url.params['instType'] == 'SPOT' else {
                       'instId': SWAP, 'ctVal': '.01', 'ctValCcy': 'BTC', 'lotSz': '.01',
                       'minSz': '.01', 'tickSz': '.1', 'state': 'live'}
            return httpx.Response(200, json={'code': '0', 'data': [row]})
        return httpx.Response(200, json={'code': '0', 'data': []})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url='https://test.invalid') as http:
        client = OkxRestClient(settings(okx_api_key='test', okx_secret_key='test', okx_passphrase='test'), http)
        instruments = await client.instruments()
        assert instruments[SPOT].market_type == 'SPOT' and instruments[SPOT].contract_value == 1
        assert instruments[SWAP].market_type == 'SWAP'
        await client.fills_history(SPOT)
        await client.fills_history(SWAP)
    assert calls[-2][1]['instType'] == 'SPOT' and calls[-1][1]['instType'] == 'SWAP'


@pytest.fixture
async def runtime(tmp_path):
    r = TradingRuntime(settings(database_url=f'sqlite+aiosqlite:///{tmp_path}/spot.db'))
    await r.store.initialize()
    r.instruments.update({SPOT: instrument(), SWAP: swap_instrument()})
    for symbol in (SPOT, SWAP):
        r.market.latest_ticks[symbol] = MarketTick(symbol=symbol, timestamp=utcnow(), last=D(50000))
        r.market.latest_books[symbol] = OrderBook(symbol=symbol, timestamp=utcnow(),
                                                 bids=[(D(49999), D(1))], asks=[(D(50000), D(1))])
    r._websockets_healthy = Mock(return_value=True)
    r.redis = AsyncMock()
    r.dead_man_healthy = True
    r.portfolio = portfolio()
    r.governor.resume(synchronized=True, healthy=True)
    try:
        yield r
    finally:
        await r.close()


async def owned_entry(r):
    request = ExecutionEngine.from_risk(decision(), instrument())
    request.risk_decision.approved_contracts = D('.01')
    await r.order_manager.create(request)
    await r.order_manager.ingest(fill(cid=request.client_order_id))
    return r.order_manager.orders[request.client_order_id]


async def configure_snapshot(r, quantity='.00999'):
    entry = await owned_entry(r)
    swap = ExecutionEngine.from_risk(decision().model_copy(update={
        'symbol': SWAP, 'approved_contracts': D(1)}), swap_instrument())
    await r.order_manager.create(swap)
    await r.order_manager.ingest(fill(cid=swap.client_order_id, oid='swap-order', trade='swap-fill',
                                     instId=SWAP, size='1', posSide='net', fee='-.1', feeCcy='USDT', fillPnl='0'))
    spot_stop = stop(entry['protective_algo_id'])
    swap_stop = {**stop(r.order_manager.orders[swap.client_order_id]['protective_algo_id'], '1'),
                 'instId': SWAP, 'reduceOnly': 'true', 'slTriggerPxType': 'mark', 'tdMode': 'isolated'}
    r.client.account = AsyncMock(return_value=account(quantity, '4500'))
    r.client.account_config = AsyncMock(return_value=[{'acctLv': '2', 'posMode': 'net_mode'}])
    r.client.positions = AsyncMock(return_value=[{
        'instId': SWAP, 'pos': '1', 'mgnRatio': '10', 'lever': '1', 'mgnMode': 'isolated',
        'margin': '500', 'markPx': '50000', 'notionalUsd': '500',
    }])
    r.client.pending_orders = AsyncMock(return_value=[])
    r.client.pending_algos = AsyncMock(return_value=[spot_stop, swap_stop])
    r.portfolio.synchronized = False
    r.governor.halt('manual acceptance')
    return entry, swap_stop


async def test_one_engine_reconciles_spot_and_swap_with_shared_equity(runtime):
    await configure_snapshot(runtime)
    await runtime.reconcile()
    assert runtime.reconciliation_healthy and runtime._last_reconcile_safe
    assert runtime.portfolio.positions == {SPOT: D('.00999'), SWAP: D(1)}
    assert runtime.portfolio.equity == D('4999.5')
    assert runtime.portfolio.spot_notional == D('499.5')
    assert runtime.portfolio.margin_used == D(500)
    assert runtime.governor.state == GovernorState.HALT


async def test_restart_reconstructs_fee_adjusted_spot_and_swap(runtime):
    await configure_snapshot(runtime)
    await runtime.order_manager.restore()
    await runtime.reconcile()
    assert runtime.reconciliation_healthy
    assert runtime._audited_positions()[SPOT] == D('.00999')


async def test_spot_close_preserves_swap_exposure(runtime):
    entry, swap_stop = await configure_snapshot(runtime)
    await runtime.reconcile()
    assert runtime.reconciliation_healthy
    await runtime.order_manager.ingest(fill(cid='', oid='spot-stop', trade='exit-fill', size='.00999',
        side='sell', fee='-.25', feeCcy='USDT', attachAlgoClOrdId=entry['protective_algo_id']))
    runtime.client.account.return_value = account('0', '4999.25')
    runtime.client.pending_algos.return_value = [swap_stop]
    await runtime.reconcile()
    assert runtime.reconciliation_healthy, runtime.governor.reason
    assert runtime.portfolio.positions == {SWAP: D(1)}
    assert runtime.portfolio.spot_notional == 0


async def test_unknown_spot_inventory_is_not_adopted(runtime):
    await configure_snapshot(runtime, quantity='.02')
    runtime._verify_protection = AsyncMock(return_value=True)
    runtime._repair_position_ledger = AsyncMock(return_value=False)
    await runtime.reconcile()
    assert runtime.governor.state == GovernorState.HALT
    assert not runtime.reconciliation_healthy
    assert runtime._audited_positions()[SPOT] == D('.00999')


async def test_unknown_cash_sell_order_halts_shared_engine(runtime):
    await configure_snapshot(runtime)
    runtime.client.pending_orders.return_value = [{
        'instId': SPOT, 'tdMode': 'cash', 'side': 'sell', 'clOrdId': 'foreign', 'ordId': 'foreign',
    }]
    await runtime.reconcile()
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.governor.reason == 'foreign risk-increasing pending order'


async def test_missing_spot_stop_uses_cash_emergency_not_contract_reduce_only(runtime):
    _, swap_stop = await configure_snapshot(runtime)
    runtime.client.pending_algos.return_value = [swap_stop]
    runtime.client.place_algo = AsyncMock(side_effect=OkxError('rejected'))
    runtime.client.place_order = AsyncMock(return_value=[{'ordId': 'cash-emergency'}])
    runtime.client.positions.return_value += spot_balance_positions(account('.00999'), (SPOT,))
    await runtime.reconcile()
    assert runtime.governor.state == GovernorState.EMERGENCY
    assert runtime.emergency.targets[SPOT] == 0
    body = runtime.client.place_order.call_args.args[0]
    assert body['instId'] == SPOT and body['side'] == 'sell' and body['tdMode'] == 'cash'
    assert body['sz'] == '0.00999' and 'reduceOnly' not in body


async def test_spot_buy_last_http_gate_requires_approved_owned_order(runtime):
    runtime.client.account = AsyncMock(return_value=account())
    request = ExecutionEngine.from_risk(decision(), instrument())
    await runtime.order_manager.create(request)
    row = runtime.order_manager.orders[request.client_order_id]
    body = {'instId': SPOT, 'clOrdId': request.client_order_id, 'sz': row['approved_contracts'],
            'side': 'buy', 'tdMode': 'cash', 'ordType': 'limit', 'px': row['entry_reference'],
            'attachAlgoOrds': [{'attachAlgoClOrdId': row['protective_algo_id'],
                              'slTriggerPx': row['stop_price'], 'slTriggerPxType': 'last', 'slOrdPx': '-1'}]}
    assert await runtime.spot_safety.write_guard('/api/v5/trade/order', body)
    for change in [{'attachAlgoOrds': []}, {'sz': '100'}, {'tdMode': 'cross'}, {'ordType': 'market'}]:
        assert not await runtime.spot_safety.write_guard('/api/v5/trade/order', body | change)
    runtime.client.account.return_value = account(cash='1')
    assert not await runtime.spot_safety.write_guard('/api/v5/trade/order', body)


async def test_spot_http_guard_rejects_new_foreign_inventory(runtime):
    runtime.client.account = AsyncMock(return_value=account('.01'))
    request = ExecutionEngine.from_risk(decision(), instrument())
    await runtime.order_manager.create(request)
    row = runtime.order_manager.orders[request.client_order_id]
    body = {'instId': SPOT, 'clOrdId': request.client_order_id, 'sz': row['approved_contracts'],
            'side': 'buy', 'tdMode': 'cash', 'ordType': 'limit', 'px': '50000'}
    assert not await runtime.spot_safety.write_guard('/api/v5/trade/order', body)


def test_spot_market_entry_with_attached_base_currency_protection_is_rejected():
    with pytest.raises(ValueError, match='spot entries require limit'):
        ExecutionEngine.from_risk(decision(), instrument(), order_type='market')


async def test_preexisting_demo_coins_are_never_adopted_or_sold(runtime):
    runtime.portfolio.synchronized = False
    runtime.client.account = AsyncMock(return_value=account('1'))
    runtime.client.positions = AsyncMock(return_value=[])
    runtime.client.account_config = AsyncMock(return_value=[{'posMode': 'net_mode', 'acctLv': '2'}])
    runtime.client.pending_orders = AsyncMock(return_value=[])
    runtime.client.pending_algos = AsyncMock(return_value=[])
    runtime.client.place_order = AsyncMock()
    runtime.client.place_algo = AsyncMock()
    await runtime.reconcile()
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.governor.reason == 'unowned spot inventory'
    assert not runtime.emergency.targets
    runtime.client.place_order.assert_not_awaited()
    runtime.client.place_algo.assert_not_awaited()


async def test_spot_partial_fill_cancel_and_restart_preserve_net_base_fee(runtime):
    request = ExecutionEngine.from_risk(decision(), instrument())
    request.risk_decision.approved_contracts = D('.04')
    await runtime.order_manager.create(request)
    partial = fill(cid=request.client_order_id, size='.02', fee='-.00002',
                   state='partially_filled', billId='200')
    await runtime.order_manager.ingest(partial)
    remote = {**partial, 'sz': '.04'}
    algo = stop(runtime.order_manager.orders[request.client_order_id]['protective_algo_id'], '.01998')
    runtime.client.account = AsyncMock(return_value=account('.01998', '4000'))
    runtime.client.positions = AsyncMock(return_value=spot_balance_positions(account('.01998', '4000'), (SPOT,)))
    runtime.client.pending_algos = AsyncMock(return_value=[algo])
    runtime.client.pending_orders = AsyncMock(return_value=[remote])
    runtime.client.account_config = AsyncMock(return_value=[{'posMode': 'net_mode', 'acctLv': '2'}])
    runtime.client.order = AsyncMock(return_value=[remote | {'state': 'canceled'}])
    runtime.client.fills_history = AsyncMock(side_effect=[[partial], [], [partial], []])
    async def cancel(*args, **kwargs):
        runtime.client.pending_orders.return_value = []
        return [{'sCode': '0'}]
    runtime.client.cancel_order = cancel
    runtime.portfolio.synchronized = False
    runtime.governor.halt('manual restart')
    await runtime.reconcile()
    assert runtime.reconciliation_healthy, runtime.governor.reason
    assert runtime.portfolio.positions == {SPOT: D('.01998')}
    assert runtime.order_manager.orders[request.client_order_id]['state'] == 'CANCELLED'
    await runtime.order_manager.restore()
    assert runtime._audited_positions()[SPOT] == D('.01998')


async def test_cash_exit_uses_safety_pool_not_entry_pool():
    c = OkxRestClient(settings())
    try:
        body = {'instId': SPOT, 'tdMode': 'cash', 'side': 'sell'}
        assert c._transport('POST', '/api/v5/trade/order', body) is c._pools['safety']
        assert c._transport('POST', '/api/v5/trade/order-algo', body) is c._pools['safety']
        assert c._transport('POST', '/api/v5/trade/order', body | {'side': 'buy'}) is c._pools['entry']
    finally:
        await c.close()


def test_spot_balance_valuation_converts_usd_to_usdt():
    a = account('.01')
    a[0]['details'][0]['eqUsd'] = '4900'
    rows = spot_balance_positions(a, (SPOT,))
    assert D(rows[0]['notionalUsd']) == D(500) / D('.98')


async def test_spot_terminal_fill_history_preserves_empty_pnl_and_base_fees(runtime):
    request = ExecutionEngine.from_risk(decision(), instrument())
    request.risk_decision.approved_contracts = D('.01')
    await runtime.order_manager.create(request)
    a = fill(cid=request.client_order_id, trade='part-a', size='.004', fee='-.000004', billId='100')
    b = fill(cid=request.client_order_id, trade='part-b', size='.006', fee='-.000006', billId='200')
    detail = {**b, 'sz': '.01', 'accFillSz': '.01', 'fee': '-.00001'}
    runtime.client.fills_history = AsyncMock(side_effect=[[b, a], []])
    await runtime._recover_terminal_order(runtime.order_manager.orders[request.client_order_id], detail)
    assert runtime._audited_positions()[SPOT] == D('.00999')
    rows = [r for _, r in await runtime.store.fill_records()]
    assert len(rows) == 2 and all(r['fillPnl'] == '' for r in rows)
    assert {r['fee'] for r in rows} == {'-.000004', '-.000006'}
    await runtime._append_terminal_fills(detail, [b, a])
    assert len(await runtime.store.fill_records()) == 2


async def test_spot_ws_terminal_fill_recovers_lost_earlier_fill(runtime):
    request = ExecutionEngine.from_risk(decision(), instrument())
    request.risk_decision.approved_contracts = D('.01')
    await runtime.order_manager.create(request)
    a = fill(cid=request.client_order_id, trade='lost-earlier', size='.004', fee='-.000004', billId='100')
    b = fill(cid=request.client_order_id, trade='last', size='.006', accFillSz='.01',
             fee='-.000006', billId='200')
    detail = {**b, 'sz': '.01', 'fee': '-.00001'}
    runtime.client.order = AsyncMock(return_value=[detail])
    runtime.client.fills_history = AsyncMock(side_effect=[[b, a], []])
    runtime.reconcile = AsyncMock()
    await runtime._on_private({'arg': {'channel': 'orders'}, 'data': [b]})
    assert len(await runtime.store.fill_records()) == 2
    assert runtime._audited_positions()[SPOT] == D('.00999')
    assert runtime.governor.state == GovernorState.NORMAL


async def test_spot_ws_terminal_history_failure_blocks_entries(runtime):
    request = ExecutionEngine.from_risk(decision(), instrument())
    await runtime.order_manager.create(request)
    event = fill(cid=request.client_order_id)
    runtime.client.order = AsyncMock(side_effect=OkxError('unavailable'))
    runtime.reconcile = AsyncMock()
    await runtime._on_private({'arg': {'channel': 'orders'}, 'data': [event]})
    assert runtime.governor.state == GovernorState.EMERGENCY
    assert not runtime.execution.entry_allowed()


async def test_wrong_side_cash_protective_event_never_appends_exit(runtime):
    entry = await owned_entry(runtime)
    with pytest.raises(OkxError):
        await runtime.order_manager.ingest(fill(cid='', oid='bad-exit', trade='bad', side='buy',
                                               attachAlgoClOrdId=entry['protective_algo_id']))
    assert len(await runtime.store.fill_records()) == 1
    assert 'protective-bad-exit' not in runtime.order_manager.orders


async def test_cash_protective_order_can_acknowledge_before_any_fill(runtime):
    entry = await owned_entry(runtime)
    event = fill(cid='', oid='exit-live', size='.00999', fillSz='0', accFillSz='0',
                 state='live', side='sell', fee='', feeCcy='',
                 attachAlgoClOrdId=entry['protective_algo_id'])
    await runtime.order_manager.ingest(event)
    assert runtime.order_manager.orders['protective-exit-live']['state'] == 'ACKNOWLEDGED'
    assert runtime._audited_positions()[SPOT] == D('.00999')
    assert len(await runtime.store.fill_records()) == 1


async def test_cash_protective_exit_cannot_spend_fee_difference_from_foreign_balance(runtime):
    entry = await owned_entry(runtime)
    with pytest.raises(OkxError):
        await runtime.order_manager.ingest(fill(cid='', oid='oversized-exit', trade='bad', side='sell',
            size='.01', feeCcy='USDT', fee='-.1', attachAlgoClOrdId=entry['protective_algo_id']))
    assert runtime._audited_positions()[SPOT] == D('.00999')


def test_bark_spot_title_and_base_quantity_are_explicit():
    from app.bark_formatter import localize_bark_event
    from app.models import NotificationCategory, NotificationEvent, NotificationLevel

    e = NotificationEvent(event_code='TRADE_FILLED', level=NotificationLevel.TRADE,
        category=NotificationCategory.TRADE, title='🟢 BTC LONG Filled',
        symbol=SPOT, message='Base quantity: 0.01 BTC', metadata={'side': 'LONG'})
    localized = localize_bark_event(e)
    assert '现货' in localized.title and '买入' in localized.title
    assert '基础币数量' in localized.message and '0.01 BTC' in localized.message


def test_spot_cannot_flatten_below_exchange_minimum_by_rounding_up():
    g = RiskGovernor()
    g.halt('test', emergency=True)
    with pytest.raises(ValueError):
        RiskEngine(settings(), g).emergency_reduce(SPOT, Side.LONG, D('.00000001'), instrument())


async def test_spot_guard_rechecks_risk_and_exact_price_before_http(runtime):
    runtime.client.account = AsyncMock(return_value=account())
    request = ExecutionEngine.from_risk(decision(), instrument())
    await runtime.order_manager.create(request)
    row = runtime.order_manager.orders[request.client_order_id]
    body = {'instId': SPOT, 'clOrdId': request.client_order_id, 'sz': row['approved_contracts'],
            'side': 'buy', 'tdMode': 'cash', 'ordType': 'limit', 'px': '50000',
            'attachAlgoOrds': [{'attachAlgoClOrdId': row['protective_algo_id'],
                              'slTriggerPx': row['stop_price'], 'slTriggerPxType': 'last', 'slOrdPx': '-1'}]}
    assert await runtime.spot_safety.write_guard('/api/v5/trade/order', body)
    assert not await runtime.spot_safety.write_guard('/api/v5/trade/order', body | {'px': '90000'})
    runtime.portfolio.open_risk = D(50)
    assert not await runtime.spot_safety.write_guard('/api/v5/trade/order', body)


async def test_shared_entry_guard_rechecks_symbol_freshness(runtime):
    request = ExecutionEngine.from_risk(decision(), instrument())
    runtime.market.latest_books[SPOT].timestamp = utcnow() - timedelta(seconds=60)
    assert not await runtime.spot_safety.entry_guard(request)
    assert runtime.governor.state == GovernorState.HALT


async def test_shared_entry_guard_rechecks_current_spread(runtime):
    request = ExecutionEngine.from_risk(decision(), instrument())
    runtime.market.latest_books[SPOT].bids = [(D(45000), D(1))]
    assert not await runtime.spot_safety.entry_guard(request)
    assert runtime.governor.reason == 'spread explosion'


async def test_emergency_target_does_not_clear_without_exit_fill_evidence(runtime):
    await owned_entry(runtime)
    runtime.governor.halt('unprotected', emergency=True)
    await runtime.emergency.target(SPOT)
    runtime.client.positions = AsyncMock(return_value=[])
    assert not await runtime.emergency.step(SPOT)
    assert SPOT in runtime.emergency.targets


def test_spot_warmup_requires_cash_feeds_and_new_trades_only():
    from types import SimpleNamespace

    from app.market import MarketDataEngine
    from app.models import MarketTick, OrderBook, Trade
    from app.public_continuity import PublicMarketContinuity

    market = MarketDataEngine(Store('sqlite+aiosqlite:///:memory:'))
    c = PublicMarketContinuity((SPOT,))
    c.restart_unverified = True
    now = utcnow()
    market.latest_ticks[SPOT] = MarketTick(symbol=SPOT, timestamp=now, last=D(50000))
    market.latest_books[SPOT] = OrderBook(symbol=SPOT, timestamp=now,
                                         bids=[(D(49999), D(1))], asks=[(D(50000), D(1))])
    for i in range(100):
        market.latest_trades[SPOT].append(Trade(symbol=SPOT, timestamp=now, trade_id=str(i),
                                              price=D(50000), size=D('.01'), side='buy'))
    ws = SimpleNamespace(worker_alive=True, _run_task=None, generation=1, business_idle=True,
                         is_transport_healthy=lambda: True, is_processing_healthy=lambda: True,
                         is_data_fresh=lambda: True)
    state = c.status(market, ws, 20)
    assert state['ready'] and state['symbols'][SPOT]['mark_ready'] is None
    assert not state['symbols'][SPOT]['derivative_feeds_required']
    market.latest_trades[SPOT].pop()
    assert not c.status(market, ws, 20)['ready']


async def test_base_fee_ledger_idempotency_and_restore(runtime):
    entry = await owned_entry(runtime)
    assert runtime._audited_positions()[SPOT] == D('.00999')
    await runtime.order_manager.ingest(fill(cid=entry['clOrdId'], fillTime=(await runtime.store.fill_records())[0][1]['fillTime'], ts=(await runtime.store.fill_records())[0][1]['ts']))
    assert len(await runtime.store.fill_records()) == 1
    manager = OrderManager(runtime.store)
    await manager.restore()
    assert manager.spot_fees[SPOT] == D('-.00001')
    assert manager.expected_deltas().get(SPOT, D(0)) == 0


async def test_cash_execution_payload_and_ambiguous_write_no_retry(runtime):
    placed = []
    async def place(body):
        placed.append(body)
        raise OkxError('transport failed')
    runtime.client.place_order = place
    runtime.client.order = AsyncMock(return_value=[{'ordId': 'known'}])
    req = ExecutionEngine.from_risk(decision(), instrument())
    await runtime.execution.submit(req)
    assert len(placed) == 1
    body = placed[0]
    assert body['tdMode'] == 'cash' and body['side'] == 'buy'
    assert not {'posSide', 'reduceOnly'} & body.keys()
    assert body['attachAlgoOrds'][0]['slTriggerPxType'] == 'last'
    runtime.client.order.assert_awaited_once_with(SPOT, req.client_order_id)


async def test_emergency_spot_market_payload_cannot_reverse(runtime):
    runtime.governor.halt('unprotected', emergency=True)
    r = runtime.risk.emergency_reduce(SPOT, Side.LONG, D('.00999'), instrument())
    request = ExecutionEngine.from_risk(r, instrument(), order_type='market', reduce_only=True)
    runtime.client.place_order = AsyncMock(return_value=[{'ordId': 'exit'}])
    await runtime.execution.submit(request)
    body = runtime.client.place_order.call_args.args[0]
    assert body['tdMode'] == 'cash' and body['side'] == 'sell'
    assert body['tgtCcy'] == 'base_ccy' and body['banAmend'] is True
    assert 'reduceOnly' not in body and 'posSide' not in body
    with pytest.raises(ValueError):
        runtime.risk.emergency_reduce(SPOT, Side.SHORT, D(1), instrument())


async def test_direct_spot_write_requires_runtime_ownership_before_http():
    calls = []
    async with httpx.AsyncClient(base_url='https://test.invalid', transport=httpx.MockTransport(
        lambda req: calls.append(req) or httpx.Response(200, json={'code': '0', 'data': []})
    )) as http:
        c = OkxRestClient(settings(okx_api_key='test', okx_secret_key='test', okx_passphrase='test'), http)
        with pytest.raises(OkxError, match='spot write blocked'):
            await c.place_order({'instId': SPOT, 'tdMode': 'cash', 'side': 'sell', 'sz': '1'})
    assert not calls


async def test_sell_guard_caps_to_owned_available_inventory(runtime):
    await owned_entry(runtime)
    runtime.client.account = AsyncMock(return_value=account('.00999'))
    d = decision()
    d = d.model_copy(update={'direction': Side.SHORT, 'approved_contracts': D('.00999')})
    req = ExecutionEngine.from_risk(d, instrument(), order_type='market', reduce_only=True)
    await runtime.order_manager.create(req)
    body = {'instId': SPOT, 'tdMode': 'cash', 'side': 'sell', 'ordType': 'market',
            'sz': '.00999', 'tgtCcy': 'base_ccy', 'banAmend': True, 'clOrdId': req.client_order_id}
    assert await runtime.spot_safety.write_guard('/api/v5/trade/order', body)
    for change in [{'sz': '.01'}, {'tdMode': 'cross'}, {'reduceOnly': True}, {'banAmend': False}, {'tgtCcy': 'quote_ccy'}, {'clOrdId': 'foreign'}]:
        assert not await runtime.spot_safety.write_guard('/api/v5/trade/order', body | change)
    runtime.client.account = AsyncMock(return_value=account('.005'))
    assert not await runtime.spot_safety.write_guard('/api/v5/trade/order', body)


async def test_shared_entry_reservation_blocks_other_product(runtime):
    spot = ExecutionEngine.from_risk(decision(), instrument())
    await runtime.order_manager.create(spot)
    swap = ExecutionEngine.from_risk(decision().model_copy(update={'symbol': SWAP}), swap_instrument())
    assert not await runtime.spot_safety.entry_guard(swap)
    await runtime.order_manager.transition(spot.client_order_id, OrderState.CANCELLED)
    assert await runtime.spot_safety.entry_guard(swap)


async def test_spot_fill_requires_reconciliation_before_next_entry(runtime):
    await owned_entry(runtime)
    request = ExecutionEngine.from_risk(decision().model_copy(update={'symbol': SWAP}), swap_instrument())
    assert not await runtime.spot_safety.entry_guard(request)


async def test_spot_cash_sell_fill_closes_exact_fee_adjusted_ledger(runtime):
    entry = await owned_entry(runtime)
    event = fill(cid='', oid='stop-order', trade='exit', size='.00999', side='sell',
                 fee='-.25', feeCcy='USDT', attachAlgoClOrdId=entry['protective_algo_id'])
    await runtime.order_manager.ingest(event)
    assert runtime._audited_positions()[SPOT] == 0
    await runtime.order_manager.restore()
    assert runtime._audited_positions()[SPOT] == 0
    assert runtime.order_manager.orders['protective-stop-order']['reduce_only']


async def test_flat_spot_orphan_algo_blocks_next_shared_entry(runtime):
    entry, swap_stop = await configure_snapshot(runtime)
    await runtime.order_manager.ingest(fill(cid='', oid='stop-order', trade='exit', size='.00999',
        side='sell', fee='-.25', feeCcy='USDT', attachAlgoClOrdId=entry['protective_algo_id']))
    runtime.client.account.return_value = account('0')
    await runtime.reconcile()
    assert runtime.governor.reason == 'orphan spot protective order'
    assert not runtime.reconciliation_healthy


async def test_mixed_fee_report_converts_base_fee_at_fill_price(runtime):
    await owned_entry(runtime)
    report = await build_daily_review(runtime.store, utcnow().date())
    assert D(report['fees']) == D('-.5')
    assert report['fees_by_currency'] == {'BTC': '-0.00001'}
    assert report['realized_pnl'] is None


def test_cash_inventory_has_no_margin_ratio_requirement():
    rows = spot_balance_positions(account('.01'), (SPOT,))
    assert PortfolioRiskMonitor(settings()).breached(portfolio(), rows) == ('', None)


@pytest.mark.parametrize('change', [{'fee': ''}, {'feeCcy': ''}, {'feeCcy': 'OKB'}, {'fee': 'NaN'}])
def test_spot_fee_evidence_cannot_be_guessed(change):
    with pytest.raises((ValueError, ArithmeticError)):
        base_fee_delta(fill(**change))
