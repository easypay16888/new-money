"""Cash spot semantics; quantities are base currency, never contract counts."""
from __future__ import annotations

from decimal import Decimal
from typing import Any


def is_spot(symbol: str) -> bool:
    return len(symbol.split('-')) == 2 and symbol.endswith('-USDT')


def instrument_type(symbol: str) -> str:
    return 'SPOT' if is_spot(symbol) else 'SWAP'


def number(value: Any) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError('invalid spot balance evidence')
    return result


def reduces_exposure(order: dict[str, Any]) -> bool:
    if is_spot(str(order.get('instId') or order.get('symbol') or '')):
        return order.get('tdMode') == 'cash' and order.get('side') == 'sell'
    return str(order.get('reduceOnly', 'false')).lower() in {'true', '1'}


def spot_balance_positions(account: list[dict[str, Any]], symbols: tuple[str, ...]) -> list[dict]:
    if not symbols:
        return []
    if len(account) != 1 or not isinstance(account[0].get('details'), list):
        raise ValueError('spot balance evidence unavailable')
    details = account[0]['details']
    currencies = [r.get('ccy') for r in details]
    if len(currencies) != len(set(currencies)):
        raise ValueError('duplicate spot balance evidence')
    balances = {r['ccy']: r for r in details}
    result = []
    for symbol in symbols:
        row = balances.get(symbol.split('-')[0])
        if row is None:
            continue  # OKX omits zero-balance currencies in an unfiltered balance query.
        quantity = number(row['cashBal'])
        if quantity < 0 or number(row.get('liab') or '0') != 0:
            raise ValueError('spot borrowing or negative inventory detected')
        if not quantity:
            continue
        quote = balances.get('USDT', {})
        quote_equity = number(quote.get('eq'))
        usd_per_usdt = number(quote.get('eqUsd')) / quote_equity if quote_equity > 0 else Decimal(0)
        value = number(row['eqUsd']) / usd_per_usdt if usd_per_usdt > 0 else Decimal(0)
        if value <= 0:
            raise ValueError('spot valuation unavailable')
        result.append({'instId': symbol, 'instType': 'SPOT', 'pos': str(quantity),
                       'markPx': str(value / quantity), 'notionalUsd': str(value),
                       'mgnMode': 'cash', 'lever': '1'})
    return result


def base_fee_delta(fill: dict[str, Any]) -> Decimal:
    symbol = str(fill.get('instId') or '')
    if not is_spot(symbol):
        return Decimal(0)
    currency = fill.get('feeCcy')
    if not currency or fill.get('fee') in (None, ''):
        raise ValueError('spot fill fee evidence unavailable')
    if currency not in {symbol.split('-')[0], 'USDT'}:
        raise ValueError('unsupported spot fee currency')
    fee = number(fill['fee'])
    return fee if currency == symbol.split('-')[0] else Decimal(0)
