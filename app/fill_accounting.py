"""Proof and immutable receipts for the legacy cumulative-fee accounting defect."""
from __future__ import annotations

import json
from decimal import Decimal
from hashlib import sha256
from typing import Any

from app.fill_identity import equivalent_fill, fill_key
from app.terminal_fills import validate_fill, validate_fill_set

REASON = 'legacy_terminal_order_cumulative_fee'
EVIDENCE_FIELDS = (
    'instId', 'ordId', 'clOrdId', 'tradeId', 'billId', 'side', 'posSide',
    'fillSz', 'fillPx', 'fillTime', 'ts', 'fee', 'feeCcy', 'fillPnl',
)
ORDER_FIELDS = (
    'instId', 'ordId', 'clOrdId', 'side', 'posSide', 'reduceOnly', 'state', 'sz',
    'accFillSz', 'fee', 'feeCcy', 'tradeId', 'fillSz', 'fillPx', 'fillTime',
)


def digest(payload: Any) -> str:
    return sha256(json.dumps(payload, sort_keys=True, separators=(',', ':'),
                             allow_nan=False).encode()).hexdigest()


def number(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
        if not result.is_finite():
            raise ValueError
        return result
    except (ArithmeticError, ValueError, TypeError):
        raise ValueError('accounting correction evidence invalid') from None


def proof(detail: dict, rows: list[dict]) -> list[dict]:
    try:
        if not isinstance(rows, list) or not 2 <= len(rows) <= 5000:
            raise ValueError
        canonical = [validate_fill(row, detail) for row in rows]
        validate_fill_set(canonical, detail)
        if len(canonical) < 2 or any(not str(r.get('billId', '')).isascii()
                                   or not str(r.get('billId', '')).isdecimal()
                                   or int(r['billId']) <= 0
                                   or str(int(r['billId'])) != r['billId'] for r in canonical):
            raise ValueError
        if len({r['billId'] for r in canonical}) != len(canonical):
            raise ValueError
        if sum((number(r['fee']) for r in canonical), Decimal(0)) != number(detail['fee']):
            raise ValueError
        return [{k:row[k] for k in EVIDENCE_FIELDS if k in row} for row in canonical]
    except Exception:
        raise ValueError('accounting correction evidence invalid') from None


def verify_owned_order(local: dict, detail: dict) -> None:
    try:
        if (
            local['clOrdId'] != detail['clOrdId'] or local['symbol'] != detail['instId']
            or local['direction'] not in {'LONG', 'SHORT'}
            or detail['side'] != ('buy' if local['direction'] == 'LONG' else 'sell')
            or local.get('reduce_only') is not False or str(detail['reduceOnly']).lower() != 'false'
            or detail.get('posSide') != 'net'
            or detail['state'] != 'filled' or local['state'] != 'FILLED'
            or not isinstance(detail['ordId'], str) or not detail['ordId']
            or local.get('order_id') != detail['ordId']
            or not (number(local['approved_contracts']) == number(detail['sz'])
                    == number(detail['accFillSz']) == number(local['filled']) > 0)
        ):
            raise ValueError
    except (KeyError, ValueError, TypeError):
        raise ValueError('accounting correction ownership unverified') from None


def make_receipt(original: dict, original_id: int, detail: dict, rows: list[dict]) -> dict:
    detail = {k:detail[k] for k in ORDER_FIELDS if k in detail}
    canonical = proof(detail, rows)
    key = fill_key(original.get('instId'), original.get('tradeId'))
    matches = [r for r in canonical if fill_key(r['instId'], r['tradeId']) == key]
    if len(matches) != 1:
        raise ValueError('accounting correction identity conflict')
    remote = matches[0]
    # Only accounting can change. Compare everything else using the existing fill invariant.
    candidate = {**original, 'fee': remote['fee'], 'fillFee': remote['fee'],
                 'fillPnl': remote['fillPnl']}
    if not equivalent_fill(candidate, remote):
        raise ValueError('accounting correction identity conflict')
    # Exact legacy signature, not a general override for arbitrary financial conflicts.
    if (original.get('fillFee') not in (None, '')
            or original.get('fillPnl') not in (None, '', remote['fillPnl'])
            or original.get('state') != 'filled'
            or original.get('clOrdId') != detail['clOrdId']
            or original.get('feeCcy') != remote['feeCcy']
            or number(original.get('fee')) != number(detail['fee'])
            or number(original.get('accFillSz')) != number(detail['accFillSz'])
            or number(original['fillSz']) >= number(detail['accFillSz'])
            or original.get('tradeId') != detail.get('tradeId')):
        raise ValueError('accounting correction legacy signature unverified')
    return {
        'version': 1, 'reason': REASON, 'instId': key[0], 'tradeId': key[1],
        'ordId': remote['ordId'], 'original_fill_id': original_id,
        'original_digest': digest(original),
        'authoritative': remote, 'evidence': {'order': dict(detail), 'fills': canonical},
        'evidence_digest': digest({'order': detail, 'fills': canonical}),
    }


def corrected_fill(original: dict, original_id: int, receipt: dict) -> dict:
    try:
        if (type(receipt.get('version')) is not int or receipt.get('version') != 1
                or type(receipt.get('original_fill_id')) is not int
                or receipt.get('reason') != REASON):
            raise ValueError
        expected = make_receipt(original, original_id, receipt['evidence']['order'],
                                receipt['evidence']['fills'])
        if receipt != expected:
            raise ValueError
        canonical = receipt['authoritative']
        return {**original, 'fee': canonical['fee'], 'fillFee': canonical['fee'],
                'fillPnl': canonical['fillPnl']}
    except Exception:
        raise ValueError('accounting correction audit conflict') from None
