"""Bounded read-only proof of every fill in a verified terminal order."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from app.fill_identity import FillKey, fill_key
from app.ledger_repair import decimal_value, parse_fill
from app.okx import OkxError

PAGE_SIZE = 100
MAX_PAGES = 51
MAX_RECORDS = 5000
HISTORY_TIMEOUT_SECONDS = 30


class TerminalFillError(OkxError):
    def __init__(self, reason_code: str, *, retryable: bool = False) -> None:
        super().__init__(reason_code, retryable=retryable)
        self.reason_code = reason_code


def validate_fill(row: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    try:
        canonical = parse_fill(row)
        if (
            canonical['instId'] != detail['instId']
            or canonical['ordId'] != detail['ordId']
            or canonical['side'] != detail['side']
            or (canonical.get('clOrdId') and canonical['clOrdId'] != detail['clOrdId'])
            or canonical.get('posSide') not in (None, '', detail.get('posSide') or 'net')
        ):
            raise TerminalFillError('terminal_fill_identity_mismatch')
        client_id = canonical.get("clOrdId")
        if client_id not in (None, "") and (not isinstance(client_id, str) or client_id != client_id.strip()):
            raise ValueError
        for field in ('fillTime', 'ts'):
            datetime.fromtimestamp(int(canonical[field]) / 1000, UTC)
        if detail.get('feeCcy') and canonical.get('feeCcy') != detail['feeCcy']:
            raise TerminalFillError('terminal_fill_identity_mismatch')
        # This path needs complete accounting; missing fee/PnL is not a zero.
        for field in ('fee', 'fillPnl'):
            decimal_value(canonical.get(field))
        if canonical.get('fillFee') not in (None, '') and decimal_value(canonical['fillFee']) != decimal_value(canonical['fee']):
            raise TerminalFillError('terminal_fill_evidence_conflict')
        if (not isinstance(canonical.get('feeCcy'), str)
                or not canonical['feeCcy'].strip()):
            raise ValueError
        return canonical
    except TerminalFillError:
        raise
    except (KeyError, ArithmeticError, TypeError, ValueError):
        raise TerminalFillError('terminal_fill_evidence_incomplete') from None


def validate_fill_set(rows: list[dict[str, Any]], detail: dict[str, Any]) -> None:
    keys: dict[FillKey, dict[str, Any]] = {}
    total = Decimal(0)
    for row in rows:
        key = fill_key(row['instId'], row['tradeId'])
        if key in keys:
            raise TerminalFillError('terminal_fill_evidence_conflict')
        keys[key] = row
        total += decimal_value(row['fillSz'], positive=True)
    if total != decimal_value(detail['accFillSz']):
        raise TerminalFillError('terminal_fill_size_mismatch')
    # Last-fill fields are optional, but a partial set is insufficient evidence.
    raw_size = detail.get('fillSz')
    try:
        has_size = raw_size not in (None, '') and decimal_value(raw_size) != 0
    except (ArithmeticError, TypeError, ValueError):
        raise TerminalFillError('terminal_fill_evidence_incomplete') from None
    if detail.get('tradeId') or has_size:
        try:
            key = fill_key(detail.get('instId'), detail.get('tradeId'))
            last = keys.get(key)
            if last is None:
                raise TerminalFillError('terminal_fill_evidence_incomplete')
            if (
                decimal_value(last['fillSz'], positive=True) != decimal_value(detail.get('fillSz'), positive=True)
                or decimal_value(last['fillPx'], positive=True) != decimal_value(detail.get('fillPx'), positive=True)
                or str(last['fillTime']) != str(detail.get('fillTime'))
            ):
                raise TerminalFillError('terminal_fill_evidence_conflict')
        except (ArithmeticError, TypeError, ValueError):
            raise TerminalFillError('terminal_fill_evidence_incomplete') from None


async def read_terminal_fills(client: Any, detail: dict[str, Any]) -> list[dict[str, Any]]:
    """Exhaust the exact order query, even after its reported size has been reached."""
    try:
        expected = decimal_value(detail['accFillSz'])
        if expected < 0:
            raise ValueError
        if detail.get('fee') not in (None, ''):
            decimal_value(detail['fee'])
    except (KeyError, ArithmeticError, TypeError, ValueError):
        raise TerminalFillError('terminal_fill_evidence_incomplete') from None
    if expected == 0:
        validate_fill_set([], detail)
        return []
    records: list[dict[str, Any]] = []
    keys: set[FillKey] = set()
    bills: set[str] = set()
    cursor = ''
    try:
        async with asyncio.timeout(HISTORY_TIMEOUT_SECONDS):
            for _ in range(MAX_PAGES):
                try:
                    page = await client.fills_history(
                        detail['instId'], order_id=detail['ordId'], after=cursor, limit=PAGE_SIZE,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    raise TerminalFillError(
                        'terminal_fill_history_unavailable',
                        retryable=isinstance(exc, OkxError) and exc.retryable,
                    ) from None
                if not isinstance(page, list) or len(page) > PAGE_SIZE or any(
                    not isinstance(row, dict) for row in page
                ):
                    raise TerminalFillError('terminal_fill_evidence_incomplete')
                if not page:
                    validate_fill_set(records, detail)
                    return sorted(records, key=lambda row: (int(row['fillTime']), row['tradeId']))
                if len(records) + len(page) > MAX_RECORDS:
                    raise TerminalFillError('terminal_fill_evidence_incomplete')
                page_bills: list[str] = []
                for row in page:
                    canonical = validate_fill(row, detail)
                    key = fill_key(canonical['instId'], canonical['tradeId'])
                    if key in keys:
                        raise TerminalFillError('terminal_fill_evidence_conflict')
                    bill = canonical.get('billId')
                    if (not isinstance(bill, str) or not bill.isascii() or not bill.isdecimal()
                            or int(bill) <= 0 or str(int(bill)) != bill or bill in bills
                            or (cursor and int(bill) >= int(cursor))):
                        raise TerminalFillError('terminal_fill_evidence_incomplete')
                    keys.add(key)
                    bills.add(bill)
                    page_bills.append(bill)
                    records.append(canonical)
                # after is exclusive and returns older billIds. Do not depend on
                # fillTime/ts ordering, or use tradeId as the pagination cursor.
                cursor = min(page_bills, key=int)
    except TimeoutError:
        raise TerminalFillError('terminal_fill_history_unavailable') from None
    raise TerminalFillError('terminal_fill_evidence_incomplete')
