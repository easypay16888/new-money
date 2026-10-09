"""Explicit PAPER-only accounting maintenance; no trading or resume authority."""
from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from app.config import Mode, Settings
from app.okx import OkxRestClient
from app.storage import Store
from app.terminal_fills import read_terminal_fills


class AccountingCorrectionService:
    def __init__(self, settings: Settings, client: Any, store: Store,
                 status: Callable[[], Awaitable[dict]]) -> None:
        self.settings, self.client, self.store, self.status = settings, client, store, status

    async def _guard(self) -> None:
        state = await self.status()
        if (self.settings.mode != Mode.PAPER or self.settings.live_trading_enabled
                or state.get('mode') != 'PAPER' or state.get('running') is not False
                or state.get('risk_state') not in {'HALT', 'EMERGENCY'}):
            raise ValueError('accounting correction requires stopped PAPER runtime')

    async def correct(self, symbol: str, client_id: str, *, apply: bool = False) -> dict:
        await self._guard()
        if symbol not in self.settings.symbols:
            raise ValueError('accounting correction ownership unverified')
        detail = await self.client.order(symbol, client_id)
        if len(detail) != 1 or detail[0].get('clOrdId') != client_id or detail[0].get('instId') != symbol:
            raise ValueError('accounting correction ownership unverified')
        rows = await read_terminal_fills(self.client, detail[0])
        await self._guard()
        count = await self.store.append_accounting_corrections(detail[0], rows, apply=apply)
        return {'evidence_complete': True, 'fills_checked': len(rows),
                'corrections_added': count if apply else 0,
                'corrections_proposed': count, 'applied': apply,
                'manual_resume_required': True}


async def run(symbol: str, client_id: str, *, apply: bool) -> dict:
    settings = Settings()
    if settings.mode != Mode.PAPER or settings.live_trading_enabled:
        raise ValueError('accounting correction is PAPER only')
    store, client = Store(settings.database_url), OkxRestClient(settings)
    try:
        async with httpx.AsyncClient(timeout=5) as status_client:
            async def status() -> dict:
                response = await status_client.get(
                    'http://127.0.0.1:8000/status',
                    headers={'Authorization': 'Bearer ' + settings.api_token},
                )
                response.raise_for_status()
                return response.json()

            service = AccountingCorrectionService(settings, client, store, status)
            await service._guard()  # Never initialize/migrate while the engine is trading.
            await store.initialize()
            return await service.correct(symbol, client_id, apply=apply)
    finally:
        await client.close()
        await store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description='Prove and append legacy PAPER fee corrections')
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--client-order-id', required=True)
    parser.add_argument('--apply', action='store_true', help='Append correction receipts; never resume')
    args = parser.parse_args()
    try:
        print(json.dumps(asyncio.run(run(args.symbol, args.client_order_id, apply=args.apply))))
    except Exception as exc:
        # Remote exception text/URLs/settings may contain secrets. Only safe error type is printed.
        print(json.dumps({'success': False, 'error_type': type(exc).__name__,
                          'reason': 'accounting correction failed closed', 'resume_called': False}))
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
