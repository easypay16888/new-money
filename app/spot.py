"""Shared-account cash safeguards. No borrowing, transfers, or independent engine."""
from __future__ import annotations

from decimal import ROUND_FLOOR, Decimal
from typing import TYPE_CHECKING, Any

from app.config import Mode
from app.models import ExecutionRequest, GovernorState, RiskDecision, TradeIntent, utcnow
from app.products import is_spot, number, spot_balance_positions

if TYPE_CHECKING:
    from app.runtime import TradingRuntime


class SpotSafety:
    def __init__(self, runtime: TradingRuntime) -> None:
        self.runtime = runtime

    async def entry_guard(self, request: ExecutionRequest) -> bool:
        r = self.runtime
        if not r.settings.spot_symbols:
            return True
        if request.signal_expires_at is None or request.signal_expires_at <= utcnow():
            return False
        if (not r.execution.entry_allowed or not r.execution.entry_allowed()
                or not r.portfolio.synchronized):
            return False
        audited = {k: v for k, v in r._audited_positions().items() if v}
        if audited != r.portfolio.positions:
            return False  # A new fill must be fully reconciled before another product can enter.
        if any(row['clOrdId'] != request.client_order_id for row in r.order_manager.pending_entries()):
            return False  # Serialize unsettled account-wide entry reservations.
        d = request.risk_decision
        if d.entry_reference is None or d.stop_price is None:
            return False
        instrument = r.instruments.get(request.symbol)
        if instrument is None:
            return False
        book = r.market.latest_books.get(request.symbol)
        tick = r.market.latest_ticks.get(request.symbol)
        now = utcnow()
        fresh = bool(r._websockets_healthy() and not r.public_continuity.required
                     and book and tick and book.bids and book.asks
                     and 0 <= (now - book.timestamp).total_seconds() < r.settings.stale_timeout_seconds
                     and 0 <= (now - tick.timestamp).total_seconds() < r.settings.stale_timeout_seconds)
        spread = Decimal(1)
        if book and book.bids and book.asks:
            bid, ask = book.bids[0][0], book.asks[0][0]
            if bid.is_finite() and ask.is_finite() and 0 < bid <= ask:
                spread = (ask - bid) / ask
        checked = r.risk.evaluate(TradeIntent(
            id=d.intent_id, symbol=d.symbol, direction=d.direction, confidence=1,
            signal_ids=(), strategies=(), entry_reference=d.entry_reference,
            stop_price=d.stop_price, take_profit_reference=d.take_profit_reference,
            expires_at=request.signal_expires_at,
        ), r.portfolio, instrument, data_fresh=fresh,
            infrastructure_healthy=r.store.healthy and r.redis is not None and r.dead_man_healthy,
            spread=spread)
        return checked.approved and d.approved_contracts <= checked.approved_contracts

    async def write_guard(self, path: str, body: dict[str, Any]) -> bool:
        r = self.runtime
        symbol = body.get('instId')
        if r.settings.mode != Mode.PAPER or symbol not in r.settings.spot_symbols:
            return False
        rows = list(r.order_manager.orders.values())
        if path == '/api/v5/trade/cancel-order':
            return any(row['symbol'] == symbol and (
                body.get('clOrdId') == row['clOrdId']
                or (body.get('ordId') and body['ordId'] == row.get('order_id'))
            ) for row in rows)
        if path == '/api/v5/trade/cancel-algos':
            return any(row['symbol'] == symbol and (
                body.get('algoClOrdId') == row.get('protective_algo_id')
                or (body.get('algoId') and any(
                    a.get('algoId') == body['algoId']
                    and a.get('algoClOrdId') == row.get('protective_algo_id')
                    for a in r.algo_manager.algos.values()))
            ) for row in rows)
        if path not in {'/api/v5/trade/order', '/api/v5/trade/order-algo'}:
            return False
        if (body.get('tdMode') != 'cash'
                or body.get('tradeQuoteCcy') not in {None, 'USDT'}
                or any(k in body for k in ('posSide', 'reduceOnly', 'autoBorrow', 'lever', 'mgnMode'))):
            return False
        instrument = r.instruments.get(symbol)
        if instrument is None or instrument.market_type != 'SPOT':
            return False
        try:
            size = number(body.get('sz'))
            if size < instrument.min_size or size % instrument.lot_size != 0:
                return False
            if body.get('ordType') == 'market' and (
                body.get('tgtCcy') != 'base_ccy' or body.get('banAmend') is not True
            ):
                return False
            account = await r.client.account()
            remote_spot = {p['instId']: number(p['pos']) for p in spot_balance_positions(account, r.settings.spot_symbols)}
            balances = {row['ccy']: row for row in account[0]['details']}
            if body.get('side') == 'buy':
                if number(balances.get('USDT', {}).get('liab') or '0') != 0:
                    return False
                owned_spot = {k: v for k, v in r._audited_positions().items() if is_spot(k) and v}
                if owned_spot != remote_spot:
                    return False
                local = r.order_manager.orders.get(str(body.get('clOrdId') or ''))
                if (path != '/api/v5/trade/order' or local is None
                        or body.get('ordType') != 'limit'
                        or local['symbol'] != symbol or local['reduce_only']
                        or local['direction'] != 'LONG' or number(local['approved_contracts']) != size
                        or not r.execution.entry_allowed or not r.execution.entry_allowed()
                        or r.governor.state != GovernorState.NORMAL):
                    return False
                attached = body.get('attachAlgoOrds')
                if (not isinstance(attached, list) or len(attached) != 1
                        or attached[0].get('attachAlgoClOrdId') != local['protective_algo_id']
                        or number(attached[0].get('slTriggerPx')) != number(local['stop_price'])
                        or attached[0].get('slTriggerPxType') != 'last'
                        or attached[0].get('slOrdPx') != '-1'):
                    return False
                quote = balances.get('USDT', {})
                available = number(quote['availBal'])
                price = number(body.get('px') or local['entry_reference'])
                expected_price = (number(local['entry_reference']) / instrument.tick_size).to_integral_value(
                    rounding=ROUND_FLOOR) * instrument.tick_size
                if price != expected_price:
                    return False
                request = ExecutionRequest(
                    risk_decision=RiskDecision.model_validate(local['risk_decision']),
                    client_order_id=local['clOrdId'], market_type='SPOT',
                    signal_expires_at=local['signal_expires_at'],
                )
                if not await self.entry_guard(request):
                    return False
                reserve = Decimal(str(r.settings.spot_fee_rate + r.settings.slippage_bps / 10000))
                return price > 0 and size * price * (1 + reserve) <= available
            if body.get('side') != 'sell':
                return False
            base = balances.get(symbol.split('-')[0], {})
            if number(base.get('liab') or '0') != 0:
                return False
            owned = r._audited_positions().get(symbol, Decimal(0))
            if not 0 < size <= min(owned, number(base['availBal']), number(base['cashBal'])):
                return False
            if path.endswith('order-algo'):
                return (size == owned and body.get('ordType') in {'conditional', 'oco'}
                        and body.get('slTriggerPxType') == 'last' and body.get('slOrdPx') == '-1'
                        and any(row['symbol'] == symbol and not row['reduce_only']
                                and number(row['filled']) > 0
                                and number(row['stop_price']) == number(body.get('slTriggerPx'))
                                for row in rows))
            local = r.order_manager.orders.get(str(body.get('clOrdId') or ''))
            return bool(local and local['symbol'] == symbol and local['reduce_only']
                        and local['direction'] == 'SHORT'
                        and number(local['approved_contracts']) == size)
        except (KeyError, IndexError, ArithmeticError, TypeError, ValueError):
            return False


def normalize_positions(account: list[dict], positions: list[dict], symbols: tuple[str, ...]) -> list[dict]:
    if not symbols:
        return positions
    return [p for p in positions if not is_spot(p['instId'])] + spot_balance_positions(account, symbols)
