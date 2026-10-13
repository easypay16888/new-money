"""Manual recovery of rejected/unapplied public observations; no trading authority."""
from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any
from uuid import uuid4

from app.market import MarketDataEngine
from app.models import utcnow
from app.okx import OkxWebSocket

REASON = "Public market continuity lost"
CODE = "public_market_continuity_lost"
HOLD = "public_market_continuity_hold"
RELEASE = "public_market_continuity_release"
SESSION_OPEN = "public_market_session_open"
SESSION_CLOSE = "public_market_session_close"
SESSION_RESUME = "public_market_session_manual_resume"
TRADE_SAMPLES = 100
LOSS_REASONS = frozenset({"WebSocket processing backlog", "WebSocket processing failed",
                          "WebSocket protocol error", "WebSocket run task stopped"})


def validate_event(payload: dict[str, Any], events: set[str]) -> None:
    try:
        timestamp = datetime.fromisoformat(payload['occurred_at'])
        valid = (
            payload['event'] in events
            and re.fullmatch(r'[0-9a-f]{32}', payload['continuity_id']) is not None
            and timestamp.tzinfo is not None
            and math.isfinite(timestamp.timestamp())
            and timestamp.timestamp() > 0
            and (timestamp - utcnow()).total_seconds() <= 60
        )
        if payload['event'] in {HOLD, RELEASE}:
            valid = valid and payload['reason_code'] == CODE
        if payload['event'] == HOLD:
            valid = (valid and payload['socket_name'] == 'public-market'
                     and type(payload['generation']) is int and payload['generation'] >= 0
                     and payload['cause'] in {'ws_processing_backlog', 'ws_worker_failed', 'ws_protocol_error', 'ws_run_task_failed', 'ws_preack_data_discarded'}
                     and type(payload.get('loss_count', 1)) is int and payload.get('loss_count', 1) >= 1)
        if not valid:
            raise ValueError
    except (KeyError, TypeError, ValueError, OverflowError):
        raise ValueError('public market continuity audit invalid') from None


class PublicMarketContinuity:
    def __init__(self, symbols: tuple[str, ...]) -> None:
        self.symbols = tuple(symbols)
        self.hold: dict[str, Any] | None = None
        self.persisted = False
        self.restart_unverified = False
        self.corrupt = False
        self.epoch = 0
        self.received_after = 0.0
        self.generation_after = -1
        self.since = utcnow()

    @property
    def required(self) -> bool:
        return self.hold is not None or self.restart_unverified or self.corrupt

    def invalidate(self, market: MarketDataEngine, *, received_after: float, generation: int) -> None:
        self.epoch += 1
        self.received_after = received_after
        self.generation_after = generation
        self.since = utcnow()
        market.invalidate_public_state(self.symbols, self.since)

    def loss(self, market: MarketDataEngine, ws: OkxWebSocket) -> bool:
        if self.hold is not None and self.generation_after == ws.generation:
            return False  # Receive fault and run-loop observer report the same session.
        self.invalidate(market, received_after=ws.clock(), generation=ws.generation)
        self.persisted = False
        count = int(self.hold.get('loss_count', 1)) + 1 if self.hold else 1
        self.hold = {
            'event': HOLD, 'continuity_id': uuid4().hex, 'reason_code': CODE,
            'socket_name': 'public-market', 'generation': ws.generation,
            'cause': ws.last_failure_reason_code if ws.last_failure_reason_code in
            {'ws_processing_backlog', 'ws_worker_failed', 'ws_protocol_error', 'ws_run_task_failed', 'ws_preack_data_discarded'} else 'ws_worker_failed',
            'occurred_at': self.since.isoformat(),
            'loss_count': count,
        }
        return True

    def accepts(self, message: dict[str, Any]) -> bool:
        if self.epoch == 0:
            return True
        received = message.get('_public_received_at')
        generation = message.get('_public_generation')
        if not isinstance(received, (int, float)) or isinstance(received, bool):
            return False
        return bool(math.isfinite(received) and received > self.received_after
                    and isinstance(generation, int) and not isinstance(generation, bool)
                    and generation > self.generation_after)

    def status(self, market: MarketDataEngine, ws: OkxWebSocket | None,
               stale_seconds: float) -> dict[str, Any]:
        now = utcnow()

        def fresh(timestamp: datetime | None) -> bool:
            return bool(timestamp is not None and timestamp >= self.since
                        and 0 <= (now - timestamp).total_seconds() < stale_seconds)

        symbols = {}
        for symbol in self.symbols:
            tick, book = market.latest_ticks.get(symbol), market.latest_books.get(symbol)
            observations = market.derivative_observations.get(symbol)
            mark = observations['mark'] if observations else None
            index = observations['index'] if observations else None
            samples = len({trade.trade_id for trade in market.latest_trades.get(symbol, [])
                           if trade.trade_id and self.since <= trade.timestamp <= now})
            symbols[symbol] = {
                'ticker_ready': bool(tick and tick.last.is_finite() and tick.last > 0
                                     and fresh(tick.timestamp)),
                'book_ready': bool(book and book.bids and book.asks and fresh(book.timestamp)),
                'mark_ready': bool(mark and mark[-1].value.is_finite() and mark[-1].value > 0
                                   and fresh(mark[-1].timestamp)),
                'index_ready': bool(index and index[-1].value.is_finite() and index[-1].value > 0
                                    and fresh(index[-1].timestamp)),
                'trade_samples': samples, 'trade_samples_required': TRADE_SAMPLES,
            }
        ready = bool(
            not self.corrupt and (self.persisted or self.hold is None)
            and ws is not None and ws.worker_alive and ws.is_transport_healthy()
            and (ws._run_task is None or not ws._run_task.done())
            and ws.is_processing_healthy() and ws.is_data_fresh() and ws.business_idle
            and (not self.required or ws.generation > self.generation_after)
            and (not self.required or all(
                row['ticker_ready'] and row['book_ready'] and row['mark_ready']
                and row['index_ready'] and row['trade_samples'] >= TRADE_SAMPLES
                for row in symbols.values()
            ))
        )
        return {'hold_active': self.hold is not None or self.corrupt,
                'restart_unverified': self.restart_unverified, 'ready': ready,
                'state': ('READY_FOR_MANUAL_RESUME' if ready else 'WARMING_UP')
                if self.required else 'NORMAL',
                'reason_code': CODE if self.required else 'healthy',
                'symbols': symbols}
