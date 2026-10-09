from __future__ import annotations

import json
from collections import defaultdict, deque
from contextvars import ContextVar
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from redis.asyncio import Redis

from app.derivatives import DerivativeHistory
from app.models import Candle, DerivativeObservation, MarketTick, OrderBook, Trade
from app.storage import Store


def okx_time(value: str, *, historical: bool = False) -> datetime:
    timestamp = datetime.fromtimestamp(int(value) / 1000, UTC)
    age = (datetime.now(UTC) - timestamp).total_seconds()
    if age < -600 or (not historical and age > 86400):
        raise ValueError("market timestamp outside permitted range")
    return timestamp


class MarketDataEngine:
    def __init__(
        self,
        store: Store,
        redis: Redis | None = None,
        *,
        orderbook_snapshot_interval_seconds: float = 5,
    ) -> None:
        self.store = store
        self.redis = redis
        self.candles: dict[tuple[str, str], deque[Candle]] = defaultdict(lambda: deque(maxlen=400))
        self.latest_ticks: dict[str, MarketTick] = {}
        self.latest_books: dict[str, OrderBook] = {}
        self.latest_derivatives: dict[str, dict[str, Decimal]] = defaultdict(dict)
        self.last_book_snapshot: dict[str, datetime] = {}
        self.orderbook_snapshot_interval_seconds = orderbook_snapshot_interval_seconds
        self.derivative_observations: dict[str, dict[str, deque[DerivativeObservation]]] = (
            defaultdict(
                lambda: {
                    "mark": deque(maxlen=512),
                    "index": deque(maxlen=512),
                    "funding": deque(maxlen=100),
                    "oi": deque(maxlen=512),
                }
            )
        )
        self.latest_trades: dict[str, deque[Trade]] = defaultdict(lambda: deque(maxlen=100))
        self._batch_cache: ContextVar[dict[str, str] | None] = ContextVar(
            "public_batch_cache", default=None
        )
        self._batch_derivatives: ContextVar[set[str] | None] = ContextVar(
            "public_batch_derivatives", default=None
        )

    async def handle_batch(self, messages: list[dict[str, Any]]) -> None:
        """Persist every public observation; pipeline only the latest Redis cache values."""
        if len(messages) > 64 or any(message.get("arg", {}).get("channel") not in {
            "tickers", "trades", "books5", "mark-price", "index-tickers",
            "funding-rate", "open-interest",
        } for message in messages):
            raise ValueError("batch only supports bounded public market messages")
        if self._batch_cache.get() is not None:
            raise ValueError("nested public market batch")
        cache: dict[str, str] = {}
        dirty: set[str] = set()
        cache_token = self._batch_cache.set(cache)
        derivative_token = self._batch_derivatives.set(dirty)
        try:
            async with self.store.market_batch():
                for message in messages:
                    await self.handle(message)
                # Same timestamped reconstruction and all observations retained;
                # avoid replaying the bounded history for every frame in this batch.
                for symbol in sorted(dirty):
                    await self._refresh_derivative_cache(symbol)
            # Redis I/O is outside the durable market transaction. Failure still
            # propagates to the WS worker, fencing freshness and new entries.
            if self.redis is not None and cache:
                async with self.redis.pipeline(transaction=False) as pipeline:
                    for key, value in cache.items():
                        pipeline.set(key, value, ex=120)
                    await pipeline.execute()
        finally:
            self._batch_derivatives.reset(derivative_token)
            self._batch_cache.reset(cache_token)

    async def handle(self, message: dict[str, Any], *, historical: bool = False) -> None:
        arg = message.get("arg", {})
        channel = arg.get("channel", "")
        symbol = arg.get("instId", "")
        for row in message.get("data", []):
            if channel.startswith("candle"):
                timeframe = channel.removeprefix("candle")
                candle = Candle(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=okx_time(row[0], historical=historical),
                    open=Decimal(row[1]),
                    high=Decimal(row[2]),
                    low=Decimal(row[3]),
                    close=Decimal(row[4]),
                    volume=Decimal(row[5]),
                    confirmed=row[8] == "1",
                )
                if candle.confirmed:
                    series = self.candles[(symbol, timeframe)]
                    if not series or candle.timestamp > series[-1].timestamp:
                        series.append(candle)
                        await self.store.append(
                            "market_candles", candle.model_dump(mode="json"), symbol=symbol
                        )
                        await self._cache(
                            f"candle:{symbol}:{timeframe}", candle.model_dump(mode="json")
                        )
            elif channel == "tickers":
                tick = MarketTick(
                    symbol=symbol,
                    timestamp=okx_time(row["ts"]),
                    last=Decimal(row["last"]),
                    bid=Decimal(row["bidPx"]) if row.get("bidPx") else None,
                    ask=Decimal(row["askPx"]) if row.get("askPx") else None,
                )
                self.latest_ticks[symbol] = tick
                await self._cache(f"ticker:{symbol}", tick.model_dump(mode="json"))
            elif channel == "trades":
                trade = Trade(
                    symbol=symbol,
                    timestamp=okx_time(row["ts"]),
                    trade_id=row["tradeId"],
                    price=Decimal(row["px"]),
                    size=Decimal(row["sz"]),
                    side=row["side"],
                )
                await self.store.append(
                    "market_trades",
                    trade.model_dump(mode="json"),
                    symbol=symbol,
                    reference_id=trade.trade_id,
                )
                self.latest_trades[symbol].append(trade)
            elif channel == "books5":
                book = OrderBook(
                    symbol=symbol,
                    timestamp=okx_time(row["ts"]),
                    bids=[(Decimal(x[0]), Decimal(x[1])) for x in row["bids"]],
                    asks=[(Decimal(x[0]), Decimal(x[1])) for x in row["asks"]],
                    sequence=row.get("seqId"),
                )
                if book.bids and book.asks and book.bids[0][0] >= book.asks[0][0]:
                    raise ValueError("crossed order book")
                self.latest_books[symbol] = book
                previous_snapshot = self.last_book_snapshot.get(symbol)
                if (
                    previous_snapshot is None
                    or (book.timestamp - previous_snapshot).total_seconds()
                    >= self.orderbook_snapshot_interval_seconds
                ):
                    await self.store.append(
                        "market_orderbook_snapshots", book.model_dump(mode="json"), symbol=symbol
                    )
                    self.last_book_snapshot[symbol] = book.timestamp
            elif channel in {"mark-price", "funding-rate", "open-interest", "index-tickers"}:
                timestamp = okx_time(row["ts"])
                if channel == "index-tickers":
                    symbol += "-SWAP"
                field = {
                    "mark-price": "mark",
                    "funding-rate": "funding",
                    "open-interest": "oi",
                    "index-tickers": "index",
                }[channel]
                source = {"mark": "markPx", "funding": "fundingRate", "oi": "oi", "index": "idxPx"}[
                    field
                ]
                if row.get(source):
                    value = Decimal(row[source])
                    observation = DerivativeObservation(
                        symbol=symbol, timestamp=timestamp, kind=field, value=value
                    )
                    self.derivative_observations[symbol][field].append(observation)
                    await self.store.append(
                        "market_derivatives", observation.model_dump(mode="json"), symbol=symbol
                    )
                    dirty = self._batch_derivatives.get()
                    if dirty is not None:
                        dirty.add(symbol)
                    else:
                        await self._refresh_derivative_cache(symbol)

    async def _refresh_derivative_cache(self, symbol: str) -> None:
        self.latest_derivatives[symbol] = {
            key: Decimal(str(item))
            for key, item in self.derivative_context(symbol, datetime.now(UTC)).items()
        }
        await self._cache(
            f"derivatives:{symbol}",
            {k: str(v) for k, v in self.latest_derivatives[symbol].items()},
        )

    def derivative_context(self, symbol: str, as_of: datetime) -> dict[str, float]:
        observations = [
            observation
            for series in self.derivative_observations[symbol].values()
            for observation in series
        ]
        return DerivativeHistory(observations).at(as_of)

    async def _cache(self, key: str, value: dict[str, Any]) -> None:
        if self.redis is not None:
            cache = self._batch_cache.get()
            if cache is not None:
                cache[key] = json.dumps(value)
                return
            await self.redis.set(key, json.dumps(value), ex=120)
