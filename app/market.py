from __future__ import annotations

import json
from collections import defaultdict, deque
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
            await self.redis.set(key, json.dumps(value), ex=120)
