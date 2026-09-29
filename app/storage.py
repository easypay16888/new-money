from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import JSON, DateTime, Integer, String, select
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class EventRow(Base):
    __abstract__ = True
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True
    )
    symbol: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    reference_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


TABLES = (
    "market_candles",
    "market_trades",
    "market_orderbook_snapshots",
    "features",
    "market_regimes",
    "signals",
    "trade_intents",
    "orders",
    "order_events",
    "algo_events",
    "emergency_targets",
    "fills",
    "positions",
    "portfolio_snapshots",
    "risk_events",
    "system_events",
    "strategy_metrics",
    "backtest_runs",
    "daily_reports",
)
ROW_TYPES: dict[str, type[EventRow]] = {}
for table in TABLES:
    ROW_TYPES[table] = type("Row_" + table, (EventRow,), {"__tablename__": table})


class Store:
    def __init__(self, url: str) -> None:
        parsed = make_url(url)
        database = parsed.database
        if (
            parsed.drivername.startswith("sqlite")
            and database is not None
            and database != ":memory:"
        ):
            Path(database).parent.mkdir(parents=True, exist_ok=True)
        self.engine: AsyncEngine = create_async_engine(url, pool_pre_ping=True)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.healthy = False

    async def initialize(self) -> None:
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.healthy = True

    async def append(
        self,
        table: str,
        payload: dict[str, Any],
        *,
        symbol: str | None = None,
        reference_id: str | None = None,
    ) -> None:
        if table not in ROW_TYPES:
            raise ValueError(f"unknown table: {table}")
        async with self.sessions.begin() as session:
            session.add(ROW_TYPES[table](symbol=symbol, reference_id=reference_id, payload=payload))
        self.healthy = True

    async def latest(self, table: str, limit: int = 100) -> list[dict[str, Any]]:
        if table not in ROW_TYPES:
            raise ValueError(f"unknown table: {table}")
        row_type = ROW_TYPES[table]
        async with self.sessions() as session:
            rows = (
                await session.scalars(select(row_type).order_by(row_type.id.desc()).limit(limit))
            ).all()
        return [row.payload for row in rows]

    async def since(
        self, table: str, timestamp: datetime, limit: int = 10000
    ) -> list[dict[str, Any]]:
        if table not in ROW_TYPES:
            raise ValueError(f"unknown table: {table}")
        row_type = ROW_TYPES[table]
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(row_type)
                    .where(row_type.timestamp >= timestamp)
                    .order_by(row_type.id)
                    .limit(limit)
                )
            ).all()
        return [row.payload for row in rows]

    async def between(
        self, table: str, start: datetime, end: datetime, limit: int = 10000
    ) -> list[dict[str, Any]]:
        if table not in ROW_TYPES:
            raise ValueError(f"unknown table: {table}")
        row_type = ROW_TYPES[table]
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(row_type)
                    .where(row_type.timestamp >= start, row_type.timestamp < end)
                    .order_by(row_type.id)
                    .limit(limit)
                )
            ).all()
        return [row.payload for row in rows]

    async def close(self) -> None:
        await self.engine.dispose()
