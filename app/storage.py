from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    Integer,
    Numeric,
    String,
    cast,
    func,
    insert,
    or_,
    select,
    text,
)
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)
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
    "market_derivatives",
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
    "notification_events",
    "account_bindings",
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
        self.engine: AsyncEngine = create_async_engine(url, pool_pre_ping=True, hide_parameters=True)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self._market_rows: ContextVar[dict[str, list[dict[str, Any]]] | None] = ContextVar(
            "market_batch_rows", default=None
        )
        self.healthy = False

    @asynccontextmanager
    async def market_batch(self) -> AsyncIterator[None]:
        """Group only market records in one durable, task-local transaction."""
        if self._market_rows.get() is not None:
            raise RuntimeError("nested market batch")
        async with self.sessions.begin() as session:
            rows: dict[str, list[dict[str, Any]]] = {}
            token = self._market_rows.set(rows)
            try:
                yield
            finally:
                self._market_rows.reset(token)
            # No RETURNING / ORM identity hydration: one executemany per table,
            # rather than one database round trip per market observation.
            for table, records in rows.items():
                await session.execute(insert(ROW_TYPES[table]), records)
        self.healthy = True

    async def initialize(self) -> None:
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
            # A constant unique expression enforces one record, including legacy tables.
            # Conflicting legacy bindings fail initialization instead of being discarded.
            await connection.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS account_bindings_singleton "
                "ON account_bindings ((1))"
            ))
            await connection.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS fills_reference_unique "
                "ON fills (reference_id) WHERE reference_id IS NOT NULL"
            ))
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
        market_rows = self._market_rows.get()
        if market_rows is not None:
            if table not in {"market_trades", "market_derivatives", "market_orderbook_snapshots"}:
                raise ValueError("non-market record forbidden in market batch")
            market_rows.setdefault(table, []).append({
                "symbol": symbol, "reference_id": reference_id, "payload": payload,
                "timestamp": datetime.now(UTC),
            })
            return
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

    async def max_equity_since(self, timestamp: datetime) -> Decimal | None:
        row_type = ROW_TYPES["portfolio_snapshots"]
        equity = cast(row_type.payload["equity"].as_string(), Numeric(38, 18))
        async with self.sessions() as session:
            peak = await session.scalar(
                select(func.max(equity)).where(row_type.timestamp >= timestamp)
            )
        return Decimal(peak) if peak is not None else None

    async def bind_live_account(self, uid: str) -> None:
        """A LIVE ledger must be new or already bound to the same verified account."""
        if not uid.strip():
            raise ValueError("LIVE account identity is required")
        digest = sha256(uid.encode()).hexdigest()
        bindings = ROW_TYPES["account_bindings"]
        events = ROW_TYPES["system_events"]
        async with self.sessions.begin() as session:
            if self.engine.dialect.name == "postgresql":
                # Serialize check/history/insert inside the same transaction, across hosts.
                await session.execute(text("SELECT pg_advisory_xact_lock(:key)"),
                                      {"key": 579483812703578118})
            rows = (await session.scalars(select(bindings))).all()
            incompatible = await session.scalar(select(func.count()).select_from(events).where(
                events.payload["event"].as_string() == "start",
                or_(events.payload["mode"].as_string() != "LIVE",
                    events.payload["mode"].as_string().is_(None)),
            ))
            if incompatible or any(
                row.payload.get("mode") != "LIVE" or row.payload.get("account_digest") != digest
                for row in rows
            ):
                raise ValueError("LIVE requires a separate ledger bound to the verified account")
            if rows:
                return
            for table in ("orders", "order_events", "fills", "emergency_targets", "portfolio_snapshots"):
                exists = await session.scalar(select(ROW_TYPES[table].id).limit(1))
                if exists is not None:
                    raise ValueError("LIVE cannot use an unbound existing trading ledger")
            session.add(bindings(payload={"mode": "LIVE", "account_digest": digest}))

    async def all_for_symbol(self, table: str, symbol: str) -> list[dict[str, Any]]:
        if table not in ROW_TYPES:
            raise ValueError(f"unknown table: {table}")
        row_type = ROW_TYPES[table]
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(row_type).where(row_type.symbol == symbol).order_by(row_type.id)
                )
            ).all()
        return [row.payload for row in rows]

    async def ledger_snapshot(self, table: str, symbols: set[str]) -> list[dict[str, Any]]:
        if table not in {"orders", "order_events", "fills"}:
            raise ValueError("invalid ledger table")
        row_type = ROW_TYPES[table]
        async with self.sessions() as session:
            rows = (await session.scalars(select(row_type).where(
                row_type.symbol.in_(symbols)
            ).order_by(row_type.id).limit(100001))).all()
        if len(rows) > 100000:
            raise ValueError("ledger repair evidence incomplete")
        return [row.payload for row in rows]

    async def append_ledger_recovery(
        self, records: list[tuple[str, dict[str, Any], str]],
        snapshots: dict[str, list[dict[str, Any]]], symbols: set[str],
    ) -> None:
        """Append a proven recovery atomically; concurrent ledger changes abort the plan."""
        async with self.sessions.begin() as session:
            if self.engine.dialect.name == "postgresql":
                await session.execute(text("SET LOCAL lock_timeout = '5s'"))
                await session.execute(text("SET LOCAL statement_timeout = '15s'"))
                await session.execute(text(
                    "LOCK TABLE orders, order_events, fills IN SHARE ROW EXCLUSIVE MODE"
                ))
            elif self.engine.dialect.name == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            else:
                raise ValueError("ledger repair evidence incomplete")
            for table, expected in snapshots.items():
                row_type = ROW_TYPES[table]
                actual = (await session.scalars(select(row_type).where(
                    row_type.symbol.in_(symbols)
                ).order_by(row_type.id).limit(100001))).all()
                if [row.payload for row in actual] != expected:
                    raise ValueError("ledger changed during repair; retry with fresh evidence")
            for table, payload, reference in records:
                if table not in {"orders", "order_events", "fills", "system_events"}:
                    raise ValueError("invalid recovery record")
                if table == "system_events" and payload.get("event") != "ledger_repair_manual_hold":
                    raise ValueError("invalid recovery audit")
                timestamp = datetime.now(UTC)
                if table == "fills":
                    # Reports use event timestamp: account for the fill on its actual trade day.
                    timestamp = datetime.fromtimestamp(int(payload["fillTime"]) / 1000, UTC)
                    existing = await session.scalar(select(ROW_TYPES["fills"].id).where(or_(
                        ROW_TYPES["fills"].reference_id == reference,
                        ROW_TYPES["fills"].payload["tradeId"].as_string() == reference,
                    )).limit(1))
                    if existing is not None:
                        raise ValueError("ledger fill conflict")
                session.add(ROW_TYPES[table](symbol=payload.get("symbol") or payload.get("instId"),
                    reference_id=reference, payload=payload, timestamp=timestamp))
        self.healthy = True

    async def ledger_repair_hold(self) -> dict[str, Any] | None:
        table = ROW_TYPES["system_events"]
        async with self.sessions() as session:
            row = await session.scalar(select(table).where(
                table.payload["event"].as_string().in_(
                    ["ledger_repair_manual_hold", "ledger_repair_manual_release"]
                )
            ).order_by(table.id.desc()).limit(1))
        return row.payload if row and row.payload["event"] == "ledger_repair_manual_hold" else None

    async def latest_per_symbol(self, table: str) -> list[dict[str, Any]]:
        if table not in ROW_TYPES:
            raise ValueError(f"unknown table: {table}")
        row_type = ROW_TYPES[table]
        latest_ids = select(func.max(row_type.id)).group_by(row_type.symbol)
        async with self.sessions() as session:
            rows = (
                await session.scalars(select(row_type).where(row_type.id.in_(latest_ids)))
            ).all()
        return [row.payload for row in rows]

    async def close(self) -> None:
        await self.engine.dispose()
