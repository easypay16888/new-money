from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any
from typing import cast as typing_cast

from sqlalchemy import (
    JSON,
    DateTime,
    Integer,
    Numeric,
    String,
    cast,
    func,
    insert,
    inspect,
    or_,
    select,
    text,
    tuple_,
)
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.fill_identity import FillKey, fill_key


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
    "fill_accounting_corrections",
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
        self.healthy = False
        async with self.engine.begin() as connection:
            if self.engine.dialect.name == "postgresql":
                await connection.execute(text("SET LOCAL lock_timeout = '5s'"))
                await connection.execute(text("SET LOCAL statement_timeout = '15s'"))
                # Serialize schema initialization across processes in this database.
                await connection.execute(text("SELECT pg_advisory_xact_lock(7265636)"))
            elif self.engine.dialect.name == "sqlite":
                # Include DDL and validation in the same durable transaction.
                await connection.execute(text("BEGIN IMMEDIATE"))
            await connection.run_sync(Base.metadata.create_all)
            # A constant unique expression enforces one record, including legacy tables.
            # Conflicting legacy bindings fail initialization instead of being discarded.
            await connection.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS account_bindings_singleton "
                "ON account_bindings ((1))"
            ))
            if self.engine.dialect.name == "postgresql":
                await connection.execute(text("LOCK TABLE fills IN SHARE ROW EXCLUSIVE MODE"))
            fills = ROW_TYPES["fills"]
            duplicate = await connection.scalar(select(func.count()).select_from(fills).where(
                fills.reference_id.is_not(None)
            ).group_by(fills.symbol, fills.reference_id).having(func.count() > 1).limit(1))
            if duplicate is not None:
                raise ValueError("ledger fill conflict")
            # Bounded keyset batches under the write lock. A PostgreSQL streaming
            # cursor would keep a portal open and prevent subsequent index DDL.
            total = await connection.scalar(select(func.count()).select_from(fills)) or 0
            last_id: int | None = None
            for _ in range(0, total, 5000):
                statement = select(
                    fills.id, fills.symbol, fills.reference_id, fills.payload
                ).order_by(fills.id).limit(5000)
                if last_id is not None:
                    statement = statement.where(fills.id > last_id)
                rows = (await connection.execute(statement)).all()
                for row in rows:
                    last_id = row.id
                    symbol, reference, payload = row.symbol, row.reference_id, row.payload
                    if not isinstance(payload, dict):
                        raise ValueError("invalid fill identity")
                    if reference is None and not payload.get("tradeId"):
                        continue  # A legacy non-fill audit record has no canonical identity.
                    key = fill_key(symbol, reference)
                    if (payload.get("instId", key[0]) != key[0]
                            or payload.get("tradeId", key[1]) != key[1]):
                        raise ValueError("ledger fill conflict")
            await connection.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS fills_instrument_reference_unique "
                "ON fills (symbol, reference_id) WHERE reference_id IS NOT NULL"
            ))
            indexes = await connection.run_sync(lambda conn: inspect(conn).get_indexes("fills"))
            index = next(i for i in indexes if i["name"] == "fills_instrument_reference_unique")
            condition = index.get("dialect_options", {}).get(self.engine.dialect.name + "_where")
            normalized = "".join(str(condition).lower().split()).replace("(", "").replace(")", "").replace('"', '')
            if not index["unique"] or index["column_names"] != ["symbol", "reference_id"] or normalized != "reference_idisnotnull":
                raise ValueError("invalid fill identity index")
            await connection.execute(text("DROP INDEX IF EXISTS fills_reference_unique"))
            await connection.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS fill_accounting_corrections_unique "
                "ON fill_accounting_corrections (symbol, reference_id)"
            ))
        await self.validate_accounting_corrections()
        self.healthy = True

    async def validate_accounting_corrections(self) -> None:
        from app.fill_accounting import corrected_fill

        corrections, fills = ROW_TYPES['fill_accounting_corrections'], ROW_TYPES['fills']
        async with self.sessions() as session:
            rows = (await session.execute(select(corrections, fills.id, fills.payload).outerjoin(
                fills, (fills.symbol == corrections.symbol)
                & (fills.reference_id == corrections.reference_id)
            ).limit(100001))).all()
            if len(rows) > 100000:
                raise ValueError('accounting correction audit incomplete')
            seen: set[FillKey] = set()
            for data in rows:
                receipt = typing_cast(Any, data[0])
                key = fill_key(receipt.symbol, receipt.reference_id)
                if key in seen or data[1] is None or data[2] is None:
                    raise ValueError('accounting correction audit conflict')
                seen.add(key)
                corrected_fill(typing_cast(dict[str, Any], data[2]),
                               typing_cast(int, data[1]), receipt.payload)

    async def fill_for_key(self, key: FillKey) -> dict[str, Any] | None:
        symbol, reference = fill_key(*key)
        table = ROW_TYPES["fills"]
        async with self.sessions() as session:
            row = await session.scalar(select(table).where(
                table.symbol == symbol, table.reference_id == reference
            ))
        if row is None:
            return None
        return (await self.effective_fill_payloads([row.payload]))[0]

    async def effective_fill_payloads(self, payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # Local import avoids the storage -> terminal proof -> ledger storage dependency cycle.
        from app.fill_accounting import corrected_fill, digest

        keys = {fill_key(r['instId'], r['tradeId']) for r in payloads
                if r.get('instId') and r.get('tradeId')}
        corrections, fills = ROW_TYPES['fill_accounting_corrections'], ROW_TYPES['fills']
        overlays: dict[FillKey, dict[str, Any]] = {}
        originals: dict[FillKey, str] = {}
        async with self.sessions() as session:
            ordered = sorted(keys)
            for offset in range(0, len(ordered), 400):
                rows = await session.execute(select(corrections, fills.id, fills.payload).outerjoin(
                    fills, (fills.symbol == corrections.symbol)
                    & (fills.reference_id == corrections.reference_id)
                ).where(tuple_(corrections.symbol, corrections.reference_id).in_(ordered[offset:offset + 400])))
                for data in rows:
                    receipt = typing_cast(Any, data[0])
                    original_id = typing_cast(int, data[1])
                    original = typing_cast(dict[str, Any] | None, data[2])
                    key = fill_key(receipt.symbol, receipt.reference_id)
                    if key in overlays or original is None:
                        raise ValueError('accounting correction audit conflict')
                    overlays[key] = corrected_fill(original, original_id, receipt.payload)
                    originals[key] = receipt.payload['original_digest']
        result = []
        for payload in payloads:
            payload_key = fill_key(payload['instId'], payload['tradeId']) if payload.get('instId') and payload.get('tradeId') else None
            if payload_key is not None and payload_key in overlays:
                # A concurrent/foreign snapshot cannot silently acquire another fill's receipt.
                if originals[payload_key] != digest(payload):
                    raise ValueError('accounting correction audit conflict')
                result.append(dict(overlays[payload_key]))
            else:
                result.append(dict(payload))
        return result

    async def append_accounting_corrections(
        self, detail: dict[str, Any], evidence: list[dict[str, Any]], *, apply: bool = True,
    ) -> int:
        from app.fill_accounting import corrected_fill, make_receipt, proof, verify_owned_order
        from app.fill_identity import equivalent_fill

        canonical = proof(detail, evidence)
        symbol, client_id = detail['instId'], detail['clOrdId']
        orders, events, fills = (ROW_TYPES[t] for t in ('orders', 'order_events', 'fills'))
        corrections = ROW_TYPES['fill_accounting_corrections']
        async with self.sessions.begin() as session:
            if self.engine.dialect.name == 'postgresql':
                await session.execute(text("SET LOCAL lock_timeout = '5s'"))
                await session.execute(text("SET LOCAL statement_timeout = '15s'"))
                await session.execute(text(
                    "LOCK TABLE orders, order_events, fills, fill_accounting_corrections IN SHARE ROW EXCLUSIVE MODE"
                ))
            elif self.engine.dialect.name == 'sqlite':
                await session.execute(text('BEGIN IMMEDIATE'))
            else:
                raise ValueError('accounting correction backend unsupported')
            owned = (await session.scalars(select(orders).where(
                orders.payload['clOrdId'].as_string() == client_id
            ).limit(2))).all()
            if len(owned) != 1:
                raise ValueError('accounting correction ownership unverified')
            local = dict(owned[0].payload)
            history = (await session.scalars(select(events).where(
                events.payload['clOrdId'].as_string() == client_id
            ).order_by(events.id).limit(100001))).all()
            if len(history) > 100000:
                raise ValueError('accounting correction evidence invalid')
            for row in history:
                local.update(row.payload)
            verify_owned_order(local, detail)
            records = (await session.scalars(select(fills).where(
                fills.symbol == symbol, or_(fills.payload['ordId'].as_string() == detail['ordId'],
                                            fills.payload['clOrdId'].as_string() == client_id)
            ).limit(5001))).all()
            by_key = {fill_key(r.payload.get('instId'), r.payload.get('tradeId')): r for r in records}
            remote = {fill_key(r['instId'], r['tradeId']): r for r in canonical}
            if len(by_key) != len(records) or by_key.keys() != remote.keys():
                raise ValueError('accounting correction complete fill set required')
            planned = []
            for key, row in by_key.items():
                existing = await session.scalar(select(corrections).where(
                    corrections.symbol == key[0], corrections.reference_id == key[1]
                ))
                effective = corrected_fill(row.payload, row.id, existing.payload) if existing else row.payload
                if equivalent_fill(effective, remote[key]):
                    continue
                if existing:
                    raise ValueError('accounting correction audit conflict')
                receipt = make_receipt(row.payload, row.id, detail, canonical)
                planned.append((key, receipt))
            if apply and planned:
                for key, receipt in planned:
                    session.add(corrections(symbol=key[0], reference_id=key[1], payload=receipt))
                session.add(ROW_TYPES['system_events'](payload={
                    'event': 'ledger_repair_manual_hold', 'emergency': True,
                    'reason': 'accounting corrected; manual resume required',
                }))
                session.add(ROW_TYPES['system_events'](symbol=symbol, reference_id=client_id, payload={
                    'event': 'fill_accounting_correction_applied', 'symbol': symbol,
                    'clOrdId': client_id, 'corrections_added': len(planned),
                    'source': 'okx_fills_history', 'manual_resume_required': True,
                }))
            return len(planned)

    async def fill_records(self) -> list[tuple[FillKey, dict[str, Any]]]:
        table = ROW_TYPES["fills"]
        async with self.sessions() as session:
            rows = (await session.scalars(select(table).order_by(table.id).limit(100001))).all()
        if len(rows) > 100000:
            raise ValueError("ledger repair evidence incomplete")
        result = []
        for row in rows:
            if row.reference_id is not None:
                if row.symbol is None:
                    raise ValueError("invalid fill identity")
                result.append((fill_key(row.symbol, row.reference_id), row.payload))
        effective = await self.effective_fill_payloads([payload for _, payload in result])
        return [(key, payload) for (key, _), payload in zip(result, effective, strict=True)]

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
        if table == 'fill_accounting_corrections':
            raise ValueError('accounting corrections require verified evidence transaction')
        if table == "fills" and reference_id is not None:
            if symbol is None:
                raise ValueError("invalid fill identity")
            key = fill_key(symbol, reference_id)
            if (payload.get("instId", key[0]) != key[0]
                    or payload.get("tradeId", key[1]) != key[1]):
                raise ValueError("ledger fill conflict")
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
        payloads = [row.payload for row in rows]
        return await self.effective_fill_payloads(payloads) if table == 'fills' else payloads

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
        payloads = [row.payload for row in rows]
        return await self.effective_fill_payloads(payloads) if table == 'fills' else payloads

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
        payloads = [row.payload for row in rows]
        return await self.effective_fill_payloads(payloads) if table == 'fills' else payloads

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
                    symbol, reference = fill_key(payload["instId"], reference)
                    if payload.get("tradeId") != reference or payload.get("symbol", symbol) != symbol:
                        raise ValueError("ledger fill conflict")
                    existing = await session.scalar(select(ROW_TYPES["fills"].id).where(
                        ROW_TYPES["fills"].symbol == symbol, or_(
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
