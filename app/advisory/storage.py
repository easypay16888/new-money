"""Dedicated research database. No trading Store, migrations, or shared transactions."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, String, inspect, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class ResearchBase(DeclarativeBase):
    pass


class ResearchRecord(ResearchBase):
    __abstract__ = True
    record_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    advisory_id: Mapped[str] = mapped_column(String(64), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


TABLE_NAMES = (
    "external_advisories",
    "external_advisory_snapshots",
    "external_advisory_outcomes",
    "external_advisory_evaluations",
)
RECORDS: dict[str, type[ResearchRecord]] = {
    name: type("Record_" + name, (ResearchRecord,), {"__tablename__": name}) for name in TABLE_NAMES
}


class ResearchStore:
    def __init__(self, url: str) -> None:
        parsed = make_url(url)
        if parsed.drivername not in {"sqlite+aiosqlite", "postgresql+asyncpg"}:
            raise ValueError("unsupported research backend")
        self.engine = create_async_engine(url, hide_parameters=True, pool_pre_ping=True)
        self.healthy = False

    async def initialize(self) -> None:
        self.healthy = False
        async with self.engine.begin() as conn:
            # Refuse every non-research table BEFORE DDL. Never initialize trading storage.
            names = await conn.run_sync(lambda c: inspect(c).get_table_names())
            if set(names) - set(TABLE_NAMES):
                raise ValueError("a dedicated research database is required")
            await conn.run_sync(ResearchBase.metadata.create_all)
            if self.engine.dialect.name == "postgresql":
                await conn.execute(
                    text("""CREATE OR REPLACE FUNCTION advisory_append_only()
                    RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
                    RAISE EXCEPTION 'research records are append only'; END; $$""")
                )
                for table in TABLE_NAMES:
                    await conn.execute(
                        text(f"DROP TRIGGER IF EXISTS immutable_research ON {table}")
                    )
                    await conn.execute(
                        text(f"""CREATE TRIGGER immutable_research
                        BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW
                        EXECUTE FUNCTION advisory_append_only()""")
                    )
            else:
                for table in TABLE_NAMES:
                    for action in ("UPDATE", "DELETE"):
                        await conn.execute(
                            text(f"""CREATE TRIGGER IF NOT EXISTS {table}_{action}
                            BEFORE {action} ON {table} BEGIN
                            SELECT RAISE(ABORT, 'research records are append only'); END""")
                        )
        self.healthy = True

    async def put(
        self,
        table: str,
        record_id: str,
        advisory_id: str,
        payload: dict[str, Any],
        *,
        now: datetime,
    ) -> bool:
        record = RECORDS[table]
        stmt = (
            pg_insert(record) if self.engine.dialect.name == "postgresql" else sqlite_insert(record)
        )
        try:
            async with self.engine.begin() as conn:
                result = await conn.execute(
                    stmt.values(
                        record_id=record_id,
                        advisory_id=advisory_id,
                        created_at=now,
                        payload=payload,
                    )
                    .on_conflict_do_nothing(index_elements=["record_id"])
                    .returning(record.record_id)
                )
                added = result.scalar_one_or_none() is not None
                if not added:
                    original = await conn.scalar(
                        select(record.payload).where(record.record_id == record_id)
                    )
                    if original is None:
                        raise ValueError("research record unavailable")
                    if table == "external_advisories":
                        # First ingestion is retained; a later identical import is not a revision.
                        ignore = {"ingested_at", "late_import", "raw_source_digest"}
                        same = {k: v for k, v in original.items() if k not in ignore} == {
                            k: v for k, v in payload.items() if k not in ignore
                        }
                    else:
                        same = original == payload
                    if not same:
                        raise ValueError("research record conflict")
            self.healthy = True
            return added
        except Exception:
            self.healthy = False
            raise

    async def get(self, table: str, record_id: str) -> dict[str, Any] | None:
        record = RECORDS[table]
        async with self.engine.connect() as conn:
            return await conn.scalar(select(record.payload).where(record.record_id == record_id))

    async def records(self, table: str, *, maximum: int = 10000) -> list[dict[str, Any]]:
        if not 1 <= maximum <= 100000:
            raise ValueError("invalid research record bound")
        record = RECORDS[table]
        async with self.engine.connect() as conn:
            values: list[dict[str, Any]] = list(
                (
                    await conn.scalars(
                        select(record.payload)
                        .order_by(record.created_at, record.record_id)
                        .limit(maximum + 1)
                    )
                ).all()
            )
        if len(values) > maximum:
            raise ValueError("research export bound exceeded; narrow the dataset")
        return values

    async def close(self) -> None:
        await self.engine.dispose()
        self.healthy = False
