"""Durable PAPER operation lease: maintenance excludes start/resume across processes."""
from __future__ import annotations

import asyncio
import math
import re
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from uuid import uuid4

from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from app.storage import MaintenanceLock, Store

NAME = 'paper_accounting_maintenance'
KINDS = {'maintenance', 'start', 'resume'}
LEASE_SECONDS = 900
HEARTBEAT_SECONDS = 15
COORDINATOR_KEY = 7265641


class MaintenanceError(ValueError):
    pass


class MaintenanceBusy(MaintenanceError):
    pass


async def db_time(connection: AsyncConnection) -> float:
    dialect = connection.dialect.name
    query = ("SELECT EXTRACT(EPOCH FROM clock_timestamp())" if dialect == 'postgresql'
             else "SELECT (julianday('now') - 2440587.5) * 86400.0")
    value = float(await connection.scalar(text(query)))
    if not math.isfinite(value):
        raise MaintenanceError('PAPER maintenance clock invalid')
    return value


async def serialize(connection: AsyncConnection) -> None:
    if connection.dialect.name == 'sqlite':
        await connection.execute(text('BEGIN IMMEDIATE'))
    elif connection.dialect.name == 'postgresql':
        await connection.execute(text("SET LOCAL lock_timeout = '5s'"))
        await connection.execute(text("SET LOCAL statement_timeout = '15s'"))
        await connection.execute(text('SELECT pg_advisory_xact_lock(:key)'),
                                 {'key': COORDINATOR_KEY})
    else:
        raise MaintenanceError('PAPER maintenance backend unsupported')


async def read_lock(connection: AsyncConnection) -> tuple[dict | None, float]:
    rows = (await connection.execute(select(MaintenanceLock))).mappings().all()
    now = await db_time(connection)
    if len(rows) > 1:
        raise MaintenanceError('PAPER maintenance state invalid')
    row = dict(rows[0]) if rows else None
    if row is not None:
        if (row['name'] != NAME or row['kind'] not in KINDS
                or not isinstance(row['owner_id'], str)
                or re.fullmatch('[0-9a-f]{32}', row['owner_id']) is None
                or not all(isinstance(row[k], (float, int)) and math.isfinite(row[k])
                           for k in ['acquired_at', 'expires_at'])
                or row['acquired_at'] > now + 1 or row['acquired_at'] <= 0
                or row['expires_at'] <= row['acquired_at']
                or row['expires_at'] > now + LEASE_SECONDS + 1):
            raise MaintenanceError('PAPER maintenance state invalid')
    return row, now


async def require_owner(connection: AsyncConnection, owner: str, kind: str) -> None:
    # Called inside the correction's existing transaction, before any ledger write.
    row, now = await read_lock(connection)
    if row is None or row['owner_id'] != owner or row['kind'] != kind or row['expires_at'] <= now:
        raise MaintenanceError('PAPER maintenance ownership lost')


async def ensure_schema(store: Store) -> None:
    async with store.engine.begin() as connection:
        await serialize(connection)
        await connection.run_sync(lambda conn: MaintenanceLock.metadata.create_all(
            conn, tables=[MaintenanceLock.metadata.tables['maintenance_locks']], checkfirst=True,
        ))
        await read_lock(connection)


async def maintenance_status(store: Store) -> dict:
    await ensure_schema(store)
    async with store.engine.connect() as connection:
        row, now = await read_lock(connection)
    active = row is not None and row['kind'] == 'maintenance' and row['expires_at'] > now
    return {'active': active, 'type': 'paper_accounting_correction' if active else None}


class PaperMaintenanceFence:
    def __init__(self, store: Store, kind: str, on_loss: Callable[[], None] | None = None) -> None:
        if kind not in KINDS:
            raise MaintenanceError('PAPER maintenance operation invalid')
        self.store, self.kind = store, kind
        self.owner = uuid4().hex
        self.on_loss = on_loss
        self.lost = False
        self._heartbeat: asyncio.Task | None = None

    async def acquire(self) -> None:
        await ensure_schema(self.store)
        async with self.store.engine.begin() as connection:
            await serialize(connection)
            row, now = await read_lock(connection)
            if row is not None and row['expires_at'] > now:
                raise MaintenanceBusy('PAPER maintenance/start/resume already in progress')
            values = dict(name=NAME, owner_id=self.owner, kind=self.kind,
                          acquired_at=now, expires_at=now + LEASE_SECONDS)
            if row is None:
                await connection.execute(insert(MaintenanceLock).values(**values))
            else:
                # Only an expired, fully validated lease can be replaced atomically.
                await connection.execute(update(MaintenanceLock).where(
                    MaintenanceLock.name == NAME).values(**values))
        self._heartbeat = asyncio.create_task(self._renew_loop())

    async def renew(self) -> None:
        if self.lost:
            raise MaintenanceError('PAPER maintenance ownership lost')
        async with self.store.engine.begin() as connection:
            await serialize(connection)
            await require_owner(connection, self.owner, self.kind)
            now = await db_time(connection)
            await connection.execute(update(MaintenanceLock).where(
                MaintenanceLock.name == NAME).values(expires_at=now + LEASE_SECONDS))

    async def _renew_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(HEARTBEAT_SECONDS)
                await self.renew()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.lost = True
            if self.on_loss is not None:
                self.on_loss()

    async def release(self) -> None:
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            await asyncio.gather(self._heartbeat, return_exceptions=True)
            self._heartbeat = None
        async with self.store.engine.begin() as connection:
            await serialize(connection)
            # Never remove a newer owner's fence, including after expiry or crash.
            await read_lock(connection)
            await connection.execute(delete(MaintenanceLock).where(
                MaintenanceLock.name == NAME, MaintenanceLock.owner_id == self.owner))

    @asynccontextmanager
    async def hold(self) -> AsyncIterator[PaperMaintenanceFence]:
        await self.acquire()
        try:
            yield self
        finally:
            await self.release()
