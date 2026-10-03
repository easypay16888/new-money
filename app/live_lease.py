"""Session ownership in a shared PostgreSQL coordinator; never automatically reacquired."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from hashlib import sha256
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool


def lease_key(uid: str) -> int:
    return int.from_bytes(sha256(("new-money-live:" + uid).encode()).digest()[:8],
                          byteorder="big", signed=True)


class LiveLeaseError(RuntimeError):
    pass


class LiveRuntimeLease:
    def __init__(self, url: str, uid: str, on_loss: Callable[[], None]) -> None:
        self._engine = create_async_engine(url, poolclass=NullPool, hide_parameters=True)
        self._key = lease_key(uid)
        self._connection: AsyncConnection | None = None
        self._on_loss = on_loss
        self._probe_lock = asyncio.Lock()
        self._lost = False
        self._closing = False
        self.held = False

    def _mark_lost(self, *_: Any) -> None:
        if self.held and not self._closing:
            self.held = False
            self._lost = True
            self._on_loss()

    async def acquire(self) -> None:
        if self._lost or self._closing or self._connection is not None:
            raise LiveLeaseError("LIVE writer lease cannot be reacquired")
        try:
            async with asyncio.timeout(3):
                self._connection = await self._engine.connect()
                owned = await self._connection.scalar(
                    text("SELECT pg_try_advisory_lock(:key)"), {"key": self._key}
                )
                await self._connection.commit()
                if not owned:
                    raise LiveLeaseError("LIVE writer lease unavailable")
                self.held = True
                raw = await self._connection.get_raw_connection()
                driver = raw.driver_connection
                if driver is None:
                    raise LiveLeaseError("LIVE writer lease session unavailable")
                driver.add_termination_listener(self._mark_lost)
        except BaseException as exc:
            await self.close()
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise LiveLeaseError("LIVE writer lease unavailable") from None

    async def verify(self) -> bool:
        if not self.held or self._lost or self._closing:
            return False
        try:
            async with asyncio.timeout(2):
                async with self._probe_lock:
                    if not self.held or self._closing or self._lost:
                        return False
                    connection = self._connection
                    if connection is None or connection.closed or connection.invalidated:
                        self._mark_lost()
                        return False
                    unsigned = self._key & ((1 << 64) - 1)
                    owned = await connection.scalar(text("""
                        SELECT EXISTS (SELECT 1 FROM pg_locks
                        WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid()
                        AND classid::bigint = :hi AND objid::bigint = :lo AND objsubid = 1)
                    """), {"hi": unsigned >> 32, "lo": unsigned & 0xffffffff})
                    await connection.commit()
                    if not owned:
                        self._mark_lost()
        except Exception:
            self._mark_lost()
        return self.held and not self._lost and not self._closing

    async def close(self) -> None:
        self._closing = True
        self.held = False
        connection, self._connection = self._connection, None
        try:
            async with asyncio.timeout(3):
                async with self._probe_lock:
                    if connection is not None and not connection.closed:
                        try:
                            await connection.execute(text("SELECT pg_advisory_unlock(:key)"),
                                                     {"key": self._key})
                            await connection.commit()
                        except Exception:
                            await connection.invalidate()
                        finally:
                            await connection.close()
        except Exception:
            if connection is not None:
                await connection.invalidate()
        finally:
            await self._engine.dispose()
