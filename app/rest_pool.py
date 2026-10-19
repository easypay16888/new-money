"""Bounded REST resources; retire a stuck pool without retrying any request."""
from __future__ import annotations

import asyncio
from collections.abc import Callable

import httpx


class RestPool:
    def __init__(self, factory: Callable[[], httpx.AsyncClient]) -> None:
        self.factory = factory
        self.client: httpx.AsyncClient | None = None
        self.active = 0
        self.failed = False
        self.closed = False

    async def request(self, method: str, path: str, **kwargs) -> httpx.Response:
        if self.closed:
            raise RuntimeError("REST pool closed")
        if self.failed:
            # Other requests still own the retired client. Never close their
            # connections or issue a replacement write behind their backs.
            raise httpx.PoolTimeout("REST pool draining")
        if self.client is None:
            self.client = self.factory()
        client = self.client
        self.active += 1
        try:
            return await client.request(method, path, **kwargs)
        except (httpx.TransportError, asyncio.CancelledError):
            # A proxy CONNECT may succeed before TLS fails or is cancelled.
            # httpcore can retain that tunnel as ACTIVE, exhausting capacity on
            # the next call. Retire on the original failure, not a later pool
            # timeout. Propagate unchanged; never replay an ambiguous write.
            self.failed = True
            raise
        finally:
            self.active -= 1
            if self.failed and self.active == 0:
                self.client = None
                # Detach first: future independent calls use a fresh pool only
                # after the old generation has no in-flight requests.
                self.failed = False
                await client.aclose()

    async def close(self) -> None:
        self.closed = True
        if self.client is not None:
            await self.client.aclose()
            self.client = None
