from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import random
import re
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import httpx
import websockets

from app.config import Mode, Settings
from app.models import Instrument

logger = logging.getLogger("okx")


class OkxError(RuntimeError):
    def __init__(
        self, message: str, *, code: str = "", data: list[Any] | None = None,
        operation: str = "", endpoint: str = "", http_status: int | None = None,
        retryable: bool = False, error_type: str = "",
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.data = data or []
        self.operation = operation
        self.endpoint = endpoint
        self.http_status = http_status
        self.retryable = retryable
        self.error_type = error_type


class OkxOrderRejected(OkxError):
    pass


RETRYABLE_OKX_CODES = frozenset({"50004", "50011", "50026", "50061"})
PERMANENT_OKX_CODES = frozenset({
    "50102", "50103", "50104", "50105", "50106", "50107",
    "50111", "50112", "50113", "50119",
})
SAFE_ERROR_TYPES = frozenset({
    "ConnectError", "ConnectTimeout", "ReadError", "ReadTimeout", "WriteError",
    "WriteTimeout", "RemoteProtocolError", "PoolTimeout", "InvalidJSON",
    "InvalidResponse", "APIError",
})
RECONCILIATION_OPERATIONS = {
    "/api/v5/account/balance": "account",
    "/api/v5/account/positions": "positions",
    "/api/v5/trade/orders-pending": "pending_orders",
    "/api/v5/trade/orders-algo-pending": "pending_algos",
    "/api/v5/account/config": "account_config",
}


def is_retryable_okx_error(exc: Exception) -> bool:
    return isinstance(exc, OkxError) and exc.retryable


def _safe_okx_code(value: Any) -> str:
    code = str(value) if value is not None else ""
    return code if re.fullmatch(r"[0-9]{1,6}", code) else ""


def safe_reconciliation_diagnostics(
    exc: OkxError,
) -> tuple[str, str, str, int | None, str]:
    endpoint = exc.endpoint if exc.endpoint in RECONCILIATION_OPERATIONS else "unknown"
    operation = RECONCILIATION_OPERATIONS.get(endpoint, "unknown")
    status = exc.http_status
    return (
        operation, endpoint, _safe_okx_code(exc.code) or "unknown",
        status if isinstance(status, int) and 100 <= status <= 599 else None,
        exc.error_type if exc.error_type in SAFE_ERROR_TYPES else "OkxError",
    )


def signature(secret: str, prehash: str) -> str:
    digest = hmac.new(secret.encode(), prehash.encode(), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


class OkxRestClient:
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None) -> None:
        self.settings = settings
        self.client = client or httpx.AsyncClient(
            base_url=settings.okx_rest_url, timeout=settings.request_timeout_seconds
        )
        self.owns_client = client is None
        self.latency_observer: Callable[[str, float], None] | None = None

    async def close(self) -> None:
        if self.owns_client:
            await self.client.aclose()

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        body: Any | None = None,
        private: bool = False,
    ) -> list[Any]:
        method = method.upper()
        query = "?" + urlencode(params) if params else ""
        request_path = path + query
        body_text = json.dumps(body, separators=(",", ":")) if body is not None else ""
        headers = {"Content-Type": "application/json"}
        if self.settings.mode != Mode.LIVE:
            headers["x-simulated-trading"] = "1"
        if private:
            if not self.settings.has_credentials:
                raise OkxError("demo credentials are missing")
            timestamp = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            headers.update(
                {
                    "OK-ACCESS-KEY": self.settings.okx_api_key,
                    "OK-ACCESS-SIGN": signature(
                        self.settings.okx_secret_key, timestamp + method + request_path + body_text
                    ),
                    "OK-ACCESS-TIMESTAMP": timestamp,
                    "OK-ACCESS-PASSPHRASE": self.settings.okx_passphrase,
                }
            )
        operation = RECONCILIATION_OPERATIONS.get(path, method.lower())
        started = time.monotonic()
        try:
            response = await self.client.request(
                method, request_path, content=body_text or None, headers=headers
            )
        except httpx.HTTPError as exc:
            raise OkxError(
                "OKX transport error", operation=operation, endpoint=path, retryable=True,
                error_type=type(exc).__name__,
            ) from exc
        finally:
            if self.latency_observer is not None:
                self.latency_observer(path, time.monotonic() - started)
        try:
            result = response.json()
        except ValueError as exc:
            raise OkxError(
                "OKX invalid response", operation=operation, endpoint=path,
                http_status=response.status_code,
                retryable=response.status_code in {429, 500, 502, 503, 504},
                error_type="InvalidJSON",
            ) from exc
        if not isinstance(result, dict):
            raise OkxError(
                "OKX invalid response", operation=operation, endpoint=path,
                http_status=response.status_code,
                error_type="InvalidResponse",
            )
        code = _safe_okx_code(result.get("code"))
        if not 200 <= response.status_code < 300 or code != "0":
            data = result.get("data")
            raise OkxError(
                "OKX request rejected", code=code,
                data=data if 200 <= response.status_code < 300 and isinstance(data, list) else None,
                operation=operation, endpoint=path, http_status=response.status_code,
                error_type="APIError",
                retryable=code not in PERMANENT_OKX_CODES and (
                    code in RETRYABLE_OKX_CODES
                    or response.status_code in {429, 500, 502, 503, 504}
                ) and response.status_code not in {401, 403},
            )
        return result.get("data", [])

    async def instruments(self) -> dict[str, Instrument]:
        rows = await self.request("GET", "/api/v5/public/instruments", params={"instType": "SWAP"})
        return {
            row["instId"]: Instrument(
                symbol=row["instId"],
                contract_value=Decimal(row["ctVal"]),
                contract_currency=row["ctValCcy"],
                lot_size=Decimal(row["lotSz"]),
                min_size=Decimal(row["minSz"]),
                tick_size=Decimal(row["tickSz"]),
            )
            for row in rows
            if row.get("state") == "live"
        }

    async def account(self) -> list[dict[str, Any]]:
        return await self.request("GET", "/api/v5/account/balance", private=True)

    async def candles(self, symbol: str, timeframe: str, limit: int = 300) -> list[list[str]]:
        data = await self.request(
            "GET",
            "/api/v5/market/candles",
            params={"instId": symbol, "bar": timeframe, "limit": str(limit)},
        )
        return sorted(data, key=lambda row: int(row[0]))

    async def account_config(self) -> list[dict[str, Any]]:
        return await self.request("GET", "/api/v5/account/config", private=True)

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        await self.request(
            "POST",
            "/api/v5/account/set-leverage",
            body={"instId": symbol, "lever": str(leverage), "mgnMode": "isolated"},
            private=True,
        )

    async def positions(self) -> list[dict[str, Any]]:
        return await self.request(
            "GET", "/api/v5/account/positions", params={"instType": "SWAP"}, private=True
        )

    async def pending_orders(self) -> list[dict[str, Any]]:
        return await self.request(
            "GET", "/api/v5/trade/orders-pending", params={"instType": "SWAP"}, private=True
        )

    async def pending_algos(self) -> list[dict[str, Any]]:
        conditional = await self.request(
            "GET",
            "/api/v5/trade/orders-algo-pending",
            params={"ordType": "conditional"},
            private=True,
        )
        oco = await self.request(
            "GET",
            "/api/v5/trade/orders-algo-pending",
            params={"ordType": "oco"},
            private=True,
        )
        return conditional + oco

    async def order(self, symbol: str, client_order_id: str) -> list[dict[str, Any]]:
        try:
            return await self.request(
                "GET",
                "/api/v5/trade/order",
                params={"instId": symbol, "clOrdId": client_order_id},
                private=True,
            )
        except OkxError as exc:
            if exc.code == "51603" and (exc.http_status is None or exc.http_status < 400):
                return []
            raise

    async def place_order(self, body: dict[str, Any]) -> list[dict[str, Any]]:
        if self.settings.mode == Mode.BACKTEST:
            raise OkxError("backtest mode cannot submit orders")
        if self.settings.mode == Mode.LIVE and not self.settings.live_trading_enabled:
            raise OkxError("live trading disabled")
        try:
            data = await self.request("POST", "/api/v5/trade/order", body=body, private=True)
        except OkxError as exc:
            if exc.data and any(item.get("sCode") not in {None, "0"} for item in exc.data):
                raise OkxOrderRejected(
                    str(exc), code=exc.code, data=exc.data, operation=exc.operation,
                    endpoint=exc.endpoint, http_status=exc.http_status,
                    retryable=exc.retryable, error_type=exc.error_type,
                ) from exc
            raise
        if any(item.get("sCode") != "0" for item in data):
            raise OkxOrderRejected(f"OKX order rejected: {[item.get('sCode') for item in data]}")
        return data

    async def place_algo(self, body: dict[str, Any]) -> list[dict[str, Any]]:
        if self.settings.mode == Mode.BACKTEST:
            raise OkxError("backtest mode cannot submit algo orders")
        if self.settings.mode == Mode.LIVE and not self.settings.live_trading_enabled:
            raise OkxError("live trading disabled")
        data = await self.request("POST", "/api/v5/trade/order-algo", body=body, private=True)
        if any(item.get("sCode") != "0" for item in data):
            raise OkxError("OKX protective algo rejected")
        return data

    async def cancel_algo(
        self, symbol: str, *, algo_id: str = "", client_algo_id: str = ""
    ) -> list[dict[str, Any]]:
        if not algo_id and not client_algo_id:
            raise ValueError("algo ID required")
        body = [
            {"instId": symbol, "algoId" if algo_id else "algoClOrdId": algo_id or client_algo_id}
        ]
        data = await self.request("POST", "/api/v5/trade/cancel-algos", body=body, private=True)
        if any(item.get("sCode") != "0" for item in data):
            raise OkxError("OKX algo cancel rejected")
        return data

    async def cancel_order(
        self, symbol: str, *, client_order_id: str = "", order_id: str = ""
    ) -> list[dict[str, Any]]:
        if not client_order_id and not order_id:
            raise ValueError("order ID required")
        body = {"instId": symbol}
        body["clOrdId" if client_order_id else "ordId"] = client_order_id or order_id
        data = await self.request(
            "POST",
            "/api/v5/trade/cancel-order",
            body=body,
            private=True,
        )
        if any(item.get("sCode") != "0" for item in data):
            raise OkxError("OKX order cancel rejected")
        return data

    async def cancel_all_after(self, seconds: int) -> None:
        await self.request(
            "POST", "/api/v5/trade/cancel-all-after", body={"timeOut": str(seconds)}, private=True
        )


MessageHandler = Callable[[dict[str, Any]], Awaitable[None]]


class OkxWebSocket:
    def __init__(
        self,
        url: str,
        subscriptions: list[dict[str, str]],
        handler: MessageHandler,
        settings: Settings,
        *,
        private: bool = False,
    ) -> None:
        self.url = url
        self.subscriptions = subscriptions
        self.handler = handler
        self.settings = settings
        self.private = private
        self.connected = False
        self.last_message_at = 0.0
        self.reconnects = 0
        self.sequences: dict[str, int] = {}

    async def run(self) -> None:
        delay = 1.0
        while True:
            try:
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=10, close_timeout=5
                ) as ws:
                    if self.private:
                        if not self.settings.has_credentials:
                            raise OkxError("private WebSocket credentials missing")
                        timestamp = str(int(time.time()))
                        await ws.send(
                            json.dumps(
                                {
                                    "op": "login",
                                    "args": [
                                        {
                                            "apiKey": self.settings.okx_api_key,
                                            "passphrase": self.settings.okx_passphrase,
                                            "timestamp": timestamp,
                                            "sign": signature(
                                                self.settings.okx_secret_key,
                                                timestamp + "GET/users/self/verify",
                                            ),
                                        }
                                    ],
                                }
                            )
                        )
                        login = json.loads(await ws.recv())
                        if login.get("event") != "login" or login.get("code") != "0":
                            raise OkxError("private WebSocket login failed")
                    await ws.send(json.dumps({"op": "subscribe", "args": self.subscriptions}))
                    self.connected = True
                    self.sequences.clear()
                    self.last_message_at = time.monotonic()
                    delay = 1.0
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=10)
                        except TimeoutError:
                            await ws.send("ping")
                            continue
                        if raw == "pong":
                            self.last_message_at = time.monotonic()
                            continue
                        message = json.loads(raw)
                        self.last_message_at = time.monotonic()
                        if message.get("event") == "error":
                            raise OkxError(f"WebSocket subscription error: {message.get('code')}")
                        if "data" not in message:
                            continue
                        self._check_sequence(message)
                        await self.handler(message)
            except Exception as exc:
                self.connected = False
                self.reconnects += 1
                logger.error("websocket disconnected: %s", type(exc).__name__)
                await asyncio.sleep(delay + random.uniform(0, 0.5))
                delay = min(delay * 2, self.settings.ws_backoff_max_seconds)
            finally:
                self.connected = False

    def _check_sequence(self, message: dict[str, Any]) -> None:
        arg = message.get("arg", {})
        key = f"{arg.get('channel')}:{arg.get('instId')}"
        for row in message.get("data", []):
            if "seqId" not in row:
                continue
            previous = self.sequences.get(key)
            prev_seq = row.get("prevSeqId")
            if previous is not None and prev_seq is not None and int(prev_seq) != previous:
                self.connected = False
                raise OkxError(f"order book sequence gap for {key}")
            self.sequences[key] = int(row["seqId"])

    def is_fresh(self) -> bool:
        return (
            self.connected
            and self.last_message_at > 0
            and time.monotonic() - self.last_message_at < self.settings.stale_timeout_seconds
        )


def public_subscriptions(symbols: tuple[str, ...]) -> list[dict[str, str]]:
    subscriptions = [
        {"channel": channel, "instId": symbol}
        for symbol in symbols
        for channel in (
            "tickers",
            "trades",
            "books5",
            "mark-price",
            "funding-rate",
            "open-interest",
        )
    ]
    subscriptions.extend(
        {"channel": "index-tickers", "instId": symbol.removesuffix("-SWAP")} for symbol in symbols
    )
    return subscriptions


def candle_subscriptions(
    symbols: tuple[str, ...], timeframes: tuple[str, ...]
) -> list[dict[str, str]]:
    return [
        {"channel": "candle" + timeframe, "instId": symbol}
        for symbol in symbols
        for timeframe in timeframes
    ]
