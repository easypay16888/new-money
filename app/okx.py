from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import logging
import random
import re
import time
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode, urlsplit

import httpx
import websockets

from app.config import Mode, Settings
from app.models import Instrument
from app.monitoring import Metrics

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
        self._verified_live_identity: bytes | None = None
        self._live_account_anchor: bytes | None = None
        self.live_writer_guard: Callable[[bool], Awaitable[bool]] | None = None

    def _live_identity(self) -> bytes:
        # Invalidate the grant if account, credentials or endpoint changes in memory.
        values = (
            self.settings.confirm_live_account_id, self.settings.okx_api_key,
            self.settings.okx_secret_key, self.settings.okx_passphrase,
            self.settings.okx_rest_url,
        )
        return hashlib.sha256(json.dumps(values).encode()).digest()

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
        if self.settings.mode == Mode.LIVE and self._verified_live_identity != self._live_identity():
            self._verified_live_identity = None
        if self.settings.mode == Mode.LIVE and method != "GET":
            if (
                not self.settings.live_trading_enabled
                or not self.settings.confirm_live_account_id
                or not self.settings.has_credentials
                or self._verified_live_identity != self._live_identity()
            ):
                raise OkxError("LIVE write blocked: account identity not verified")
            # Three classes: entry/configuration, emergency/protection, account-global CAA.
            # Identity is mandatory for all. Only emergency/protection can survive lease loss.
            account_global = path == "/api/v5/trade/cancel-all-after"
            if account_global:
                timeout = body.get("timeOut") if isinstance(body, dict) else None
                if (
                    method != "POST" or type(timeout) not in {str, int}
                    or re.fullmatch(r"0|[1-9][0-9]*", str(timeout)) is None
                ):
                    raise OkxError("LIVE CAA write blocked: invalid timeout")
            safe = method == "POST" and (
                path in {
                    "/api/v5/trade/cancel-order", "/api/v5/trade/cancel-algos",
                } or (
                    path in {"/api/v5/trade/order", "/api/v5/trade/order-algo"}
                    and isinstance(body, dict)
                    and (body.get("reduceOnly") is True or body.get("reduceOnly") == "true")
                )
            )
            if not safe:
                entry = path not in {
                    "/api/v5/account/set-leverage", "/api/v5/trade/cancel-all-after",
                }
                try:
                    allowed = self.live_writer_guard is not None and await self.live_writer_guard(entry)
                except Exception:
                    allowed = False
                # Recheck identity after the asynchronous lease probe.
                if not allowed or self._verified_live_identity != self._live_identity():
                    raise OkxError("LIVE write blocked: writer ownership or entry permission missing")
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
        if response.status_code in {401, 403}:
            self._verified_live_identity = None
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
                    # Busy responses are transient only for known reconciliation reads.
                    # Do not expand recovery classification for any trading write.
                    or (
                        code == "50013" and method == "GET"
                        and path in RECONCILIATION_OPERATIONS
                    )
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
        identity = self._live_identity()
        try:
            rows = await self.request("GET", "/api/v5/account/config", private=True)
        except Exception as exc:
            # A transient read failure must not disable emergency writes to an
            # already verified account with unchanged credentials. Runtime still HALTs entries.
            if not is_retryable_okx_error(exc):
                self._verified_live_identity = None
            raise
        if self.settings.mode == Mode.LIVE:
            self._verified_live_identity = None
            if len(rows) != 1 or not isinstance(rows[0], dict):
                raise OkxError("LIVE account configuration invalid")
            config = rows[0]
            if config.get("uid") != self.settings.confirm_live_account_id:
                raise OkxError("LIVE account identity mismatch")
            account_anchor = hashlib.sha256(self.settings.confirm_live_account_id.encode()).digest()
            if self._live_account_anchor is not None and self._live_account_anchor != account_anchor:
                raise OkxError("LIVE account identity changed; runtime restart required")
            if config.get("posMode") != "net_mode" or config.get("acctLv") not in {"2", "3", "4"}:
                raise OkxError("LIVE derivatives account mode required")
            permissions = {item.strip() for item in str(config.get("perm") or "").split(",")}
            if permissions != {"read_only", "trade"}:
                raise OkxError("LIVE requires read and trade permissions without withdrawal")
            try:
                addresses = str(config.get("ip") or "").split(",")
                for address in addresses:
                    ipaddress.ip_address(address.strip())
            except ValueError:
                raise OkxError("LIVE requires API key IP binding") from None
            if identity != self._live_identity():
                raise OkxError("LIVE configuration changed during identity verification")
            self._live_account_anchor = account_anchor
            self._verified_live_identity = identity
        return rows

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

    async def fills(
        self, symbol: str | None = None, *, after: str = "", before: str = "", limit: int = 100,
    ) -> list[dict[str, Any]]:
        return await self._fills_page("fills", symbol, after=after, before=before, limit=limit)

    async def fills_history(
        self, symbol: str | None = None, *, after: str = "", before: str = "", limit: int = 100,
        begin: int | None = None, end: int | None = None, order_id: str = "",
    ) -> list[dict[str, Any]]:
        return await self._fills_page(
            "fills-history", symbol, after=after, before=before, limit=limit, begin=begin, end=end,
            order_id=order_id,
        )

    async def _fills_page(
        self, endpoint: str, symbol: str | None, *, after: str, before: str, limit: int,
        begin: int | None = None, end: int | None = None, order_id: str = "",
    ) -> list[dict[str, Any]]:
        if not 1 <= limit <= 100:
            raise ValueError("invalid fill page limit")
        params = {"instType": "SWAP", "limit": str(limit)}
        for key, value in (("instId", symbol), ("ordId", order_id), ("after", after), ("before", before)):
            if value:
                params[key] = value
        for key, timestamp in (("begin", begin), ("end", end)):
            if timestamp is not None:
                params[key] = str(timestamp)
        return await self.request("GET", "/api/v5/trade/" + endpoint, params=params, private=True)

    async def fills_history_window(
        self, symbol: str, *, history_start_ms: int, history_end_ms: int,
        max_pages: int = 50, max_records: int = 5000, page_size: int = 100,
    ) -> list[dict[str, Any]]:
        # OKX paginates by billId, NOT tradeId. begin/end filter the record timestamp ts.
        if not (
            symbol and 0 < history_start_ms <= history_end_ms
            and 1 <= max_pages <= 100 and 1 <= max_records <= 10000
            and 1 <= page_size <= 100
        ):
            raise OkxError("ledger repair evidence incomplete")
        records: list[dict[str, Any]] = []
        cursor = ""
        cursors: set[str] = set()
        for _ in range(max_pages):
            page = await self.fills_history(
                symbol, after=cursor, limit=page_size, begin=history_start_ms, end=history_end_ms,
            )
            if len(page) > page_size or len(records) + len(page) > max_records:
                raise OkxError("ledger repair evidence incomplete")
            records.extend(page)
            if len(page) < page_size:
                return records
            cursor = str(page[-1].get("billId") or "")
            if not cursor or cursor in cursors:
                raise OkxError("ledger repair evidence incomplete")
            cursors.add(cursor)
        raise OkxError("ledger repair evidence incomplete")

    async def order_by_id(self, symbol: str, order_id: str) -> list[dict[str, Any]]:
        return await self.request(
            "GET", "/api/v5/trade/order", params={"instId": symbol, "ordId": order_id}, private=True,
        )

    async def algo_order(self, client_algo_id: str) -> list[dict[str, Any]]:
        return await self.request(
            "GET", "/api/v5/trade/order-algo", params={"algoClOrdId": client_algo_id}, private=True,
        )

    def live_identity_verified(self) -> bool:
        return self._verified_live_identity == self._live_identity()

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
BatchMessageHandler = Callable[[list[dict[str, Any]]], Awaitable[None]]


class WebSocketFault(OkxError):
    """Only fixed, non-secret diagnostics cross the transport boundary."""

    def __init__(self, reason: str, *, kind: str = "transport") -> None:
        super().__init__(reason)
        self.kind = kind


class OkxWebSocket:
    CRITICAL_CHANNELS = frozenset({"books5", "tickers", "mark-price", "index-tickers"})

    def __init__(
        self, url: str, subscriptions: list[dict[str, str]], handler: MessageHandler,
        settings: Settings, *, private: bool = False, name: str = "websocket",
        metrics: Metrics | None = None,
        on_fault: Callable[[OkxWebSocket, str], None] | None = None,
        on_ready: Callable[[OkxWebSocket], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
        batch_handler: BatchMessageHandler | None = None,
    ) -> None:
        self.url = url
        self.name = name
        self.subscriptions = subscriptions
        self.handler = handler
        if batch_handler is not None and (private or any(
            arg.get("channel") not in {
                "tickers", "trades", "books5", "mark-price", "index-tickers",
                "funding-rate", "open-interest",
            } for arg in subscriptions
        )):
            raise ValueError("batch handler only supports public market subscriptions")
        self.batch_handler = batch_handler
        self.settings = settings
        self.private = private
        self.metrics = metrics
        self.on_fault = on_fault
        self.on_ready = on_ready
        self.clock = clock
        self.connected = False
        self.login_ok = not private
        self.subscribed: set[str] = set()
        self.last_rx_at = 0.0
        self.last_ping_at = 0.0
        self.last_pong_at = 0.0
        self.ping_pending = False
        self.ping_rtt: float | None = None
        self.last_data_at: dict[str, float] = {}
        self.reconnects = 0
        self.maintenance_reconnects = 0
        self.generation = 0
        self.sequences: dict[str, int] = {}
        self.queue: asyncio.Queue[tuple[dict[str, Any], float]] = asyncio.Queue(
            maxsize=settings.ws_queue_maxsize
        )
        self.processing_unsafe = False
        self.reconciliation_required = private
        self.disconnect_reason = "connection not established"
        self.disconnect_kind = "transport"
        self.last_close_code: int | None = None
        self.last_close_reason = ""
        self._connected_at = 0.0
        self._queued_at: deque[float] = deque()
        self._handler_started_at: float | None = None
        self._stale_keys: set[str] = set()
        self._phase = "DISCONNECTED"
        self.login_started_at: float | None = None
        self.login_completed_at: float | None = None
        self.subscribe_sent_at: float | None = None
        self.subscriptions_completed_at: float | None = None
        self.last_connect_attempt_at: float | None = None
        self.last_reconnect_progress_at: float | None = None
        self.last_disconnect_at: float | None = None
        self.current_backoff = 0.0
        self._next_retry_at: float | None = None
        self.consecutive_failures = 0
        self.reason_code = "ws_connection_unavailable"
        self.last_failure_reason_code: str | None = None
        self.last_disconnect_phase: str | None = None
        self.last_close_side: str | None = None
        self._run_task: asyncio.Task[Any] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self.worker_exception_type: str | None = None
        self._stopping = False
        self._queue_metric()

    @staticmethod
    def feed_key(arg: dict[str, Any]) -> str:
        return f"{arg.get('channel', '')}:{arg.get('instId', '')}"

    @property
    def last_message_at(self) -> float:
        # Compatibility for diagnostics; never used as market-data freshness.
        return self.last_rx_at

    @last_message_at.setter
    def last_message_at(self, value: float) -> None:
        self.last_rx_at = value

    def _reset_transport(self) -> None:
        self.generation += 1
        self.connected = True
        self.login_ok = not self.private
        self.subscribed.clear()
        self.sequences.clear()
        self.last_data_at.clear()
        self.last_rx_at = self.clock()
        self._connected_at = self.last_rx_at
        self.last_reconnect_progress_at = self.clock()
        self.last_ping_at = self.last_pong_at = 0.0
        self.ping_pending = False
        self.ping_rtt = None
        self._stale_keys.clear()
        self.login_started_at = self.login_completed_at = None
        self.subscribe_sent_at = self.subscriptions_completed_at = None
        self._phase = "AUTHENTICATING" if self.private else "SUBSCRIBING"
        self.reconciliation_required = self.private
        if not self.private:
            self.processing_unsafe = False

    def _fault(self, reason: str, *, kind: str = "transport") -> None:
        self.disconnect_reason, self.disconnect_kind = reason, kind
        self.reason_code = {
            "connection closed": "ws_connection_closed",
            "heartbeat timeout": "ws_heartbeat_timeout",
            "login failed": "ws_login_failed",
            "login credentials missing": "ws_login_failed",
            "login timeout": "ws_login_timeout",
            "subscription rejected": "ws_subscription_rejected",
            "subscription acknowledgement timeout": "ws_subscription_timeout",
            "server maintenance notice 64008": "ws_server_maintenance",
            "processing queue full": "ws_processing_backlog",
            "business worker terminated": "ws_worker_failed",
            "business handler failed": "ws_worker_failed",
            "connection attempt timeout": "ws_connect_timeout",
        }.get(reason, "ws_protocol_error" if kind == "protocol" else "ws_connection_error")
        self.last_failure_reason_code = self.reason_code
        if self.private:
            self.reconciliation_required = True
        if kind == "backlog":
            self.processing_unsafe = True
        if self.on_fault is not None:
            self.on_fault(self, {"backlog": "WebSocket processing backlog",
                                "protocol": "WebSocket protocol error",
                                "handler": "WebSocket processing failed"}.get(
                                    kind, "WebSocket transport unavailable"))

    @property
    def phase(self) -> str:
        if self.is_transport_healthy():
            return "RECONCILING" if self.reconciliation_required else "ACTIVE"
        return self._phase

    @property
    def worker_alive(self) -> bool:
        return self._worker_task is not None and not self._worker_task.done()

    def _worker_done(self, task: asyncio.Task[None]) -> None:
        if self._stopping:
            return
        error = None if task.cancelled() else task.exception()
        self.worker_exception_type = type(error).__name__ if error else (
            "CancelledError" if task.cancelled() else "UnexpectedWorkerExit"
        )
        self.processing_unsafe = True
        self._fault("business worker terminated", kind="handler")

    def _ensure_worker(self) -> None:
        if not self.worker_alive:
            self._worker_task = asyncio.create_task(
                self._business_worker(), name=f"{self.name}-handler"
            )
            self._worker_task.add_done_callback(self._worker_done)

    def reconnect_stalled(self) -> bool:
        if not self.private or self.is_transport_healthy() or self._stopping:
            return False
        now = self.clock()
        tolerance = 1.0  # Scheduling tolerance, not a heartbeat/handshake extension.
        if self._next_retry_at is not None and now <= self._next_retry_at + tolerance:
            return False
        deadline = None
        if self._phase == "CONNECTING" and self.last_connect_attempt_at is not None:
            deadline = self.last_connect_attempt_at + self.settings.ws_connect_timeout_seconds
        elif self._phase == "AUTHENTICATING" and self.login_started_at is not None:
            deadline = self.login_started_at + self.settings.request_timeout_seconds
        elif self._phase == "SUBSCRIBING" and self.subscribe_sent_at is not None:
            deadline = self.subscribe_sent_at + self.settings.request_timeout_seconds
        if deadline is not None and now <= deadline + tolerance:
            return False
        anchor = self.last_reconnect_progress_at
        return bool(anchor is not None and now - anchor > (
            2 * self.settings.ws_backoff_max_seconds + self.settings.ws_connect_timeout_seconds
        ))

    def endpoint_log(self) -> None:
        try:
            endpoint = urlsplit(self.url)
            port = endpoint.port or (443 if endpoint.scheme == "wss" else 80)
        except ValueError:
            # Parsing errors can include the invalid port or URL. Leave rejection
            # to the bounded connection loop; never format that exception here.
            logger.warning("invalid WebSocket endpoint configured", extra={"socket_name": self.name})
            return
        # Never emit userinfo, query strings, or arbitrary path components.
        path = endpoint.path.rsplit("/", 1)[-1]
        path = path if path in {"public", "private", "business"} else "custom"
        logger.info("websocket endpoint", extra={"socket_name": self.name,
                    "endpoint_host": self._safe_close_reason(endpoint.hostname or "unavailable"),
                    "endpoint_port": port, "endpoint_path": path})
        if port == 8443:
            logger.warning("legacy OKX WebSocket port configured", extra={"socket_name": self.name,
                           "endpoint_port": port})

    async def _handshake(self, ws: Any) -> None:
        if self.private:
            self._phase = "AUTHENTICATING"
            self.login_started_at = self.clock()
            if not self.settings.has_credentials:
                raise WebSocketFault("login credentials missing", kind="protocol")
            timestamp = str(int(time.time()))
            try:
                async with asyncio.timeout(self.settings.request_timeout_seconds):
                    await ws.send(json.dumps({"op": "login", "args": [{
                        "apiKey": self.settings.okx_api_key,
                        "passphrase": self.settings.okx_passphrase,
                        "timestamp": timestamp,
                        "sign": signature(self.settings.okx_secret_key,
                                          timestamp + "GET/users/self/verify"),
                    }]}))
                    while True:
                        raw = await ws.recv()
                        self.last_rx_at = self.clock()
                        if raw == "pong":
                            self.last_pong_at = self.last_rx_at
                            continue
                        login = json.loads(raw)
                        if isinstance(login, dict) and login.get("event") == "notice":
                            if str(login.get("code")) == "64008":
                                raise WebSocketFault("server maintenance notice 64008", kind="maintenance")
                            raise WebSocketFault("unknown server notice", kind="protocol")
                        break
            except TimeoutError:
                raise WebSocketFault("login timeout") from None
            if self.clock() - self.login_started_at >= self.settings.request_timeout_seconds:
                raise WebSocketFault("login timeout")
            if not isinstance(login, dict) or login.get("event") != "login" or login.get("code") != "0":
                raise WebSocketFault("login failed", kind="protocol")
            self.login_ok = True
            self.last_rx_at = self.login_completed_at = self.clock()
            self.last_reconnect_progress_at = self.clock()
        self._phase = "SUBSCRIBING"
        # A full independent budget begins here, after successful authentication.
        self.subscribe_sent_at = self.clock()
        self.last_reconnect_progress_at = self.clock()
        try:
            async with asyncio.timeout(self.settings.request_timeout_seconds):
                await ws.send(json.dumps({"op": "subscribe", "args": self.subscriptions}))
        except TimeoutError:
            raise WebSocketFault("subscription acknowledgement timeout") from None

    async def _sleep_backoff(self) -> None:
        await asyncio.sleep(self.current_backoff)

    async def run(self) -> None:
        self._run_task = asyncio.current_task()
        self._stopping = False
        self.endpoint_log()
        self._ensure_worker()
        delay = min(1.0, self.settings.ws_backoff_max_seconds)
        try:
            while True:
                self._ensure_worker()
                self._phase = "CONNECTING"
                self._connected_at = 0.0
                self.subscriptions_completed_at = None
                self.login_ok = not self.private
                self.subscribed.clear()
                self.current_backoff = 0.0
                self.last_connect_attempt_at = self.clock()
                self.last_reconnect_progress_at = self.clock()
                self._next_retry_at = None
                if self.metrics is not None:
                    self.metrics.ws_connect_attempts.labels(socket=self.name).inc()
                try:
                    if not self.private:
                        try:
                            async with asyncio.timeout(self.settings.stale_timeout_seconds):
                                await self.queue.join()
                        except TimeoutError:
                            raise WebSocketFault("processing queue full", kind="backlog") from None
                    # Keep accepted events in their ordered worker across sessions.
                    # Private reconnect never waits on a handler; full reconciliation cannot
                    # clear its gate until that worker is idle in the same generation.
                    async with websockets.connect(
                        self.url, open_timeout=self.settings.ws_connect_timeout_seconds,
                        ping_interval=20, ping_timeout=10, close_timeout=5,
                        max_queue=16, max_size=2**20,
                    ) as ws:
                        self._reset_transport()
                        await self._handshake(ws)
                        await self._receive(ws)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.last_disconnect_phase = self.phase
                    self.last_disconnect_at = self.clock()
                    self.last_reconnect_progress_at = self.clock()
                    self.connected = False
                    self.reconnects += 1
                    reason = exc.message if isinstance(exc, WebSocketFault) else "connection error"
                    self.last_close_code, self.last_close_reason, self.last_close_side = None, "", None
                    if isinstance(exc, websockets.exceptions.ConnectionClosed):
                        reason = "connection closed"
                        self.last_close_code = exc.rcvd.code if exc.rcvd else None
                        self.last_close_reason = self._safe_close_reason(exc.rcvd.reason if exc.rcvd else "")
                        self.last_close_side = (
                            "server" if exc.rcvd_then_sent is True else "client"
                            if exc.rcvd_then_sent is False else "unavailable"
                        )
                    elif isinstance(exc, TimeoutError) and self._phase == "CONNECTING":
                        reason = "connection attempt timeout"
                    kind = (exc.kind if isinstance(exc, WebSocketFault) else "transport"
                            if isinstance(exc, (OSError, TimeoutError, websockets.exceptions.ConnectionClosed))
                            else "protocol")
                    if kind == "protocol" and not isinstance(exc, WebSocketFault):
                        reason = "invalid protocol or configuration"
                    if kind == "maintenance":
                        self.maintenance_reconnects += 1
                    if self.subscriptions_completed_at is not None and self.clock() - self.subscriptions_completed_at >= 30:
                        delay = min(1.0, self.settings.ws_backoff_max_seconds)
                        self.consecutive_failures = 0
                    self.consecutive_failures += 1
                    self.current_backoff = min(
                        delay + random.uniform(0, 0.5), self.settings.ws_backoff_max_seconds
                    )
                    self._next_retry_at = self.clock() + self.current_backoff
                    self.last_reconnect_progress_at = self.clock()
                    self._fault(reason, kind=kind)
                    if self.metrics is not None:
                        self.metrics.ws_transport_disconnects.labels(socket=self.name).inc()
                        self.metrics.ws_connect_failures.labels(socket=self.name, reason_class=self.reason_code).inc()
                        if self._connected_at > 0:
                            self.metrics.ws_session_duration.labels(socket=self.name).observe(
                                max(0, self.clock() - self._connected_at)
                            )
                    logger.log(logging.INFO if kind == "maintenance" else logging.ERROR,
                               "websocket session ended", extra={
                        "socket_name": self.name, "exception_type": type(exc).__name__,
                        "close_code": self.last_close_code, "close_reason": self.last_close_reason,
                        "close_side": self.last_close_side, "phase": self.last_disconnect_phase,
                        "ws_reason": reason, "reason_class": self.reason_code,
                        "session_age": self._age(self._connected_at),
                        "last_rx_age": self._age(self.last_rx_at),
                        "last_pong_age": self._age(self.last_pong_at), "ping_pending": self.ping_pending,
                        "login_ok": self.login_ok, "subscriptions_acked": len(self.subscribed),
                        "reconciliation_required": self.reconciliation_required,
                        "reconnect_count": self.reconnects, "queue_depth": self.queue.qsize(),
                        "backoff_seconds": self.current_backoff,
                    })
                    self._phase = "BACKOFF"
                    await self._sleep_backoff()
                    delay = min(delay * 2, self.settings.ws_backoff_max_seconds)
                finally:
                    self.connected = False
        finally:
            self._stopping = True
            self.connected = False
            self._phase = "DISCONNECTED"
            if self._worker_task is not None:
                self._worker_task.cancel()
                await asyncio.gather(self._worker_task, return_exceptions=True)

    async def _receive(self, ws: Any) -> None:
        wanted = {self.feed_key(arg) for arg in self.subscriptions}
        # OKX may push the initial snapshot before its subscribe acknowledgement.
        # Buffer requested feeds until every acknowledgement arrives; neither
        # transport readiness nor applied-data freshness is granted early.
        awaiting_ack: list[tuple[dict[str, Any], float]] = []
        ready_announced = False
        subscribe_started = self.subscribe_sent_at if self.subscribe_sent_at is not None else self._connected_at
        while True:
            if self._worker_task is not None and self._worker_task.done():
                raise WebSocketFault("business worker terminated", kind="handler")
            now = self.clock()
            if self.ping_pending:
                timeout = self.settings.ws_pong_timeout_seconds - (now - self.last_ping_at)
                if timeout <= 0:
                    raise WebSocketFault("heartbeat timeout")
            else:
                timeout = self.settings.ws_idle_ping_seconds - (now - self.last_rx_at)
                if timeout <= 0:
                    await ws.send("ping")
                    self.last_ping_at = self.clock()
                    self.ping_pending = True
                    continue
            if not wanted <= self.subscribed and now - subscribe_started > self.settings.request_timeout_seconds:
                raise WebSocketFault("subscription acknowledgement timeout")
            timeout = min(timeout, max(0.001, self.settings.request_timeout_seconds -
                          (now - subscribe_started))) if not wanted <= self.subscribed else timeout
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except TimeoutError:
                continue
            received_at = self.clock()
            # A frame returned after a scheduler delay doesn't extend either
            # deadline, even if wait_for raced with task completion.
            if self.ping_pending and received_at - self.last_ping_at >= self.settings.ws_pong_timeout_seconds:
                raise WebSocketFault("heartbeat timeout")
            if not wanted <= self.subscribed and received_at - subscribe_started >= self.settings.request_timeout_seconds:
                raise WebSocketFault("subscription acknowledgement timeout")
            self.last_rx_at = received_at
            if raw == "pong":
                self.last_pong_at = self.last_rx_at
                if self.ping_pending:
                    self.ping_rtt = max(0, self.last_pong_at - self.last_ping_at)
                    if self.metrics is not None:
                        self.metrics.ws_ping_rtt.labels(socket=self.name).observe(self.ping_rtt)
                self.ping_pending = False
                continue
            message = json.loads(raw)
            if not isinstance(message, dict):
                raise WebSocketFault("invalid frame", kind="protocol")
            event = message.get("event")
            if event == "notice":
                code = str(message.get("code", ""))
                if code == "64008":
                    raise WebSocketFault("server maintenance notice 64008", kind="maintenance")
                # Unknown notices are never guessed to be harmless.
                raise WebSocketFault("unknown server notice", kind="protocol")
            if event in {"error", "channel-conn-count-error", "unsubscribe"}:
                raise WebSocketFault("subscription rejected", kind="protocol")
            if event == "subscribe":
                key = self.feed_key(message.get("arg", {}))
                arg = message.get("arg", {})
                exact = any(all(arg.get(k) == v for k, v in requested.items())
                            for requested in self.subscriptions)
                if key not in wanted or not exact or str(message.get("code", "0")) != "0":
                    raise WebSocketFault("invalid subscription acknowledgement", kind="protocol")
                self.subscribed.add(key)
            if not ready_announced and wanted <= self.subscribed:
                for buffered, arrival in awaiting_ack:
                    self._enqueue_message(buffered, arrival)
                awaiting_ack.clear()
                ready_announced = True
                self.subscriptions_completed_at = self.clock()
                self.last_reconnect_progress_at = self.clock()
                self.reason_code = "healthy"
                self._phase = "RECONCILING" if self.private else "ACTIVE"
                if self.on_ready is not None:
                    self.on_ready(self)
            if "data" not in message:
                continue
            key = self.feed_key(message.get("arg", {}))
            if key not in wanted:
                raise WebSocketFault("unacknowledged subscription data", kind="protocol")
            self._check_sequence(message)
            if not ready_announced:
                if len(awaiting_ack) + self.queue.qsize() >= self.queue.maxsize:
                    self._fault("processing queue full", kind="backlog")
                    raise WebSocketFault("processing queue full", kind="backlog")
                awaiting_ack.append((message, received_at))
            else:
                self._enqueue_message(message, received_at)

    def _enqueue_message(self, message: dict[str, Any], received_at: float) -> None:
        try:
            self.queue.put_nowait((message, received_at))
            self._queued_at.append(received_at)
            self._queue_metric()
        except asyncio.QueueFull:
            # Reject the frame and require reconciliation, never silently drop it.
            self._fault("processing queue full", kind="backlog")
            raise WebSocketFault("processing queue full", kind="backlog") from None

    async def _business_worker(self) -> None:
        while True:
            message, received_at = await self.queue.get()
            batch = [(message, received_at)]
            # Every successful get is covered, including metrics/batch failures.
            try:
                if self.batch_handler is not None:
                    while len(batch) < 64 and not self.queue.empty():
                        batch.append(self.queue.get_nowait())
                for _ in batch:
                    if self._queued_at:
                        self._queued_at.popleft()
                self._queue_metric()
                self._handler_started_at = self.clock()
                if self.metrics is not None:
                    self.metrics.ws_queue_wait.labels(socket=self.name).observe(
                        max(0, self.clock() - received_at)
                    )
                if self.batch_handler is None:
                    await self.handler(message)
                else:
                    await self.batch_handler([item for item, _ in batch])
                for item, arrival in batch:
                    if item.get("data"):
                        self.last_data_at[self.feed_key(item.get("arg", {}))] = arrival
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.processing_unsafe = True
                self._fault("business handler failed", kind="handler")
                logger.error("websocket handler failed", extra={
                    "socket_name": self.name, "exception_type": type(exc).__name__,
                    "queue_depth": self.queue.qsize(),
                })
            finally:
                # Complete the queue accounting even if an observer fails.
                started_at = self._handler_started_at
                self._handler_started_at = None
                for _ in batch:
                    self.queue.task_done()
                if self.metrics is not None:
                    self.metrics.ws_handler_latency.labels(socket=self.name).observe(
                        max(0, self.clock() - started_at) if started_at is not None else 0
                    )

    def _queue_metric(self) -> None:
        if self.metrics is not None:
            self.metrics.ws_queue_depth.labels(socket=self.name).set(self.queue.qsize())

    def _safe_close_reason(self, value: str) -> str:
        for secret in (self.settings.okx_api_key, self.settings.okx_secret_key,
                       self.settings.okx_passphrase, self.settings.bark_device_key.get_secret_value(),
                       self.settings.api_token, self.settings.status_api_token,
                       self.settings.confirm_live_account_id):
            if secret:
                value = value.replace(secret, "[redacted]")
        value = re.sub(r"[\x00-\x1f\x7f]", " ", value)
        value = re.sub(r"(?:https?|wss?)://\S+", "[endpoint redacted]", value)
        return re.sub(r"(?i)(?:token|key|secret|passphrase|authorization)\s*[:=]\s*\S+",
                      "[redacted]", value)[:200]

    def _check_sequence(self, message: dict[str, Any]) -> None:
        arg = message.get("arg", {})
        key = self.feed_key(arg)
        for row in message.get("data", []):
            if not isinstance(row, dict) or "seqId" not in row:
                continue
            previous = self.sequences.get(key)
            prev_seq = row.get("prevSeqId")
            # A new incremental session must start with a snapshot. Existing
            # sequence continuity is never bypassed by an unexpected snapshot.
            snapshot = message.get("action") == "snapshot"
            if previous is not None and prev_seq is not None and int(prev_seq) != previous:
                self.connected = False
                logger.error("websocket sequence gap", extra={
                    "socket_name": self.name, "channel": arg.get("channel"),
                    "symbol": arg.get("instId"), "expected": previous, "actual": int(prev_seq),
                })
                raise WebSocketFault("sequence gap")
            if previous is None and arg.get("channel") in {"books", "books-l2-tbt", "books50-l2-tbt"} and not snapshot:
                raise WebSocketFault("incremental book requires snapshot")
            self.sequences[key] = int(row["seqId"])

    def _age(self, timestamp: float) -> float | None:
        return max(0, self.clock() - timestamp) if timestamp > 0 else None

    def is_transport_healthy(self) -> bool:
        return bool(
            self.connected and self.login_ok
            and {self.feed_key(arg) for arg in self.subscriptions} <= self.subscribed
            and self.last_rx_at > 0
            and self.clock() - self.last_rx_at < self.settings.ws_idle_ping_seconds + self.settings.ws_pong_timeout_seconds
            and (not self.ping_pending or
                 self.clock() - self.last_ping_at < self.settings.ws_pong_timeout_seconds)
        )

    @property
    def business_idle(self) -> bool:
        return self.queue.empty() and self._handler_started_at is None

    def is_processing_healthy(self) -> bool:
        starts = [t for t in (self._queued_at[0] if self._queued_at else None,
                               self._handler_started_at) if t is not None]
        oldest = min(starts) if starts else None
        return (not self.processing_unsafe
                and (self._run_task is None or self.worker_alive)
                and (self._run_task is None or not self._run_task.done())
                and (
            oldest is None or self.clock() - oldest < self.settings.stale_timeout_seconds
        ))

    def stale_feeds(self) -> list[dict[str, Any]]:
        if self.private:
            return []
        result = []
        for arg in self.subscriptions:
            channel = arg["channel"]
            threshold = self.settings.stale_timeout_seconds
            if channel.startswith("candle"):
                match = re.fullmatch(r"candle(\d+)([mHD])", channel)
                if not match:
                    continue
                unit = {"m": 60, "H": 3600, "D": 86400}[match[2]]
                threshold = int(match[1]) * unit * 2 + threshold
            elif channel not in self.CRITICAL_CHANNELS:
                continue
            key = self.feed_key(arg)
            age = self._age(self.last_data_at.get(key, 0))
            if age is None or age >= threshold:
                result.append({"feed": channel, "symbol": arg.get("instId", ""),
                               "age_seconds": age, "threshold_seconds": threshold})
        return result

    def is_data_fresh(self) -> bool:
        stale = self.stale_feeds()
        keys = {f"{row['feed']}:{row['symbol']}" for row in stale}
        if self.metrics is not None and keys - self._stale_keys:
            self.metrics.ws_market_stale_events.labels(socket=self.name).inc(len(keys - self._stale_keys))
        self._stale_keys = keys
        return not stale

    def is_fresh(self) -> bool:
        """Compatibility alias for the composite safety gate, not business activity."""
        return (self.is_transport_healthy() and self.is_data_fresh()
                and self.is_processing_healthy() and not self.reconciliation_required)

    def status(self) -> dict[str, Any]:
        if (self.is_transport_healthy() and self.subscriptions_completed_at is not None
                and self.clock() - self.subscriptions_completed_at >= 30):
            self.consecutive_failures = 0
        attempt_age = (max(0, self.clock() - self.last_connect_attempt_at)
                       if self.last_connect_attempt_at is not None else None)
        if self.metrics is not None:
            self.metrics.ws_consecutive_failures.labels(socket=self.name).set(self.consecutive_failures)
            self.metrics.ws_reconnect_backoff.labels(socket=self.name).set(self.current_backoff)
            self.metrics.ws_attempt_age.labels(socket=self.name).set(attempt_age or 0)
            self.metrics.ws_worker_alive.labels(socket=self.name).set(self.worker_alive)
        return {
            "name": self.name, "connected": self.connected, "phase": self.phase,
            "generation": self.generation,
            "time_basis": "monotonic_seconds",
            "login_ok": self.login_ok,
            "subscriptions_ok": {self.feed_key(arg) for arg in self.subscriptions} <= self.subscribed,
            "subscriptions_acked": len(self.subscribed),
            "login_started_at": self.login_started_at, "login_completed_at": self.login_completed_at,
            "subscribe_sent_at": self.subscribe_sent_at,
            "subscriptions_completed_at": self.subscriptions_completed_at,
            "last_disconnect_at": self.last_disconnect_at,
            "last_connect_attempt_at": self.last_connect_attempt_at,
            "last_reconnect_progress_at": self.last_reconnect_progress_at,
            "last_reconnect_progress_age_seconds": (
                max(0, self.clock() - self.last_reconnect_progress_at)
                if self.last_reconnect_progress_at is not None else None
            ),
            "last_connect_attempt_age_seconds": attempt_age,
            "next_retry_in": max(0, self._next_retry_at - self.clock()) if self._next_retry_at is not None else None,
            "current_backoff": self.current_backoff,
            "consecutive_failures": self.consecutive_failures,
            "run_task_alive": self._run_task is not None and not self._run_task.done(),
            "worker_task_alive": self.worker_alive, "worker_alive": self.worker_alive,
            "worker_exception_type": self.worker_exception_type,
            "reconnect_stalled": self.reconnect_stalled(),
            "reason_code": self.reason_code,
            "last_failure_reason_code": self.last_failure_reason_code,
            "last_disconnect_phase": self.last_disconnect_phase,
            "close_code": self.last_close_code, "close_reason": self.last_close_reason,
            "close_side": self.last_close_side,
            "last_ping_age_seconds": self._age(self.last_ping_at),
            "transport_healthy": self.is_transport_healthy(),
            "critical_data_fresh": self.is_data_fresh(),
            "processing_healthy": self.is_processing_healthy(),
            "reconciliation_required": self.reconciliation_required,
            "fresh": self.is_fresh(), "reconnects": self.reconnects,
            "maintenance_reconnects": self.maintenance_reconnects,
            "last_rx_age_seconds": self._age(self.last_rx_at),
            "last_pong_age_seconds": self._age(self.last_pong_at),
            "ping_pending": self.ping_pending, "ping_rtt_seconds": self.ping_rtt,
            "queue_depth": self.queue.qsize(), "queue_capacity": self.queue.maxsize,
            "handler_age_seconds": (
                max(0, self.clock() - self._handler_started_at)
                if self._handler_started_at is not None else None
            ),
            "oldest_queue_age_seconds": (
                max(0, self.clock() - self._queued_at[0]) if self._queued_at else None
            ),
            "stale_feeds": self.stale_feeds(),
            "disconnect_reason": self.disconnect_reason,
        }


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
