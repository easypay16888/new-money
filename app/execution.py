from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any
from uuid import uuid4

from app.fill_identity import FillKey, equivalent_fill, fill_key
from app.models import ExecutionRequest, Instrument, OrderState, RiskDecision, Side
from app.okx import OkxError, OkxOrderRejected, OkxRestClient
from app.storage import Store

logger = logging.getLogger("execution")


class OrderManager:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.orders: dict[str, dict] = {}
        self.seen_trade_ids: set[FillKey] = set()
        self.ledger_lock = asyncio.Lock()
        self.terminal_fill_recovery: Callable[[dict, dict], Awaitable[None]] | None = None

    async def create(self, request: ExecutionRequest) -> None:
        if request.client_order_id in self.orders:
            raise ValueError("duplicate client order ID")
        if not request.reduce_only and request.signal_expires_at is None:
            raise ValueError("entry order requires signal expiry")
        expires = request.signal_expires_at
        if expires is not None and expires.tzinfo is None:
            expires = expires.replace(tzinfo=UTC)
        row = {
            "clOrdId": request.client_order_id,
            "symbol": request.symbol,
            "state": OrderState.CREATED.value,
            "filled": "0",
            "reconciled_filled": "0",
            "intent_id": request.risk_decision.intent_id,
            "approved_contracts": str(request.risk_decision.approved_contracts),
            "direction": request.risk_decision.direction.value,
            "stop_price": (
                str(request.risk_decision.stop_price)
                if request.risk_decision.stop_price is not None
                else ""
            ),
            "entry_reference": (
                str(request.risk_decision.entry_reference)
                if request.risk_decision.entry_reference is not None
                else ""
            ),
            "reduce_only": request.reduce_only,
            "created_at": datetime.now(UTC).isoformat(),
            "signal_expires_at": (expires.isoformat() if expires else ""),
            "protective_algo_id": (
                "a" + request.client_order_id[1:] if not request.reduce_only else ""
            ),
        }
        await self.store.append(
            "orders", row, symbol=request.symbol, reference_id=request.client_order_id
        )
        self.orders[request.client_order_id] = row

    async def restore(self) -> None:
        self.orders = {
            row["clOrdId"]: row for row in reversed(await self.store.latest("orders", limit=10000))
        }
        for event in reversed(await self.store.latest("order_events", limit=100000)):
            client_id = event.get("clOrdId")
            if client_id in self.orders:
                self.orders[client_id].update(event)
        for row in self.orders.values():
            row["reconciled_filled"] = row.get("filled", "0")
        self.seen_trade_ids = {key for key, _ in await self.store.fill_records()}

    async def transition(self, client_order_id: str, state: OrderState, **changes: str) -> None:
        row = self.orders[client_order_id]
        current = OrderState(row["state"])
        terminal = {OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED}
        if current in terminal and state != current:
            return
        if current == OrderState.PARTIALLY_FILLED and state in {
            OrderState.SUBMITTED,
            OrderState.ACKNOWLEDGED,
        }:
            return
        if row["state"] == state.value and all(row.get(k) == v for k, v in changes.items()):
            return
        row.update(state=state.value, **changes)
        await self.store.append(
            "order_events", row.copy(), symbol=row["symbol"], reference_id=client_order_id
        )

    async def ingest(self, event: dict) -> None:
        async with self.ledger_lock:
            await self._ingest(event)

    async def _ingest(self, event: dict) -> None:
        key = None
        existing_fill = None
        if event.get("fillSz") and Decimal(event["fillSz"]) > 0:
            key = fill_key(event.get("instId"), event.get("tradeId"))
            existing_fill = await self.store.fill_for_key(key)
            if existing_fill is not None and not equivalent_fill(existing_fill, event):
                # Validate immutable evidence before appending any order event.
                raise OkxError("ledger fill conflict")
            if key in self.seen_trade_ids and existing_fill is None:
                raise OkxError("ledger fill conflict")
        client_id = str(event.get("clOrdId", ""))
        if client_id in self.orders and key is not None and self.orders[client_id]["symbol"] != key[0]:
            raise OkxError("ledger fill conflict")
        if client_id not in self.orders:
            algo_id = event.get("algoClOrdId") or event.get("attachAlgoClOrdId")
            parent = next(
                (
                    row
                    for row in self.orders.values()
                    if algo_id and row["protective_algo_id"] == algo_id
                ),
                None,
            )
            order_id = str(event.get("ordId", ""))
            if parent is None or not order_id:
                raise OkxError("unexpected order event")
            if key is not None and parent["symbol"] != key[0]:
                raise OkxError("ledger fill conflict")
            client_id = "protective-" + order_id
            if client_id not in self.orders:
                synthetic = {
                    "clOrdId": client_id,
                    "symbol": parent["symbol"],
                    "state": OrderState.CREATED.value,
                    "filled": "0",
                    "reconciled_filled": "0",
                    "intent_id": parent["intent_id"],
                    "direction": (
                        Side.SHORT.value
                        if parent["direction"] == Side.LONG.value
                        else Side.LONG.value
                    ),
                    "stop_price": "",
                    "entry_reference": "",
                    "reduce_only": True,
                    "protective_algo_id": "",
                }
                await self.store.append(
                    "orders", synthetic, symbol=parent["symbol"], reference_id=client_id
                )
                self.orders[client_id] = synthetic
        state = {
            "live": OrderState.ACKNOWLEDGED,
            "partially_filled": OrderState.PARTIALLY_FILLED,
            "filled": OrderState.FILLED,
            "canceled": OrderState.CANCELLED,
            "mmp_canceled": OrderState.CANCELLED,
        }.get(str(event.get("state", "")), OrderState.UNKNOWN)
        await self.transition(
            client_id, state, filled=event.get("accFillSz", "0"), order_id=event.get("ordId", "")
        )

        if key is not None:
            if existing_fill is None:
                await self.store.append(
                    "fills", event, symbol=key[0], reference_id=key[1]
                )
            self.seen_trade_ids.add(key)

    def expected_deltas(self) -> dict[str, Decimal]:
        deltas: dict[str, Decimal] = {}
        for row in self.orders.values():
            size = Decimal(row["filled"]) - Decimal(row["reconciled_filled"])
            signed = size if row["direction"] == Side.LONG.value else -size
            deltas[row["symbol"]] = deltas.get(row["symbol"], Decimal(0)) + signed
        return deltas

    def pending_entries(self) -> list[dict]:
        return [
            row
            for row in self.orders.values()
            if not row["reduce_only"] and row["state"] not in {"FILLED", "CANCELLED", "REJECTED"}
        ]

    def mark_reconciled(self) -> None:
        for row in self.orders.values():
            row["reconciled_filled"] = row["filled"]


class ExecutionEngine:
    def __init__(
        self,
        client: OkxRestClient,
        manager: OrderManager,
        entry_allowed: Callable[[], bool] | None = None,
        entry_lock: asyncio.Lock | None = None,
    ) -> None:
        self.client = client
        self.manager = manager
        self.entry_allowed = entry_allowed
        self.entry_lock = entry_lock

    @staticmethod
    def from_risk(
        decision: RiskDecision,
        instrument: Instrument,
        *,
        order_type: str = "limit",
        reduce_only: bool = False,
        signal_expires_at: datetime | None = None,
    ) -> ExecutionRequest:
        if (
            not decision.approved
            or decision.approved_contracts <= 0
            or (not reduce_only and decision.stop_price is None)
        ):
            raise ValueError("execution requires approved risk decision")
        if order_type not in {"limit", "market"}:
            raise ValueError("unsupported order type")
        price = None
        if order_type == "limit":
            if decision.entry_reference is None:
                raise ValueError("limit price missing")
            rounding = ROUND_FLOOR if decision.direction == Side.LONG else ROUND_CEILING
            price = (decision.entry_reference / instrument.tick_size).to_integral_value(
                rounding=rounding
            ) * instrument.tick_size
        return ExecutionRequest(
            risk_decision=decision,
            client_order_id="q" + uuid4().hex[:30],
            order_type=order_type,
            price=price,
            reduce_only=reduce_only,
            signal_expires_at=(
                signal_expires_at
                or decision.signal_expires_at
                or (datetime.now(UTC) + timedelta(minutes=15) if not reduce_only else None)
            ),
        )

    async def submit(self, request: ExecutionRequest) -> None:
        if not request.reduce_only and self.entry_lock is not None:
            async with self.entry_lock:
                await self._submit_unlocked(request)
            return
        await self._submit_unlocked(request)

    async def _submit_unlocked(self, request: ExecutionRequest) -> None:
        if not request.reduce_only and self.entry_allowed and not self.entry_allowed():
            raise OkxError("risk-increasing order blocked")
        if not request.reduce_only and request.signal_expires_at:
            expires = request.signal_expires_at
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=UTC)
            if expires <= datetime.now(UTC):
                raise OkxError("expired signal cannot submit entry")
        await self.manager.create(request)
        decision = request.risk_decision
        body: dict[str, Any] = {
            "instId": request.symbol,
            "tdMode": "isolated",
            "side": "buy" if decision.direction == Side.LONG else "sell",
            "posSide": "net",
            "ordType": request.order_type,
            "sz": str(decision.approved_contracts),
            "clOrdId": request.client_order_id,
            "reduceOnly": request.reduce_only,
        }
        if request.price is not None:
            body["px"] = str(request.price)
        if not request.reduce_only:
            if decision.stop_price is None:
                raise ValueError("protective stop required")
            body["attachAlgoOrds"] = [
                {
                    "attachAlgoClOrdId": "a" + request.client_order_id[1:],
                    "slTriggerPx": str(decision.stop_price),
                    "slTriggerPxType": "mark",
                    "slOrdPx": "-1",
                }
            ]
            if decision.take_profit_reference is not None:
                body["attachAlgoOrds"][0].update(
                    {
                        "tpTriggerPx": str(decision.take_profit_reference),
                        "tpTriggerPxType": "mark",
                        "tpOrdPx": "-1",
                    }
                )
        await self.manager.transition(request.client_order_id, OrderState.SUBMITTED)
        if not request.reduce_only and request.signal_expires_at:
            expiry = request.signal_expires_at
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=UTC)
            if expiry <= datetime.now(UTC):
                await self.manager.transition(request.client_order_id, OrderState.CANCELLED)
                raise OkxError("expired signal cannot submit entry")
        if not request.reduce_only and self.entry_allowed and not self.entry_allowed():
            await self.manager.transition(request.client_order_id, OrderState.CANCELLED)
            raise OkxError("risk-increasing order blocked")
        queried_order = False
        try:
            result = await self.client.place_order(body)
        except OkxOrderRejected:
            await self.manager.transition(request.client_order_id, OrderState.REJECTED)
            raise
        except OkxError:
            # An HTTP timeout does not prove rejection. Query by the same clOrdId, never retry placement.
            try:
                result = await self.client.order(request.symbol, request.client_order_id)
                queried_order = True
            except OkxError:
                await self.manager.transition(request.client_order_id, OrderState.UNKNOWN)
                raise
        if not result:
            await self.manager.transition(request.client_order_id, OrderState.UNKNOWN)
            raise OkxError("order state unconfirmed")
        remote_state = result[0].get("state")
        if (queried_order and remote_state in {"filled", "canceled", "mmp_canceled"}
                and self.manager.terminal_fill_recovery is not None):
            await self.manager.terminal_fill_recovery(
                self.manager.orders[request.client_order_id], result[0]
            )
            if remote_state in {"canceled", "mmp_canceled"}:
                raise OkxError("order rejected or canceled by exchange")
            return
        if remote_state in {"rejected", "canceled"}:
            await self.manager.transition(
                request.client_order_id,
                OrderState.REJECTED if remote_state == "rejected" else OrderState.CANCELLED,
            )
            raise OkxError("order rejected or canceled by exchange")
        state = {"filled": OrderState.FILLED, "partially_filled": OrderState.PARTIALLY_FILLED}.get(
            str(remote_state), OrderState.ACKNOWLEDGED
        )
        await self.manager.transition(
            request.client_order_id,
            state,
            order_id=result[0].get("ordId", ""),
            filled=result[0].get("accFillSz", "0"),
        )
