from __future__ import annotations

import logging
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any
from uuid import uuid4

from app.models import ExecutionRequest, Instrument, OrderState, RiskDecision, Side
from app.okx import OkxError, OkxRestClient
from app.storage import Store

logger = logging.getLogger("execution")


class OrderManager:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.orders: dict[str, dict] = {}
        self.seen_trade_ids: set[str] = set()

    async def create(self, request: ExecutionRequest) -> None:
        if request.client_order_id in self.orders:
            raise ValueError("duplicate client order ID")
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
        self.seen_trade_ids = {
            str(row["tradeId"])
            for row in await self.store.latest("fills", limit=100000)
            if row.get("tradeId")
        }

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
        client_id = str(event.get("clOrdId", ""))
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
        if event.get("fillSz") and Decimal(event["fillSz"]) > 0:
            trade_id = str(event.get("tradeId", ""))
            if trade_id and trade_id not in self.seen_trade_ids:
                await self.store.append(
                    "fills", event, symbol=event.get("instId"), reference_id=trade_id
                )
                self.seen_trade_ids.add(trade_id)

    def expected_deltas(self) -> dict[str, Decimal]:
        deltas: dict[str, Decimal] = {}
        for row in self.orders.values():
            size = Decimal(row["filled"]) - Decimal(row["reconciled_filled"])
            signed = size if row["direction"] == Side.LONG.value else -size
            deltas[row["symbol"]] = deltas.get(row["symbol"], Decimal(0)) + signed
        return deltas

    def mark_reconciled(self) -> None:
        for row in self.orders.values():
            row["reconciled_filled"] = row["filled"]


class ExecutionEngine:
    def __init__(self, client: OkxRestClient, manager: OrderManager) -> None:
        self.client = client
        self.manager = manager

    @staticmethod
    def from_risk(
        decision: RiskDecision,
        instrument: Instrument,
        *,
        order_type: str = "limit",
        reduce_only: bool = False,
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
        )

    async def submit(self, request: ExecutionRequest) -> None:
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
        try:
            result = await self.client.place_order(body)
        except OkxError:
            # An HTTP timeout does not prove rejection. Query by the same clOrdId, never retry placement.
            try:
                result = await self.client.order(request.symbol, request.client_order_id)
            except OkxError:
                await self.manager.transition(request.client_order_id, OrderState.UNKNOWN)
                raise
        if not result:
            await self.manager.transition(request.client_order_id, OrderState.UNKNOWN)
            raise OkxError("order state unconfirmed")
        await self.manager.transition(
            request.client_order_id, OrderState.ACKNOWLEDGED, order_id=result[0].get("ordId", "")
        )
