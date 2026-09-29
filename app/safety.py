from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from app.config import Settings
from app.execution import ExecutionEngine, OrderManager
from app.models import OrderState, PortfolioState, Side
from app.storage import Store

logger = logging.getLogger("safety")


class EntryOrderController:
    """An entry stays blocked until a terminal exchange state is observed."""

    def __init__(
        self,
        client: Any,
        manager: OrderManager,
        store: Store,
        action_lock: asyncio.Lock | None = None,
    ) -> None:
        self.client, self.manager, self.store = client, manager, store
        self.blocked: set[str] = set()
        self._lock = action_lock or asyncio.Lock()

    async def cancel(self, row: dict, *, force: bool = False) -> bool:
        async with self._lock:
            return await self._cancel_locked(row, force=force)

    async def _cancel_locked(self, row: dict, *, force: bool = False) -> bool:
        symbol, client_id = row["symbol"], row["clOrdId"]
        self.blocked.add(symbol)
        if not force and row["state"] in {"FILLED", "CANCELLED", "REJECTED"}:
            return row["state"] == "CANCELLED"
        try:
            await self.client.cancel_order(symbol, client_order_id=client_id)
        except Exception as exc:
            logger.error("entry cancellation request failed for %s: %s", client_id, exc)
        if row["state"] not in {"FILLED", "CANCELLED", "REJECTED"}:
            await self.manager.transition(client_id, OrderState.CANCEL_REQUESTED)
        return await self.confirm(row, force=force)

    async def confirm(self, row: dict, *, force: bool = False) -> bool:
        symbol, client_id = row["symbol"], row["clOrdId"]
        if row["state"] == "CANCELLED" and not force:
            if not any(other["symbol"] == symbol for other in self.manager.pending_entries()):
                self.blocked.discard(symbol)
            return True
        try:
            pending = await self.client.pending_orders()
        except Exception:
            self.blocked.add(symbol)
            return False
        if any(order.get("clOrdId") == client_id for order in pending):
            self.blocked.add(symbol)
            return False
        # An absent pending order can have filled. Query the order before clearing the block.
        try:
            detail = await self.client.order(symbol, client_id)
        except Exception:
            self.blocked.add(symbol)
            return False
        if detail:
            remote = detail[0]
            state = remote.get("state")
            if state == "filled":
                await self.manager.transition(
                    client_id, OrderState.FILLED, filled=remote.get("accFillSz", row["filled"])
                )
                self.blocked.add(symbol)
                return False
            if state not in {"canceled", "mmp_canceled"}:
                self.blocked.add(symbol)
                return False
            if Decimal(str(remote.get("accFillSz") or "0")) > 0:
                await self.manager.transition(
                    client_id, OrderState.CANCELLED, filled=remote["accFillSz"]
                )
                self.blocked.add(symbol)
                return False
        await self.manager.transition(client_id, OrderState.CANCELLED)
        if not any(other["symbol"] == symbol for other in self.manager.pending_entries()):
            self.blocked.discard(symbol)
        return True

    async def cancel_all(self) -> bool:
        results = []
        for row in self.manager.pending_entries():
            try:
                results.append(await self.cancel(row))
            except Exception as exc:
                self.blocked.add(row["symbol"])
                logger.error(
                    "entry cancellation could not be audited for %s: %s", row["clOrdId"], exc
                )
                results.append(False)
        return all(results)

    async def expire(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(UTC)
        results = []
        for row in self.manager.pending_entries():
            expiry = row.get("signal_expires_at")
            expiry_at = datetime.fromisoformat(expiry) if expiry else None
            if expiry_at and expiry_at.tzinfo is None:
                expiry_at = expiry_at.replace(tzinfo=UTC)
            if expiry_at and expiry_at <= now:
                try:
                    results.append(await self.cancel(row))
                except Exception as exc:
                    self.blocked.add(row["symbol"])
                    logger.error("entry expiry cancellation failed for %s: %s", row["clOrdId"], exc)
                    results.append(False)
        return all(results)


class AlgoOrderManager:
    ACTIVE = {"live"}

    def __init__(self, store: Store) -> None:
        self.store = store
        self.algos: dict[str, dict[str, Any]] = {}

    async def ingest(self, event: dict[str, Any]) -> None:
        client_id = str(event.get("algoClOrdId") or "")
        if not client_id:
            return
        row = {
            "algoClOrdId": client_id,
            "algoId": event.get("algoId", ""),
            "symbol": event.get("instId", ""),
            "side": event.get("side", ""),
            "size": event.get("sz", ""),
            "trigger": event.get("slTriggerPx", ""),
            "state": event.get("state", ""),
            "failCode": event.get("failCode", ""),
            "failReason": event.get("failReason", ""),
            "reduceOnly": event.get("reduceOnly", ""),
        }
        self.algos[client_id] = row
        await self.store.append("algo_events", row, symbol=row["symbol"], reference_id=client_id)

    async def restore(self) -> None:
        for row in reversed(await self.store.latest("algo_events", limit=10000)):
            self.algos[row["algoClOrdId"]] = row

    def valid(
        self,
        algo: dict[str, Any] | None,
        *,
        symbol: str,
        position: Decimal,
        stop_price: Decimal,
        tolerance: Decimal,
    ) -> bool:
        if not algo or algo.get("instId", algo.get("symbol")) != symbol:
            return False
        if algo.get("side") != ("sell" if position > 0 else "buy"):
            return False
        try:
            size = Decimal(str(algo.get("sz", algo.get("size", ""))))
            trigger = Decimal(str(algo.get("slTriggerPx", algo.get("trigger", ""))))
        except Exception:
            return False
        return (
            size >= abs(position)
            and abs(trigger - stop_price) <= tolerance
            and algo.get("state") in self.ACTIVE
            and not algo.get("failCode")
            and str(algo.get("reduceOnly", "false")).lower() in {"true", "1"}
        )


class EmergencyController:
    """Persist target positions and reconcile actual position before every reduce attempt."""

    def __init__(
        self,
        client: Any,
        execution: ExecutionEngine,
        manager: OrderManager,
        store: Store,
        instruments: dict,
        risk: Any,
    ) -> None:
        self.client, self.execution, self.manager = client, execution, manager
        self.store, self.instruments, self.risk = store, instruments, risk
        self.targets: dict[str, Decimal] = {}
        self.inflight: dict[str, str] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def restore(self) -> None:
        for row in await self.store.latest_per_symbol("emergency_targets"):
            self.targets[row["symbol"]] = Decimal(row["target"])
            if row.get("completed"):
                self.targets.pop(row["symbol"], None)
        for row in self.manager.orders.values():
            if row.get("intent_id", "").startswith("emergency-") and row["state"] not in {
                "FILLED",
                "CANCELLED",
                "REJECTED",
            }:
                self.inflight[row["symbol"]] = row["clOrdId"]

    async def target(self, symbol: str, target: Decimal = Decimal(0)) -> None:
        self.targets[symbol] = target
        await self.store.append(
            "emergency_targets",
            {"symbol": symbol, "target": str(target), "completed": False},
            symbol=symbol,
        )

    async def step(self, symbol: str) -> bool:
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            return await self._step_locked(symbol)

    async def _step_locked(self, symbol: str) -> bool:
        if symbol not in self.targets:
            return True
        target = self.targets[symbol]
        try:
            positions = await self.client.positions()
        except Exception:
            return False
        remote = next(
            (Decimal(p["pos"]) for p in positions if p.get("instId") == symbol), Decimal(0)
        )
        remaining = remote - target
        if remaining == 0:
            self.targets.pop(symbol)
            self.inflight.pop(symbol, None)
            await self.store.append(
                "emergency_targets",
                {"symbol": symbol, "target": str(target), "completed": True},
                symbol=symbol,
            )
            return True
        if remote == 0 or remaining * remote <= 0:
            return False
        previous = self.inflight.get(symbol)
        if previous:
            row = self.manager.orders.get(previous)
            if row and row["state"] not in {"FILLED", "REJECTED", "CANCELLED"}:
                try:
                    details = await self.client.order(symbol, previous)
                except Exception:
                    await self.manager.transition(previous, OrderState.UNKNOWN)
                    return False
                if not details:
                    return False
                detail = details[0]
                state = {
                    "filled": OrderState.FILLED,
                    "canceled": OrderState.CANCELLED,
                    "rejected": OrderState.REJECTED,
                    "partially_filled": OrderState.PARTIALLY_FILLED,
                    "live": OrderState.ACKNOWLEDGED,
                }.get(detail.get("state"), OrderState.UNKNOWN)
                await self.manager.transition(
                    previous, state, filled=detail.get("accFillSz", row["filled"])
                )
                if state == OrderState.PARTIALLY_FILLED:
                    try:
                        await self.client.cancel_order(symbol, client_order_id=previous)
                    except Exception:
                        pass
                    return False
                if state in {OrderState.UNKNOWN, OrderState.ACKNOWLEDGED}:
                    return False
            self.inflight.pop(symbol, None)
            return False
        instrument = self.instruments.get(symbol)
        if instrument is None:
            return False
        direction = Side.LONG if remaining > 0 else Side.SHORT
        decision = self.risk.emergency_reduce(symbol, direction, abs(remaining), instrument)
        request = ExecutionEngine.from_risk(
            decision, instrument, order_type="market", reduce_only=True
        )
        self.inflight[symbol] = request.client_order_id
        try:
            await self.execution.submit(request)
        except Exception:
            # No quantity is credited here. The next step queries this clOrdId and position.
            return False
        return False

    async def step_all(self) -> None:
        for symbol in list(self.targets):
            await self.step(symbol)


class PortfolioRiskMonitor:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def breached(
        self, portfolio: PortfolioState, positions: list[dict[str, Any]]
    ) -> tuple[str, str | None]:
        p = portfolio
        for row in positions:
            if Decimal(row.get("pos") or "0") and Decimal(row.get("mgnRatio") or "0") < Decimal(
                str(self.settings.min_margin_ratio)
            ):
                return "margin ratio danger", row["instId"]
        if p.equity > 0 and p.daily_pnl <= -p.equity * Decimal(str(self.settings.max_daily_loss)):
            return "daily loss limit", None
        if p.weekly_drawdown >= Decimal(str(self.settings.max_weekly_drawdown)):
            return "weekly drawdown limit", None
        if p.equity > 0 and p.margin_used / p.equity >= Decimal(
            str(self.settings.max_margin_usage)
        ):
            return "margin usage limit", None
        return "", None
