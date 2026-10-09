from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from app.config import Mode, Settings
from app.execution import OrderManager
from app.fill_identity import FillKey, equivalent_fill, fill_key
from app.storage import Store


class LedgerEvidenceError(ValueError):
    pass


def decimal_value(value: Any, *, positive: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
        if not result.is_finite() or (positive and result <= 0):
            raise ValueError
        return result
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise LedgerEvidenceError("ledger repair evidence incomplete") from exc


def parse_fill(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise LedgerEvidenceError("ledger repair evidence incomplete")
    fill_key(row.get("instId"), row.get("tradeId"))
    for key in ("ordId",):
        if not isinstance(row.get(key), str) or not row[key].strip():
            raise LedgerEvidenceError("ledger repair evidence incomplete")
    if row.get("side") not in {"buy", "sell"} or row.get("posSide") not in {None, "", "net"}:
        raise LedgerEvidenceError("ledger repair evidence incomplete")
    decimal_value(row.get("fillSz"), positive=True)
    decimal_value(row.get("fillPx"), positive=True)
    for key in ("fee", "fillPnl"):
        if row.get(key) not in (None, ""):
            decimal_value(row[key])
    for key in ("fillTime", "ts"):
        try:
            if type(row.get(key)) not in {str, int}:
                raise ValueError
            if int(row[key]) <= 0:
                raise ValueError
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise LedgerEvidenceError("ledger repair evidence incomplete") from exc
    return dict(row)



@dataclass
class LedgerRepairResult:
    attempted: bool = True
    repaired: bool = False  # ONLY runtime full reconciliation may set this true.
    evidence_complete: bool = False
    fills_added: int = 0
    orders_added: int = 0
    conflicts: list[str] = field(default_factory=list)
    unresolved: list[str] = field(default_factory=list)
    reason: str = ""

    def report(self) -> dict[str, Any]:
        return {
            "attempted": self.attempted,
            "repaired": self.repaired,
            "evidence_complete": self.evidence_complete,
            "fills_added": self.fills_added,
            "orders_added": self.orders_added,
            "conflicts": len(self.conflicts),
            "unresolved": len(self.unresolved),
            "reason": self.reason,
        }


class LedgerRepairService:
    """Read evidence and append accounting records. No trading or Governor authority."""

    def __init__(self, client: Any, manager: OrderManager, store: Store) -> None:
        self.client, self.manager, self.store = client, manager, store

    async def repair(self, symbols: set[str], *, emergency: bool = False) -> LedgerRepairResult:
        result = LedgerRepairResult()
        try:
            await self._authorize()
            async with self.manager.ledger_lock:
                snapshots = {
                    table: await self.store.ledger_snapshot(table, symbols)
                    for table in ("orders", "order_events", "fills")
                }
                orders = {
                    cid: dict(row)
                    for cid, row in self.manager.orders.items()
                    if row["symbol"] in symbols
                }
            # Network evidence never holds the ingestion lock; Emergency/WS fills can proceed.
            async with asyncio.timeout(30):
                records, fills_added, orders_added = await self._plan(symbols, snapshots, orders)
            if records:
                await self._authorize()
                async with self.manager.ledger_lock:
                    current = {
                        cid: dict(row)
                        for cid, row in self.manager.orders.items()
                        if row["symbol"] in symbols
                    }
                    if current != orders:
                        raise LedgerEvidenceError("ledger repair evidence incomplete")
                    records.insert(
                        0,
                        (
                            "system_events",
                            {
                                "event": "ledger_repair_manual_hold",
                                "emergency": emergency,
                                "reason": "ledger repaired; manual resume required",
                            },
                            "ledger-repair",
                        ),
                    )
                    await self.store.append_ledger_recovery(records, snapshots, symbols)
                    await self.manager.restore()
            result.fills_added, result.orders_added = fills_added, orders_added
            result.evidence_complete = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Never log arbitrary remote exception strings/payloads or credentials.
            allowed = {
                "ledger fill conflict",
                "ledger repair ownership unverified",
                "ledger repair evidence incomplete",
                "ledger repair LIVE authorization unavailable",
            }
            candidate = str(exc) if isinstance(exc, ValueError) else ""
            reason = candidate if candidate in allowed else ""
            result.reason = reason or "ledger repair evidence incomplete"
            if result.reason == "ledger fill conflict":
                result.conflicts.append(result.reason)
            else:
                result.unresolved.append(result.reason)
        await self.store.append(
            "system_events", {"event": "ledger_repair_attempt", **result.report()}
        )
        return result

    async def _authorize(self) -> None:
        settings = getattr(self.client, "settings", None)
        if isinstance(settings, Settings) and settings.mode == Mode.LIVE:
            await self.client.account_config()
            if (
                not self.client.live_identity_verified()
                or self.client.live_writer_guard is None
                or not await self.client.live_writer_guard(False)
            ):
                raise LedgerEvidenceError("ledger repair LIVE authorization unavailable")

    async def _plan(
        self,
        symbols: set[str],
        snapshots: dict[str, list[dict[str, Any]]],
        initial_orders: dict[str, dict[str, Any]],
    ) -> tuple[list[tuple[str, dict[str, Any], str]], int, int]:
        local_orders = {cid: dict(row) for cid, row in initial_orders.items()}
        if not symbols or not local_orders or len(local_orders) > 10000:
            raise LedgerEvidenceError("ledger repair evidence incomplete")
        if {row["clOrdId"] for row in snapshots["orders"]} != set(local_orders):
            raise LedgerEvidenceError("ledger repair evidence incomplete")
        starts = []
        for row in local_orders.values():
            if (
                row.get("direction") not in {"LONG", "SHORT"}
                or type(row.get("reduce_only")) is not bool
                or decimal_value(row.get("filled")) < 0
            ):
                raise LedgerEvidenceError("ledger fill conflict")
            if not row.get("created_at"):
                if not row.get("reduce_only"):
                    raise LedgerEvidenceError("ledger repair evidence incomplete")
                continue  # synthetic children are bounded by their known parent entry.
            try:
                start = datetime.fromisoformat(row["created_at"])
                if start.tzinfo is None:
                    raise ValueError
                starts.append(start)
            except (ValueError, TypeError) as exc:
                raise LedgerEvidenceError("ledger repair evidence incomplete") from exc
        now = datetime.now(UTC)
        if not starts:
            raise LedgerEvidenceError("ledger repair evidence incomplete")
        start = min(starts) - timedelta(hours=24)
        # Conservative supported retention bound (OKX documents the last three months).
        if start < now - timedelta(days=89) or start > now:
            raise LedgerEvidenceError("ledger repair evidence incomplete")
        start_ms, end_ms = int(start.timestamp() * 1000), int(now.timestamp() * 1000)
        remote: dict[FillKey, dict[str, Any]] = {}
        for symbol in sorted(symbols):
            rows = await self.client.fills_history_window(
                symbol,
                history_start_ms=start_ms,
                history_end_ms=end_ms,
                max_pages=50,
                max_records=5000,
            )
            for raw in rows:
                row = parse_fill(raw)
                if row["instId"] != symbol:
                    raise LedgerEvidenceError("ledger fill conflict")
                if not (
                    start_ms <= int(row["ts"]) <= end_ms
                    and start_ms <= int(row["fillTime"]) <= end_ms
                ):
                    raise LedgerEvidenceError("ledger repair evidence incomplete")
                tid = fill_key(row["instId"], row["tradeId"])
                if tid in remote:
                    if not equivalent_fill(remote[tid], row):
                        raise LedgerEvidenceError("ledger fill conflict")
                    continue
                remote[tid] = row
        existing: dict[FillKey, dict[str, Any]] = {}
        for row in await self.store.effective_fill_payloads(snapshots["fills"]):
            tid = fill_key(row.get("instId"), row.get("tradeId"))
            if tid in existing:
                raise LedgerEvidenceError("ledger fill conflict")
            existing[tid] = row
        for tid, row in remote.items():
            if tid in existing and not equivalent_fill(existing[tid], row):
                raise LedgerEvidenceError("ledger fill conflict")
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in remote.values():
            groups.setdefault(row["ordId"], []).append(row)
        if len(groups) > 100:
            raise LedgerEvidenceError("ledger repair evidence incomplete")
        positions: dict[str, Decimal] = {}
        for row in local_orders.values():
            signed = decimal_value(row["filled"]) * (1 if row["direction"] == "LONG" else -1)
            positions[row["symbol"]] = positions.get(row["symbol"], Decimal(0)) + signed
        records: list[tuple[str, dict[str, Any], str]] = []
        fills_added = orders_added = 0
        algo_cache: dict[str, list[dict[str, Any]]] = {}
        historical_parents: dict[tuple[str, str], dict[str, Any]] = {}
        for historical in snapshots["orders"] + snapshots["order_events"]:
            cid, aid = historical.get("clOrdId"), historical.get("protective_algo_id")
            parent = initial_orders.get(cid or "")
            if parent and not parent["reduce_only"] and aid:
                if not isinstance(cid, str) or not isinstance(aid, str):
                    raise LedgerEvidenceError("ledger repair evidence incomplete")
                if (
                    historical.get("symbol") != parent["symbol"]
                    or historical.get("direction") != parent["direction"]
                ):
                    raise LedgerEvidenceError("ledger fill conflict")
                historical_parents[(cid, aid)] = {**parent, "protective_algo_id": aid}
        for oid, fills in sorted(
            groups.items(), key=lambda x: min(int(r["fillTime"]) for r in x[1])
        ):
            total = sum((decimal_value(r["fillSz"], positive=True) for r in fills), Decimal(0))
            known = [
                r
                for r in local_orders.values()
                if (
                    (fills[0].get("clOrdId") and r["clOrdId"] == fills[0]["clOrdId"])
                    or r.get("order_id") == oid
                    or r["clOrdId"] == "protective-" + oid
                )
            ]
            if (
                len(known) == 1
                and all(fill_key(r["instId"], r["tradeId"]) in existing for r in fills)
                and decimal_value(known[0]["filled"]) == total
            ):
                # Already-complete historical orders need no reconstruction. This also avoids
                # requiring /order's shorter retention for old, fully audited history.
                if known[0]["symbol"] != fills[0]["instId"] or any(
                    (known[0]["direction"] == "LONG") != (r["side"] == "buy")
                    or (r.get("clOrdId") and r["clOrdId"] != fills[0].get("clOrdId"))
                    for r in fills
                ):
                    raise LedgerEvidenceError("ledger fill conflict")
                continue
            detail = await self.client.order_by_id(fills[0]["instId"], oid)
            if len(detail) != 1:
                raise LedgerEvidenceError("ledger repair evidence incomplete")
            d = detail[0]
            row, parent = await self._owned_order(
                oid,
                fills,
                d,
                local_orders,
                list(historical_parents.values()),
                algo_cache,
            )
            if d.get("state") not in {"filled", "canceled", "mmp_canceled"}:
                raise LedgerEvidenceError("ledger repair evidence incomplete")
            if decimal_value(d.get("accFillSz")) != total:
                raise LedgerEvidenceError("ledger repair evidence incomplete")
            if any(r.get("ordId") == oid and tid not in remote for tid, r in existing.items()):
                raise LedgerEvidenceError("ledger repair evidence incomplete")
            local_filled = decimal_value(row["filled"])
            if local_filled > total:
                raise LedgerEvidenceError("ledger fill conflict")
            delta = total - local_filled
            sign = 1 if row["direction"] == "LONG" else -1
            symbol = row["symbol"]
            if row["reduce_only"] and delta:
                exposure = positions.get(symbol, Decimal(0))
                if exposure * sign >= 0 or delta > abs(exposure):
                    raise LedgerEvidenceError("ledger fill conflict")
            if not row["reduce_only"] and total > decimal_value(row["approved_contracts"]):
                raise LedgerEvidenceError("ledger fill conflict")
            positions[symbol] = positions.get(symbol, Decimal(0)) + sign * delta
            cid = row["clOrdId"]
            if parent:
                records.append(("orders", row.copy(), cid))
                local_orders[cid] = row
                orders_added += 1
            state = "FILLED" if d["state"] == "filled" else "CANCELLED"
            if row["state"] in {"FILLED", "CANCELLED", "REJECTED"} and row["state"] != state:
                raise LedgerEvidenceError("ledger fill conflict")
            if parent or local_filled != total or row["state"] != state:
                event = {
                    **row,
                    "state": state,
                    "filled": str(total),
                    "order_id": oid,
                    "recovered": True,
                    "recovery_source": "okx_fills_history",
                }
                records.append(("order_events", event, cid))
            for r in fills:
                if fill_key(r["instId"], r["tradeId"]) not in existing:
                    payload = {
                        **r,
                        "recovered": True,
                        "recovery_source": "okx_fills_history",
                        "recovered_clOrdId": cid,
                        "recovered_at": now.isoformat(),
                    }
                    records.append(("fills", payload, r["tradeId"]))
                    fills_added += 1
        if not remote:
            raise LedgerEvidenceError("ledger repair evidence incomplete")
        return records, fills_added, orders_added

    async def _owned_order(
        self,
        oid: str,
        fills: list[dict[str, Any]],
        d: dict[str, Any],
        orders: dict[str, dict[str, Any]],
        historical_parents: list[dict[str, Any]],
        algo_cache: dict[str, list[dict[str, Any]]],
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        first = fills[0]
        symbol, side = first["instId"], first["side"]
        if (
            d.get("instId") != symbol
            or d.get("ordId") != oid
            or d.get("side") != side
            or d.get("posSide") not in {None, "", "net"}
            or any(r["instId"] != symbol or r["side"] != side for r in fills)
        ):
            raise LedgerEvidenceError("ledger fill conflict")
        candidates = [
            r
            for r in orders.values()
            if (
                (first.get("clOrdId") and r["clOrdId"] == first["clOrdId"])
                or r.get("order_id") == oid
                or r["clOrdId"] == "protective-" + oid
            )
        ]
        parent = None
        if not candidates:
            # Exact known algo identity AND its authoritative spawned order ID are mandatory.
            claimed_algo = d.get("algoClOrdId") or d.get("attachAlgoClOrdId")
            parents = [
                r
                for r in historical_parents
                if r["symbol"] == symbol
                and (not claimed_algo or r["protective_algo_id"] == claimed_algo)
            ]
            if len(parents) > 100:
                raise LedgerEvidenceError("ledger repair evidence incomplete")
            proofs = []
            for p in parents:
                if d.get("algoClOrdId") and d["algoClOrdId"] != p["protective_algo_id"]:
                    continue
                if p["symbol"] != symbol:
                    continue
                key = p["protective_algo_id"]
                if key not in algo_cache:
                    algo_cache[key] = await self.client.algo_order(key)
                algos = algo_cache[key]
                for algo in algos:
                    spawned = algo.get("ordIdList") or []
                    if not isinstance(spawned, list) or any(
                        not isinstance(x, str) for x in spawned
                    ):
                        raise LedgerEvidenceError("ledger repair evidence incomplete")
                    if algo.get("ordId"):
                        spawned = [*spawned, algo["ordId"]]
                    if (
                        algo.get("algoClOrdId") == p["protective_algo_id"]
                        and algo.get("instId") == symbol
                        and oid in spawned
                        and algo.get("side") == side
                        and algo.get("posSide") in {None, "", "net"}
                        and algo.get("state") == "effective"
                        and (not d.get("algoId") or d["algoId"] == algo.get("algoId"))
                        and str(algo.get("failCode") or "0") == "0"
                    ):
                        proofs.append((p, algo))
            if len(proofs) != 1:
                raise LedgerEvidenceError("ledger repair ownership unverified")
            parent, algo = proofs[0]
            opposite = "SHORT" if parent["direction"] == "LONG" else "LONG"
            if (side == "buy") != (opposite == "LONG"):
                raise LedgerEvidenceError("ledger fill conflict")
            row = {
                "clOrdId": "protective-" + oid,
                "symbol": symbol,
                "state": "CREATED",
                "filled": "0",
                "reconciled_filled": "0",
                "intent_id": parent["intent_id"],
                "direction": opposite,
                "stop_price": "",
                "entry_reference": "",
                "reduce_only": True,
                "protective_algo_id": parent["protective_algo_id"],
                "recovered_algo_id": algo.get("algoId", ""),
                "order_id": oid,
                "recovered": True,
                "recovery_source": "okx_fills_history",
                "created_at": datetime.fromtimestamp(
                    int(first["fillTime"]) / 1000, UTC
                ).isoformat(),
            }
        elif len(candidates) == 1:
            row = candidates[0]
        else:
            raise LedgerEvidenceError("ledger fill conflict")
        if (
            row["symbol"] != symbol
            or (row["direction"] == "LONG") != (side == "buy")
            or (str(d.get("reduceOnly")).lower() in {"true", "1"}) != bool(row["reduce_only"])
            or any(r.get("clOrdId") and d.get("clOrdId") != r["clOrdId"] for r in fills)
            or (row.get("order_id") and row["order_id"] != oid)
        ):
            raise LedgerEvidenceError("ledger fill conflict")
        return row, parent
