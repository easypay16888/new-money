from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from collections.abc import Awaitable
from datetime import datetime, timedelta
from decimal import Decimal
from time import monotonic
from typing import Any

from redis.asyncio import Redis

from app.config import Mode, Settings
from app.daily_review import save_daily_review
from app.decision import DecisionPipeline
from app.execution import ExecutionEngine, OrderManager
from app.features import compute_features
from app.market import MarketDataEngine
from app.models import (
    GovernorState,
    Instrument,
    NotificationCategory,
    NotificationEvent,
    NotificationLevel,
    NotificationPriority,
    OrderState,
    PortfolioState,
    utcnow,
)
from app.monitoring import ConsoleNotification, Metrics, Notification, WebhookNotification
from app.notifications import BarkNotification, NotificationManager
from app.okx import OkxRestClient, OkxWebSocket, candle_subscriptions, public_subscriptions
from app.portfolio import summarize_positions
from app.regime import classify
from app.risk import RiskEngine, RiskGovernor
from app.safety import (
    AlgoOrderManager,
    EmergencyController,
    EntryOrderController,
    PortfolioRiskMonitor,
)
from app.storage import Store
from app.strategies import build_strategies

logger = logging.getLogger("runtime")


class TradingRuntime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = Store(settings.database_url)
        self.redis: Redis | None = None
        self.client = OkxRestClient(settings)
        self.market = MarketDataEngine(
            self.store,
            orderbook_snapshot_interval_seconds=settings.orderbook_snapshot_interval_seconds,
        )
        self.governor = RiskGovernor()
        self.risk = RiskEngine(settings, self.governor)
        self.order_manager = OrderManager(self.store)
        self.entry_action_lock = asyncio.Lock()
        self.execution = ExecutionEngine(
            self.client,
            self.order_manager,
            lambda: self.governor.state == GovernorState.NORMAL,
            self.entry_action_lock,
        )
        self.instruments: dict[str, Instrument] = {}
        self.entry_controller = EntryOrderController(
            self.client, self.order_manager, self.store, self.entry_action_lock
        )
        self.algo_manager = AlgoOrderManager(self.store)
        self.portfolio_monitor = PortfolioRiskMonitor(settings)
        self.emergency = EmergencyController(
            self.client, self.execution, self.order_manager, self.store, self.instruments, self.risk
        )
        self.portfolio = PortfolioState(equity=Decimal(0), available_balance=Decimal(0))
        self.sockets: list[OkxWebSocket] = []
        self.tasks: list[asyncio.Task[Any]] = []
        self.running = False
        self.evaluated_candles: dict[str, int] = {}
        self.dead_man_healthy = False
        self._caa_lock = asyncio.Lock()
        self._caa_shutting_down = False
        self.reconciliation_healthy = False
        self.strategies = build_strategies(settings)
        self.decision_pipeline = DecisionPipeline(settings, self.strategies)
        self.metrics = Metrics()
        self.client.latency_observer = self.metrics.observe_latency
        channels: list[Notification] = [ConsoleNotification()]
        if settings.alert_webhook_url:
            channels.append(WebhookNotification(settings.alert_webhook_url))
        if settings.bark_enabled:
            if not settings.bark_device_key.get_secret_value():
                logger.error("Bark disabled: device key is missing")
            else:
                try:
                    channels.append(BarkNotification(settings))
                except Exception as exc:
                    logger.error("Bark disabled: %s", type(exc).__name__)
        self.notifications = NotificationManager(
            channels, self.metrics, store=self.store,
            dedup_seconds=settings.bark_dedup_seconds,
        )
        self.started_at = utcnow()
        self._started_notified = False
        self._last_risk_notice: tuple[GovernorState, str] | None = None
        self._component_health: dict[str, bool] = {}
        self._component_pending: dict[str, tuple[bool, int]] = {}
        self._last_reconnect_total = 0
        self._reconnect_times: deque[float] = deque()
        self._last_reconnect_alert = 0.0
        self._entry_filled_announced: set[str] = set()
        self._protection_announced: set[str] = set()
        self._intent_strategies: dict[str, str] = {}
        self._last_exit_fill: dict[str, dict[str, Any]] = {}

    async def enter_halt(self, reason: str, *, emergency: bool = False) -> None:
        self.entry_controller.client = self.client
        emergency = emergency or self.governor.state == GovernorState.EMERGENCY
        repeated = (
            self.governor.reason == reason
            and self.governor.state in {GovernorState.HALT, GovernorState.EMERGENCY}
            and (not emergency or self.governor.state == GovernorState.EMERGENCY)
        )
        self.governor.halt(reason, emergency=emergency)
        await self._send_observation(self._notify_risk_state())
        if not repeated:
            try:
                await self.store.append(
                    "risk_events", {"event": "enter_halt", "reason": reason, "emergency": emergency}
                )
            except Exception:
                logger.error("HALT audit persistence unavailable")
        confirmed = await self.entry_controller.cancel_all()
        if not confirmed:
            await self.alert("ERROR", "Entry cancellation unconfirmed", reason)

    async def enter_emergency(self, symbol: str, reason: str) -> None:
        self.emergency.client = self.client
        self.entry_controller.client = self.client
        self.governor.halt(reason, emergency=True)
        await self._send_observation(self._notify_risk_state(symbol=symbol))
        await self.emergency.target(symbol)
        try:
            await self.store.append(
                "risk_events", {"event": "enter_emergency", "reason": reason, "symbol": symbol}
            )
        except Exception:
            logger.error("EMERGENCY audit persistence unavailable")
        cancellation, reduction = await asyncio.gather(
            self.entry_controller.cancel_all(),
            self.emergency.step(symbol),
            return_exceptions=True,
        )
        if cancellation is not True:
            await self.alert("ERROR", "Entry cancellation unconfirmed", reason)
        if isinstance(reduction, BaseException):
            await self.alert(
                "CRITICAL", "🚨 Emergency Reduction Failed", reason,
                category=NotificationCategory.RISK,
                priority=NotificationPriority.CRITICAL,
                symbol=symbol,
                dedup_key=f"emergency-reduction:{symbol}",
            )

    async def alert(
        self, level: str, title: str, message: str, *,
        category: NotificationCategory = NotificationCategory.SYSTEM,
        priority: NotificationPriority = NotificationPriority.ACTIVE,
        symbol: str | None = None,
        dedup_key: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        try:
            await self.notifications.publish(
                NotificationEvent(
                    level=NotificationLevel(level), category=category, title=title,
                    message=message, symbol=symbol, dedup_key=dedup_key,
                    priority=priority, metadata=metadata or {},
                )
            )
        except Exception as exc:
            logger.error("notification publish failed: %s", type(exc).__name__)

    async def _send_observation(self, notification: Awaitable[None]) -> None:
        try:
            await notification
        except Exception as exc:
            logger.error("notification observation failed: %s", type(exc).__name__)

    async def _notify_risk_state(self, *, symbol: str | None = None) -> None:
        current = (self.governor.state, self.governor.reason)
        if current == self._last_risk_notice:
            return
        previous = self._last_risk_notice
        self._last_risk_notice = current
        if self.governor.state == GovernorState.NORMAL:
            if previous and previous[0] in {GovernorState.HALT, GovernorState.EMERGENCY}:
                await self.alert(
                    "INFO", "✅ Risk State Recovered",
                    f"Previous: {previous[0].value}\nCurrent: NORMAL",
                    category=NotificationCategory.RISK,
                    dedup_key="risk-state",
                    metadata={"recovery": True},
                )
            return
        if self.governor.state not in {GovernorState.HALT, GovernorState.EMERGENCY}:
            return
        emergency_state = self.governor.state == GovernorState.EMERGENCY
        critical = emergency_state or self.governor.reason in {
            "position mismatch", "startup position mismatch", "startup fill size mismatch",
        }
        lines = [f"Reason: {self.governor.reason}", "New entries: blocked"]
        if symbol:
            lines.insert(0, symbol)
        if emergency_state:
            lines.append("Target position: 0")
        await self.alert(
            "CRITICAL" if critical else "ERROR",
            "🚨 EMERGENCY" if emergency_state else "🚨 HALT",
            "\n".join(lines),
            category=NotificationCategory.RISK,
            priority=NotificationPriority.CRITICAL if critical else NotificationPriority.TIME_SENSITIVE,
            symbol=symbol,
            dedup_key=f"risk-state:{symbol or 'all'}",
            metadata={"transition": True},
        )

    async def initialize(self) -> None:
        await self.store.initialize()
        await self.order_manager.restore()
        await self.algo_manager.restore()
        await self.emergency.restore()
        if self.emergency.targets:
            await self.enter_halt("emergency recovery pending", emergency=True)
        if self.redis is not None:
            await self.redis.aclose()
        self.redis = Redis.from_url(self.settings.redis_url, decode_responses=True)
        try:
            await self.redis.ping()
        except Exception:
            await self.enter_halt("Redis unavailable")
            await self.redis.aclose()
            self.redis = None
        self.market.redis = self.redis
        try:
            self.instruments.clear()
            self.instruments.update(await self.client.instruments())
        except Exception:
            await self.enter_halt("instrument metadata unavailable")
        for symbol in self.settings.symbols:
            for timeframe in self.settings.timeframes:
                try:
                    rows = await self.client.candles(symbol, timeframe)
                    await self.market.handle(
                        {"arg": {"channel": "candle" + timeframe, "instId": symbol}, "data": rows},
                        historical=True,
                    )
                except Exception as exc:
                    await self.enter_halt("candle preload failed")
                    logger.error("candle preload failed: %s", type(exc).__name__)
        if self.settings.mode != Mode.BACKTEST and self.settings.has_credentials:
            await self.reconcile()
            await self.emergency.step_all()

    async def reconcile(self) -> None:
        self.entry_controller.client = self.client
        self.emergency.client = self.client
        self.reconciliation_healthy = False
        prior_positions = dict(self.portfolio.positions) if self.portfolio.synchronized else {}
        try:
            account, positions, orders, algos = await asyncio.gather(
                self.client.account(),
                self.client.positions(),
                self.client.pending_orders(),
                self.client.pending_algos(),
            )
            config = await self.client.account_config()
            if not config or config[0].get("posMode") != "net_mode":
                raise RuntimeError("net position mode required")
            derivatives_mode_eligible = config[0].get("acctLv") in {"2", "3", "4"}
            if (
                self.settings.mode == Mode.LIVE
                and config[0].get("uid") != self.settings.confirm_live_account_id
            ):
                raise RuntimeError("live account ID mismatch")
            details = account[0]
            usdt: dict[str, Any] = next(
                (x for x in details.get("details", []) if x.get("ccy") == "USDT"), {}
            )
            equity = Decimal(usdt.get("eq") or "0")
            available = Decimal(usdt.get("availEq") or usdt.get("availBal") or "0")
            remote_positions = {
                p["instId"]: Decimal(p["pos"]) for p in positions if Decimal(p["pos"]) != 0
            }
            for algo in algos:
                await self.algo_manager.ingest(algo)
            startup_entries: list[dict[str, Any]] = []
            partial_entries: list[dict[str, Any]] = []
            foreign_order: dict[str, Any] | None = None
            for order in orders:
                local = self.order_manager.orders.get(order.get("clOrdId", ""))
                remote_reduce_only = str(order.get("reduceOnly", "false")).lower() in {"true", "1"}
                if (local is None and not remote_reduce_only) or (
                    local is not None
                    and (
                        local["symbol"] != order.get("instId")
                        or bool(local["reduce_only"]) != remote_reduce_only
                    )
                ):
                    foreign_order = order
                    continue
                if local and not local["reduce_only"] and not self.portfolio.synchronized:
                    raw_filled = order.get("accFillSz")
                    if raw_filled in (None, ""):
                        if remote_positions.get(local["symbol"]):
                            await self.enter_emergency(
                                local["symbol"], "startup entry fill size unavailable"
                            )
                        else:
                            await self.enter_halt("startup entry fill size unavailable")
                        return
                    filled = Decimal(str(raw_filled))
                    if filled < 0 or filled < Decimal(local["filled"]):
                        await self.enter_emergency(local["symbol"], "startup fill size mismatch")
                        return
                    if filled > 0:
                        await self.order_manager.transition(
                            local["clOrdId"],
                            OrderState.PARTIALLY_FILLED,
                            filled=str(filled),
                            order_id=str(order.get("ordId") or ""),
                        )
                        partial_entries.append(local)
                    startup_entries.append(local)
            unprotected = self._unprotected_startup_partials(
                partial_entries, remote_positions, algos
            )
            if unprotected:
                await asyncio.gather(
                    *(
                        self.enter_emergency(symbol, "startup partial fill unprotected")
                        for symbol in unprotected
                    )
                )
                return
            cancellation_unconfirmed = False
            for entry in startup_entries:
                try:
                    confirmed = await self.entry_controller.cancel(entry, force=True)
                except Exception:
                    confirmed = False
                    self.entry_controller.blocked.add(entry["symbol"])
                if (
                    not confirmed
                    and entry in partial_entries
                    and entry["state"] == OrderState.CANCELLED.value
                    and Decimal(entry["filled"]) > 0
                ):
                    self.entry_controller.blocked.discard(entry["symbol"])
                    confirmed = True
                if not confirmed:
                    cancellation_unconfirmed = True
            if partial_entries:
                try:
                    positions, algos = await asyncio.gather(
                        self.client.positions(), self.client.pending_algos()
                    )
                except Exception:
                    await asyncio.gather(
                        *(
                            self.enter_emergency(entry["symbol"], "startup coverage uncertain")
                            for entry in partial_entries
                        )
                    )
                    return
                remote_positions = {
                    p["instId"]: Decimal(p["pos"]) for p in positions if Decimal(p["pos"]) != 0
                }
                for algo in algos:
                    await self.algo_manager.ingest(algo)
                unprotected = self._unprotected_startup_partials(
                    partial_entries, remote_positions, algos
                )
                if unprotected:
                    await asyncio.gather(
                        *(
                            self.enter_emergency(symbol, "startup partial fill unprotected")
                            for symbol in unprotected
                        )
                    )
                    return
                for entry in partial_entries:
                    await self._send_observation(
                        self._notify_partial_fill(entry, protected=True)
                    )
                    match = next(
                        (
                            algo for algo in algos
                            if algo.get("algoClOrdId") == entry.get("protective_algo_id")
                        ),
                        None,
                    )
                    if match is not None:
                        await self._send_observation(
                            self._notify_protection_active(entry["symbol"], entry, match)
                        )
            if foreign_order is not None:
                await self.enter_halt("foreign risk-increasing pending order")
                await self.alert(
                    "ERROR", "Foreign pending order", str(foreign_order.get("ordId", ""))
                )
                return
            if cancellation_unconfirmed:
                await self.enter_halt("startup entry cancellation unconfirmed")
                return
            for symbol, quantity in remote_positions.items():
                if not await self._verify_protection(symbol, quantity, algos):
                    return
            if any(
                Decimal(p.get("mgnRatio") or "0") < Decimal(str(self.settings.min_margin_ratio))
                for p in positions
                if Decimal(p.get("pos") or "0") != 0
            ):
                for symbol in remote_positions:
                    await self.enter_emergency(symbol, "margin ratio danger")
                return
            if not self.portfolio.synchronized:
                if any(
                    Decimal(p.get("lever") or "999") > self.settings.max_leverage
                    for p in positions
                    if Decimal(p.get("pos") or "0") != 0
                ):
                    await self.enter_halt("startup leverage exceeds limit")
                    return
                expected_positions = {k: v for k, v in self._audited_positions().items() if v != 0}
                if expected_positions != remote_positions:
                    await self.enter_halt("startup position mismatch")
                    return
            if not self.portfolio.synchronized and not remote_positions and derivatives_mode_eligible:
                for symbol in self.settings.symbols:
                    await self.client.set_leverage(symbol, self.settings.leverage)
            if self.portfolio.synchronized and remote_positions != self.portfolio.positions:
                expected = self.order_manager.expected_deltas()
                unexpected = False
                for symbol in set(remote_positions) | set(self.portfolio.positions):
                    remote = remote_positions.get(symbol, Decimal(0))
                    current = self.portfolio.positions.get(symbol, Decimal(0))
                    delta = remote - current
                    authorized = expected.get(symbol, Decimal(0))
                    if delta * authorized <= 0 or abs(delta) > abs(authorized):
                        unexpected = True
                        break
                if unexpected:
                    await self.enter_halt("position mismatch")
                    await self.store.append(
                        "risk_events",
                        {
                            "reason": "position mismatch",
                            "remote": {k: str(v) for k, v in remote_positions.items()},
                        },
                    )
                    return
            remote_algo_ids = {algo.get("algoClOrdId") for algo in algos}
            known_algos = {
                row.get("protective_algo_id") for row in self.order_manager.orders.values()
            }
            if any(not algo_id or algo_id not in known_algos for algo_id in remote_algo_ids):
                await self.enter_halt("unexpected algo order")
                return
            if self.portfolio.synchronized and {o.get("clOrdId") for o in orders} != {
                cid
                for cid, row in self.order_manager.orders.items()
                if row["state"] in {"SUBMITTED", "ACKNOWLEDGED", "PARTIALLY_FILLED"}
            }:
                await self.enter_halt("order mismatch")
                return
            now = utcnow()
            daily_rows = await self.store.since(
                "portfolio_snapshots", now.replace(hour=0, minute=0, second=0, microsecond=0)
            )
            daily_baseline = Decimal(daily_rows[0]["equity"]) if daily_rows else equity
            stored_peak = await self.store.max_equity_since(now - timedelta(days=7))
            weekly_peak = max(equity, stored_peak or equity)
            open_risk = Decimal(0)
            for symbol, quantity in remote_positions.items():
                instrument = self.instruments.get(symbol)
                entry_row = next(
                    (
                        row
                        for row in reversed(list(self.order_manager.orders.values()))
                        if row["symbol"] == symbol
                        and not row["reduce_only"]
                        and Decimal(row["filled"]) > 0
                        and (
                            (quantity > 0 and row["direction"] == "LONG")
                            or (quantity < 0 and row["direction"] == "SHORT")
                        )
                    ),
                    None,
                )
                if instrument is None or entry_row is None:
                    await self.enter_halt("position risk cannot be reconstructed")
                    return
                open_risk += (
                    abs(Decimal(entry_row["entry_reference"]) - Decimal(entry_row["stop_price"]))
                    * abs(quantity)
                    * instrument.contract_value
                )
            for row in self.order_manager.orders.values():
                if (
                    row["state"] in {"CREATED", "SUBMITTED", "ACKNOWLEDGED", "PARTIALLY_FILLED"}
                    and not row["reduce_only"]
                ):
                    instrument = self.instruments.get(row["symbol"])
                    if instrument is None:
                        await self.enter_halt("pending order risk unknown")
                        return
                    open_risk += (
                        abs(Decimal(row["entry_reference"]) - Decimal(row["stop_price"]))
                        * Decimal(row["approved_contracts"])
                        * instrument.contract_value
                    )
            margin_used = Decimal(details.get("imr") or "0") + sum(
                (
                    Decimal(p.get("margin") or "0")
                    for p in positions
                    if p.get("mgnMode") == "isolated"
                ),
                Decimal(0),
            )
            exposure = summarize_positions(positions, self.instruments, equity)
            self.portfolio = PortfolioState(
                equity=equity,
                available_balance=available,
                margin_used=margin_used,
                **exposure,
                daily_pnl=equity - daily_baseline,
                weekly_drawdown=(weekly_peak - equity) / weekly_peak if weekly_peak else Decimal(0),
                open_risk=open_risk,
                positions=remote_positions,
                synchronized=True,
            )
            await self.store.append("portfolio_snapshots", self.portfolio.model_dump(mode="json"))
            self.order_manager.mark_reconciled()
            if self.redis is not None:
                await self.redis.set(
                    "positions",
                    json.dumps({k: str(v) for k, v in remote_positions.items()}),
                    ex=120,
                )
            await self.store.append(
                "system_events", {"event": "reconciled", "orders": len(orders), "algos": len(algos)}
            )
            self.reconciliation_healthy = True
            for symbol, quantity in prior_positions.items():
                if quantity != 0 and remote_positions.get(symbol, Decimal(0)) == 0:
                    await self._send_observation(self._notify_position_closed(symbol))
            await self._check_portfolio_limits(positions)
            if not derivatives_mode_eligible:
                await self.enter_halt("derivatives account mode required")
            if equity <= 0 or available <= 0:
                await self.enter_halt("USDT margin unavailable")
            await self.entry_controller.expire()
            if self.emergency.targets:
                await self.emergency.step_all()
        except Exception as exc:
            await self.enter_halt("reconciliation failed")
            logger.error("reconciliation failed: %s", type(exc).__name__)

    def _unprotected_startup_partials(
        self,
        entries: list[dict[str, Any]],
        positions: dict[str, Decimal],
        algos: list[dict[str, Any]],
    ) -> set[str]:
        unprotected: set[str] = set()
        for entry in entries:
            symbol = entry["symbol"]
            quantity = positions.get(symbol, Decimal(0))
            direction = Decimal(1) if entry["direction"] == "LONG" else Decimal(-1)
            instrument = self.instruments.get(symbol)
            stop = entry.get("stop_price")
            protective = next(
                (
                    algo
                    for algo in algos
                    if algo.get("algoClOrdId") == entry.get("protective_algo_id")
                ),
                None,
            )
            if not (
                quantity * direction > 0
                and abs(quantity) >= Decimal(entry["filled"])
                and instrument is not None
                and stop
                and self.algo_manager.valid(
                    protective,
                    symbol=symbol,
                    position=quantity,
                    stop_price=Decimal(stop),
                    tolerance=instrument.tick_size,
                )
            ):
                unprotected.add(symbol)
        return unprotected

    async def _verify_protection(
        self, symbol: str, quantity: Decimal, algos: list[dict[str, Any]]
    ) -> bool:
        if symbol in self.emergency.targets:
            await self.emergency.step(symbol)
            return False
        entry = next(
            (
                row
                for row in reversed(list(self.order_manager.orders.values()))
                if row["symbol"] == symbol and not row["reduce_only"] and Decimal(row["filled"]) > 0
            ),
            None,
        )
        instrument = self.instruments.get(symbol)
        if entry is None or instrument is None or not entry.get("stop_price"):
            await self.enter_emergency(symbol, "protective stop cannot be verified")
            return False
        stop = Decimal(entry["stop_price"])
        match = next(
            (algo for algo in algos if algo.get("algoClOrdId") == entry.get("protective_algo_id")),
            None,
        )
        if self.algo_manager.valid(
            match, symbol=symbol, position=quantity, stop_price=stop, tolerance=instrument.tick_size
        ):
            if match is not None:
                await self._send_observation(self._notify_protection_active(symbol, entry, match))
            return True
        await self.enter_halt("protective stop invalid", emergency=True)
        await self.emergency.target(symbol)
        body = {
            "instId": symbol,
            "tdMode": "isolated",
            "posSide": "net",
            "side": "sell" if quantity > 0 else "buy",
            "ordType": "conditional",
            "sz": str(abs(quantity)),
            "slTriggerPx": str(stop),
            "slTriggerPxType": "mark",
            "slOrdPx": "-1",
            "reduceOnly": "true",
            "algoClOrdId": "a" + __import__("uuid").uuid4().hex[:30],
        }
        try:
            await self.client.place_algo(body)
            deadline = asyncio.get_running_loop().time() + self.settings.protection_confirm_seconds
            while asyncio.get_running_loop().time() < deadline:
                current = await self.client.pending_algos()
                replacement = next(
                    (a for a in current if a.get("algoClOrdId") == body["algoClOrdId"]), None
                )
                if replacement and self.algo_manager.valid(
                    replacement,
                    symbol=symbol,
                    position=quantity,
                    stop_price=stop,
                    tolerance=instrument.tick_size,
                ):
                    await self.algo_manager.ingest(replacement)
                    if match and match.get("state") == "live":
                        await self.client.cancel_algo(
                            symbol,
                            algo_id=match.get("algoId", ""),
                            client_algo_id=match.get("algoClOrdId", ""),
                        )
                        remaining_algos = await self.client.pending_algos()
                        if any(
                            a.get("algoClOrdId") == match.get("algoClOrdId")
                            for a in remaining_algos
                        ):
                            raise RuntimeError("invalid protective algo cancellation unconfirmed")
                    await self.order_manager.transition(
                        entry["clOrdId"],
                        OrderState(entry["state"]),
                        protective_algo_id=body["algoClOrdId"],
                    )
                    await self._send_observation(
                        self._notify_protection_active(symbol, entry, replacement)
                    )
                    self.emergency.targets.pop(symbol, None)
                    await self.store.append(
                        "emergency_targets",
                        {"symbol": symbol, "target": "0", "completed": True},
                        symbol=symbol,
                    )
                    return True
                await asyncio.sleep(0.5)
        except Exception:
            pass
        await self.emergency.step(symbol)
        return False

    async def _check_portfolio_limits(self, positions: list[dict[str, Any]]) -> None:
        reason, symbol = self.portfolio_monitor.breached(self.portfolio, positions)
        if symbol:
            await self.enter_emergency(symbol, reason)
        elif reason:
            await self.enter_halt(reason)

    async def start(self) -> None:
        if self.running:
            return
        if self.settings.mode != Mode.BACKTEST:
            try:
                self.notifications.start()
            except Exception as exc:
                logger.error("notification worker unavailable: %s", type(exc).__name__)
        self.started_at = utcnow()
        await self.initialize()
        if self.settings.mode == Mode.BACKTEST:
            return
        base = self.settings.ws_base
        self.sockets = [
            OkxWebSocket(
                base + "/public",
                public_subscriptions(self.settings.symbols),
                self._on_market,
                self.settings,
            ),
            OkxWebSocket(
                base + "/business",
                candle_subscriptions(self.settings.symbols, self.settings.timeframes),
                self._on_market,
                self.settings,
            ),
        ]
        if self.settings.has_credentials:
            self.sockets.append(
                OkxWebSocket(
                    base + "/private",
                    [
                        {"channel": "orders", "instType": "SWAP"},
                        {"channel": "positions", "instType": "SWAP"},
                        {"channel": "account"},
                    ],
                    self._on_private,
                    self.settings,
                    private=True,
                )
            )
            self.sockets.append(
                OkxWebSocket(
                    base + "/business",
                    [{"channel": "orders-algo", "instType": "ANY"}],
                    self._on_private,
                    self.settings,
                    private=True,
                )
            )
        self.running = True
        self.tasks = [asyncio.create_task(ws.run()) for ws in self.sockets]
        self.tasks += [
            asyncio.create_task(self._watchdog()),
            asyncio.create_task(self._reconcile_loop()),
            asyncio.create_task(self._daily_review_loop()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._safety_loop()),
        ]
        if self.settings.has_credentials:
            self.tasks.append(asyncio.create_task(self._dead_man_loop()))
        await self.store.append(
            "system_events", {"event": "start", "mode": self.settings.mode.value}
        )

    async def stop(self) -> None:
        await self.enter_halt("manual stop")
        if self.settings.has_credentials:
            deadline = (
                asyncio.get_running_loop().time() + self.settings.entry_cancel_confirm_seconds
            )
            while self.entry_controller.blocked and asyncio.get_running_loop().time() < deadline:
                await self.entry_controller.cancel_all()
                await asyncio.sleep(0.5)
            await self.reconcile()
            if (
                self.entry_controller.blocked
                or not self.reconciliation_healthy
                or self.emergency.targets
            ):
                await self.alert(
                    "ERROR", "Unsafe shutdown", "Cancellation or protection unconfirmed"
                )
                raise RuntimeError("shutdown cancellation or protection unconfirmed")
            pending = await self.client.pending_orders()
            if self.order_manager.pending_entries() or any(
                str(order.get("reduceOnly", "false")).lower() not in {"true", "1"}
                for order in pending
            ):
                raise RuntimeError("shutdown risk-increasing entry remains pending")
            if pending:
                self._caa_shutting_down = True
                try:
                    async with self._caa_lock:
                        await self.client.cancel_all_after(0)
                except Exception:
                    self._caa_shutting_down = False
                    raise RuntimeError("shutdown could not disable Cancel All After") from None
        elif (
            self.order_manager.pending_entries()
            or self.entry_controller.blocked
            or self.emergency.targets
            or self.portfolio.positions
            or any(size != 0 for size in self._audited_positions().values())
        ):
            raise RuntimeError(
                "shutdown positions or cancellations cannot be verified without credentials"
            )
        self.running = False
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.store.append("system_events", {"event": "stop"})
        await self.alert(
            "INFO", "⚪ Quant System Stopped", f"Mode: {self.settings.mode.value}",
            category=NotificationCategory.SYSTEM,
            dedup_key="system-stopped",
        )
        try:
            await self.notifications.stop(drain_seconds=3)
        except Exception as exc:
            logger.error("notification shutdown failed: %s", type(exc).__name__)

    async def close(self) -> None:
        if not self.running:
            try:
                await self.notifications.close()
            except Exception as exc:
                logger.error("notification close failed: %s", type(exc).__name__)
        if self.redis:
            await self.redis.aclose()
        await self.client.close()
        await self.store.close()

    def _audited_positions(self) -> dict[str, Decimal]:
        positions: dict[str, Decimal] = {}
        for row in self.order_manager.orders.values():
            signed = Decimal(row["filled"]) * (1 if row["direction"] == "LONG" else -1)
            positions[row["symbol"]] = positions.get(row["symbol"], Decimal(0)) + signed
        return positions

    @staticmethod
    def _short_symbol(symbol: str) -> str:
        return symbol.split("-")[0]

    @staticmethod
    def _positive_decimal(value: Any) -> Decimal | None:
        try:
            amount = Decimal(str(value))
            return amount if amount.is_finite() and amount > 0 else None
        except (ArithmeticError, TypeError, ValueError):
            return None

    async def _notify_entry_submitted(self, order: dict[str, Any]) -> None:
        symbol = order["symbol"]
        lines = [
            f"Side: {order['direction']}",
            f"Size: {order['approved_contracts']} contracts",
            f"Entry ref: {order['entry_reference']}",
            f"Stop: {order['stop_price']}",
        ]
        strategy = self._intent_strategies.get(order["intent_id"])
        if strategy:
            lines.insert(0, f"Strategy: {strategy}")
        await self.alert(
            "TRADE", f"📤 {self._short_symbol(symbol)} Entry Submitted",
            "\n".join(lines), category=NotificationCategory.TRADE,
            symbol=symbol, dedup_key=f"entry-submitted:{order['clOrdId']}",
        )

    async def _notify_partial_fill(self, order: dict[str, Any], *, protected: bool) -> None:
        symbol = order["symbol"]
        filled = Decimal(order["filled"])
        remaining = max(Decimal(0), Decimal(order["approved_contracts"]) - filled)
        await self.alert(
            "WARNING", f"⚠️ {self._short_symbol(symbol)} Partial Fill",
            f"Filled: {filled} contracts\nRemaining: {remaining} contracts"
            f"\nProtection: {'confirmed' if protected else 'pending / emergency'}",
            category=NotificationCategory.TRADE,
            priority=NotificationPriority.TIME_SENSITIVE,
            symbol=symbol, dedup_key=f"partial-fill:{order['clOrdId']}:{filled}",
        )

    async def _notify_entry_filled(
        self, order: dict[str, Any], exchange_event: dict[str, Any] | None = None
    ) -> None:
        client_id = order["clOrdId"]
        if client_id in self._entry_filled_announced:
            return
        self._entry_filled_announced.add(client_id)
        symbol = order["symbol"]
        lines = [f"Contracts: {order['filled']}"]
        price = (exchange_event or {}).get("avgPx") or (exchange_event or {}).get("fillPx")
        if self._positive_decimal(price) is not None:
            lines.insert(0, f"Entry: {price}")
            instrument = self.instruments.get(symbol)
            if instrument:
                if instrument.contract_currency == symbol.split("-")[0]:
                    notional = Decimal(order["filled"]) * instrument.contract_value * Decimal(str(price))
                    lines.append(f"Notional: {notional} USDT")
                elif instrument.contract_currency == "USD":
                    notional = Decimal(order["filled"]) * instrument.contract_value
                    lines.append(f"Notional: {notional} USD")
        else:
            lines.insert(0, f"Entry ref: {order['entry_reference']}")
        strategy = self._intent_strategies.get(order["intent_id"])
        if strategy:
            lines.append(f"Strategy: {strategy}")
        await self.alert(
            "TRADE", f"🟢 {self._short_symbol(symbol)} {order['direction']} Filled",
            "\n".join(lines), category=NotificationCategory.TRADE,
            priority=NotificationPriority.TIME_SENSITIVE,
            symbol=symbol, dedup_key=f"entry-filled:{client_id}",
        )

    async def _notify_protection_active(
        self, symbol: str, entry: dict[str, Any], algo: dict[str, Any]
    ) -> None:
        algo_id = str(algo.get("algoClOrdId") or "")
        if not algo_id or algo_id in self._protection_announced:
            return
        self._protection_announced.add(algo_id)
        await self.alert(
            "TRADE", f"🛡 {self._short_symbol(symbol)} Protection Active",
            f"Stop: {entry['stop_price']}\nCoverage: 100%",
            category=NotificationCategory.TRADE,
            priority=NotificationPriority.TIME_SENSITIVE,
            symbol=symbol, dedup_key=f"protection:{algo_id}",
        )

    async def _notify_position_closed(self, symbol: str) -> None:
        lines: list[str] = []
        exit_event = self._last_exit_fill.pop(symbol, {})
        exit_price = exit_event.get("avgPx") or exit_event.get("fillPx")
        if self._positive_decimal(exit_price) is not None:
            lines.append(f"Exit: {exit_price}")
        lines.append(f"Daily PnL: {self.portfolio.daily_pnl} USDT")
        await self.alert(
            "TRADE", f"💰 {self._short_symbol(symbol)} Position Closed",
            "\n".join(lines), category=NotificationCategory.TRADE,
            priority=NotificationPriority.TIME_SENSITIVE,
            symbol=symbol, dedup_key=f"position-closed:{symbol}:{utcnow().isoformat()}",
        )

    async def _on_market(self, message: dict[str, Any]) -> None:
        try:
            await self.market.handle(message)
            arg = message.get("arg", {})
            if arg.get("channel") == "candle15m":
                symbol = arg["instId"]
                for row in message.get("data", []):
                    candle_time = int(row[0])
                    if row[8] == "1" and candle_time > self.evaluated_candles.get(symbol, 0):
                        self.evaluated_candles[symbol] = candle_time
                        await self._evaluate(symbol)
        except Exception as exc:
            await self.enter_halt("market processing failed")
            logger.error("market processing failed: %s", type(exc).__name__)

    async def _evaluate(self, symbol: str) -> None:
        if not self.portfolio.synchronized or self.governor.state != GovernorState.NORMAL:
            return
        if symbol in self.entry_controller.blocked:
            return
        if any(
            row["symbol"] == symbol
            and row["state"]
            in {
                "CREATED",
                "SUBMITTED",
                "ACKNOWLEDGED",
                "PARTIALLY_FILLED",
                "UNKNOWN",
                "CANCEL_REQUESTED",
            }
            for row in self.order_manager.orders.values()
        ):
            return
        state = compute_features(
            self.market.candles[(symbol, "15m")],
            self.market.latest_books.get(symbol),
            self.market.derivative_context(
                symbol, self.market.candles[(symbol, "15m")][-1].timestamp + timedelta(minutes=15)
            ),
            self.market.latest_trades[symbol],
        )
        if state is None:
            return
        await self.store.append("features", state.model_dump(mode="json"), symbol=symbol)
        if self.redis is not None:
            await self.redis.set(f"features:{symbol}", state.model_dump_json(), ex=120)
        regime = classify(state, self.settings)
        await self.store.append("market_regimes", regime.model_dump(mode="json"), symbol=symbol)
        if self.redis is not None:
            await self.redis.set(f"regime:{symbol}", regime.model_dump_json(), ex=120)
        signals, intent = await self.decision_pipeline.decide(
            {
                timeframe: self.market.candles[(symbol, timeframe)]
                for timeframe in ("15m", "1H", "4H", "5m")
            },
            self.portfolio,
            main_state=state,
        )
        for signal in signals:
            await self.store.append(
                "signals", signal.model_dump(mode="json"), symbol=symbol, reference_id=signal.id
            )
            if self.redis is not None:
                await self.redis.set(f"signal:{symbol}", signal.model_dump_json(), ex=300)
            self.metrics.signals.inc()
        if intent is None or symbol not in self.instruments:
            return
        await self.store.append(
            "trade_intents", intent.model_dump(mode="json"), symbol=symbol, reference_id=intent.id
        )
        book = self.market.latest_books.get(symbol)
        tick = self.market.latest_ticks.get(symbol)
        now = utcnow()
        market_fresh = bool(
            book
            and tick
            and 0 <= (now - book.timestamp).total_seconds() < self.settings.stale_timeout_seconds
            and 0 <= (now - tick.timestamp).total_seconds() < self.settings.stale_timeout_seconds
        )
        spread = (
            (book.asks[0][0] - book.bids[0][0]) / book.asks[0][0]
            if book and book.asks and book.bids
            else Decimal(1)
        )
        decision = self.risk.evaluate(
            intent,
            self.portfolio,
            self.instruments[symbol],
            data_fresh=market_fresh and all(ws.is_fresh() for ws in self.sockets),
            infrastructure_healthy=self.store.healthy
            and self.redis is not None
            and self.dead_man_healthy,
            spread=spread,
        )
        if self.governor.state in {GovernorState.HALT, GovernorState.EMERGENCY}:
            await self.enter_halt(
                self.governor.reason, emergency=self.governor.state == GovernorState.EMERGENCY
            )
        await self.store.append(
            "risk_events", decision.model_dump(mode="json"), symbol=symbol, reference_id=intent.id
        )
        if decision.approved:
            try:
                request = ExecutionEngine.from_risk(
                    decision, self.instruments[symbol], signal_expires_at=intent.expires_at
                )
                self._intent_strategies[intent.id] = ", ".join(intent.strategies)
                await self.execution.submit(request)
                submitted = self.order_manager.orders[request.client_order_id]
                await self._send_observation(self._notify_entry_submitted(submitted))
                if submitted["state"] == "FILLED":
                    await self._send_observation(self._notify_entry_filled(submitted))
                    await self.reconcile()
                self.metrics.orders.inc()
            except Exception:
                self.metrics.order_errors.inc()
                raise

    async def _on_private(self, message: dict[str, Any]) -> None:
        channel = message.get("arg", {}).get("channel")
        if channel == "orders-algo":
            for row in message.get("data", []):
                try:
                    await self.algo_manager.ingest(row)
                except Exception:
                    await self.enter_halt("algo state audit failed", emergency=True)
                    await self.reconcile()
                    continue
                if not row.get("algoClOrdId"):
                    await self.enter_halt("unowned algo order")
                    continue
                if row.get("state") != "live" or row.get("failCode"):
                    symbol = row.get("instId", "")
                    if self.portfolio.positions.get(symbol):
                        await self.reconcile()
            return
        if channel == "orders":
            for row in message["data"]:
                try:
                    before = self.order_manager.orders.get(row.get("clOrdId", ""), {})
                    prior = before.get("state")
                    prior_filled = Decimal(before.get("filled") or "0")
                    expiry = before.get("signal_expires_at")
                    expiry_at = datetime.fromisoformat(expiry) if expiry else None
                    if expiry_at and expiry_at.tzinfo is None:
                        expiry_at = expiry_at.replace(tzinfo=utcnow().tzinfo)
                    expired = bool(expiry_at and expiry_at <= utcnow())
                    await self.order_manager.ingest(row)
                    local_after = self.order_manager.orders.get(row.get("clOrdId", ""))
                    if (
                        local_after and local_after["reduce_only"]
                        and self._positive_decimal(row.get("fillSz")) is not None
                    ):
                        self._last_exit_fill[local_after["symbol"]] = row
                    if row.get("state") in {"canceled", "mmp_canceled"}:
                        local_cancelled = self.order_manager.orders.get(row.get("clOrdId", ""))
                        if local_cancelled and not local_cancelled["reduce_only"]:
                            await self.entry_controller.confirm(local_cancelled)
                    if (
                        row.get("state") in {"partially_filled", "canceled"}
                        or (
                            row.get("state") == "filled"
                            and (
                                prior == "CANCEL_REQUESTED"
                                or expired
                                or self.governor.state != GovernorState.NORMAL
                            )
                        )
                    ) and Decimal(row.get("accFillSz") or "0") > 0:
                        client_id = row.get("clOrdId", "")
                        local = self.order_manager.orders.get(client_id)
                        if local and not local["reduce_only"]:
                            protected = await self._handle_partial_fill(client_id, local)
                            if (
                                row.get("state") in {"partially_filled", "canceled"}
                                and Decimal(local["filled"]) > prior_filled
                            ):
                                await self._send_observation(
                                    self._notify_partial_fill(local, protected=protected)
                                )
                    if row.get("state") == "filled" and before and not before["reduce_only"]:
                        if local_after:
                            await self._send_observation(
                                self._notify_entry_filled(local_after, row)
                            )
                        await self.reconcile()
                except Exception:
                    await self.enter_halt("unexpected order", emergency=True)
                    await self.reconcile()
        elif channel in {"positions", "account"}:
            await self.reconcile()

    async def _refresh_redis(self) -> None:
        if self.redis is None:
            candidate = Redis.from_url(self.settings.redis_url, decode_responses=True)
            try:
                await candidate.ping()
                self.redis = candidate
                self.market.redis = candidate
            except Exception:
                await candidate.aclose()
                await self.enter_halt("Redis unavailable")
                return
        try:
            await self.redis.ping()
            await self.redis.set("risk_state", self.governor.state.value, ex=120)
            await self.redis.set("system_state", "running" if self.running else "stopped", ex=120)
        except Exception:
            await self.enter_halt("Redis unavailable")
            await self.redis.aclose()
            self.redis = None
            self.market.redis = None

    async def _observe_component(
        self, name: str, healthy: bool, *, outage: str, recovery: str, threshold: int = 1
    ) -> None:
        previous = self._component_health.get(name)
        if previous is None:
            self._component_health[name] = healthy
            return
        if healthy == previous:
            self._component_pending.pop(name, None)
            return
        pending_state, count = self._component_pending.get(name, (healthy, 0))
        count = count + 1 if pending_state == healthy else 1
        if count < threshold:
            self._component_pending[name] = (healthy, count)
            return
        self._component_health[name] = healthy
        self._component_pending.pop(name, None)
        await self.alert(
            "INFO" if healthy else "WARNING",
            recovery if healthy else outage,
            f"Component: {name}\nStatus: {'recovered' if healthy else 'unavailable'}",
            category=NotificationCategory.INFRASTRUCTURE,
            dedup_key=f"infrastructure:{name}",
            metadata={"recovery": healthy},
        )

    async def _observe_infrastructure(self) -> None:
        await self._observe_component(
            "WebSocket", all(ws.is_fresh() for ws in self.sockets),
            outage="🚨 WebSocket Disconnected", recovery="✅ WS Recovered", threshold=2,
        )
        await self._observe_component(
            "Redis", self.redis is not None,
            outage="🚨 Redis Unavailable", recovery="✅ Redis Recovered",
        )
        await self._observe_component(
            "Reconciliation", self.reconciliation_healthy,
            outage="🚨 Reconciliation Unhealthy", recovery="✅ Reconciliation Recovered",
            threshold=2,
        )
        if self.settings.has_credentials:
            await self._observe_component(
                "Cancel-All-After", self.dead_man_healthy,
                outage="🚨 CAA Unavailable", recovery="✅ CAA Recovered",
            )
        reconnect_total = sum(ws.reconnects for ws in self.sockets)
        now = monotonic()
        self._reconnect_times.extend(
            [now] * max(0, reconnect_total - self._last_reconnect_total)
        )
        self._last_reconnect_total = reconnect_total
        while self._reconnect_times and now - self._reconnect_times[0] > 300:
            self._reconnect_times.popleft()
        if len(self._reconnect_times) >= 3 and now - self._last_reconnect_alert >= 300:
            self._last_reconnect_alert = now
            await self.alert(
                "ERROR", "🚨 Repeated WebSocket Reconnects",
                f"Reconnects in 5 minutes: {len(self._reconnect_times)}",
                category=NotificationCategory.INFRASTRUCTURE,
                dedup_key="ws-reconnect-flap",
            )

    async def _publish_started_if_ready(self) -> None:
        if self._started_notified or self.governor.state != GovernorState.NORMAL:
            return
        if not (
            self.portfolio.synchronized and self.reconciliation_healthy
            and self.store.healthy and self.redis is not None
            and self.dead_man_healthy and all(ws.is_fresh() for ws in self.sockets)
        ):
            return
        self._started_notified = True
        healthy_ws = sum(ws.is_fresh() for ws in self.sockets)
        await self.alert(
            "INFO", "🟢 Quant System Started",
            f"Mode: {self.settings.mode.value}\nEquity: {self.portfolio.equity} USDT"
            f"\nRisk: {self.governor.state.value}\nWS: {healthy_ws}/{len(self.sockets)}",
            category=NotificationCategory.SYSTEM,
            dedup_key="system-started",
        )

    async def _publish_heartbeat(self) -> None:
        ws_healthy = sum(ws.is_fresh() for ws in self.sockets)
        lines = [
            f"Mode: {self.settings.mode.value}",
            f"Uptime: {utcnow() - self.started_at}",
            f"Risk: {self.governor.state.value}",
        ]
        if self.governor.state != GovernorState.NORMAL:
            lines.append(f"Reason: {self.governor.reason}")
        lines.extend(
            [
                f"Equity: {self.portfolio.equity} USDT",
                f"Daily PnL: {self.portfolio.daily_pnl} USDT",
                f"Weekly DD: {self.portfolio.weekly_drawdown}",
                f"Positions: {len(self.portfolio.positions)}",
                f"Open Risk: {self.portfolio.open_risk} USDT",
                f"WS: {ws_healthy}/{len(self.sockets)} healthy",
                f"Reconnects: {sum(ws.reconnects for ws in self.sockets)}",
                f"Emergency targets: {len(self.emergency.targets)}",
            ]
        )
        await self.alert(
            "INFO" if self.governor.state == GovernorState.NORMAL else "WARNING",
            "❤️ Quant Heartbeat" if self.governor.state == GovernorState.NORMAL
            else "⚠️ Quant Heartbeat",
            "\n".join(lines), category=NotificationCategory.HEARTBEAT,
            priority=NotificationPriority.PASSIVE,
        )

    async def _heartbeat_loop(self) -> None:
        while self.running:
            await asyncio.sleep(self.settings.bark_heartbeat_hours * 3600)
            if self.running:
                await self._send_observation(self._publish_heartbeat())

    async def _watchdog(self) -> None:
        while self.running:
            await asyncio.sleep(5)
            await self._refresh_redis()
            self.metrics.update(
                self.portfolio,
                self.governor.state,
                {str(i): ws.reconnects for i, ws in enumerate(self.sockets)},
                stale=any(not ws.is_fresh() for ws in self.sockets),
                trade_count=len(self.order_manager.seen_trade_ids),
            )
            if any(not ws.is_fresh() for ws in self.sockets):
                await self.enter_halt("WebSocket disconnected or stale")
            elif (
                self.governor.reason == "WebSocket disconnected or stale"
                and self.portfolio.synchronized
                and self.governor.state != GovernorState.EMERGENCY
            ):
                await self.reconcile()
                self.governor.resume(
                    synchronized=self.portfolio.synchronized,
                    healthy=self.reconciliation_healthy
                    and self.store.healthy
                    and self.redis is not None
                    and self.dead_man_healthy
                    and not self.entry_controller.blocked
                    and not self.emergency.targets
                    and not self.portfolio_monitor.breached(self.portfolio, [])[0]
                    and all(ws.is_fresh() for ws in self.sockets),
                )
            elif (
                self.governor.reason == "startup reconciliation pending"
                and self.governor.state != GovernorState.EMERGENCY
            ):
                self.governor.resume(
                    synchronized=self.portfolio.synchronized,
                    healthy=self.reconciliation_healthy
                    and self.store.healthy
                    and self.redis is not None
                    and self.dead_man_healthy
                    and not self.entry_controller.blocked
                    and not self.emergency.targets
                    and not self.portfolio_monitor.breached(self.portfolio, [])[0]
                    and all(ws.is_fresh() for ws in self.sockets),
                )
            await self._send_observation(self._notify_risk_state())
            await self._send_observation(self._observe_infrastructure())
            await self._send_observation(self._publish_started_if_ready())

    async def _reconcile_loop(self) -> None:
        while self.running:
            await asyncio.sleep(self.settings.reconcile_interval_seconds)
            if self.settings.has_credentials:
                await self.reconcile()

    async def _dead_man_loop(self) -> None:
        while self.running:
            try:
                async with self._caa_lock:
                    if self._caa_shutting_down:
                        return
                    await self.client.cancel_all_after(self.settings.cancel_all_after_seconds)
                self.dead_man_healthy = True
            except Exception:
                self.dead_man_healthy = False
                await self.enter_halt("dead man switch unavailable")
            await asyncio.sleep(self.settings.cancel_all_after_refresh_seconds)

    async def _handle_partial_fill(self, client_id: str, order: dict) -> bool:
        self.emergency.client = self.client
        await self.enter_halt("partial fill before protective stop active", emergency=True)
        try:
            algos = await self.client.pending_algos()
            remote = next(
                (algo for algo in algos if algo.get("algoClOrdId") == order["protective_algo_id"]),
                None,
            )
            instrument = self.instruments.get(order["symbol"])
            if instrument and self.algo_manager.valid(
                remote,
                symbol=order["symbol"],
                position=Decimal(order["filled"]) * (1 if order["direction"] == "LONG" else -1),
                stop_price=Decimal(order["stop_price"]),
                tolerance=instrument.tick_size,
            ):
                await self.store.append(
                    "risk_events",
                    {
                        "event": "protective algo confirmed",
                        "algoClOrdId": order["protective_algo_id"],
                    },
                )
                if remote is not None:
                    await self._send_observation(
                        self._notify_protection_active(order["symbol"], order, remote)
                    )
                return True
        except Exception:
            pass
        await self.emergency.target(order["symbol"])
        await self.emergency.step(order["symbol"])
        return False

    async def _safety_loop(self) -> None:
        while self.running:
            try:
                await self.entry_controller.expire()
                await self.emergency.step_all()
                if self.portfolio.synchronized:
                    await self._check_portfolio_limits([])
            except Exception as exc:
                await self.enter_halt("safety monitor failure", emergency=True)
                logger.error("safety monitor failed: %s", type(exc).__name__)
            await asyncio.sleep(1)

    async def _daily_review_loop(self) -> None:
        while self.running:
            yesterday = (utcnow() - timedelta(days=1)).date()
            try:
                recent = await self.store.latest("daily_reports", limit=7)
                if not any(row.get("date") == yesterday.isoformat() for row in recent):
                    report = await save_daily_review(self.store, yesterday)
                    await self._send_observation(self._publish_daily_report(report))
            except Exception as exc:
                logger.error("daily review failed: %s", type(exc).__name__)
            await asyncio.sleep(3600)

    async def _publish_daily_report(self, report: dict[str, Any]) -> None:
        fields = (
            ("Equity", "equity_end"),
            ("Daily PnL", "equity_change"),
            ("Orders", "orders"),
            ("Fills", "fills"),
            ("Fees", "fees"),
            ("Max DD", "max_drawdown"),
            ("HALT count", "halt_count"),
            ("EMERGENCY count", "emergency_count"),
        )
        lines = [f"Date: {report['date']}"]
        lines.extend(
            f"{label}: {report[key]}" for label, key in fields
            if report.get(key) is not None
        )
        if report.get("realized_pnl_after_fees") is not None:
            lines.append(f"Realized PnL after fees: {report['realized_pnl_after_fees']}")
        await self.alert(
            "INFO", "📊 Daily Trading Report", "\n".join(lines),
            category=NotificationCategory.DAILY_REPORT,
            priority=NotificationPriority.PASSIVE,
            dedup_key=f"daily-report:{report['date']}",
        )
