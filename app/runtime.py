from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from collections.abc import Awaitable
from datetime import datetime, timedelta
from decimal import Decimal
from functools import partial
from time import monotonic
from typing import Any

from redis.asyncio import Redis

from app.config import Mode, Settings
from app.daily_review import save_daily_review
from app.decision import DecisionPipeline
from app.execution import ExecutionEngine, OrderManager
from app.features import compute_features
from app.fill_identity import equivalent_fill, fill_key
from app.incidents import IncidentManager
from app.ledger_repair import LedgerRepairResult, LedgerRepairService
from app.live_lease import LiveLeaseError, LiveRuntimeLease
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
from app.notification_policy import NotificationPolicy
from app.notifications import BarkNotification, NotificationManager
from app.okx import (
    OkxError,
    OkxRestClient,
    OkxWebSocket,
    candle_subscriptions,
    is_retryable_okx_error,
    public_subscriptions,
    safe_reconciliation_diagnostics,
)
from app.portfolio import summarize_positions
from app.recovery import HaltClass, classify_halt_reason
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
from app.terminal_fills import (
    TerminalFillError,
    read_terminal_fills,
    validate_fill,
    validate_fill_set,
)

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
        self.live_lease: LiveRuntimeLease | None = None
        self._lease_loss_task: asyncio.Task[None] | None = None
        self.client.live_writer_guard = self._live_writer_guard
        self.risk = RiskEngine(settings, self.governor)
        self.order_manager = OrderManager(self.store)
        self.order_manager.terminal_fill_recovery = self._recover_terminal_order
        self.entry_action_lock = asyncio.Lock()
        self.execution = ExecutionEngine(
            self.client,
            self.order_manager,
            lambda: self.governor.state == GovernorState.NORMAL and (
                self.settings.mode != Mode.LIVE or
                (self.live_lease is not None and self.live_lease.held)
            ),
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
        self._notification_remote_exposure = False
        self.sockets: list[OkxWebSocket] = []
        self._ws_diagnostic_state: dict[str, tuple[Any, ...]] = {}
        self.tasks: list[asyncio.Task[Any]] = []
        self.running = False
        self.evaluated_candles: dict[str, int] = {}
        self.dead_man_healthy = False
        self._last_caa_success_at: float | None = None
        self._caa_lock = asyncio.Lock()
        self._caa_shutting_down = False
        self.reconciliation_healthy = False
        self._reconciliation_observed_result = False
        self._reconciliation_completed_at: float | None = None
        self._last_reconcile_safe = False
        self._reconcile_lock = asyncio.Lock()
        self._ledger_repair_active = False
        self._ledger_repair_attempted = False
        self._ledger_repair_manual_hold = False
        self.ledger_repair_result: LedgerRepairResult | None = None
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
            policy=NotificationPolicy(
                heartbeat_enabled=settings.bark_heartbeat_enabled,
                entry_submitted=settings.bark_notify_entry_submitted,
                system_stopping=settings.bark_notify_system_stopping,
                trade_enabled=settings.bark_trade_notifications,
                risk_enabled=settings.bark_risk_notifications,
                daily_enabled=settings.bark_daily_report,
                webhook_verbose=settings.webhook_notifications_verbose,
            ),
            incidents=IncidentManager(
                delay_seconds=settings.bark_infra_alert_delay_seconds,
                merge_window_seconds=settings.bark_incident_merge_window_seconds,
                notify_fast_recovery=settings.bark_notify_fast_recovery,
                retry_initial_seconds=settings.bark_incident_retry_initial_seconds,
                retry_max_seconds=settings.bark_incident_retry_max_seconds,
            ),
        )
        self.started_at = utcnow()
        self._started_notified = False
        self._planned_shutdown = False
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
        self._clock = monotonic
        self._auto_recovery_reason: str | None = None
        self._auto_recovery_started_at = 0.0
        self._auto_recovery_last_check = 0.0
        self._auto_recovery_successes = 0
        self._auto_recovery_forbidden = False
        self._auto_recovery_circuit_breaker = False
        self._auto_resume_times: deque[float] = deque()
        self._auto_recovery_flaps: deque[float] = deque()
        self._auto_recovery_notice: tuple[str, float] | None = None
        self._ws_safety_tasks: dict[str, asyncio.Task[None]] = {}
        self._ws_reconcile_requested: set[str] = set()

    def _halt_state(self, reason: str, *, emergency: bool = False) -> tuple[str, bool, bool]:
        self.entry_controller.client = self.client
        emergency = emergency or self.governor.state == GovernorState.EMERGENCY
        if (
            self._auto_recovery_notice is not None
            and reason == self._auto_recovery_notice[0]
            and self._clock() < self._auto_recovery_notice[1]
        ):
            self._auto_recovery_flaps.append(self._clock())
        if (
            self.governor.state == GovernorState.HALT
            and self.governor.reason != "startup reconciliation pending"
            and classify_halt_reason(self.governor.reason) != HaltClass.TRANSIENT_INFRA
        ):
            self._auto_recovery_forbidden = True
        transient = classify_halt_reason(reason) == HaltClass.TRANSIENT_INFRA
        if self._auto_recovery_forbidden and transient and not emergency:
            reason = self.governor.reason
            transient = False
        changed = self.governor.state != GovernorState.HALT or self.governor.reason != reason
        if emergency or not transient or self._planned_shutdown:
            self._auto_recovery_forbidden = True
            self._auto_recovery_reason = None
            self._auto_recovery_successes = 0
        elif changed or self._auto_recovery_reason is None:
            self._auto_recovery_reason = reason
            self._auto_recovery_started_at = self._clock()
            self._auto_recovery_last_check = self._auto_recovery_started_at
            self._auto_recovery_successes = 0
        if changed:
            self._auto_recovery_notice = None
        repeated = (
            self.governor.reason == reason
            and self.governor.state in {GovernorState.HALT, GovernorState.EMERGENCY}
            and (not emergency or self.governor.state == GovernorState.EMERGENCY)
        )
        self.governor.halt(reason, emergency=emergency)
        return reason, repeated, emergency

    async def enter_halt(self, reason: str, *, emergency: bool = False) -> None:
        reason, repeated, emergency = self._halt_state(reason, emergency=emergency)
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
        self._auto_recovery_forbidden = True
        self._auto_recovery_reason = None
        self._auto_recovery_successes = 0
        self._auto_recovery_notice = None
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
                    priority=priority,
                    metadata={
                        "risk_state": self.governor.state.value,
                        "has_exposure": (
                            self._notification_remote_exposure
                            or any(size != 0 for size in self.portfolio.positions.values())
                        ),
                        **(metadata or {}),
                    },
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
        if self._planned_shutdown and current == (GovernorState.HALT, "manual stop"):
            self._last_risk_notice = current
            return
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
            "LIVE writer lease lost",
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
            metadata={"transition": True, "ws_incident_managed": bool(self.sockets)},
        )

    def _lease_lost(self) -> None:
        # Driver termination callback fences entries synchronously, before any await.
        self.governor.halt("LIVE writer lease lost")
        self.dead_man_healthy = False
        self._last_caa_success_at = None
        self._auto_recovery_forbidden = True
        self._auto_recovery_reason = None
        if self._lease_loss_task is None or self._lease_loss_task.done():
            self._lease_loss_task = asyncio.create_task(
                self._handle_lease_loss()
            )

    async def _handle_lease_loss(self) -> None:
        # Observation comes first; persistence and cancellation cannot delay the alert.
        await self._send_observation(self._notify_risk_state())
        try:
            async with asyncio.timeout(0.5):
                await self.store.append("risk_events", {
                    "event": "enter_halt", "reason": "LIVE writer lease lost",
                    "emergency": self.governor.state == GovernorState.EMERGENCY,
                })
        except Exception:
            logger.error("LIVE lease loss audit unavailable")
        try:
            await self.enter_halt("LIVE writer lease lost")
        except Exception as exc:
            logger.error("LIVE lease loss cancellation unconfirmed: %s", type(exc).__name__)

    async def _live_writer_guard(self, entry: bool) -> bool:
        if self.live_lease is None or not await self.live_lease.verify():
            return False
        return not entry or self.governor.state == GovernorState.NORMAL

    async def _caa_owner_held(self) -> bool:
        if self.settings.mode != Mode.LIVE:
            return True
        if await self._live_writer_guard(False):
            return True
        self.dead_man_healthy = False
        self._last_caa_success_at = None
        if self.governor.reason != "LIVE writer lease lost":
            self._lease_lost()
        return False

    async def _lease_monitor(self) -> None:
        while self.running and self.live_lease is not None:
            if not await self.live_lease.verify():
                return
            await asyncio.sleep(1)

    async def initialize(self) -> None:
        try:
            await self._initialize()
        except BaseException:
            if self.live_lease is not None:
                await self.live_lease.close()
            raise

    async def _initialize(self) -> None:
        await self.store.initialize()
        if self.settings.mode == Mode.LIVE:
            # Verify the target account before restored cancellation/emergency actions.
            await self.client.account_config()
            self.live_lease = LiveRuntimeLease(
                self.settings.live_lease_database_url.get_secret_value(),
                self.settings.confirm_live_account_id, self._lease_lost,
            )
            await self.live_lease.acquire()
            await self.store.bind_live_account(self.settings.confirm_live_account_id)
            if not await self.live_lease.verify():
                raise LiveLeaseError("LIVE writer lease lost")
            # Restarting LIVE must never bypass an earlier safety/manual HALT.
            # Position recovery runs, but entry requires an explicit, fully checked resume.
            self.governor.halt("LIVE startup requires manual resume")
            self._auto_recovery_forbidden = True
        await self.order_manager.restore()
        await self.algo_manager.restore()
        await self.emergency.restore()
        hold = await self.store.ledger_repair_hold()
        if hold:
            self._ledger_repair_manual_hold = True
            await self.enter_halt("ledger repaired; manual resume required",
                                  emergency=bool(hold.get("emergency")))
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
        async with self._reconcile_lock:
            await self._run_reconciliation()

    async def _reconcile_and_assess(self) -> tuple[bool, list[str]]:
        async with self._reconcile_lock:
            await self._run_reconciliation()
            return await self._resume_health()

    async def _run_reconciliation(self) -> None:
        completed = False
        epochs = [(ws, ws.generation) for ws in self.sockets
                  if ws.private and ws.is_transport_healthy() and ws.business_idle]
        try:
            await self._reconcile_impl()
            completed = True
        finally:
            # Observation uses a completed result; trading gates retain their in-flight False.
            self._reconciliation_observed_result = completed and self.reconciliation_healthy
            self._reconciliation_completed_at = self._clock()
            if completed and self.reconciliation_healthy and self._last_reconcile_safe:
                for ws, generation in epochs:
                    if ws.generation == generation and ws.is_transport_healthy() and ws.business_idle:
                        ws.reconciliation_required = False
                        ws.processing_unsafe = False

    def _reconciliation_notification_health(self) -> bool:
        max_age = max(
            60.0, self.settings.reconcile_interval_seconds * 2,
            self.settings.request_timeout_seconds * 4,
        )
        return (
            self._reconciliation_observed_result
            and self._reconciliation_completed_at is not None
            and self._clock() - self._reconciliation_completed_at <= max_age
        )

    def _entry_block_settled(self, entry: dict[str, Any], pending: list[dict[str, Any]]) -> bool:
        symbol = entry["symbol"]
        quantity = self.portfolio.positions.get(symbol, Decimal(0))
        instrument = self.instruments.get(symbol)
        covered = quantity == 0 or (
            instrument is not None and bool(entry.get("stop_price"))
            and self.algo_manager.valid(
                self.algo_manager.algos.get(entry.get("protective_algo_id", "")),
                symbol=symbol, position=quantity,
                stop_price=Decimal(entry["stop_price"]), tolerance=instrument.tick_size,
            )
        )
        return (
            self.governor.state in {GovernorState.HALT, GovernorState.EMERGENCY}
            and self._last_reconcile_safe and not self.emergency.targets and covered
            and not any(row["symbol"] == symbol for row in self.order_manager.pending_entries())
            and not any(
                row.get("instId") == symbol
                and str(row.get("reduceOnly", "false")).lower() not in {"true", "1"}
                for row in pending
            )
            and {key: value for key, value in self._audited_positions().items() if value}
            == self.portfolio.positions
        )

    async def _release_reconciled_entry_blocks(self, pending: list[dict[str, Any]]) -> None:
        """Release cancellation bookkeeping only; never resume the Governor here."""
        if self.governor.state not in {GovernorState.HALT, GovernorState.EMERGENCY}:
            return
        for symbol in tuple(self.entry_controller.blocked):
            entry = next((
                row for row in reversed(list(self.order_manager.orders.values()))
                if row["symbol"] == symbol and not row["reduce_only"]
            ), None)
            if (
                entry is None or entry["state"] not in {"FILLED", "CANCELLED"}
                or Decimal(entry["filled"]) <= 0 or not self._entry_block_settled(entry, pending)
            ):
                continue
            try:
                details = await self.client.order(symbol, entry["clOrdId"])
                if (
                    len(details) != 1 or not self._verified_terminal_order(entry, details[0])
                    or Decimal(str(details[0]["accFillSz"])) != Decimal(entry["filled"])
                    or not self._entry_block_settled(entry, pending)
                ):
                    continue
                await self.store.append("risk_events", {
                    "event": "entry block release verified", "symbol": symbol,
                    "clOrdId": entry["clOrdId"], "filled": entry["filled"],
                    "risk_state": self.governor.state.value,
                })
                if (
                    self._entry_block_settled(entry, pending)
                    and self._verified_terminal_order(entry, details[0])
                    and Decimal(str(details[0]["accFillSz"])) == Decimal(entry["filled"])
                ):
                    self.entry_controller.blocked.discard(symbol)
            except Exception as exc:
                logger.warning("entry block recovery deferred error_type=%s", type(exc).__name__)

    def _pending_order_ids(self) -> set[str]:
        return {
            cid for cid, row in self.order_manager.orders.items()
            if row["state"] in {"SUBMITTED", "ACKNOWLEDGED", "PARTIALLY_FILLED"}
        }

    @staticmethod
    def _verified_terminal_order(local: dict[str, Any], remote: dict[str, Any]) -> bool:
        """Only exchange-confirmed owned terminal orders can resolve a snapshot race."""
        if (
            remote.get("clOrdId") != local["clOrdId"]
            or remote.get("instId") != local["symbol"]
            or remote.get("side") != ("buy" if local["direction"] == "LONG" else "sell")
            or str(remote.get("reduceOnly", "")).lower()
            != ("true" if local["reduce_only"] else "false")
            or not remote.get("ordId")
            or (local.get("order_id") and remote["ordId"] != local["order_id"])
            or remote.get("state") not in {"filled", "canceled", "mmp_canceled"}
        ):
            return False
        try:
            size = Decimal(str(remote["sz"]))
            filled = Decimal(str(remote["accFillSz"]))
            previous = Decimal(local["filled"])
            approved = Decimal(local["approved_contracts"])
            return (
                all(value.is_finite() for value in (size, filled, previous, approved))
                and size == approved and size > 0 and 0 <= previous <= filled <= size
                and (remote["state"] != "filled" or filled == size)
            )
        except (KeyError, ArithmeticError, ValueError):
            return False

    async def _refresh_terminal_order_snapshot(
        self, account: list[dict[str, Any]], positions: list[dict[str, Any]],
        orders: list[dict[str, Any]], algos: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        snapshot = (account, positions, orders, algos)
        if not self.portfolio.synchronized:
            return snapshot
        difference = self._pending_order_ids() ^ {row.get("clOrdId", "") for row in orders}
        if not difference:
            return snapshot
        # Never turn foreign orders or unconfirmed active orders into an authorized delta.
        verified: list[dict[str, Any]] = []
        for client_id in sorted(difference):
            local = self.order_manager.orders.get(client_id)
            if local is None:
                return snapshot
            details = await self.client.order(local["symbol"], client_id)
            if len(details) != 1 or not self._verified_terminal_order(local, details[0]):
                return snapshot
            verified.append(details[0])
        for detail in verified:
            local = self.order_manager.orders[detail["clOrdId"]]
            if not self._verified_terminal_order(local, detail):
                return snapshot
            await self._recover_terminal_order(local, detail)
        # Fills may have changed positions and protection since the initial parallel reads.
        # One bounded refresh; the usual identity, position and protection gates still run.
        refreshed = await asyncio.gather(
            self.client.account(), self.client.positions(),
            self.client.pending_orders(), self.client.pending_algos(),
        )
        return refreshed[0], refreshed[1], refreshed[2], refreshed[3]

    async def _recover_terminal_order(self, local: dict, detail: dict) -> None:
        if not self._verified_terminal_order(local, detail):
            raise TerminalFillError("terminal_fill_identity_mismatch")
        evidence = await read_terminal_fills(self.client, detail)
        await self._append_terminal_fills(detail, evidence)

    async def _append_terminal_fills(
        self, detail: dict[str, Any], evidence: list[dict[str, Any]],
    ) -> None:
        evidence = [validate_fill(row, detail) for row in evidence]
        validate_fill_set(evidence, detail)
        manager = self.order_manager
        async with manager.ledger_lock:
            client_id, symbol = detail["clOrdId"], detail["instId"]
            local = manager.orders.get(client_id)
            if local is None or not self._verified_terminal_order(local, detail):
                raise TerminalFillError("terminal_fill_identity_mismatch")
            terminal_state = "FILLED" if detail["state"] == "filled" else "CANCELLED"
            if local["state"] in {"FILLED", "CANCELLED", "REJECTED"} and local["state"] != terminal_state:
                raise TerminalFillError("terminal_fill_evidence_conflict")
            snapshots = {
                table: await self.store.ledger_snapshot(table, {symbol})
                for table in ("orders", "order_events", "fills")
            }
            if len([row for row in snapshots["orders"] if row.get("clOrdId") == client_id]) != 1:
                raise TerminalFillError("terminal_fill_identity_mismatch")
            remote = {fill_key(row["instId"], row["tradeId"]): row for row in evidence}
            existing = {
                fill_key(row.get("instId"), row.get("tradeId")): row
                for row in snapshots["fills"]
                if row.get("ordId") == detail["ordId"] or row.get("clOrdId") == client_id
            }
            if not existing.keys() <= remote.keys():
                raise TerminalFillError("terminal_fill_evidence_conflict")
            records: list[tuple[str, dict[str, Any], str]] = []
            for key, canonical in remote.items():
                stored = await self.store.fill_for_key(key)
                if stored is not None:
                    if not equivalent_fill(stored, canonical):
                        raise TerminalFillError("terminal_fill_evidence_conflict")
                else:
                    if key in manager.seen_trade_ids:
                        raise TerminalFillError("terminal_fill_evidence_conflict")
                    payload = {**canonical, "fill_evidence_source": "okx_fills_history"}
                    records.append(("fills", payload, key[1]))
            terminal = {
                **local,
                "state": terminal_state,
                "filled": detail["accFillSz"], "order_id": detail["ordId"],
                "order_fee": detail.get("fee", ""),
                "fill_evidence_source": "okx_fills_history",
            }
            if terminal != local:
                records.append(("order_events", terminal, client_id))
            if records:
                # Existing durable ledger transaction: all missing fills and the
                # terminal event commit together; never use public market_batch.
                await self.store.append_ledger_recovery(records, snapshots, {symbol})
            # No in-memory advance before durable success. Preserve reconciled_filled
            # so the existing position delta reconciliation still sees new fills.
            local.update(terminal)
            manager.seen_trade_ids.update(remote)

    async def _reconcile_impl(self) -> None:
        self.entry_controller.client = self.client
        self.emergency.client = self.client
        self.reconciliation_healthy = False
        self._last_reconcile_safe = False
        prior_positions = dict(self.portfolio.positions) if self.portfolio.synchronized else {}
        read_phase = True
        try:
            account, positions, orders, algos = await asyncio.gather(
                self.client.account(),
                self.client.positions(),
                self.client.pending_orders(),
                self.client.pending_algos(),
            )
            config = await self.client.account_config()
            account, positions, orders, algos = await self._refresh_terminal_order_snapshot(
                account, positions, orders, algos
            )
            read_phase = False
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
            self._notification_remote_exposure = bool(remote_positions)
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
                self._notification_remote_exposure = bool(remote_positions)
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
                    await self._repair_position_ledger("startup position mismatch", remote_positions)
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
                    await self._repair_position_ledger("position mismatch", remote_positions)
                    return
            remote_algo_ids = {algo.get("algoClOrdId") for algo in algos}
            known_algos = {
                row.get("protective_algo_id") for row in self.order_manager.orders.values()
            }
            if any(not algo_id or algo_id not in known_algos for algo_id in remote_algo_ids):
                await self.enter_halt("unexpected algo order")
                return
            if self.portfolio.synchronized and {o.get("clOrdId") for o in orders} != self._pending_order_ids():
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
            if await self._check_portfolio_limits(positions):
                return
            if not derivatives_mode_eligible:
                await self.enter_halt("derivatives account mode required")
                return
            if equity <= 0 or available <= 0:
                await self.enter_halt("USDT margin unavailable")
                return
            await self.entry_controller.expire()
            if self.emergency.targets:
                await self.emergency.step_all()
            self._last_reconcile_safe = True
            await self._release_reconciled_entry_blocks(orders)
        except OkxError as exc:
            operation, endpoint, code, status, error_type = safe_reconciliation_diagnostics(exc)
            reason = (
                "reconciliation failed" if read_phase and is_retryable_okx_error(exc)
                else "reconciliation permanent failure"
            )
            await self.enter_halt(reason)
            logger.error(
                "reconciliation OKX failure operation=%s endpoint=%s code=%s "
                "http_status=%s retryable=%s error_type=%s",
                operation, endpoint, code, status, exc.retryable, error_type,
                extra={"error_code": exc.reason_code if isinstance(exc, TerminalFillError) else None},
            )
        except (TimeoutError, ConnectionError, OSError) as exc:
            await self.enter_halt(
                "reconciliation failed" if read_phase else "reconciliation permanent failure"
            )
            logger.error(
                "reconciliation transport failure error_type=%s read_phase=%s",
                type(exc).__name__, read_phase,
            )
        except Exception as exc:
            await self.enter_halt("reconciliation permanent failure")
            logger.error("reconciliation failure error_type=%s", type(exc).__name__)

    async def _repair_position_ledger(
        self, reason: str, remote_positions: dict[str, Decimal],
    ) -> None:
        # Already HALTed by the caller. Never retry indefinitely during the same incident.
        if self._ledger_repair_active or self._ledger_repair_attempted:
            return
        local = {k: v for k, v in self._audited_positions().items() if v}
        symbols = {s for s in set(local) | set(remote_positions)
                   if local.get(s, Decimal(0)) != remote_positions.get(s, Decimal(0))}
        if not symbols:
            return  # A portfolio snapshot discrepancy is not evidence of a missing ledger fill.
        self._ledger_repair_attempted = True
        self._auto_recovery_forbidden = True
        self._ledger_repair_active = True
        try:
            service = LedgerRepairService(self.client, self.order_manager, self.store)
            result = await service.repair(
                symbols, emergency=self.governor.state == GovernorState.EMERGENCY,
            )
            self._ledger_repair_manual_hold = bool(await self.store.ledger_repair_hold())
            self.ledger_repair_result = result
            if result.evidence_complete:
                # Re-fetch account, positions, pending orders and algos; run all existing safety gates.
                self.portfolio.synchronized = False
                await self._reconcile_impl()
                audited = {k: v for k, v in self._audited_positions().items() if v}
                result.repaired = bool(
                    self.reconciliation_healthy and self._last_reconcile_safe
                    and self.portfolio.synchronized and audited == self.portfolio.positions
                    and not self.emergency.targets and not self.order_manager.pending_entries()
                )
                if not result.repaired:
                    result.reason = "ledger repair full reconciliation incomplete"
                    result.unresolved.append(result.reason)
            await self.enter_halt(
                "ledger repaired; manual resume required" if result.repaired
                else result.reason or "ledger repair evidence incomplete"
            )
            await self.store.append("system_events", {
                "event": "ledger_repair_result", "trigger": reason, **result.report(),
            })
            title = "⚠️ 账本已从 OKX 成交记录修复" if result.repaired else "🚨 账本自动修复失败"
            await self.alert(
                "WARNING" if result.repaired else "ERROR", title,
                (f"Symbols: {', '.join(sorted(symbols))}\n"
                         f"Recovered fills: {result.fills_added}\n"
                         f"Reason: {result.reason or 'full reconciliation verified'}\n"
                         "New entries: blocked\nManual resume required"),
                category=NotificationCategory.RISK, priority=NotificationPriority.TIME_SENSITIVE,
                dedup_key="ledger-repair-result",
            )
        finally:
            self._ledger_repair_active = False

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

    async def _check_portfolio_limits(self, positions: list[dict[str, Any]]) -> bool:
        reason, symbol = self.portfolio_monitor.breached(self.portfolio, positions)
        if symbol:
            await self.enter_emergency(symbol, reason)
        elif reason:
            await self.enter_halt(reason)
        return bool(reason)

    async def start(self) -> None:
        if self.running:
            return
        if self.settings.mode == Mode.PAPER:
            try:
                self.notifications.start()
            except Exception as exc:
                logger.error("notification worker unavailable: %s", type(exc).__name__)
        self.started_at = utcnow()
        await self.initialize()
        if self.settings.mode == Mode.LIVE:
            try:
                self.notifications.start()
            except Exception as exc:
                logger.error("notification worker unavailable: %s", type(exc).__name__)
        if self.settings.mode == Mode.BACKTEST:
            return
        base = self.settings.ws_base
        self.sockets = [
            OkxWebSocket(
                base + "/public",
                public_subscriptions(self.settings.symbols),
                self._on_market,
                self.settings, name="public-market", metrics=self.metrics,
                on_fault=self._on_ws_fault, on_ready=self._on_ws_ready,
                batch_handler=self._on_public_market_batch,
            ),
            OkxWebSocket(
                base + "/business",
                candle_subscriptions(self.settings.symbols, self.settings.timeframes),
                self._on_market,
                self.settings, name="business-candles", metrics=self.metrics,
                on_fault=self._on_ws_fault, on_ready=self._on_ws_ready,
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
                    private=True, name="private-account", metrics=self.metrics,
                    on_fault=self._on_ws_fault, on_ready=self._on_ws_ready,
                )
            )
            self.sockets.append(
                OkxWebSocket(
                    base + "/business",
                    [{"channel": "orders-algo", "instType": "ANY"}],
                    self._on_private,
                    self.settings,
                    private=True, name="private-algo", metrics=self.metrics,
                    on_fault=self._on_ws_fault, on_ready=self._on_ws_ready,
                )
            )
        self.running = True
        self.tasks = [asyncio.create_task(ws.run()) for ws in self.sockets]
        for ws, task in zip(self.sockets, self.tasks, strict=True):
            task.add_done_callback(partial(self._on_ws_run_done, ws))
        self.tasks += [
            asyncio.create_task(self._watchdog()),
            asyncio.create_task(self._auto_recovery_loop()),
            asyncio.create_task(self._reconcile_loop()),
            asyncio.create_task(self._daily_review_loop()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._safety_loop()),
        ]
        if self.settings.mode == Mode.LIVE:
            self.tasks.append(asyncio.create_task(self._lease_monitor()))
        if self.settings.has_credentials:
            self.tasks.append(asyncio.create_task(self._dead_man_loop()))
        await self.store.append(
            "system_events", {"event": "start", "mode": self.settings.mode.value}
        )

    async def stop(self) -> None:
        self._planned_shutdown = True
        await self.alert(
            "INFO", "🟡 Quant System Stopping",
            f"Mode: {self.settings.mode.value}\nReason: manual stop",
            category=NotificationCategory.SYSTEM,
            dedup_key="system-stopping",
        )
        try:
            await self._safe_stop()
        except BaseException:
            self._planned_shutdown = False
            self._last_risk_notice = None
            await self._send_observation(self._notify_risk_state())
            raise
        self._planned_shutdown = False
        await self.alert(
            "INFO", "⚪ Quant System Stopped", f"Mode: {self.settings.mode.value}",
            category=NotificationCategory.SYSTEM,
            dedup_key="system-stopped",
        )
        try:
            await self.notifications.stop(drain_seconds=3)
        except Exception as exc:
            logger.error("notification shutdown failed: %s", type(exc).__name__)

    async def _safe_stop(self) -> None:
        owner = await self._caa_owner_held()
        await self.enter_halt("manual stop" if owner else "LIVE writer lease lost")
        if self.settings.has_credentials:
            deadline = (
                asyncio.get_running_loop().time() + self.settings.entry_cancel_confirm_seconds
            )
            while self.entry_controller.blocked and asyncio.get_running_loop().time() < deadline:
                await self.entry_controller.cancel_all()
                await asyncio.sleep(0.5)
            # Settle accepted private events before the final shutdown snapshot.
            # Timeout cancels only the join waiter, never a trading handler/write.
            try:
                async with asyncio.timeout(self.settings.entry_cancel_confirm_seconds):
                    await asyncio.gather(*(ws.queue.join() for ws in self.sockets if ws.private))
            except TimeoutError:
                raise RuntimeError("shutdown WebSocket processing unconfirmed") from None
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
                        if not await self._caa_owner_held():
                            raise OkxError("CAA disable requires LIVE writer ownership")
                        await self.client.cancel_all_after(0)
                except Exception:
                    self._caa_shutting_down = False
                    if not await self._caa_owner_held():
                        logger.error("CAA disable skipped: LIVE writer lease not owned")
                        raise RuntimeError("shutdown CAA disable requires LIVE writer ownership") from None
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
        shutdown_tasks = [*self.tasks, *self._ws_safety_tasks.values()]
        for task in shutdown_tasks:
            task.cancel()
        await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        self._ws_safety_tasks.clear()
        self._ws_reconcile_requested.clear()
        try:
            await self.store.append("system_events", {"event": "stop"})
        finally:
            if self.live_lease is not None:
                await self.live_lease.close()

    async def close(self) -> None:
        if self.live_lease is not None:
            self.governor.halt("LIVE runtime closing")
            if self._lease_loss_task is not None:
                await asyncio.gather(self._lease_loss_task, return_exceptions=True)
            await self.live_lease.close()
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

    def _ws_health_reason(self) -> str | None:
        if any(ws._run_task is not None and ws._run_task.done() for ws in self.sockets):
            return "WebSocket run task stopped"
        if any(ws.reconnect_stalled() for ws in self.sockets):
            return "WebSocket reconnect loop stalled"
        if any(not ws.is_processing_healthy() for ws in self.sockets):
            return "WebSocket processing backlog"
        if any(not ws.is_transport_healthy() for ws in self.sockets):
            return "WebSocket transport unavailable"
        if any(not ws.is_data_fresh() for ws in self.sockets):
            return "Market data stale"
        return None

    def _websockets_healthy(self) -> bool:
        return self._ws_health_reason() is None and not any(
            ws.reconciliation_required for ws in self.sockets
        )

    def _on_ws_fault(self, ws: OkxWebSocket, reason: str) -> None:
        # Receive-loop callback performs immediate entry fencing without awaiting
        # notifications, cancellation HTTP, or reconciliation.
        self._halt_state(reason)
        if ws.private:
            self.reconciliation_healthy = False
            self._last_reconcile_safe = False
            self.portfolio.synchronized = False
        self._schedule_ws_reconciliation(ws, reason)

    def _on_ws_ready(self, ws: OkxWebSocket) -> None:
        if ws.private:
            self._schedule_ws_reconciliation(ws)

    def _on_ws_run_done(self, ws: OkxWebSocket, task: asyncio.Task[Any]) -> None:
        if not self.running:
            return  # Expected cancellation during safe shutdown.
        if not task.cancelled():
            task.exception()  # Retrieve, never format potentially sensitive exceptions.
        ws.reason_code = "ws_run_task_failed"
        ws.last_failure_reason_code = ws.reason_code
        self._on_ws_fault(ws, "WebSocket run task stopped")
        notice = asyncio.create_task(self._report_ws_task_failure(ws))
        self.tasks.append(notice)

    async def _report_ws_task_failure(self, ws: OkxWebSocket) -> None:
        await self.alert(
            "CRITICAL", "🚨 WebSocket 重连任务停止响应", self._ws_details(ws),
            category=NotificationCategory.INFRASTRUCTURE,
            priority=NotificationPriority.CRITICAL,
            dedup_key=f"ws-liveness:{ws.name}:{ws.reason_code}",
            metadata={"transition": True, "reason": ws.reason_code},
        )

    def _schedule_ws_reconciliation(self, ws: OkxWebSocket, reason: str | None = None) -> None:
        self._ws_reconcile_requested.add(ws.name)
        current = self._ws_safety_tasks.get(ws.name)
        if current is not None and not current.done():
            return

        async def recover() -> None:
            try:
                if reason:
                    await self._send_observation(self._observe_infrastructure())
                    await self.enter_halt(reason)
                    try:
                        await self.store.append("risk_events", {
                            "event": "websocket_fault", "reason": self.governor.reason,
                            "socket_name": ws.name,
                        })
                    except Exception:
                        logger.error("WS fault audit unavailable")
                while ws.name in self._ws_reconcile_requested:
                    self._ws_reconcile_requested.discard(ws.name)
                    if ws.is_transport_healthy():
                        try:
                            async with asyncio.timeout(self.settings.stale_timeout_seconds):
                                await ws.queue.join()
                        except TimeoutError:
                            # Cancel only this join waiter, never an accepted handler/write.
                            # The normal reconciliation loop retries; its idle/generation gate
                            # cannot clear recovery while processing remains outstanding.
                            logger.warning("WS recovery waiting for business worker", extra={
                                "socket_name": ws.name, "phase": ws.phase,
                                "queue_depth": ws.queue.qsize(),
                            })
                    if self.settings.has_credentials:
                        await self.reconcile()
                    await self._send_observation(self._observe_infrastructure())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("WS recovery failed", extra={
                    "socket_name": ws.name, "exception_type": type(exc).__name__,
                })
        self._ws_safety_tasks[ws.name] = asyncio.create_task(recover(), name=f"{ws.name}-recovery")

    async def _on_public_market_batch(self, messages: list[dict[str, Any]]) -> None:
        # This callback is used only by public market subscriptions, never by
        # candles/decisions, orders, protection, emergency or reconciliation.
        await self.market.handle_batch(messages)

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
        ws_reason = self._ws_health_reason()
        if ws_reason:
            await self.enter_halt(ws_reason)
        decision = self.risk.evaluate(
            intent,
            self.portfolio,
            self.instruments[symbol],
            data_fresh=market_fresh and self._websockets_healthy(),
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
                if row.get("state") != "live" or self.algo_manager.has_failure(row):
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
                except Exception as exc:
                    codes = {"ledger fill conflict": "ledger_fill_conflict",
                             "unexpected order event": "unexpected_order_event"}
                    logger.error("private order processing failed", extra={
                        "exception_type": type(exc).__name__,
                        "error_code": codes.get(str(exc), "order_processing_failed"),
                        "socket_name": "private-account",
                    })
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
        self, name: str, healthy: bool, *, outage: str, recovery: str, threshold: int = 1,
        details: str = "", component: str | None = None, incident_key: str | None = None,
        update_details: bool = False,
    ) -> None:
        previous = self._component_health.get(name)
        if previous is None:
            self._component_health[name] = healthy
            return
        if healthy == previous:
            self._component_pending.pop(name, None)
            if not healthy and incident_key and update_details:
                # Update incident evidence before manager dedup, without another phone alert.
                await self.alert(
                    "WARNING", outage, details, category=NotificationCategory.INFRASTRUCTURE,
                    dedup_key=f"infrastructure:{name}",
                    metadata={"component": component, "ws_details": details,
                              "ws_incident_key": incident_key},
                )
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
            f"Component: {name}\nStatus: {'recovered' if healthy else 'unavailable'}"
            + ("\n" + details if details else ""),
            category=NotificationCategory.INFRASTRUCTURE,
            dedup_key=f"infrastructure:{name}",
            metadata={"recovery": healthy, **({"component": component, "ws_details": details}
                                           if component else {}),
                      **({"ws_incident_key": incident_key} if incident_key else {})},
        )

    @staticmethod
    def _ws_details(ws: OkxWebSocket) -> str:
        data = ws.status()
        pong = data["last_pong_age_seconds"]
        return (
            f"Socket: {ws.name}\nPhase: {ws.phase}"
            f"\nFailure phase: {ws.last_disconnect_phase or 'unavailable'}"
            f"\nReason: {'ws_reconciliation_pending' if ws.is_transport_healthy() and ws.reconciliation_required else 'healthy' if ws.is_fresh() else ws.reason_code}"
            f"\nReason code: {ws.reason_code}"
            f"\nLast failure reason code: {ws.last_failure_reason_code or 'unavailable'}"
            f"\nClose code: {ws.last_close_code if ws.last_close_code is not None else 'unavailable'}"
            f"\nClose reason: {ws.last_close_reason or 'unavailable'}"
            f"\nClose side: {ws.last_close_side or 'unavailable'}"
            f"\nLast Pong: {f'{pong:.1f}s' if pong is not None else 'unavailable'}"
            f"\nFailures: {ws.consecutive_failures}\nReconnects: {ws.reconnects}"
            f"\nNext retry: {data['next_retry_in'] if data['next_retry_in'] is not None else 'unavailable'}"
            f"\nTransport: {'healthy' if ws.is_transport_healthy() else 'unavailable'}"
            f"\nLogin: {'healthy' if ws.login_ok else 'unavailable'}"
            f"\nSubscription: {'healthy' if data['subscriptions_ok'] else 'unavailable'}"
            f"\nWorker: {'healthy' if ws.worker_alive else 'unavailable'}"
            f"\nReconciliation: {'pending' if ws.reconciliation_required else 'healthy'}"
        )

    async def _observe_private_ws(self, ws: OkxWebSocket) -> None:
        transport = ws.is_transport_healthy()
        fingerprint = (ws.phase, ws.reason_code, ws.reconnects, ws.worker_alive, ws.reconciliation_required)
        update = self._ws_diagnostic_state.get(ws.name) != fingerprint
        self._ws_diagnostic_state[ws.name] = fingerprint
        name = f"{ws.name}:Transport"
        self._component_health.setdefault(name, True)
        await self._observe_component(
            name, transport, outage="🚨 WebSocket 连接中断", recovery="⚠️ WebSocket 已重连，等待安全对账",
            component="websocket", details=self._ws_details(ws),
            incident_key=f"ws:transport:{ws.name}", update_details=update,
        )
        if transport:
            name = f"{ws.name}:Recovery"
            self._component_health.setdefault(name, True)
            await self._observe_component(
                name, ws.is_fresh(), outage="🚨 WebSocket 重连后对账未完成", recovery="✅ WebSocket 已恢复",
                component="ws_recovery", details=self._ws_details(ws),
                incident_key=f"ws:recovery:{ws.name}", update_details=update,
            )

    async def _observe_infrastructure(self) -> None:
        for ws in self.sockets:
            if ws.private:
                await self._observe_private_ws(ws)
        disconnected = [ws for ws in self.sockets if not ws.private and not ws.is_transport_healthy()]
        stale = [(ws, feed) for ws in self.sockets for feed in ws.stale_feeds()]
        backlog = [ws for ws in self.sockets if not ws.is_processing_healthy()]
        for component in ("WebSocket", "MarketData", "WSProcessing"):
            self._component_health.setdefault(component, True)
        await self._observe_component(
            "WebSocket", not disconnected,
            outage="🚨 WebSocket 连接中断", recovery="✅ WebSocket 已恢复",
            component="websocket", details="\n".join(
                f"Socket: {ws.name}\nReason: {ws.disconnect_reason}\nReconnects: {ws.reconnects}"
                for ws in disconnected
            ),
        )
        await self._observe_component(
            "MarketData", not stale,
            outage="⚠️ 市场数据过期", recovery="✅ 市场数据已恢复",
            component="market_data", details="\n".join(
                f"Socket: {ws.name}\nFeed: {feed['feed']}\nSymbol: {feed['symbol']}"
                f"\nAge: {feed['age_seconds'] if feed['age_seconds'] is not None else 'no data'}s"
                for ws, feed in stale
            ),
        )
        await self._observe_component(
            "WSProcessing", not backlog,
            outage="🚨 WebSocket 消息处理积压", recovery="✅ WS 消息处理恢复",
            component="ws_backlog", details="\n".join(
                f"Socket: {ws.name}\nQueue: {ws.queue.qsize()}/{ws.queue.maxsize}"
                "\nNew entries: blocked" for ws in backlog
            ),
        )
        await self._observe_component(
            "Redis", self.redis is not None,
            outage="🚨 Redis Unavailable", recovery="✅ Redis Recovered",
        )
        await self._observe_component(
            "Reconciliation", self._reconciliation_notification_health(),
            outage="🚨 Reconciliation Unhealthy", recovery="✅ Reconciliation Recovered",
            threshold=2,
        )
        # A fenced writer cannot probe CAA; ownership loss is not an endpoint outage.
        if self.settings.has_credentials and (
            self.settings.mode != Mode.LIVE
            or (self.live_lease is not None and self.live_lease.held)
        ):
            await self._observe_component(
                "Cancel-All-After", self.dead_man_healthy,
                outage="🚨 CAA Unavailable", recovery="✅ CAA Recovered",
            )
        reconnect_total = sum(ws.reconnects - ws.maintenance_reconnects for ws in self.sockets)
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
            and self.dead_man_healthy and self._websockets_healthy()
        ):
            return
        self._started_notified = True
        healthy_ws = sum(ws.is_fresh() and not ws.reconciliation_required for ws in self.sockets)
        await self.alert(
            "INFO", "🟢 Quant System Started",
            f"Mode: {self.settings.mode.value}\nEquity: {self.portfolio.equity} USDT"
            f"\nRisk: {self.governor.state.value}\nWS: {healthy_ws}/{len(self.sockets)}",
            category=NotificationCategory.SYSTEM,
            dedup_key="system-started",
        )

    async def _publish_heartbeat(self) -> None:
        ws_healthy = sum(ws.is_fresh() and not ws.reconciliation_required for ws in self.sockets)
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

    async def _resume_health(self) -> tuple[bool, list[str]]:
        issues: list[str] = []
        if self.settings.mode == Mode.LIVE and (
            self.live_lease is None or not await self.live_lease.verify()
        ):
            issues.append("live_writer_lease")
        if not self.reconciliation_healthy:
            issues.append("reconciliation")
        if not self._last_reconcile_safe:
            issues.append("reconciliation_safety")
        if not self.portfolio.synchronized:
            issues.append("portfolio_sync")
        if not self.store.healthy:
            issues.append("database")
        if self.redis is None:
            issues.append("redis")
        else:
            try:
                async def probe_redis() -> bool:
                    assert self.redis is not None
                    await self.redis.ping()
                    await self.redis.set("auto_recovery_probe", "1", ex=60)
                    return await self.redis.get("auto_recovery_probe") == "1"

                if not await asyncio.wait_for(probe_redis(), timeout=2):
                    issues.append("redis_read_write")
            except Exception:
                issues.append("redis_read_write")
        caa_max_age = min(
            self.settings.cancel_all_after_seconds,
            self.settings.cancel_all_after_refresh_seconds * 2,
        )
        if (
            not self.dead_man_healthy or self._caa_shutting_down
            or (self.running and (
                self._last_caa_success_at is None
                or self._clock() - self._last_caa_success_at > caa_max_age
            ))
        ):
            issues.append("cancel_all_after")
        if self.entry_controller.blocked:
            issues.append("entry_cancellation")
        if self.emergency.targets:
            issues.append("emergency_targets")
        if self.settings.mode != Mode.BACKTEST and (
            not self.sockets
            or not self._websockets_healthy()
        ):
            issues.append("websockets")
        if self.portfolio_monitor.breached(self.portfolio, [])[0]:
            issues.append("portfolio_limits")
        return not issues, issues

    async def resume(self) -> bool:
        healthy, _ = await self._reconcile_and_assess()
        if self._ledger_repair_manual_hold:
            if not healthy or not self.portfolio.synchronized:
                return False
            await self.store.append("system_events", {"event": "ledger_repair_manual_release"})
            self._ledger_repair_manual_hold = False
        if not self.governor.resume(
            synchronized=self.portfolio.synchronized, healthy=healthy
        ):
            return False
        self._auto_recovery_forbidden = False
        self._ledger_repair_attempted = False
        self._auto_recovery_circuit_breaker = False
        self._auto_recovery_reason = None
        self._auto_recovery_successes = 0
        self._auto_resume_times.clear()
        self._auto_recovery_flaps.clear()
        self._auto_recovery_notice = None
        await self._send_observation(self._notify_risk_state())
        await self._send_observation(self._publish_started_if_ready())
        return True

    def auto_recovery_status(self) -> dict[str, bool | int]:
        enabled = self.settings.auto_recovery_enabled and self.settings.mode == Mode.PAPER
        eligible = (
            enabled and not self._planned_shutdown and not self._auto_recovery_forbidden
            and not self._auto_recovery_circuit_breaker
            and self.governor.state == GovernorState.HALT
            and self._auto_recovery_reason == self.governor.reason
            and classify_halt_reason(self.governor.reason) == HaltClass.TRANSIENT_INFRA
        )
        return {
            "enabled": enabled, "eligible": eligible,
            "successes": self._auto_recovery_successes if eligible else 0,
            "required": self.settings.auto_recovery_success_threshold,
            "circuit_breaker": self._auto_recovery_circuit_breaker,
        }

    async def _auto_recovery_check(self) -> None:
        now = self._clock()
        if self._auto_recovery_notice is not None:
            reason, due = self._auto_recovery_notice
            if now >= due and self.governor.state == GovernorState.NORMAL:
                self._auto_recovery_notice = None
                await self.alert(
                    "INFO", "✅ Auto Recovery Completed",
                    f"Previous state: HALT\nReason: {reason}\n"
                    f"Healthy checks: {self.settings.auto_recovery_success_threshold}/"
                    f"{self.settings.auto_recovery_success_threshold}\n"
                    f"Reconciliation: healthy\nWS: {sum(ws.is_fresh() and not ws.reconciliation_required for ws in self.sockets)}/"
                    f"{len(self.sockets)}\nPositions: {len(self.portfolio.positions)}\n"
                    f"Orders: {len(self.order_manager.pending_entries())}\nRisk: NORMAL",
                    category=NotificationCategory.RISK, priority=NotificationPriority.ACTIVE,
                    dedup_key="auto-recovery-completed", metadata={"recovery": True},
                )
        if not self.auto_recovery_status()["eligible"]:
            return
        if now - self._auto_recovery_started_at < self.settings.auto_recovery_min_halt_seconds:
            return
        if now - self._auto_recovery_last_check < self.settings.auto_recovery_check_seconds:
            return
        self._auto_recovery_last_check = now
        candidate_reason = self._auto_recovery_reason
        assert candidate_reason is not None
        label = HaltClass.TRANSIENT_INFRA.value
        self.metrics.auto_recovery_attempts.labels(reason_class=label).inc()
        healthy, issues = await self._reconcile_and_assess()
        if (not healthy or not self.auto_recovery_status()["eligible"]
                or candidate_reason != self.governor.reason):
            self._auto_recovery_successes = 0
            self.metrics.auto_recovery_failed_checks.labels(reason_class=label).inc()
            logger.warning("auto_recovery check failed reason_class=%s issues=%s", label, issues)
            return
        self._auto_recovery_successes += 1
        if self._auto_recovery_successes < self.settings.auto_recovery_success_threshold:
            return
        while self._auto_resume_times and now - self._auto_resume_times[0] >= 3600:
            self._auto_resume_times.popleft()
        while self._auto_recovery_flaps and now - self._auto_recovery_flaps[0] >= 3600:
            self._auto_recovery_flaps.popleft()
        if (
            len(self._auto_resume_times) + len(self._auto_recovery_flaps)
            >= self.settings.auto_recovery_max_resumes_per_hour
        ):
            self._auto_recovery_circuit_breaker = True
            self.metrics.auto_recovery_circuit_breaker.labels(reason_class=label).inc()
            await self.enter_halt("auto recovery circuit breaker")
            await self.alert(
                "CRITICAL", "🚨 Auto Recovery Disabled",
                "Repeated transient failures\nManual resume required",
                category=NotificationCategory.RISK, priority=NotificationPriority.CRITICAL,
                dedup_key="auto-recovery-circuit-breaker", metadata={"transition": True},
            )
            return
        healthy, issues = await self._reconcile_and_assess()
        if (not healthy or not self.auto_recovery_status()["eligible"]
                or candidate_reason != self.governor.reason):
            self._auto_recovery_successes = 0
            self.metrics.auto_recovery_failed_checks.labels(reason_class=label).inc()
            logger.warning("auto_recovery final gate failed reason_class=%s issues=%s", label, issues)
            return
        if self.governor.resume(synchronized=self.portfolio.synchronized, healthy=healthy):
            self._auto_resume_times.append(now)
            self._auto_recovery_reason = None
            self._auto_recovery_successes = 0
            self._last_risk_notice = (GovernorState.NORMAL, self.governor.reason)
            self._auto_recovery_notice = (
                candidate_reason, now + self.settings.auto_recovery_stability_seconds,
            )
            self.metrics.auto_recovery_success.labels(reason_class=label).inc()

    async def _auto_recovery_loop(self) -> None:
        while self.running:
            await asyncio.sleep(self.settings.auto_recovery_check_seconds)
            try:
                await self._auto_recovery_check()
            except Exception as exc:
                self._auto_recovery_successes = 0
                self.metrics.auto_recovery_failed_checks.labels(
                    reason_class=HaltClass.TRANSIENT_INFRA.value
                ).inc()
                logger.error("auto_recovery check error_type=%s", type(exc).__name__)

    async def _check_ws_liveness(self) -> None:
        for ws in self.sockets:
            ws.status()  # Refresh bounded socket metrics independently of /status requests.
            dead_run = ws._run_task is not None and ws._run_task.done()
            dead_worker = ws._run_task is not None and not ws.worker_alive
            if dead_run or dead_worker or ws.reconnect_stalled():
                reason = ("WebSocket run task stopped" if dead_run else
                          "WebSocket processing failed" if dead_worker else
                          "WebSocket reconnect loop stalled")
                ws.reason_code = ("ws_run_task_failed" if dead_run else
                                  "ws_worker_failed" if dead_worker else "ws_reconnect_stalled")
                ws.last_failure_reason_code = ws.reason_code
                self._on_ws_fault(ws, reason)
                await self._report_ws_task_failure(ws)

    async def _watchdog(self) -> None:
        while self.running:
            await asyncio.sleep(5)
            await self._refresh_redis()
            await self._check_ws_liveness()
            self.metrics.update(
                self.portfolio,
                self.governor.state,
                {ws.name: ws.reconnects for ws in self.sockets},
                stale=any(not ws.is_data_fresh() for ws in self.sockets),
                trade_count=len(self.order_manager.seen_trade_ids),
            )
            ws_reason = self._ws_health_reason()
            if ws_reason:
                await self.enter_halt(ws_reason)
            elif (
                self.governor.reason == "startup reconciliation pending"
                and self.governor.state != GovernorState.EMERGENCY
            ):
                async with self._reconcile_lock:
                    healthy, _ = await self._resume_health()
                    self.governor.resume(
                        synchronized=self.portfolio.synchronized,
                        healthy=healthy,
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
                    if not await self._caa_owner_held():
                        return
                    await self.client.cancel_all_after(self.settings.cancel_all_after_seconds)
                    if not await self._caa_owner_held():
                        return
                self.dead_man_healthy = True
                self._last_caa_success_at = self._clock()
            except Exception:
                self.dead_man_healthy = False
                self._last_caa_success_at = None
                if not await self._caa_owner_held():
                    return
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
