from __future__ import annotations

import asyncio
import json
import logging
from datetime import timedelta
from decimal import Decimal
from typing import Any

from redis.asyncio import Redis

from app.config import Mode, Settings
from app.daily_review import save_daily_review
from app.execution import ExecutionEngine, OrderManager
from app.features import compute_features
from app.market import MarketDataEngine
from app.meta import fuse
from app.models import GovernorState, Instrument, PortfolioState, Side, utcnow
from app.monitoring import ConsoleNotification, Metrics, WebhookNotification
from app.okx import OkxRestClient, OkxWebSocket, candle_subscriptions, public_subscriptions
from app.portfolio import summarize_positions
from app.regime import classify
from app.risk import RiskEngine, RiskGovernor
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
        self.execution = ExecutionEngine(self.client, self.order_manager)
        self.portfolio = PortfolioState(equity=Decimal(0), available_balance=Decimal(0))
        self.instruments: dict[str, Instrument] = {}
        self.sockets: list[OkxWebSocket] = []
        self.tasks: list[asyncio.Task[Any]] = []
        self.running = False
        self.evaluated_candles: dict[str, int] = {}
        self.dead_man_healthy = False
        self.reconciliation_healthy = False
        self.strategies = build_strategies(settings)
        self.metrics = Metrics()
        self.client.latency_observer = self.metrics.observe_latency
        self.notification = (
            WebhookNotification(settings.alert_webhook_url)
            if settings.alert_webhook_url
            else ConsoleNotification()
        )
        self.last_alert: tuple[GovernorState, str] | None = None
        self.emergency_reduced: dict[str, Decimal] = {}

    async def alert(self, level: str, title: str, message: str) -> None:
        try:
            await self.notification.send_alert(level, title, message)
        except Exception as exc:
            logger.error("notification failed: %s", type(exc).__name__)

    async def initialize(self) -> None:
        await self.store.initialize()
        await self.order_manager.restore()
        if self.redis is not None:
            await self.redis.aclose()
        self.redis = Redis.from_url(self.settings.redis_url, decode_responses=True)
        try:
            await self.redis.ping()
        except Exception:
            self.governor.halt("Redis unavailable")
            await self.redis.aclose()
            self.redis = None
        self.market.redis = self.redis
        try:
            self.instruments = await self.client.instruments()
        except Exception:
            self.governor.halt("instrument metadata unavailable")
        for symbol in self.settings.symbols:
            for timeframe in self.settings.timeframes:
                try:
                    rows = await self.client.candles(symbol, timeframe)
                    await self.market.handle(
                        {"arg": {"channel": "candle" + timeframe, "instId": symbol}, "data": rows},
                        historical=True,
                    )
                except Exception as exc:
                    self.governor.halt("candle preload failed")
                    logger.error("candle preload failed: %s", type(exc).__name__)
        if self.settings.mode != Mode.BACKTEST and self.settings.has_credentials:
            await self.reconcile()

    async def reconcile(self) -> None:
        self.reconciliation_healthy = False
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
            if (
                self.settings.mode == Mode.LIVE
                and config[0].get("uid") != self.settings.confirm_live_account_id
            ):
                raise RuntimeError("live account ID mismatch")
            details = account[0]
            equity = Decimal(details["totalEq"])
            available = sum(
                (
                    Decimal(x.get("availEq") or "0")
                    for x in details.get("details", [])
                    if x.get("ccy") == "USDT"
                ),
                Decimal(0),
            )
            remote_positions = {
                p["instId"]: Decimal(p["pos"]) for p in positions if Decimal(p["pos"]) != 0
            }
            if any(
                Decimal(p.get("mgnRatio") or "999") < Decimal(str(self.settings.min_margin_ratio))
                for p in positions
                if Decimal(p.get("pos") or "0") != 0
            ):
                self.governor.halt("margin ratio danger", emergency=True)
                await self._emergency_cancel_pending()
                return
            if not self.portfolio.synchronized:
                if orders:
                    self.governor.halt("startup has pending orders")
                    return
                if any(
                    Decimal(p.get("lever") or "999") > self.settings.max_leverage
                    for p in positions
                    if Decimal(p.get("pos") or "0") != 0
                ):
                    self.governor.halt("startup leverage exceeds limit")
                    return
                expected_positions: dict[str, Decimal] = {}
                for order_row in self.order_manager.orders.values():
                    size = Decimal(order_row["filled"])
                    direction = Decimal(1) if order_row["direction"] == "LONG" else Decimal(-1)
                    expected_positions[order_row["symbol"]] = (
                        expected_positions.get(order_row["symbol"], Decimal(0)) + size * direction
                    )
                expected_positions = {k: v for k, v in expected_positions.items() if v != 0}
                if expected_positions != remote_positions:
                    self.governor.halt("startup position mismatch")
                    return
            if not self.portfolio.synchronized and not remote_positions:
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
                    self.governor.halt("position mismatch")
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
                self.governor.halt("unexpected algo order")
                return
            for symbol in remote_positions:
                protective = {
                    row["protective_algo_id"]
                    for row in self.order_manager.orders.values()
                    if row["symbol"] == symbol and row.get("protective_algo_id")
                }
                if not protective.intersection(remote_algo_ids):
                    self.governor.halt("protective stop missing")
                    return
            if self.portfolio.synchronized and {o.get("clOrdId") for o in orders} != {
                cid
                for cid, row in self.order_manager.orders.items()
                if row["state"] in {"SUBMITTED", "ACKNOWLEDGED", "PARTIALLY_FILLED"}
            }:
                self.governor.halt("order mismatch")
                return
            now = utcnow()
            daily_rows = await self.store.since(
                "portfolio_snapshots", now.replace(hour=0, minute=0, second=0, microsecond=0)
            )
            weekly_rows = await self.store.since("portfolio_snapshots", now - timedelta(days=7))
            daily_baseline = Decimal(daily_rows[0]["equity"]) if daily_rows else equity
            weekly_peak = max([equity] + [Decimal(row["equity"]) for row in weekly_rows])
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
                    self.governor.halt("position risk cannot be reconstructed")
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
                        self.governor.halt("pending order risk unknown")
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
        except Exception as exc:
            self.governor.halt("reconciliation failed")
            logger.error("reconciliation failed: %s", type(exc).__name__)

    async def start(self) -> None:
        if self.running:
            return
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
        self.running = True
        self.tasks = [asyncio.create_task(ws.run()) for ws in self.sockets]
        self.tasks += [
            asyncio.create_task(self._watchdog()),
            asyncio.create_task(self._reconcile_loop()),
            asyncio.create_task(self._daily_review_loop()),
        ]
        if self.settings.has_credentials:
            self.tasks.append(asyncio.create_task(self._dead_man_loop()))
        await self.store.append(
            "system_events", {"event": "start", "mode": self.settings.mode.value}
        )
        await self.alert("INFO", "System start", self.settings.mode.value)

    async def stop(self) -> None:
        self.governor.halt("manual stop")
        self.running = False
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.store.append("system_events", {"event": "stop"})
        await self.alert("INFO", "System stop", "Trading runtime stopped")

    async def close(self) -> None:
        if self.redis:
            await self.redis.aclose()
        await self.client.close()
        await self.store.close()

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
            self.governor.halt("market processing failed")
            logger.error("market processing failed: %s", type(exc).__name__)

    async def _evaluate(self, symbol: str) -> None:
        if not self.portfolio.synchronized or self.governor.state != GovernorState.NORMAL:
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
            {key: float(value) for key, value in self.market.latest_derivatives[symbol].items()},
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
        higher_regimes = []
        for timeframe in ("1H", "4H"):
            higher_state = compute_features(self.market.candles[(symbol, timeframe)])
            higher_regimes.append(
                classify(higher_state, self.settings).regime if higher_state else None
            )
        entry_state = compute_features(self.market.candles[(symbol, "5m")])
        signals = [
            s
            for strategy in self.strategies
            if (s := await strategy.generate_signal(state, self.portfolio, regime))
            and (s.strategy != "trend" or all(r == regime.regime for r in higher_regimes))
            and (
                s.strategy not in {"trend", "breakout"}
                or (
                    entry_state is not None
                    and entry_state.ema20 is not None
                    and entry_state.ema50 is not None
                    and (
                        (s.side.value == "LONG" and entry_state.ema20 >= entry_state.ema50)
                        or (s.side.value == "SHORT" and entry_state.ema20 <= entry_state.ema50)
                    )
                )
            )
        ]
        for signal in signals:
            await self.store.append(
                "signals", signal.model_dump(mode="json"), symbol=symbol, reference_id=signal.id
            )
            if self.redis is not None:
                await self.redis.set(f"signal:{symbol}", signal.model_dump_json(), ex=300)
            self.metrics.signals.inc()
        intent = fuse(
            signals,
            self.portfolio,
            self.settings.min_signal_confidence,
            weights=self.settings.strategy_weights,
        )
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
        await self.store.append(
            "risk_events", decision.model_dump(mode="json"), symbol=symbol, reference_id=intent.id
        )
        if decision.approved:
            try:
                await self.execution.submit(
                    ExecutionEngine.from_risk(decision, self.instruments[symbol])
                )
                self.metrics.orders.inc()
            except Exception:
                self.metrics.order_errors.inc()
                raise

    async def _on_private(self, message: dict[str, Any]) -> None:
        channel = message.get("arg", {}).get("channel")
        if channel == "orders":
            for row in message["data"]:
                try:
                    await self.order_manager.ingest(row)
                    if (
                        row.get("state") in {"partially_filled", "canceled"}
                        and Decimal(row.get("accFillSz") or "0") > 0
                    ):
                        client_id = row.get("clOrdId", "")
                        local = self.order_manager.orders.get(client_id)
                        if local and not local["reduce_only"]:
                            await self._handle_partial_fill(client_id, local)
                except Exception:
                    self.governor.halt("unexpected order", emergency=True)
                    await self._emergency_cancel_pending()
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
                self.governor.halt("Redis unavailable")
                return
        try:
            await self.redis.ping()
            await self.redis.set("risk_state", self.governor.state.value, ex=120)
            await self.redis.set("system_state", "running" if self.running else "stopped", ex=120)
        except Exception:
            self.governor.halt("Redis unavailable")
            await self.redis.aclose()
            self.redis = None
            self.market.redis = None

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
            current_alert = (self.governor.state, self.governor.reason)
            if current_alert != self.last_alert and self.governor.state in {
                GovernorState.HALT,
                GovernorState.EMERGENCY,
            }:
                await self.alert("ERROR", self.governor.state.value, self.governor.reason)
            self.last_alert = current_alert
            if any(not ws.is_fresh() for ws in self.sockets):
                self.governor.halt("WebSocket disconnected or stale")
            elif (
                self.governor.reason == "WebSocket disconnected or stale"
                and self.portfolio.synchronized
            ):
                await self.reconcile()
                self.governor.resume(
                    synchronized=self.portfolio.synchronized,
                    healthy=self.reconciliation_healthy
                    and self.store.healthy
                    and self.redis is not None
                    and self.dead_man_healthy
                    and all(ws.is_fresh() for ws in self.sockets),
                )
            elif self.governor.reason == "startup reconciliation pending":
                self.governor.resume(
                    synchronized=self.portfolio.synchronized,
                    healthy=self.reconciliation_healthy
                    and self.store.healthy
                    and self.redis is not None
                    and self.dead_man_healthy
                    and all(ws.is_fresh() for ws in self.sockets),
                )

    async def _reconcile_loop(self) -> None:
        while self.running:
            await asyncio.sleep(self.settings.reconcile_interval_seconds)
            if self.settings.has_credentials:
                await self.reconcile()

    async def _dead_man_loop(self) -> None:
        while self.running:
            try:
                await self.client.cancel_all_after(self.settings.cancel_all_after_seconds)
                self.dead_man_healthy = True
            except Exception:
                self.dead_man_healthy = False
                self.governor.halt("dead man switch unavailable")
            await asyncio.sleep(self.settings.cancel_all_after_refresh_seconds)

    async def _emergency_cancel_pending(self) -> None:
        try:
            pending = await self.client.pending_orders()
            for order in pending:
                local = self.order_manager.orders.get(order.get("clOrdId", ""))
                if local and local["reduce_only"]:
                    continue
                await self.client.cancel_order(
                    order["instId"],
                    client_order_id=order.get("clOrdId", ""),
                    order_id=order.get("ordId", ""),
                )
            await self.store.append(
                "risk_events", {"event": "emergency cancel requested", "orders": len(pending)}
            )
        except Exception as exc:
            logger.error("emergency cancellation failed: %s", type(exc).__name__)

    async def _handle_partial_fill(self, client_id: str, order: dict) -> None:
        self.governor.halt("partial fill before protective stop active", emergency=True)
        filled = Decimal(order["filled"])
        previous = self.emergency_reduced.get(client_id, Decimal(0))
        delta = filled - previous
        if delta <= 0:
            return
        await self._emergency_cancel_pending()
        if not self.settings.emergency_reduce_on_partial_fill:
            return
        try:
            algos = await self.client.pending_algos()
            if any(algo.get("algoClOrdId") == order["protective_algo_id"] for algo in algos):
                await self.store.append(
                    "risk_events",
                    {
                        "event": "protective algo confirmed",
                        "algoClOrdId": order["protective_algo_id"],
                    },
                )
                return
        except Exception:
            pass
        symbol = order["symbol"]
        instrument = self.instruments.get(symbol)
        if instrument is None:
            return
        self.emergency_reduced[client_id] = filled
        direction = Side(order["direction"])
        decision = self.risk.emergency_reduce(symbol, direction, delta, instrument)
        request = ExecutionEngine.from_risk(
            decision, instrument, order_type="market", reduce_only=True
        )
        await self.execution.submit(request)

    async def _daily_review_loop(self) -> None:
        while self.running:
            yesterday = (utcnow() - timedelta(days=1)).date()
            try:
                recent = await self.store.latest("daily_reports", limit=7)
                if not any(row.get("date") == yesterday.isoformat() for row in recent):
                    report = await save_daily_review(self.store, yesterday)
                    await self.alert("INFO", "Daily report", str(report))
            except Exception as exc:
                logger.error("daily review failed: %s", type(exc).__name__)
            await asyncio.sleep(3600)
