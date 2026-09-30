from __future__ import annotations

import logging
from abc import ABC, abstractmethod

import httpx
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from app.models import GovernorState, NotificationEvent, PortfolioState

logger = logging.getLogger("monitoring")


class Notification(ABC):
    async def send(self, event: NotificationEvent) -> None:
        await self.send_alert(event.level.value, event.title, event.message)

    @abstractmethod
    async def send_alert(self, level: str, title: str, message: str) -> None: ...


class ConsoleNotification(Notification):
    async def send_alert(self, level: str, title: str, message: str) -> None:
        logger.warning("%s: %s: %s", level, title, message)


class WebhookNotification(Notification):
    def __init__(self, url: str) -> None:
        self.url = url

    async def send_alert(self, level: str, title: str, message: str) -> None:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.post(
                self.url, json={"level": level, "title": title, "message": message}
            )
            response.raise_for_status()


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.equity = Gauge("quant_equity_usdt", "Account equity", registry=self.registry)
        self.daily_pnl = Gauge(
            "quant_daily_pnl_usdt", "Daily equity change", registry=self.registry
        )
        self.positions = Gauge(
            "quant_open_positions", "Open position count", registry=self.registry
        )
        self.margin_usage = Gauge(
            "quant_margin_usage_ratio", "Margin usage", registry=self.registry
        )
        self.drawdown = Gauge(
            "quant_weekly_drawdown_ratio", "Rolling weekly drawdown", registry=self.registry
        )
        self.exposure = Gauge(
            "quant_directional_exposure_ratio", "Directional exposure", registry=self.registry
        )
        self.leverage = Gauge(
            "quant_effective_leverage", "Effective leverage", registry=self.registry
        )
        self.data_stale = Gauge(
            "quant_data_stale", "One when market data is stale", registry=self.registry
        )
        self.trade_count = Gauge(
            "quant_trade_count", "Observed unique fills", registry=self.registry
        )
        self.risk_state = Gauge("quant_risk_state", "Risk state severity", registry=self.registry)
        self.ws_reconnects = Gauge(
            "quant_ws_reconnects", "WebSocket reconnect count", ["socket"], registry=self.registry
        )
        self.orders = Counter("quant_orders_total", "Order submissions", registry=self.registry)
        self.order_errors = Counter(
            "quant_order_errors_total", "Order submission errors", registry=self.registry
        )
        self.signals = Counter("quant_signals_total", "Strategy signals", registry=self.registry)
        self.api_latency = Histogram(
            "quant_api_latency_seconds", "OKX REST latency", ["path"], registry=self.registry
        )
        notification_labels = ["channel", "priority", "category"]
        self.notification_sent = Counter(
            "quant_notifications_sent_total", "Notifications delivered", notification_labels,
            registry=self.registry,
        )
        self.notification_failed = Counter(
            "quant_notifications_failed_total", "Notifications that exhausted retries",
            notification_labels, registry=self.registry,
        )
        self.notification_dropped = Counter(
            "quant_notifications_dropped_total", "Notifications discarded before delivery",
            ["priority", "category"], registry=self.registry,
        )
        self.notification_queue_size = Gauge(
            "quant_notification_queue_size", "Queued notifications", registry=self.registry
        )
        self.notification_latency = Histogram(
            "quant_notification_latency_seconds", "Notification delivery latency",
            notification_labels, registry=self.registry,
        )
        self.notification_policy_suppressed = Counter(
            "quant_notification_policy_suppressed_total", "Notifications filtered by channel policy",
            ["channel", "category"], registry=self.registry,
        )
        incident_labels = ["category", "severity", "component"]
        self.incidents_open = Gauge(
            "quant_incidents_open", "Open notification incidents", incident_labels,
            registry=self.registry,
        )
        self.incidents_total = Counter(
            "quant_incidents_total", "Notification incidents opened", incident_labels,
            registry=self.registry,
        )
        self.incidents_resolved = Counter(
            "quant_incidents_resolved_total", "Notification incidents resolved",
            incident_labels, registry=self.registry,
        )
        self.incident_duration = Histogram(
            "quant_incident_duration_seconds", "Resolved notification incident duration",
            incident_labels, registry=self.registry,
        )
        self.auto_recovery_attempts = Counter(
            "quant_auto_recovery_attempts_total", "Auto recovery health checks",
            ["reason_class"], registry=self.registry,
        )
        self.auto_recovery_success = Counter(
            "quant_auto_recovery_success_total", "Successful auto resumes",
            ["reason_class"], registry=self.registry,
        )
        self.auto_recovery_failed_checks = Counter(
            "quant_auto_recovery_failed_checks_total", "Failed auto recovery checks",
            ["reason_class"], registry=self.registry,
        )
        self.auto_recovery_circuit_breaker = Counter(
            "quant_auto_recovery_circuit_breaker_total", "Auto recovery circuit trips",
            ["reason_class"], registry=self.registry,
        )

    def observe_latency(self, path: str, seconds: float) -> None:
        self.api_latency.labels(path=path).observe(seconds)

    def update(
        self,
        portfolio: PortfolioState,
        state: GovernorState,
        reconnects: dict[str, int],
        *,
        stale: bool = False,
        trade_count: int = 0,
    ) -> None:
        self.equity.set(float(portfolio.equity))
        self.daily_pnl.set(float(portfolio.daily_pnl))
        self.positions.set(sum(bool(x) for x in portfolio.positions.values()))
        self.margin_usage.set(
            float(portfolio.margin_used / portfolio.equity) if portfolio.equity else 0
        )
        self.drawdown.set(float(portfolio.weekly_drawdown))
        self.exposure.set(float(portfolio.directional_exposure))
        self.leverage.set(float(portfolio.effective_leverage))
        self.data_stale.set(1 if stale else 0)
        self.trade_count.set(trade_count)
        self.risk_state.set(list(GovernorState).index(state))
        for socket, count in reconnects.items():
            self.ws_reconnects.labels(socket=socket).set(count)

    def render(self) -> bytes:
        return generate_latest(self.registry)
