"""Stable notification identity; legacy titles are accepted only at this boundary."""

from __future__ import annotations

from app.models import NotificationEvent

LEGACY_CODES = {
    "🟢 Quant System Started": "SYSTEM_STARTED",
    "🟡 Quant System Stopping": "SYSTEM_STOPPING",
    "⚪ Quant System Stopped": "SYSTEM_STOPPED",
    "🚨 HALT": "RISK_HALT",
    "🚨 EMERGENCY": "RISK_EMERGENCY",
    "✅ Risk State Recovered": "RISK_RECOVERED",
    "🚨 Emergency Reduction Failed": "EMERGENCY_REDUCTION_FAILED",
    "✅ Auto Recovery Completed": "AUTO_RECOVERY_COMPLETED",
    "🚨 Auto Recovery Disabled": "AUTO_RECOVERY_DISABLED",
    "⚠️ Trading Temporarily Halted": "INCIDENT_OPEN",
    "⚠️ Trading Infrastructure Unhealthy": "INCIDENT_OPEN",
    "✅ Trading Recovered": "INCIDENT_RESOLVED",
    "✅ Trading Infrastructure Recovered": "INFRASTRUCTURE_RECOVERED",
    "ℹ️ Trading Incident Resolved": "INCIDENT_RETROSPECTIVE",
    "⚠️ Safety Incident Resolved": "SAFETY_RETROSPECTIVE",
    "🚨 Emergency Incident Resolved": "EMERGENCY_RETROSPECTIVE",
    "🚨 Trading App Offline": "WATCHDOG_OFFLINE",
    "⚠️ Trading App Unhealthy": "WATCHDOG_UNHEALTHY",
    "✅ Trading App Recovered": "WATCHDOG_RECOVERED",
    "❤️ Quant Heartbeat": "HEARTBEAT_HEALTHY",
    "⚠️ Quant Heartbeat": "HEARTBEAT_UNHEALTHY",
    "📊 Daily Trading Report": "DAILY_REPORT",
    "🚨 WebSocket Disconnected": "WS_DISCONNECTED",
    "🚨 WebSocket 连接中断": "WS_DISCONNECTED",
    "✅ WebSocket 已恢复": "WS_RECOVERED",
    "⚠️ WebSocket 已重连，等待安全对账": "WS_TRANSPORT_RECOVERED",
    "🚨 WebSocket 重连后对账未完成": "WS_RECOVERY_PENDING",
    "🚨 WebSocket 重连任务停止响应": "WS_TASK_STALLED",
    "⚠️ 市场数据过期": "WS_MARKET_STALE",
    "✅ 市场数据已恢复": "WS_MARKET_RECOVERED",
    "🚨 WebSocket 消息处理积压": "WS_BACKLOG",
    "✅ WS 消息处理恢复": "WS_BACKLOG_RECOVERED",
    "✅ WS Recovered": "WS_RECOVERED",
    "🚨 Repeated WebSocket Reconnects": "WS_RECONNECT_FLAP",
    "🚨 Redis Unavailable": "REDIS_UNAVAILABLE",
    "✅ Redis Recovered": "REDIS_RECOVERED",
    "🚨 Reconciliation Unhealthy": "RECONCILIATION_UNHEALTHY",
    "✅ Reconciliation Recovered": "RECONCILIATION_RECOVERED",
    "🚨 CAA Unavailable": "CAA_UNAVAILABLE",
    "✅ CAA Recovered": "CAA_RECOVERED",
    "Entry cancellation unconfirmed": "ENTRY_CANCEL_UNCONFIRMED",
    "Foreign pending order": "FOREIGN_PENDING_ORDER",
    "Unsafe shutdown": "UNSAFE_SHUTDOWN",
}
TRADE_SUFFIXES = {
    " Entry Submitted": "ENTRY_SUBMITTED",
    " Partial Fill": "TRADE_PARTIAL_FILL",
    " Filled": "TRADE_FILLED",
    " Protection Active": "PROTECTION_ACTIVE",
    " Position Closed": "POSITION_CLOSED",
}
COMPONENTS = {
    "WS_DISCONNECTED": "websocket",
    "WS_RECOVERED": "websocket",
    "WS_RECONNECT_FLAP": "websocket",
    "WS_MARKET_STALE": "market_data",
    "WS_MARKET_RECOVERED": "market_data",
    "WS_BACKLOG": "ws_backlog",
    "WS_BACKLOG_RECOVERED": "ws_backlog",
    "REDIS_UNAVAILABLE": "redis",
    "REDIS_RECOVERED": "redis",
    "RECONCILIATION_UNHEALTHY": "reconciliation",
    "RECONCILIATION_RECOVERED": "reconciliation",
    "CAA_UNAVAILABLE": "caa",
    "CAA_RECOVERED": "caa",
}


def event_code(event: NotificationEvent) -> str:
    if event.event_code:
        return event.event_code
    metadata_code = event.metadata.get("event_code")
    if isinstance(metadata_code, str) and metadata_code:
        return metadata_code
    if event.title in LEGACY_CODES:
        return LEGACY_CODES[event.title]
    return next(
        (code for suffix, code in TRADE_SUFFIXES.items() if event.title.endswith(suffix)),
        "GENERIC_ALERT",
    )


def reason_code(event: NotificationEvent) -> str:
    reason = event.metadata.get("reason")
    if isinstance(reason, str):
        return reason
    return next(
        (
            line.removeprefix("Reason: ")
            for line in event.message.splitlines()
            if line.startswith("Reason: ")
        ),
        "",
    )


def normalize_event(event: NotificationEvent) -> NotificationEvent:
    """Attach machine identity before routing without changing display text or dedup keys."""
    metadata = dict(event.metadata)
    reason = reason_code(event)
    if reason:
        metadata.setdefault("reason", reason)
    code = event_code(event)
    component = COMPONENTS.get(code)
    if component:
        metadata.setdefault("component", component)
    if code == "TRADE_FILLED" and "side" not in metadata:
        if " SHORT " in event.title:
            metadata["side"] = "SHORT"
        elif " LONG " in event.title:
            metadata["side"] = "LONG"
    return event.model_copy(update={"event_code": code, "metadata": metadata})
