"""Chinese phone presentation. Machine fields and original events remain untouched."""

from __future__ import annotations

import os
import re

from app.models import NotificationEvent, NotificationPriority
from app.notification_events import event_code, normalize_event, reason_code

REASONS = {
    "LIVE writer lease lost": "LIVE 单写权限已丢失，需要人工检查并重启",
    "reconciliation failed": "OKX 对账暂时失败",
    "reconciliation permanent failure": "OKX 对账发生不可自动恢复的错误",
    "LIVE startup requires manual resume": "实盘启动等待人工确认恢复",
    "WebSocket disconnected or stale": "WebSocket 连接中断或数据过期",
    "Redis unavailable": "Redis 不可用",
    "dead man switch unavailable": "Cancel-All-After 不可用",
    "position mismatch": "本地与交易所仓位不一致",
    "startup position mismatch": "启动时仓位对账不一致",
    "startup fill size mismatch": "启动时成交数量不一致",
    "startup partial fill unprotected": "启动时发现部分成交仓位缺少有效保护",
    "protective stop cannot be verified": "无法确认保护性止损",
    "protective stop invalid": "保护性止损无效",
    "margin ratio danger": "保证金风险过高",
    "foreign risk-increasing pending order": "发现未知的风险增加挂单",
    "unexpected algo order": "发现未知条件单",
    "unexpected order": "发现未知订单",
    "order mismatch": "本地与交易所订单状态不一致",
    "position risk cannot be reconstructed": "无法重建当前持仓风险",
    "pending order risk unknown": "无法确认挂单风险",
    "manual kill switch": "人工触发停止交易",
    "manual cancel all": "人工执行全部撤单",
    "manual stop": "人工正常停止",
    "auto recovery circuit breaker": "自动恢复触发熔断",
    "startup reconciliation pending": "等待启动对账完成",
    "startup entry cancellation unconfirmed": "启动时尚未确认开仓挂单已撤销",
    "daily loss limit": "已达到当日亏损上限",
    "weekly drawdown limit": "已达到周回撤上限",
    "weekly DD limit": "已达到周回撤上限",
    "market stale": "行情数据过期",
    "market processing failed": "行情处理失败",
    "unprotected position": "持仓缺少有效止损保护",
    "state or data unhealthy": "系统状态或数据异常",
    "margin usage limit": "已达到保证金使用上限",
    "spread explosion": "买卖价差异常扩大",
    "instrument metadata unavailable": "无法获取合约元数据",
    "candle preload failed": "历史K线预加载失败",
    "emergency recovery pending": "等待紧急风控恢复处理",
    "startup entry fill size unavailable": "启动时无法确认开仓成交数量",
    "startup coverage uncertain": "启动时无法确认止损保护覆盖",
    "startup leverage exceeds limit": "启动时杠杆超过限制",
    "derivatives account mode required": "账户需要使用合约模式",
    "USDT margin unavailable": "USDT 保证金不可用",
    "algo state audit failed": "条件单状态审计失败",
    "unowned algo order": "发现非本系统条件单",
    "partial fill before protective stop active": "止损保护生效前出现部分成交",
    "safety monitor failure": "安全监控异常",
    "Cancellation or protection unconfirmed": "尚未确认撤单或持仓保护",
    "websocket unavailable": "WebSocket 不可用",
    "redis unavailable": "Redis 不可用",
    "caa unavailable": "Cancel-All-After 不可用",
    "reconciliation unavailable": "OKX 对账不可用",
}
COMPONENT_NAMES = {
    "reconciliation": "OKX 对账",
    "Reconciliation": "OKX 对账",
    "websocket": "WebSocket",
    "redis": "Redis",
    "caa": "Cancel-All-After",
    "safety": "安全风控",
    "emergency": "紧急风控",
    "auto_recovery": "自动恢复",
}
TITLES = {
    "SYSTEM_STARTED": "🟢 量化系统已启动",
    "SYSTEM_STOPPING": "🟡 量化系统正在安全停止",
    "SYSTEM_STOPPED": "⚪ 量化系统已停止",
    "RISK_HALT": "🚨 风控已暂停交易",
    "RISK_EMERGENCY": "🚨 紧急风控已触发",
    "RISK_RECOVERED": "✅ 风控状态已恢复",
    "EMERGENCY_REDUCTION_FAILED": "🚨 紧急减仓失败",
    "AUTO_RECOVERY_COMPLETED": "✅ 系统已自动恢复交易",
    "AUTO_RECOVERY_DISABLED": "🚨 自动恢复已停用",
    "INCIDENT_RESOLVED": "✅ 交易系统已恢复",
    "INFRASTRUCTURE_RECOVERED": "✅ 交易基础设施已恢复",
    "INCIDENT_RETROSPECTIVE": "ℹ️ 交易异常已恢复",
    "EMERGENCY_RETROSPECTIVE": "🚨 紧急风控事件已结束",
    "SAFETY_RETROSPECTIVE": "⚠️ 安全风控事件已结束",
    "WATCHDOG_OFFLINE": "🚨 交易程序已离线",
    "WATCHDOG_UNHEALTHY": "⚠️ 交易程序状态异常",
    "WATCHDOG_RECOVERED": "✅ 交易程序已恢复",
    "HEARTBEAT_HEALTHY": "❤️ 量化系统运行正常",
    "HEARTBEAT_UNHEALTHY": "⚠️ 量化系统状态异常",
    "DAILY_REPORT": "📊 每日交易报告",
    "WS_DISCONNECTED": "⚠️ WebSocket 连接中断",
    "WS_RECOVERED": "✅ WebSocket 已恢复",
    "WS_RECONNECT_FLAP": "⚠️ WebSocket 频繁重连",
    "REDIS_UNAVAILABLE": "⚠️ Redis 不可用",
    "REDIS_RECOVERED": "✅ Redis 已恢复",
    "RECONCILIATION_UNHEALTHY": "⚠️ OKX 对账异常",
    "RECONCILIATION_RECOVERED": "✅ OKX 对账已恢复",
    "CAA_UNAVAILABLE": "⚠️ Cancel-All-After 不可用",
    "CAA_RECOVERED": "✅ Cancel-All-After 已恢复",
    "ENTRY_CANCEL_UNCONFIRMED": "⚠️ 尚未确认开仓挂单已撤销",
    "FOREIGN_PENDING_ORDER": "⚠️ 发现未知挂单",
    "UNSAFE_SHUTDOWN": "🚨 安全停机尚未完成",
}
LABELS = {
    "Mode": "模式",
    "Equity": "账户权益",
    "Risk": "风控状态",
    "WS": "WebSocket",
    "Reason": "原因",
    "New entries": "新开仓",
    "Target position": "目标仓位",
    "Side": "方向",
    "Direction": "方向",
    "Size": "数量",
    "Contracts": "数量",
    "Entry": "成交价",
    "Entry ref": "参考入场价",
    "Strategy": "策略",
    "Stop": "止损价",
    "Notional": "名义金额",
    "Filled": "已成交",
    "Remaining": "剩余数量",
    "Protection": "保护状态",
    "Coverage": "保护覆盖率",
    "Exit": "平仓价",
    "Daily PnL": "当日盈亏",
    "Net PnL": "净盈亏",
    "Realized PnL": "已实现盈亏",
    "Realized PnL after fees": "扣费后已实现盈亏",
    "Weekly DD": "周回撤",
    "Previous": "此前状态",
    "Previous state": "此前状态",
    "Current": "当前状态",
    "Current Risk": "当前风控",
    "Duration": "异常持续",
    "Downtime": "异常持续",
    "Components": "异常组件",
    "Component": "组件",
    "Status": "状态",
    "Failures": "连续失败",
    "Last error": "最后错误",
    "Uptime": "运行时间",
    "Positions": "持仓数量",
    "Open Risk": "未平仓风险",
    "Reconnects": "重连次数",
    "Emergency targets": "紧急平仓目标数",
    "Orders": "订单数量",
    "Fills": "成交次数",
    "Trades": "交易次数",
    "Wins": "盈利次数",
    "Losses": "亏损次数",
    "Fees": "手续费",
    "Funding": "资金费",
    "Max DD": "最大回撤",
    "Date": "日期",
    "HALT count": "HALT 次数",
    "EMERGENCY count": "EMERGENCY 次数",
    "Transient count": "瞬态异常次数",
    "Auto recovery count": "自动恢复次数",
    "Healthy checks": "连续健康检查",
    "Reconciliation": "OKX 对账",
    "Reconnects in 5 minutes": "5分钟内重连次数",
}
VALUES = {
    "LONG": "做多",
    "SHORT": "做空",
    "blocked": "已禁止",
    "confirmed": "已保护",
    "pending": "等待保护",
    "pending / emergency": "等待保护 / 紧急风控处理中",
    "healthy": "正常",
    "recovered": "已恢复",
    "unavailable": "不可用",
    "APP_DOWN": "离线",
    "APP_ALIVE_BUT_UNHEALTHY": "程序状态异常",
    "INFRASTRUCTURE_UNHEALTHY": "基础设施异常",
    "unknown": "未知",
    "breakout": "突破",
    "trend": "趋势",
}
SENTENCES = {
    "Status endpoint unavailable": "状态接口无法访问",
    "Status endpoint healthy again": "状态接口已恢复正常",
    "App responds but is not ready": "程序仍可访问，但尚未达到正常交易条件",
    "Repeated transient failures": "短时间内重复发生故障",
    "Manual resume required": "系统将继续保持 HALT，需要人工检查后恢复",
    "Alert delivery was delayed; incident occurred while notifications were unavailable": "通知服务此前不可用，因此本条消息延迟送达",
    "Alert delivery was delayed; incident has ended": "此前告警延迟送达，基础设施异常现已结束",
}


def safe_detail(value: str) -> str:
    for name in ("BARK_DEVICE_KEY", "OKX_API_KEY", "OKX_SECRET_KEY", "OKX_PASSPHRASE"):
        secret = os.environ.get(name)
        if secret:
            value = value.replace(secret, "[已隐藏]")
    value = re.sub(r"https?://\S+", "[地址已隐藏]", value)
    value = re.sub(
        r"(?i)(?:device[_-]?key|api[_-]?key|secret|password|passphrase|authorization|token)\s*[:=]\s*\S+",
        "[凭证已隐藏]",
        value,
    )
    return value[:256]


def localized_reason(reason: str) -> str:
    if reason in REASONS:
        return REASONS[reason]
    if not reason:
        return "未提供原因"
    return f"系统异常（reason_code: {safe_detail(reason)}）"


def duration_text(value: str) -> str:
    match = re.fullmatch(r"(\d+)s(\+)?", value)
    if not match:
        return safe_detail(value).replace("days", "天").replace("day", "天")
    seconds = int(match[1])
    hours, remaining = divmod(seconds, 3600)
    minutes, seconds = divmod(remaining, 60)
    text = (f"{hours}小时" if hours else "") + (f"{minutes}分" if minutes else "")
    text += f"{seconds}秒" if seconds or not text else ""
    return text + ("以上" if match[2] else "")


def _body_line(line: str, event: NotificationEvent) -> str:
    if line in SENTENCES:
        return SENTENCES[line]
    if event.symbol and line == event.symbol:
        return f"交易对：{line}"
    label, separator, value = line.partition(": ")
    if not separator:
        return localized_reason(line)
    if label == "Reason":
        value = ", ".join(localized_reason(part) for part in value.split(", "))
    elif label in {"Duration", "Downtime", "Uptime"}:
        value = duration_text(value)
    elif label in {"Components", "Component"}:
        value = "、".join(COMPONENT_NAMES.get(part, part) for part in value.split(", "))
    else:
        value = VALUES.get(value, safe_detail(value))
        value = value.replace(" contracts", " 张").replace(" healthy", " 正常")
        if label == "WS" and "正常" not in value:
            value += " 正常" if event_code(event) == "SYSTEM_STARTED" else ""
        if label in {"Failures", "Reconnects", "Reconnects in 5 minutes"}:
            value += " 次"
    return f"{LABELS.get(label, '详细信息（' + safe_detail(label) + '）')}：{value}"


def localize_bark_event(event: NotificationEvent) -> NotificationEvent:
    context = normalize_event(event)
    code = event_code(context)
    symbol = (event.symbol or "").split("-")[0]
    if not symbol and code in {
        "TRADE_FILLED",
        "ENTRY_SUBMITTED",
        "TRADE_PARTIAL_FILL",
        "PROTECTION_ACTIVE",
        "POSITION_CLOSED",
    }:
        parts = event.title.split()
        symbol = parts[1] if len(parts) > 2 else ""
    symbol = symbol or "交易"
    side = context.metadata.get("side")
    if code == "TRADE_FILLED":
        title = (
            f"🔴 {symbol} 空单已成交"
            if side == "SHORT"
            else f"🟢 {symbol} 多单已成交" if side == "LONG" else f"🟢 {symbol} 订单已成交"
        )
    elif code == "ENTRY_SUBMITTED":
        title = f"📤 {symbol} 开仓订单已提交"
    elif code == "TRADE_PARTIAL_FILL":
        title = f"🟡 {symbol} 订单部分成交"
    elif code == "PROTECTION_ACTIVE":
        title = f"🛡 {symbol} 止损保护已生效"
    elif code == "POSITION_CLOSED":
        title = f"💰 {symbol} 仓位已平仓"
    elif code == "INCIDENT_OPEN":
        risk = context.metadata.get("risk_state") or next(
            (line[6:] for line in event.message.splitlines() if line.startswith("Risk: ")), ""
        )
        title = "⚠️ 交易已暂时停止" if risk == "HALT" else "⚠️ 交易基础设施异常"
    else:
        title = TITLES.get(
            code,
            "🚨 系统严重异常" if event.priority == NotificationPriority.CRITICAL else "⚠️ 系统通知",
        )
    lines = [_body_line(line, context) for line in event.message.splitlines() if line]
    if code in {"ENTRY_CANCEL_UNCONFIRMED", "EMERGENCY_REDUCTION_FAILED", "UNSAFE_SHUTDOWN"}:
        lines = [f"原因：{localized_reason(reason_code(context) or event.message)}"]
    if code == "FOREIGN_PENDING_ORDER":
        lines = [f"订单标识：{safe_detail(event.message)}"]
    if code == "TRADE_FILLED" and side in {"LONG", "SHORT"}:
        lines.insert(0, f"方向：{VALUES[str(side)]}")
    if code == "PROTECTION_ACTIVE":
        lines.append("保护状态：正常")
    if code == "EMERGENCY_REDUCTION_FAILED":
        if event.symbol:
            lines.insert(0, f"交易对：{event.symbol}")
        lines.append("需要人工检查")
    if code in {"INCIDENT_RETROSPECTIVE", "SAFETY_RETROSPECTIVE", "EMERGENCY_RETROSPECTIVE"}:
        lines = [
            line
            for line in lines
            if line
            != SENTENCES[
                "Alert delivery was delayed; incident occurred while notifications were unavailable"
            ]
        ]
        lines.insert(0, "通知因 Bark 暂时不可用而延迟")
        if code == "EMERGENCY_RETROSPECTIVE":
            lines.insert(0, "此前曾发生紧急风控事件")
        elif code == "SAFETY_RETROSPECTIVE":
            lines.insert(0, "此前曾发生安全风控事件")
    return event.model_copy(update={"title": title, "message": "\n".join(lines)}, deep=True)
