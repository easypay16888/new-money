from __future__ import annotations

import json

import httpx
import pytest

from app.bark_formatter import localize_bark_event, localized_reason
from app.models import (
    NotificationCategory,
    NotificationEvent,
    NotificationLevel,
    NotificationPriority,
)
from app.notification_policy import NotificationPolicy
from app.notifications import BarkNotification
from tests.test_notifications import bark_settings


def notice(code: str, message: str = "", **kwargs) -> NotificationEvent:
    return NotificationEvent(
        event_code=code,
        title="internal title",
        message=message,
        level=NotificationLevel.INFO,
        category=NotificationCategory.SYSTEM,
        **kwargs,
    )


@pytest.mark.parametrize(
    ("code", "message", "title", "body"),
    [
        (
            "SYSTEM_STARTED",
            "Mode: PAPER\nEquity: 5000 USDT\nRisk: NORMAL\nWS: 4/4",
            "🟢 量化系统已启动",
            "账户权益：5000 USDT",
        ),
        (
            "TRADE_FILLED",
            "Entry: 50000\nContracts: 1\nStrategy: breakout",
            "🟢 BTC 多单已成交",
            "成交价：50000",
        ),
        (
            "PROTECTION_ACTIVE",
            "Stop: 48000\nCoverage: 100%",
            "🛡 BTC 止损保护已生效",
            "止损价：48000",
        ),
        (
            "POSITION_CLOSED",
            "Exit: 51000\nDaily PnL: 10 USDT",
            "💰 BTC 仓位已平仓",
            "当日盈亏：10 USDT",
        ),
        (
            "RISK_HALT",
            "Reason: position mismatch\nNew entries: blocked",
            "🚨 风控已暂停交易",
            "原因：本地与交易所仓位不一致",
        ),
        (
            "RISK_EMERGENCY",
            "Reason: protective stop cannot be verified\nTarget position: 0",
            "🚨 紧急风控已触发",
            "目标仓位：0",
        ),
        (
            "INCIDENT_OPEN",
            "Components: reconciliation\nReason: reconciliation failed\nDuration: 60s+\nRisk: HALT",
            "⚠️ 交易已暂时停止",
            "原因：OKX 对账暂时失败",
        ),
        (
            "INCIDENT_RESOLVED",
            "Previous: HALT\nReason: Redis unavailable\nDowntime: 151s\nRisk: NORMAL",
            "✅ 交易系统已恢复",
            "异常持续：2分31秒",
        ),
        (
            "WATCHDOG_OFFLINE",
            "Status endpoint unavailable\nFailures: 3\nLast error: ConnectTimeout",
            "🚨 交易程序已离线",
            "最后错误：ConnectTimeout",
        ),
        (
            "WATCHDOG_RECOVERED",
            "Status endpoint healthy again\nPrevious: APP_DOWN",
            "✅ 交易程序已恢复",
            "此前状态：离线",
        ),
        (
            "DAILY_REPORT",
            "Date: 2026-09-30\nEquity: 5000\nDaily PnL: 10\nHALT count: 1",
            "📊 每日交易报告",
            "HALT 次数：1",
        ),
        (
            "INCIDENT_RETROSPECTIVE",
            "Reason: reconciliation failed\nDuration: 192s\nCurrent Risk: NORMAL",
            "ℹ️ 交易异常已恢复",
            "异常持续：3分12秒",
        ),
    ],
)
@pytest.mark.asyncio
async def test_bark_core_notifications_are_chinese(code, message, title, body):
    payloads: list[dict] = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: (
                payloads.append(json.loads(request.content))
                or httpx.Response(200, json={"code": 200})
            )
        )
    ) as client:
        original = notice(
            code, message, symbol="BTC-USDT-SWAP", metadata={"side": "LONG", "risk_state": "HALT"}
        )
        await BarkNotification(bark_settings(), client).send(original)
    assert payloads[0]["title"] == title
    assert body in payloads[0]["body"]
    assert original.title == "internal title"
    assert "Reason:" not in payloads[0]["body"]


@pytest.mark.parametrize(
    "field", ["dedup_key", "priority", "category", "event_code", "id", "timestamp", "metadata"]
)
def test_localization_preserves_machine_fields(field):
    event = notice(
        "RISK_EMERGENCY",
        "Reason: protective stop invalid",
        dedup_key="risk-state",
        priority=NotificationPriority.CRITICAL,
        metadata={"incident_id": "same-id"},
    )
    translated = localize_bark_event(event)
    assert getattr(translated, field) == getattr(event, field)
    assert translated is not event


@pytest.mark.parametrize(
    ("code", "message"),
    [
        ("RISK_EMERGENCY", "Reason: protective stop invalid"),
        ("RISK_HALT", "Reason: position mismatch"),
        ("WATCHDOG_OFFLINE", "Status endpoint unavailable"),
    ],
)
def test_chinese_critical_events_still_bypass_policy(code, message):
    event = notice(code, message, metadata={"reason": "position mismatch"})
    translated = localize_bark_event(event)
    policy = NotificationPolicy(risk_enabled=False)
    assert policy.is_critical(translated)
    decision = policy.evaluate(translated, "bark")
    assert decision.send and decision.priority == NotificationPriority.CRITICAL


def test_known_reasons_are_localized_and_unknown_reason_is_redacted():
    assert (
        localized_reason("startup partial fill unprotected") == "启动时发现部分成交仓位缺少有效保护"
    )
    assert "reason_code: unknown condition" in localized_reason("unknown condition")
    assert "secret-value" not in localized_reason("device_key=secret-value")
    assert "password" not in localized_reason("https://user:password@example.com/private")


def test_short_and_partial_trade_copy_is_chinese():
    short = localize_bark_event(
        notice("TRADE_FILLED", "Contracts: 1", symbol="BTC-USDT-SWAP", metadata={"side": "SHORT"})
    )
    partial = localize_bark_event(
        notice(
            "TRADE_PARTIAL_FILL",
            "Filled: 0.5 contracts\nRemaining: 0.5 contracts\nProtection: confirmed",
            symbol="BTC-USDT-SWAP",
        )
    )
    assert short.title == "🔴 BTC 空单已成交"
    assert "方向：做空" in short.message
    assert partial.title == "🟡 BTC 订单部分成交"
    assert "保护状态：已保护" in partial.message


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["retry", "recovery", "retrospective"])
async def test_chinese_incident_lifecycle(scenario):
    import asyncio

    from app.incidents import IncidentManager
    from app.monitoring import Metrics
    from app.notifications import NotificationManager

    now = [0.0]
    failing = [scenario != "recovery"]
    payloads: list[dict] = []

    def respond(request):
        if failing[0]:
            return httpx.Response(503)
        payloads.append(json.loads(request.content))
        return httpx.Response(200, json={"code": 200})

    async def wait_for(condition):
        async def poll():
            while not condition():
                await asyncio.sleep(0)

        await asyncio.wait_for(poll(), 1)

    incidents = IncidentManager(clock=lambda: now[0])
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        notifier = NotificationManager(
            [BarkNotification(bark_settings(), client)],
            Metrics(),
            policy=NotificationPolicy(),
            incidents=incidents,
            retry_delays=(),
        )
        notifier.start()
        try:
            await notifier.publish(
                NotificationEvent(
                    event_code="RISK_HALT",
                    title="🚨 风控已暂停交易",
                    message="原因：对账异常",
                    level=NotificationLevel.ERROR,
                    category=NotificationCategory.RISK,
                    metadata={
                        "reason": "reconciliation failed",
                        "risk_state": "HALT",
                        "transition": True,
                    },
                )
            )
            now[0] = 60
            for event in incidents.due():
                await notifier.publish(event)
            incident = incidents.active["infra:trading"]
            if scenario == "recovery":
                await wait_for(lambda: incident.notified_open)
            else:
                await wait_for(lambda: incident.open_delivery_failures == 1)
            if scenario in {"retrospective", "recovery"}:
                await notifier.publish(
                    NotificationEvent(
                        event_code="RISK_RECOVERED",
                        title="✅ 风控状态已恢复",
                        message="当前状态：NORMAL",
                        level=NotificationLevel.INFO,
                        category=NotificationCategory.RISK,
                        metadata={"recovery": True, "risk_state": "NORMAL"},
                    )
                )
            failing[0] = False
            now[0] = 90
            for event in incidents.due():
                await notifier.publish(event)
            await wait_for(
                lambda: (
                    incident.notified_open if scenario == "retry" else incident.notified_resolved
                )
            )
        finally:
            await notifier.stop(drain_seconds=1)
    expected = {
        "retry": ["⚠️ 交易已暂时停止"],
        "recovery": ["⚠️ 交易已暂时停止", "✅ 交易系统已恢复"],
        "retrospective": ["ℹ️ 交易异常已恢复"],
    }
    assert [payload["title"] for payload in payloads] == expected[scenario]
    assert all("Reason:" not in payload["body"] for payload in payloads)


@pytest.mark.parametrize(
    ("code", "title"),
    [
        ("SYSTEM_STOPPING", "🟡 量化系统正在安全停止"),
        ("SYSTEM_STOPPED", "⚪ 量化系统已停止"),
        ("AUTO_RECOVERY_DISABLED", "🚨 自动恢复已停用"),
        ("HEARTBEAT_HEALTHY", "❤️ 量化系统运行正常"),
        ("HEARTBEAT_UNHEALTHY", "⚠️ 量化系统状态异常"),
        ("WATCHDOG_UNHEALTHY", "⚠️ 交易程序状态异常"),
        ("EMERGENCY_REDUCTION_FAILED", "🚨 紧急减仓失败"),
        ("EMERGENCY_RETROSPECTIVE", "🚨 紧急风控事件已结束"),
        ("SAFETY_RETROSPECTIVE", "⚠️ 安全风控事件已结束"),
    ],
)
def test_remaining_bark_core_titles_are_chinese(code, title):
    event = notice(code, "Reason: protective stop invalid", priority=NotificationPriority.CRITICAL)
    translated = localize_bark_event(event)
    assert translated.title == title
    assert translated.priority == NotificationPriority.CRITICAL
    assert "protective stop invalid" not in translated.message


def test_formatter_does_not_invent_trade_direction_or_pnl():
    translated = localize_bark_event(notice("TRADE_FILLED", "Contracts: 1", symbol="BTC-USDT-SWAP"))
    assert translated.title == "🟢 BTC 订单已成交"
    assert "方向" not in translated.message
    assert "盈亏" not in translated.message


@pytest.mark.asyncio
async def test_display_language_does_not_change_dedup_identity():
    from app.monitoring import Metrics
    from app.notifications import NotificationManager
    from tests.test_notifications import Recorder

    notifier = NotificationManager([Recorder()], Metrics())
    original = notice(
        "SYSTEM_STARTED", "Mode: PAPER", dedup_key="system-started", metadata={"transition": True}
    )
    translated = localize_bark_event(original)
    await notifier.publish(original)
    await notifier.publish(translated)
    assert notifier.queue_size == 1


@pytest.mark.asyncio
async def test_unknown_reason_cannot_expose_configured_bark_key():
    payloads = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: (
                payloads.append(json.loads(request.content))
                or httpx.Response(200, json={"code": 200})
            )
        )
    ) as client:
        await BarkNotification(bark_settings(), client).send(
            notice("RISK_HALT", "Reason: unexpected test-device-secret")
        )
    assert "test-device-secret" not in payloads[0]["body"]
    assert payloads[0]["device_key"] == "test-device-secret"
