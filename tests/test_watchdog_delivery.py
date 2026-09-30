from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import httpx
import pytest

from app.monitoring import ConsoleNotification, Metrics
from app.notification_policy import NotificationPolicy
from app.notifications import BarkNotification, NotificationManager
from app.watchdog import WatchdogMonitor, WatchdogSettings
from tests.test_notifications import bark_settings
from tests.test_watchdog import healthy


async def wait_for(condition):
    async def poll():
        while not condition():
            await asyncio.sleep(0)

    await asyncio.wait_for(poll(), 1)


@asynccontextmanager
async def watchdog_setup():
    now = [0.0]
    state = {"app_down": True, "bark_down": True}
    attempts: list[dict] = []
    sent: list[dict] = []
    probes: list[httpx.Request] = []

    def probe(request):
        probes.append(request)
        return httpx.Response(503) if state["app_down"] else healthy()

    def push(request):
        payload = json.loads(request.content)
        attempts.append(payload)
        if state["bark_down"]:
            return httpx.Response(503)
        sent.append(payload)
        return httpx.Response(200, json={"code": 200})

    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(probe)) as app_client,
        httpx.AsyncClient(transport=httpx.MockTransport(push)) as bark_client,
    ):
        notifier = NotificationManager(
            [ConsoleNotification(), BarkNotification(bark_settings(), bark_client)],
            Metrics(),
            policy=NotificationPolicy(),
            retry_delays=(0, 0, 0),
        )
        monitor = WatchdogMonitor(
            WatchdogSettings(_env_file=None, watchdog_startup_grace_seconds=0),
            app_client,
            notifier,
            clock=lambda: now[0],
        )
        notifier.start()
        try:
            for _ in range(3):
                await monitor.check()
            await wait_for(lambda: monitor.alert_delivery_failures == 1)
            yield monitor, now, state, attempts, sent, probes
        finally:
            await notifier.stop(drain_seconds=1)


@pytest.mark.asyncio
async def test_watchdog_offline_rearms_after_final_bark_failure():
    async with watchdog_setup() as (monitor, now, _, attempts, sent, _):
        assert len(attempts) == 4
        assert not sent
        assert not monitor.alert_delivery_confirmed
        assert not monitor.alert_delivery_pending
        assert monitor.next_alert_retry_at == 30
        now[0] = 29
        await monitor.check()
        assert len(attempts) == 4


@pytest.mark.asyncio
async def test_watchdog_offline_retries_when_app_still_down():
    async with watchdog_setup() as (monitor, now, state, _, sent, _):
        original_id = monitor.outage_id
        state["bark_down"] = False
        now[0] = 30
        await monitor.check()
        await wait_for(lambda: monitor.alert_delivery_confirmed)
        assert monitor.outage_id == original_id
        assert [item["title"] for item in sent] == ["🚨 交易程序已离线"]


@pytest.mark.asyncio
async def test_watchdog_offline_retry_uses_bounded_backoff():
    async with watchdog_setup() as (monitor, now, _, _, _, _):
        original_id = monitor.outage_id
        for failures, delay in enumerate((60, 120, 240, 300, 300), start=2):
            now[0] = monitor.next_alert_retry_at
            await monitor.check()
            await wait_for(lambda expected=failures: monitor.alert_delivery_failures == expected)
            assert monitor.next_alert_retry_at == now[0] + delay
            assert monitor.outage_id == original_id


@pytest.mark.asyncio
async def test_watchdog_successful_offline_alert_is_not_repeated():
    async with watchdog_setup() as (monitor, now, state, _, sent, _):
        state["bark_down"] = False
        now[0] = 30
        await monitor.check()
        await wait_for(lambda: monitor.alert_delivery_confirmed)
        for current in (60, 120, 600, 3600):
            now[0] = current
            await monitor.check()
        assert len(sent) == 1


@pytest.mark.asyncio
async def test_watchdog_recovery_only_sent_after_confirmed_offline_delivery():
    async with watchdog_setup() as (monitor, now, state, _, sent, _):
        state["bark_down"] = False
        now[0] = 30
        await monitor.check()
        await wait_for(lambda: monitor.alert_delivery_confirmed)
        state["app_down"] = False
        await monitor.check()
        assert len(sent) == 1
        await monitor.check()
        await wait_for(lambda: len(sent) == 2)
        assert [item["title"] for item in sent] == ["🚨 交易程序已离线", "✅ 交易程序已恢复"]
        await monitor.check()
        assert len(sent) == 2


@pytest.mark.asyncio
async def test_watchdog_failed_offline_then_recovery_sends_no_false_recovery():
    async with watchdog_setup() as (monitor, now, state, attempts, sent, _):
        state["app_down"] = False
        await monitor.check()
        await monitor.check()
        state["bark_down"] = False
        now[0] = 600
        await monitor.check()
        await asyncio.sleep(0)
        assert len(attempts) == 4
        assert sent == []


@pytest.mark.asyncio
async def test_watchdog_retry_never_calls_trading_endpoints():
    async with watchdog_setup() as (monitor, now, _, _, _, probes):
        now[0] = 30
        await monitor.check()
        await wait_for(lambda: monitor.alert_delivery_failures == 2)
        assert {(request.method, request.url.path) for request in probes} == {("GET", "/status")}


@pytest.mark.asyncio
async def test_watchdog_queue_eviction_rearms_offline_alert():
    from app.models import (
        NotificationCategory,
        NotificationEvent,
        NotificationLevel,
        NotificationPriority,
    )

    now = [0.0]
    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(503))) as client,
        httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"code": 200}))
        ) as bark_client,
    ):
        notifier = NotificationManager(
            [BarkNotification(bark_settings(), bark_client)], Metrics(), max_queue=1
        )
        monitor = WatchdogMonitor(
            WatchdogSettings(_env_file=None, watchdog_startup_grace_seconds=0),
            client,
            notifier,
            clock=lambda: now[0],
        )
        for _ in range(3):
            await monitor.check()
        assert monitor.alert_delivery_pending
        await notifier.publish(
            NotificationEvent(
                level=NotificationLevel.CRITICAL,
                category=NotificationCategory.SYSTEM,
                title="Other critical alert",
                message="another",
                priority=NotificationPriority.CRITICAL,
            )
        )
        assert monitor.alert_delivery_failures == 1
        assert not monitor.alert_delivery_pending
        assert monitor.next_alert_retry_at == 30


@pytest.mark.asyncio
async def test_watchdog_cancels_delayed_offline_after_undelivered_recovery():
    state = {"down": True}
    attempted = asyncio.Event()
    attempts = []

    def push(request):
        attempts.append(request)
        attempted.set()
        return httpx.Response(503)

    async with (
        httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(503) if state["down"] else healthy()
            )
        ) as client,
        httpx.AsyncClient(transport=httpx.MockTransport(push)) as bark_client,
    ):
        notifier = NotificationManager(
            [BarkNotification(bark_settings(), bark_client)],
            Metrics(),
            policy=NotificationPolicy(),
            retry_delays=(3600,),
        )
        monitor = WatchdogMonitor(
            WatchdogSettings(_env_file=None, watchdog_startup_grace_seconds=0), client, notifier
        )
        notifier.start()
        try:
            for _ in range(3):
                await monitor.check()
            await asyncio.wait_for(attempted.wait(), 1)
            state["down"] = False
            await monitor.check()
            await monitor.check()
            channel = notifier._channels[0]
            _, _, delivery = channel.delayed.pop()
            assert not monitor.should_send(delivery.event, "bark")
            await notifier._send_once(channel, delivery)
            channel.size -= 1
            assert len(attempts) == 1
        finally:
            await notifier.stop(drain_seconds=1)


@pytest.mark.asyncio
async def test_watchdog_console_and_webhook_cannot_confirm_bark_delivery():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(503))
    ) as client:
        notifier = NotificationManager(
            [ConsoleNotification()], Metrics(), policy=NotificationPolicy()
        )
        monitor = WatchdogMonitor(
            WatchdogSettings(_env_file=None, watchdog_startup_grace_seconds=0), client, notifier
        )
        for _ in range(3):
            await monitor.check()
        event = next(item for queue in notifier._pending.values() for item in queue)
        monitor.delivered(event, "console")
        monitor.delivered(event, "webhook")
        assert not monitor.alert_delivery_confirmed
        assert monitor.alert_delivery_pending


@pytest.mark.asyncio
async def test_new_watchdog_outage_is_not_suppressed_by_previous_outage():
    async with watchdog_setup() as (monitor, now, state, _, sent, _):
        first_id = monitor.outage_id
        state["bark_down"] = False
        now[0] = 30
        await monitor.check()
        await wait_for(lambda: monitor.alert_delivery_confirmed)
        state["app_down"] = False
        await monitor.check()
        await monitor.check()
        await wait_for(lambda: len(sent) == 2)
        state["app_down"] = True
        for _ in range(3):
            await monitor.check()
        await wait_for(lambda: len(sent) == 3)
        assert monitor.outage_id != first_id
        assert sent[-1]["title"] == "🚨 交易程序已离线"
