from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock

import httpx
import pytest

from app.models import GovernorState, NotificationPriority
from app.monitoring import Metrics
from app.notifications import BarkDeliveryError, BarkNotification, NotificationManager
from app.storage import Store
from tests.test_notifications import Recorder, bark_settings, event
from tests.test_production_safety import runtime_with_entry


async def _bark_result(response: httpx.Response) -> tuple[int, NotificationManager]:
    calls = 0

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return response

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        notifier = NotificationManager(
            [BarkNotification(bark_settings(), client)], Metrics(), retry_delays=(0, 0, 0)
        )
        notifier.start()
        await notifier.publish(event())
        await notifier.stop(drain_seconds=1)
    return calls, notifier


@pytest.mark.asyncio
async def test_bark_200_non_json_retries():
    calls, notifier = await _bark_result(httpx.Response(200, text="<html>proxy error</html>"))
    assert calls == 4
    assert notifier.metrics.notification_failed.labels(
        channel="bark", priority="ACTIVE", category="SYSTEM"
    )._value.get() == 1


@pytest.mark.asyncio
async def test_bark_200_empty_body_retries():
    calls, notifier = await _bark_result(httpx.Response(200, content=b""))
    assert calls == 4
    assert notifier.metrics.notification_failed.labels(
        channel="bark", priority="ACTIVE", category="SYSTEM"
    )._value.get() == 1


@pytest.mark.asyncio
async def test_bark_missing_code_is_failure():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"message": "ok"}))
    ) as client:
        with pytest.raises(BarkDeliveryError) as exc:
            await BarkNotification(bark_settings(), client).send(event())
    assert exc.value.status_code == 200


@pytest.mark.asyncio
async def test_bark_success_code_200():
    calls, notifier = await _bark_result(httpx.Response(200, json={"code": 200}))
    assert calls == 1
    assert notifier.metrics.notification_sent.labels(
        channel="bark", priority="ACTIVE", category="SYSTEM"
    )._value.get() == 1


@pytest.mark.asyncio
async def test_bark_success_code_0():
    calls, notifier = await _bark_result(httpx.Response(200, json={"code": 0}))
    assert calls == 1
    assert notifier.metrics.notification_sent.labels(
        channel="bark", priority="ACTIVE", category="SYSTEM"
    )._value.get() == 1


@pytest.mark.asyncio
async def test_broken_webhook_does_not_delay_bark():
    webhook_entered = asyncio.Event()
    release_webhook = asyncio.Event()
    bark_received = asyncio.Event()

    class BrokenWebhook(Recorder):
        async def send(self, item):
            webhook_entered.set()
            await release_webhook.wait()
            raise TimeoutError("webhook unavailable")

    def bark_response(_: httpx.Request) -> httpx.Response:
        bark_received.set()
        return httpx.Response(200, json={"code": 200})

    async with httpx.AsyncClient(transport=httpx.MockTransport(bark_response)) as client:
        notifier = NotificationManager(
            [BrokenWebhook(), BarkNotification(bark_settings(), client)],
            Metrics(), retry_delays=(),
        )
        notifier.start()
        await notifier.publish(event("EMERGENCY", priority=NotificationPriority.CRITICAL))
        await asyncio.wait_for(webhook_entered.wait(), timeout=0.5)
        await asyncio.wait_for(bark_received.wait(), timeout=0.5)
        release_webhook.set()
        await notifier.stop(drain_seconds=1)


@pytest.mark.asyncio
async def test_bark_failure_does_not_block_console():
    received = asyncio.Event()

    class Console(Recorder):
        async def send(self, item):
            await super().send(item)
            received.set()

    def bark_failure(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    async with httpx.AsyncClient(transport=httpx.MockTransport(bark_failure)) as client:
        console = Console()
        notifier = NotificationManager(
            [BarkNotification(bark_settings(), client), console], Metrics(), retry_delays=()
        )
        notifier.start()
        await notifier.publish(event())
        await asyncio.wait_for(received.wait(), timeout=0.5)
        await notifier.stop(drain_seconds=1)
    assert [item.title for item in console.events] == ["Test"]


@pytest.mark.asyncio
async def test_channel_worker_failure_does_not_stop_other_channels():
    first, second = Recorder(), Recorder()
    notifier = NotificationManager([first, second], Metrics(), retry_delays=())
    original = notifier._send_once
    crashed = asyncio.Event()

    async def fail_once(state, delivery):
        if state is notifier._channels[0] and not crashed.is_set():
            crashed.set()
            raise RuntimeError("worker crashed")
        await original(state, delivery)

    notifier._send_once = fail_once  # type: ignore[method-assign]
    notifier.start()
    await notifier.publish(event())
    await asyncio.wait_for(crashed.wait(), timeout=0.5)
    for _ in range(20):
        if first.events and second.events:
            break
        await asyncio.sleep(0)
    await notifier.stop(drain_seconds=1)
    assert [item.title for item in first.events] == ["Test"]
    assert [item.title for item in second.events] == ["Test"]


@pytest.mark.asyncio
async def test_critical_event_not_blocked_by_passive_retry_backoff():
    passive_failed = asyncio.Event()
    critical_received = asyncio.Event()

    class FailsPassive(Recorder):
        async def send(self, item):
            if item.title == "passive":
                passive_failed.set()
                raise OSError("temporary failure")
            await super().send(item)
            critical_received.set()

    channel = FailsPassive()
    notifier = NotificationManager([channel], Metrics(), retry_delays=(30,))
    notifier.start()
    await notifier.publish(event("passive", priority=NotificationPriority.PASSIVE))
    await asyncio.wait_for(passive_failed.wait(), timeout=0.5)
    await notifier.publish(event("critical", priority=NotificationPriority.CRITICAL))
    await asyncio.wait_for(critical_received.wait(), timeout=0.5)
    await notifier.stop(drain_seconds=0.01)
    assert [item.title for item in channel.events] == ["critical"]


@pytest.mark.asyncio
async def test_critical_is_delivered_before_queued_passive_events():
    entered = asyncio.Event()
    release = asyncio.Event()

    class SlowFirst(Recorder):
        async def send(self, item):
            if item.title == "in flight":
                entered.set()
                await release.wait()
            await super().send(item)

    channel = SlowFirst()
    notifier = NotificationManager([channel], Metrics(), retry_delays=())
    notifier.start()
    await notifier.publish(event("in flight"))
    await asyncio.wait_for(entered.wait(), timeout=0.5)
    await notifier.publish(event("passive 1", priority=NotificationPriority.PASSIVE))
    await notifier.publish(event("passive 2", priority=NotificationPriority.PASSIVE))
    await notifier.publish(event("critical", priority=NotificationPriority.CRITICAL))
    release.set()
    await notifier.stop(drain_seconds=1)
    assert [item.title for item in channel.events] == [
        "in flight", "critical", "passive 1", "passive 2"
    ]


@pytest.mark.asyncio
async def test_full_channel_drops_oldest_critical_and_records_metric(caplog):
    entered = asyncio.Event()
    release = asyncio.Event()

    class SlowFirst(Recorder):
        async def send(self, item):
            if item.title == "in flight":
                entered.set()
                await release.wait()
            await super().send(item)

    channel = SlowFirst()
    notifier = NotificationManager([channel], Metrics(), max_queue=1, retry_delays=())
    notifier.start()
    await notifier.publish(event("in flight"))
    await asyncio.wait_for(entered.wait(), timeout=0.5)
    with caplog.at_level(logging.ERROR):
        await notifier.publish(event("old", priority=NotificationPriority.CRITICAL))
        for _ in range(20):
            if notifier._channels[0].size == 1:
                break
            await asyncio.sleep(0)
        await notifier.publish(event("new", priority=NotificationPriority.CRITICAL))
        release.set()
        await notifier.stop(drain_seconds=1)
    assert [item.title for item in channel.events] == ["in flight", "new"]
    assert notifier.metrics.notification_dropped.labels(
        priority="CRITICAL", category="SYSTEM"
    )._value.get() == 1
    assert "critical notification dropped" in caplog.text


@pytest.mark.asyncio
async def test_each_channel_has_independent_delivery_audit(tmp_path):
    class Good(Recorder):
        pass

    class Broken(Recorder):
        async def send(self, item):
            raise OSError("unavailable")

    store = Store(f"sqlite+aiosqlite:///{tmp_path}/audit.db")
    await store.initialize()
    notifier = NotificationManager([Good(), Broken()], Metrics(), store=store, retry_delays=())
    notifier.start()
    await notifier.publish(event("audit"))
    await notifier.stop(drain_seconds=1)
    rows = await store.latest("notification_events")
    assert {(row["channel"], row["status"], row["attempts"]) for row in rows} == {
        ("good", "SENT", 1), ("broken", "FAILED", 1),
    }
    assert len({row["event_id"] for row in rows}) == 1
    await store.close()


@pytest.mark.asyncio
async def test_manual_stop_does_not_send_halt_alert(tmp_path):
    runtime, _, _ = await runtime_with_entry(tmp_path)
    channel = Recorder()
    runtime.notifications = NotificationManager([channel], Metrics(), retry_delays=())
    runtime.notifications.start()
    await runtime.stop()
    assert runtime.governor.state == GovernorState.HALT
    assert not any(item.title == "🚨 HALT" for item in channel.events)
    await runtime.store.close()


@pytest.mark.asyncio
async def test_manual_stop_sends_stopping_and_stopped(tmp_path):
    runtime, _, _ = await runtime_with_entry(tmp_path)
    channel = Recorder()
    runtime.notifications = NotificationManager([channel], Metrics(), retry_delays=())
    runtime.notifications.start()
    await runtime.stop()
    titles = [item.title for item in channel.events]
    assert titles == ["🟡 Quant System Stopping", "⚪ Quant System Stopped"]
    await runtime.store.close()


@pytest.mark.asyncio
async def test_real_halt_still_sends_halt_alert(tmp_path):
    runtime, _, _ = await runtime_with_entry(tmp_path)
    channel = Recorder()
    runtime.notifications = NotificationManager([channel], Metrics(), retry_delays=())
    runtime.notifications.start()
    await runtime.enter_halt("Redis unavailable")
    await runtime.notifications.stop(drain_seconds=1)
    assert any(item.title == "🚨 HALT" for item in channel.events)
    await runtime.store.close()


@pytest.mark.asyncio
async def test_failed_manual_stop_restores_halt_alert(tmp_path):
    runtime, _, _ = await runtime_with_entry(tmp_path)
    channel = Recorder()
    runtime.notifications = NotificationManager([channel], Metrics(), retry_delays=())
    runtime.notifications.start()
    runtime.reconcile = AsyncMock()
    runtime.reconciliation_healthy = False
    with pytest.raises(RuntimeError):
        await runtime.stop()
    await runtime.notifications.stop(drain_seconds=1)
    assert any(item.title == "🚨 HALT" for item in channel.events)
    await runtime.store.close()
