"""A market gap never clears through reconnect, freshness, account sync or restart."""

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from app.api import create_app
from app.config import Settings
from app.models import GovernorState, PortfolioState, utcnow
from app.public_continuity import CODE, HOLD, REASON, RELEASE, SESSION_CLOSE
from app.recovery import HaltClass, classify_halt_reason
from app.runtime import TradingRuntime
from tests.test_live_lease import postgres_store as postgres_store
from tests.test_ws_reliability import BTC, ready_socket

ETH = "ETH-USDT-SWAP"


@pytest.fixture
async def continuity_runtime(tmp_path):
    runtime = TradingRuntime(
        Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/continuity.db")
    )
    await runtime.store.initialize()
    now = [100.0]
    ws, _ = ready_socket(
        channels=("trades", "tickers", "books5", "mark-price", "index-tickers"), now=now
    )
    ws.batch_handler = runtime._on_public_market_batch
    ws.on_fault = runtime._on_ws_fault
    ws._ensure_worker()
    runtime.sockets = [ws]
    runtime._clock = lambda: now[0]
    runtime._schedule_ws_reconciliation = Mock()
    runtime.alert = AsyncMock()
    runtime.portfolio = PortfolioState(equity=5000, available_balance=5000, synchronized=True)
    runtime._reconcile_and_assess = AsyncMock(return_value=(True, []))
    runtime._resume_health = AsyncMock(return_value=(True, []))
    yield runtime, ws, now
    await drain(runtime)
    ws._stopping = True
    ws._worker_task.cancel()
    await asyncio.gather(ws._worker_task, return_exceptions=True)
    await runtime.close()


async def drain(runtime):
    if runtime._public_audit_tasks:
        await asyncio.gather(*runtime._public_audit_tasks)


def gap(runtime, ws):
    ws._fault("processing queue full", kind="backlog")
    assert runtime.governor.state == GovernorState.HALT
    assert runtime.governor.reason == REASON
    assert runtime._auto_recovery_forbidden
    assert runtime.public_continuity.hold is not None


def reconnect(runtime, ws, now):
    ws._reset_transport()
    ws.subscribed = {ws.feed_key(a) for a in ws.subscriptions}
    now[0] += 1
    ws.last_rx_at = now[0]
    # Deterministic wall-clock epoch: messages use real recent timestamps.
    runtime.public_continuity.since = utcnow() - timedelta(seconds=2)
    runtime.market.public_since = runtime.public_continuity.since


async def feed(
    runtime,
    ws,
    now,
    symbol=BTC,
    count=100,
    *,
    book=True,
    ticker=True,
    mark=True,
    index=True,
    old=False,
):
    ts = str(int((utcnow() - timedelta(milliseconds=100)).timestamp() * 1000))
    rows = [
        dict(
            arg=dict(channel="trades", instId=symbol),
            data=[dict(ts=ts, tradeId=f"{symbol}-{n}", px="100", sz="1", side="buy")],
        )
        for n in range(count)
    ]
    for enabled, channel, data in [
        (book, "books5", dict(ts=ts, bids=[["99", "1"]], asks=[["101", "1"]])),
        (ticker, "tickers", dict(ts=ts, last="100", bidPx="99", askPx="101")),
        (mark, "mark-price", dict(ts=ts, markPx="100")),
        (index, "index-tickers", dict(ts=ts, idxPx="100")),
    ]:
        if enabled:
            rows.append(
                dict(
                    arg=dict(
                        channel=channel,
                        instId=(
                            symbol.removesuffix("-SWAP") if channel == "index-tickers" else symbol
                        ),
                    ),
                    data=[data],
                )
            )
    for offset in range(0, len(rows), 64):
        batch = [
            dict(
                m, _public_received_at=now[0], _public_generation=ws.generation - (1 if old else 0)
            )
            for m in rows[offset : offset + 64]
        ]
        await runtime._on_public_market_batch(batch)
    for channel in ws.CRITICAL_CHANNELS:
        for arg in ws.subscriptions:
            if arg["channel"] == channel:
                ws.last_data_at[ws.feed_key(arg)] = now[0]


async def test_public_queuefull_fences_synchronously_before_hold_io(continuity_runtime):
    r, ws, _ = continuity_runtime
    r.governor.resume(synchronized=True, healthy=True)
    write = AsyncMock(side_effect=OSError("test DB unavailable"))
    r.store.append = write
    gap(r, ws)
    write.assert_not_awaited()
    assert not r.auto_recovery_status()["eligible"]
    await drain(r)
    assert not r.public_continuity.persisted and r.governor.state == GovernorState.HALT


async def test_public_admission_timeout_creates_durable_hold(continuity_runtime):
    r, ws, now = continuity_runtime
    ws.queue = asyncio.Queue(maxsize=4)
    # Keep the worker blocked elsewhere so it cannot drain this queue.
    ws._worker_task.cancel()
    ws._stopping = True
    await asyncio.gather(ws._worker_task, return_exceptions=True)
    ws._worker_task = None
    for _ in range(3):
        ws._enqueue_message(dict(arg=ws.subscriptions[0], data=[]), now[0])
    from app.okx import WebSocketFault

    with pytest.raises(WebSocketFault, match="backpressure timeout"):
        await ws._admit_message(dict(arg=ws.subscriptions[0], data=[]), now[0])
    assert r.governor.reason == REASON and ws.queue.qsize() == 3
    await drain(r)
    assert (await r.store.public_audit_state(HOLD, RELEASE))["reason_code"] == CODE
    # Restore an idle worker for fixture cleanup.
    ws._ensure_worker()


async def test_gap_invalidates_all_public_state_but_not_candles_or_ledger(continuity_runtime):
    r, ws, now = continuity_runtime
    await feed(r, ws, now, BTC)
    await feed(r, ws, now, ETH)
    r.market.candles[(BTC, "15m")].append("sentinel")
    await r.store.append("fills", {"test": "immutable"})
    gap(r, ws)
    for state in [
        r.market.latest_trades,
        r.market.latest_books,
        r.market.latest_ticks,
        r.market.latest_derivatives,
        r.market.derivative_observations,
        r.market.last_book_snapshot,
    ]:
        assert not state
    assert list(r.market.candles[(BTC, "15m")]) == ["sentinel"]
    assert await r.store.latest("fills") == [{"test": "immutable"}]
    assert len(await r.store.latest("market_trades", limit=300)) == 200


@pytest.mark.parametrize(
    "count,book,ticker,mark,index,ready",
    [
        (0, True, True, True, True, False),
        (99, True, True, True, True, False),
        (100, False, True, True, True, False),
        (100, True, False, True, True, False),
        (100, True, True, False, True, False),
        (100, True, True, True, False, False),
        (100, True, True, True, True, True),
    ],
)
async def test_post_gap_warmup_requires_complete_window_for_each_symbol(
    continuity_runtime, count, book, ticker, mark, index, ready
):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    reconnect(r, ws, now)
    await feed(r, ws, now, BTC, count, book=book, ticker=ticker, mark=mark, index=index)
    assert not r.public_market_continuity_ready()  # ETH remains incomplete.
    await feed(r, ws, now, ETH)
    assert r.public_market_continuity_ready() == ready
    assert r.public_continuity.required and r.governor.state == GovernorState.HALT


async def warm(r, ws, now):
    reconnect(r, ws, now)
    await feed(r, ws, now, BTC)
    await feed(r, ws, now, ETH)
    assert r.public_market_continuity_ready()


async def test_pre_gap_queued_frames_are_durable_but_not_warmup(continuity_runtime):
    r, ws, now = continuity_runtime
    await feed(r, ws, now, BTC)
    gap(r, ws)
    await drain(r)
    reconnect(r, ws, now)
    await feed(r, ws, now, BTC, old=True)
    assert not r.market.latest_trades and not r.market.latest_ticks
    assert len(await r.store.latest("market_trades", limit=300)) == 200
    assert not r.public_market_continuity_ready()


async def test_complete_warmup_and_account_sync_never_auto_resume(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    for _ in range(5):
        now[0] += 30
        ws.last_rx_at = now[0]
        await r._auto_recovery_check()
    assert r.governor.state == GovernorState.HALT
    r._reconcile_and_assess.assert_not_awaited()
    assert classify_halt_reason(REASON) == HaltClass.SAFETY_OR_MANUAL


async def test_resume_before_warmup_http_409(continuity_runtime, monkeypatch):
    r, ws, _ = continuity_runtime
    gap(r, ws)
    await drain(r)
    monkeypatch.setattr("app.api.TradingRuntime", lambda _: r)
    app = create_app(r.settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/system/resume")
    assert response.status_code == 409 and r.public_continuity.required


async def test_warmup_does_not_replace_reconciliation(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    r._reconcile_and_assess = AsyncMock(return_value=(False, ["reconciliation"]))
    assert not await r.resume()
    assert r.public_continuity.hold and r.governor.state == GovernorState.HALT


async def test_manual_resume_releases_hold_durably(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    assert await r.resume()
    assert not r.public_continuity.required and r.governor.state == GovernorState.NORMAL
    assert await r.store.public_audit_state(HOLD, RELEASE) is None
    assert sum(x.get("event") == RELEASE for x in await r.store.latest("system_events")) == 1


async def test_release_write_failure_returns_halt(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    append = r.store.append

    async def fail(table, payload, **kw):
        if payload.get("event") == RELEASE:
            raise OSError("injected")
        return await append(table, payload, **kw)

    r.store.append = fail
    assert not await r.resume()
    assert r.governor.state == GovernorState.HALT and r.public_continuity.required
    assert await r.store.public_audit_state(HOLD, RELEASE)


async def test_held_restart_restores_manual_gate_and_empty_features(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    other = TradingRuntime(r.settings)
    try:
        await other.store.initialize()
        await other._restore_public_continuity()
        assert other.governor.state == GovernorState.HALT and other._auto_recovery_forbidden
        assert other.public_continuity.hold and not other.market.latest_trades
    finally:
        await other.close()


async def test_completed_release_is_not_restored_as_hold(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    assert await r.resume()
    other = TradingRuntime(r.settings)
    try:
        await other.store.initialize()
        await other._restore_public_continuity()
        assert not other.public_continuity.hold
    finally:
        await other.close()


async def test_crash_before_hold_commit_is_caught_by_prearmed_session(continuity_runtime):
    r, ws, _ = continuity_runtime
    await r._arm_public_session()
    append = r.store.append

    async def fail(table, payload, **kw):
        if payload.get("event") == HOLD:
            raise OSError("injected")
        return await append(table, payload, **kw)

    r.store.append = fail
    gap(r, ws)
    await drain(r)
    other = TradingRuntime(r.settings)
    try:
        await other.store.initialize()
        await other._restore_public_continuity()
        assert other.public_continuity.restart_unverified
        assert (
            other.governor.state == GovernorState.HALT
            and not other.auto_recovery_status()["eligible"]
        )
    finally:
        await other.close()


async def test_clean_session_close_does_not_create_restart_hold(continuity_runtime):
    r, _, _ = continuity_runtime
    await r._arm_public_session()
    await r.store.append(
        "system_events",
        dict(
            event=SESSION_CLOSE, continuity_id=r._public_session, occurred_at=utcnow().isoformat()
        ),
    )
    await r._restore_public_continuity()
    assert not r.public_continuity.required


@pytest.mark.parametrize("event", [HOLD, RELEASE])
async def test_corrupt_public_audit_fails_closed(continuity_runtime, event):
    r, _, _ = continuity_runtime
    await r.store.append("system_events", {"event": event, "continuity_id": "bad"})
    with pytest.raises(ValueError, match="continuity audit invalid"):
        await r._restore_public_continuity()
    assert r.public_continuity.corrupt and r.governor.state == GovernorState.HALT
    assert not await r.resume()


async def test_public_worker_failure_preserves_classification_and_holds(continuity_runtime):
    r, ws, _ = continuity_runtime
    ws._fault("business handler failed", kind="handler")
    assert ws.last_failure_reason_code == "ws_worker_failed"
    assert r.public_continuity.hold and r.governor.reason == REASON
    await drain(r)
    assert (await r.store.public_audit_state(HOLD, RELEASE))["cause"] == "ws_worker_failed"


async def test_normal_transient_transport_remains_auto_recoverable(continuity_runtime):
    r, ws, _ = continuity_runtime
    ws._fault("connection closed")
    assert not r.public_continuity.required
    assert r.auto_recovery_status()["eligible"]
    assert r.governor.reason == "WebSocket transport unavailable"


async def test_independent_entry_gate_and_evaluation_block_hold(continuity_runtime):
    r, ws, _ = continuity_runtime
    gap(r, ws)
    r.governor.resume(
        synchronized=True, healthy=True
    )  # defence even against an accidental state change.
    assert not r.execution.entry_allowed()
    r.decision_pipeline.decide = AsyncMock()
    await r._evaluate(BTC)
    r.decision_pipeline.decide.assert_not_awaited()


async def test_postgres_continuity_audit_is_append_only(postgres_store):
    from uuid import uuid4

    hold = dict(
        event=HOLD,
        continuity_id=uuid4().hex,
        reason_code=CODE,
        socket_name="public-market",
        generation=2,
        cause="ws_processing_backlog",
        occurred_at=utcnow().isoformat(),
    )
    await postgres_store.append("system_events", hold)
    assert await postgres_store.public_audit_state(HOLD, RELEASE) == hold
    await postgres_store.append(
        "system_events",
        dict(
            event=RELEASE,
            continuity_id=hold["continuity_id"],
            reason_code=CODE,
            occurred_at=utcnow().isoformat(),
        ),
    )
    assert await postgres_store.public_audit_state(HOLD, RELEASE) is None
    assert hold in await postgres_store.latest("system_events")


async def test_successful_6000_frame_burst_has_no_hold_halt_or_reset(continuity_runtime):
    import json

    from tests.test_market_backpressure import BurstWire

    r, ws, now = continuity_runtime
    ws.subscriptions = [dict(channel="trades", instId=BTC)]
    r.governor.resume(synchronized=True, healthy=True)
    wire = BurstWire(ws.subscriptions)
    ts = str(int(utcnow().timestamp() * 1000))
    wire.frames = iter(
        [json.dumps(dict(event="subscribe", arg=ws.subscriptions[0]))]
        + [
            json.dumps(
                dict(
                    arg=ws.subscriptions[0],
                    data=[dict(ts=ts, tradeId=str(n), px="100", sz="1", side="buy")],
                )
            )
            for n in range(6000)
        ]
        + ["pong"]
    )
    receiver = asyncio.create_task(ws._receive(wire))
    try:
        await asyncio.wait_for(wire.pong_read.wait(), 10)
        await asyncio.wait_for(ws.queue.join(), 10)
        assert len(await r.store.latest("market_trades", limit=6001)) == 6000
        assert r.governor.state == GovernorState.NORMAL
        assert not r.public_continuity.required and r.market.public_epoch == 0
        assert len(r.market.latest_trades[BTC]) == 100
        r.alert.assert_not_awaited()
    finally:
        receiver.cancel()
        await asyncio.gather(receiver, return_exceptions=True)


async def test_repeated_successful_highwater_waits_do_not_create_hold(continuity_runtime):
    r, ws, now = continuity_runtime
    ws.queue = asyncio.Queue(maxsize=4)
    # The old worker is waiting on its original queue. Replace it cleanly.
    ws._stopping = True
    ws._worker_task.cancel()
    await asyncio.gather(ws._worker_task, return_exceptions=True)
    ws._ensure_worker()
    ws.metrics = r.metrics
    for _ in range(3):
        message = dict(arg=ws.subscriptions[0], data=[])
        for _ in range(3):
            ws._enqueue_message(message, now[0])
        await ws._admit_message(message, now[0])
        await ws.queue.join()
    assert not r.public_continuity.required and r.market.public_epoch == 0
    assert (
        r.metrics.registry.get_sample_value(
            "quant_ws_backpressure_seconds_count", {"socket": "public-market"}
        )
        == 3
    )


async def test_old_inflight_batch_cannot_publish_state_after_invalidation(continuity_runtime):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from tests.test_market_batch import CachePipeline

    r, ws, now = continuity_runtime
    entered, release = asyncio.Event(), asyncio.Event()
    original = r.store.market_batch

    @asynccontextmanager
    async def delayed_commit():
        async with original():
            yield
            entered.set()
            await release.wait()

    r.store.market_batch = delayed_commit
    cache = CachePipeline()
    r.market.redis = SimpleNamespace(pipeline=lambda **kw: cache)
    task = asyncio.create_task(feed(r, ws, now, BTC, count=1))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert r.market.latest_trades[BTC]
        gap(r, ws)
        release.set()
        await task
        assert not r.market.latest_trades and not r.market.latest_ticks
        assert not r.market.latest_derivatives and not r.market.derivative_observations
        cache.execute.assert_not_awaited()
        assert len(await r.store.latest("market_trades")) == 1
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_duplicate_fault_observers_notify_once_and_persist_once(continuity_runtime):
    r, ws, _ = continuity_runtime
    gap(r, ws)
    gap(r, ws)
    await drain(r)
    assert r.alert.await_count == 1
    assert r.alert.await_args.args[0] == "CRITICAL"
    assert sum(x.get("event") == HOLD for x in await r.store.latest("system_events")) == 1


async def test_crash_during_manual_release_keeps_durable_hold(continuity_runtime):
    r, ws, now = continuity_runtime
    await r._arm_public_session()
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    entered = asyncio.Event()
    append = r.store.append

    async def pending_release(table, payload, **kw):
        if payload.get("event") == RELEASE:
            entered.set()
            await asyncio.Future()
        return await append(table, payload, **kw)

    r.store.append = pending_release
    task = asyncio.create_task(r.resume())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert r.public_continuity.required and not r.execution.entry_allowed()
        other = TradingRuntime(r.settings)
        try:
            await other.store.initialize()
            await other._restore_public_continuity()
            assert other.public_continuity.hold and other.governor.state == GovernorState.HALT
        finally:
            await other.close()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert r.governor.state == GovernorState.HALT


async def test_new_gap_during_release_never_clears_new_hold(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    old_id = r.public_continuity.hold["continuity_id"]
    append = r.store.append

    async def new_loss(table, payload, **kw):
        if payload.get("event") == RELEASE:
            gap(r, ws)
        return await append(table, payload, **kw)

    r.store.append = new_loss
    assert not await r.resume()
    await drain(r)
    assert (
        r.governor.state == GovernorState.HALT
        and r.public_continuity.hold["continuity_id"] != old_id
    )
    assert await r.store.public_audit_state(HOLD, RELEASE) == r.public_continuity.hold


@pytest.mark.parametrize("field", ["book", "ticker", "mark", "trades"])
async def test_pre_epoch_timestamps_never_satisfy_warmup(continuity_runtime, field):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    old = r.public_continuity.since - timedelta(seconds=1)
    if field == "book":
        r.market.latest_books[BTC].timestamp = old
    elif field == "ticker":
        r.market.latest_ticks[BTC].timestamp = old
    elif field == "mark":
        r.market.derivative_observations[BTC]["mark"][-1].timestamp = old
    else:
        for t in r.market.latest_trades[BTC]:
            t.timestamp = old
    assert not r.public_market_continuity_ready()


async def test_warmup_complete_still_restores_manual_hold_on_restart(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    other = TradingRuntime(r.settings)
    try:
        await other.store.initialize()
        await other._restore_public_continuity()
        # Existing healthy transport does not constitute post-restart warm-up.
        other.sockets = [ws]
        assert not other.public_market_continuity_ready()
        assert other.governor.state == GovernorState.HALT
    finally:
        await other.close()


async def test_missing_optional_funding_and_oi_does_not_block_manual_recovery(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    context = r.market.derivative_context(BTC, utcnow())
    assert "funding" not in context and "oi" not in context
    assert await r.resume()


async def test_stale_release_cannot_clear_unrelated_hold(continuity_runtime):
    from uuid import uuid4

    r, ws, _ = continuity_runtime
    gap(r, ws)
    await drain(r)
    await r.store.append(
        "system_events",
        dict(
            event=RELEASE,
            continuity_id=uuid4().hex,
            reason_code=CODE,
            occurred_at=utcnow().isoformat(),
        ),
    )
    with pytest.raises(ValueError):
        await r.store.public_audit_state(HOLD, RELEASE)
    assert not await r.resume()
    assert r.public_continuity.required


async def test_live_direct_entry_guard_blocks_provisional_resume_but_not_safety_actions(
    continuity_runtime,
):
    from types import SimpleNamespace

    r, ws, _ = continuity_runtime
    gap(r, ws)
    r.live_lease = SimpleNamespace(verify=AsyncMock(return_value=True), close=AsyncMock())
    r.governor.resume(synchronized=True, healthy=True)
    assert not await r._live_writer_guard(True)
    assert await r._live_writer_guard(False)  # CAA, protective/cancellation ownership is unchanged.


async def test_initialization_restores_hold_before_market_preload(continuity_runtime, monkeypatch):
    r, ws, _ = continuity_runtime
    gap(r, ws)
    await drain(r)
    other = TradingRuntime(r.settings)
    other.client.instruments = AsyncMock(return_value={})
    other.client.candles = AsyncMock(return_value=[])
    fake = Mock(ping=AsyncMock(), aclose=AsyncMock())
    monkeypatch.setattr("app.runtime.Redis.from_url", lambda *a, **kw: fake)
    try:
        await other.initialize()
        assert other.public_continuity.hold and other._auto_recovery_forbidden
        assert other.governor.state == GovernorState.HALT
        assert other.governor.reason == REASON
    finally:
        await other.close()


async def test_epoch_prevents_warmup_before_durable_batch_commit(continuity_runtime):
    from contextlib import asynccontextmanager

    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    reconnect(r, ws, now)
    entered, release = asyncio.Event(), asyncio.Event()
    original = r.store.market_batch

    @asynccontextmanager
    async def delayed():
        async with original():
            yield
            entered.set()
            await release.wait()

    r.store.market_batch = delayed
    # Actual worker tracks a batch in flight, not only queue size.

    ts = str(int(utcnow().timestamp() * 1000))
    for n in range(64):
        ws._enqueue_message(
            dict(
                arg=dict(channel="trades", instId=BTC),
                data=[dict(ts=ts, tradeId=str(n), px="100", sz="1", side="buy")],
            ),
            now[0],
        )
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert ws.queue.qsize() == 0 and not ws.business_idle
        assert not r.public_market_continuity_ready()
    finally:
        release.set()
        await asyncio.wait_for(ws.queue.join(), 2)


async def test_private_and_candle_fault_do_not_invalidate_public_features(continuity_runtime):
    r, _, now = continuity_runtime
    ws, _ = ready_socket(private=True, now=now)
    r._on_ws_fault(ws, "WebSocket processing backlog")
    assert not r.public_continuity.required and r.market.public_epoch == 0
    candle, _ = ready_socket(channels=("candle15m",), now=now)
    r._on_ws_fault(candle, "WebSocket processing failed")
    assert not r.public_continuity.required and r.market.public_epoch == 0


async def test_failed_session_arm_cannot_start_receiver(continuity_runtime):
    r, _, _ = continuity_runtime
    r.store.append = AsyncMock(side_effect=OSError("unavailable"))
    with pytest.raises(OSError):
        await r._arm_public_session()
    assert r._public_session is None and r.governor.state == GovernorState.HALT
    assert r._auto_recovery_forbidden


async def test_public_audit_writer_is_bounded_during_many_new_generations(continuity_runtime):
    r, ws, _ = continuity_runtime
    entered, release = asyncio.Event(), asyncio.Event()
    original = r.store.append

    async def slow(table, payload, **kw):
        if payload.get("event") == HOLD:
            entered.set()
            await release.wait()
        return await original(table, payload, **kw)

    r.store.append = slow
    gap(r, ws)
    await asyncio.wait_for(entered.wait(), 2)
    for _ in range(20):
        ws.generation += 1
        gap(r, ws)
    assert len(r._public_audit_tasks) == 1
    release.set()
    await drain(r)
    assert r.public_continuity.persisted
    assert (await r.store.public_audit_state(HOLD, RELEASE))["loss_count"] == 21
    assert r.alert.await_count == 1


async def test_public_run_task_death_is_durable_continuity_uncertainty(continuity_runtime):
    r, ws, _ = continuity_runtime
    task = asyncio.create_task(asyncio.sleep(0))
    await task
    ws._run_task = task
    r.running = True
    r._on_ws_run_done(ws, task)
    await drain(r)
    await asyncio.gather(*r.tasks)
    r.running = False
    assert r.public_continuity.hold["cause"] == "ws_run_task_failed"
    assert r.governor.state == GovernorState.HALT


async def test_post_gap_frame_with_pre_gap_source_timestamp_is_not_reused(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    reconnect(r, ws, now)
    old = str(int((r.public_continuity.since - timedelta(seconds=1)).timestamp() * 1000))
    message = dict(
        arg=dict(channel="trades", instId=BTC),
        data=[dict(ts=old, tradeId="pre-epoch", px="100", sz="1", side="buy")],
        _public_received_at=now[0],
        _public_generation=ws.generation,
    )
    await r._on_public_market_batch([message])
    assert not r.market.latest_trades
    assert (await r.store.latest("market_trades"))[0]["trade_id"] == "pre-epoch"


async def test_old_receive_timestamp_on_new_generation_is_not_reused(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    reconnect(r, ws, now)
    message = dict(
        arg=dict(channel="trades", instId=BTC),
        data=[
            dict(
                ts=str(int(utcnow().timestamp() * 1000)),
                tradeId="old-recv",
                px="100",
                sz="1",
                side="buy",
            )
        ],
        _public_received_at=r.public_continuity.received_after,
        _public_generation=ws.generation,
    )
    await r._on_public_market_batch([message])
    assert not r.market.latest_trades


async def test_restart_warmup_complete_still_requires_explicit_resume(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    other = TradingRuntime(r.settings)
    other._clock = lambda: now[0]
    try:
        await other.store.initialize()
        await other._restore_public_continuity()
        other.sockets = [ws]
        other.portfolio = PortfolioState(equity=5000, available_balance=5000, synchronized=True)
        other._reconcile_and_assess = AsyncMock(return_value=(True, []))
        other._resume_health = AsyncMock(return_value=(True, []))
        other.alert = AsyncMock()
        await warm(other, ws, now)
        await other._auto_recovery_check()
        assert other.governor.state == GovernorState.HALT
        assert await other.resume()
        assert other.governor.state == GovernorState.NORMAL
    finally:
        await other.close()


async def test_status_does_not_expose_continuity_or_session_ids(continuity_runtime, monkeypatch):
    r, ws, _ = continuity_runtime
    await r._arm_public_session()
    gap(r, ws)
    await drain(r)
    monkeypatch.setattr("app.api.TradingRuntime", lambda _: r)
    app = create_app(r.settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        response = await c.get("/status")
    assert response.status_code == 200
    data = response.json()["public_market_continuity"]
    assert data["hold_active"] and data["state"] == "WARMING_UP"
    assert "continuity_id" not in data
    assert r._public_session not in response.text
    assert r.public_continuity.hold["continuity_id"] not in response.text


async def test_hold_persistence_error_does_not_log_exception_secret(continuity_runtime, caplog):
    r, ws, _ = continuity_runtime
    r.store.append = AsyncMock(side_effect=OSError("fake-secret-12345"))
    gap(r, ws)
    await drain(r)
    assert "fake-secret-12345" not in caplog.text
    assert r.governor.state == GovernorState.HALT


async def test_real_resume_health_keeps_all_original_safety_gates(continuity_runtime):
    r, ws, now = continuity_runtime
    del r._resume_health  # This test exercises real dependencies, unlike the unit fixture.
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    healthy, issues = await r._resume_health()
    assert not healthy
    assert "reconciliation" in issues and "reconciliation_safety" in issues
    assert "redis" in issues and "cancel_all_after" in issues
    assert "public market warm-up incomplete" not in issues


async def test_pre_gap_inflight_entry_cannot_outlive_manual_recovery(continuity_runtime):
    r, ws, now = continuity_runtime
    token = r._entry_public_epoch.set(r.public_continuity.epoch)
    try:
        gap(r, ws)
        await drain(r)
        await warm(r, ws, now)
        assert await r.resume()
        assert r.governor.state == GovernorState.NORMAL
        assert not r.execution.entry_allowed()  # The request still carries its old epoch.
    finally:
        r._entry_public_epoch.reset(token)
    assert r.execution.entry_allowed()


async def test_committed_release_with_lost_response_can_be_manually_verified_idempotently(
    continuity_runtime,
):
    r, ws, now = continuity_runtime
    await r._arm_public_session()
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    append = r.store.append
    first = True

    async def lost_response(table, payload, **kw):
        nonlocal first
        result = await append(table, payload, **kw)
        if payload.get("event") == RELEASE and first:
            first = False
            raise OSError("acknowledgement lost")
        return result

    r.store.append = lost_response
    assert not await r.resume()
    assert r.public_continuity.required and r.governor.state == GovernorState.HALT
    assert await r.resume()
    assert r.governor.state == GovernorState.NORMAL and not r.public_continuity.required
    assert sum(e.get("event") == RELEASE for e in await r.store.latest("system_events")) == 1
    assert await r.store.public_audit_state(HOLD, RELEASE) is None


async def test_corrupt_loss_count_is_rejected(continuity_runtime):
    r, ws, _ = continuity_runtime
    gap(r, ws)
    await drain(r)
    payload = dict(r.public_continuity.hold, loss_count="secret-invalid-count")
    await r.store.append("system_events", payload)
    with pytest.raises(ValueError, match="continuity audit invalid"):
        await r._restore_public_continuity()
    assert r.governor.state == GovernorState.HALT and r.public_continuity.corrupt


async def test_stop_without_manual_recovery_cannot_clean_an_unverified_restart(continuity_runtime):
    from app.public_continuity import SESSION_OPEN

    r, _, _ = continuity_runtime
    await r._arm_public_session()
    other = TradingRuntime(r.settings)
    try:
        await other.store.initialize()
        await other._restore_public_continuity()
        assert other.public_continuity.restart_unverified
        await other.stop()
        assert await other.store.public_audit_state(SESSION_OPEN, SESSION_CLOSE)
        final = TradingRuntime(r.settings)
        try:
            await final.store.initialize()
            await final._restore_public_continuity()
            assert final.public_continuity.required and final._auto_recovery_forbidden
        finally:
            await final.close()
    finally:
        await other.close()


async def test_mixed_epoch_batch_keeps_one_transaction_and_only_post_gap_state(continuity_runtime):
    from sqlalchemy import event

    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    reconnect(r, ws, now)
    ts = str(int((utcnow() - timedelta(milliseconds=100)).timestamp() * 1000))
    messages = [
        dict(
            arg=dict(channel="trades", instId=BTC),
            data=[dict(ts=ts, tradeId=label, px="100", sz="1", side="buy")],
            _public_received_at=now[0],
            _public_generation=gen,
        )
        for label, gen in [("old", ws.generation - 1), ("new", ws.generation)]
    ]
    commits = []
    event.listen(r.store.engine.sync_engine, "commit", lambda _: commits.append(True))
    await r._on_public_market_batch(messages)
    assert len(commits) == 1
    assert [t.trade_id for t in r.market.latest_trades[BTC]] == ["new"]
    rows = await r.store.latest("market_trades")
    assert {row["trade_id"] for row in rows} == {"old", "new"}
    assert all("_public_generation" not in row and "_public_received_at" not in row for row in rows)


async def test_old_generation_is_still_rejected_after_successful_manual_resume(continuity_runtime):
    r, ws, now = continuity_runtime
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    assert await r.resume()
    original = [t.trade_id for t in r.market.latest_trades[BTC]]
    message = dict(
        arg=dict(channel="trades", instId=BTC),
        data=[
            dict(
                ts=str(int(utcnow().timestamp() * 1000)),
                tradeId="old-generation-replay",
                px="100",
                sz="1",
                side="buy",
            )
        ],
        _public_received_at=now[0],
        _public_generation=ws.generation - 1,
    )
    await r._on_public_market_batch([message])
    assert [t.trade_id for t in r.market.latest_trades[BTC]] == original
    assert r.governor.state == GovernorState.NORMAL


async def test_same_runtime_clean_restart_rebases_public_generation_after_release(
    continuity_runtime,
):
    r, ws, now = continuity_runtime
    ws.generation = 7
    gap(r, ws)
    await drain(r)
    await warm(r, ws, now)
    assert await r.resume()
    await r._restore_public_continuity()
    assert not r.public_continuity.required and not r.market.latest_trades
    ws.generation = 0
    await warm(r, ws, now)
    assert r.public_market_continuity_ready()


async def test_unadmitted_preack_data_also_creates_continuity_hold(continuity_runtime):
    import json

    from app.okx import WebSocketFault

    r, ws, _ = continuity_runtime
    ws.subscribed.clear()

    class Wire:
        first = True

        async def recv(self):
            if self.first:
                self.first = False
                return json.dumps(
                    dict(
                        arg=ws.subscriptions[0],
                        data=[
                            dict(
                                ts=str(int(utcnow().timestamp() * 1000)),
                                tradeId="buffered-not-applied",
                                px="100",
                                sz="1",
                                side="buy",
                            )
                        ],
                    )
                )
            raise WebSocketFault("subscription acknowledgement timeout")

        async def send(self, _):
            pass

    with pytest.raises(WebSocketFault, match="subscription acknowledgement timeout"):
        await ws._receive(Wire())
    await drain(r)
    assert r.public_continuity.hold["cause"] == "ws_preack_data_discarded"
    assert r.governor.state == GovernorState.HALT and r._auto_recovery_forbidden
    assert not r.market.latest_trades and not await r.store.latest("market_trades")
