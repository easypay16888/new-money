from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.execution import OrderManager
from app.ledger_repair import LedgerRepairService
from app.storage import Store

SYMBOL = "BTC-USDT-SWAP"
NOW = datetime.now(UTC)


def entry():
    return {
        "clOrdId": "entry",
        "symbol": SYMBOL,
        "state": "FILLED",
        "filled": "1",
        "reconciled_filled": "1",
        "intent_id": "intent",
        "direction": "LONG",
        "reduce_only": False,
        "protective_algo_id": "known-stop",
        "created_at": (NOW - timedelta(hours=2)).isoformat(),
        "order_id": "entry-order",
        "approved_contracts": "1",
        "entry_reference": "100",
        "stop_price": "90",
    }


def fill(trade="exit-001", size="1", order="exit-order", side="sell", client=""):
    return {
        "instId": SYMBOL,
        "tradeId": trade,
        "ordId": order,
        "clOrdId": client,
        "billId": trade,
        "side": side,
        "posSide": "net",
        "fillPx": "110",
        "fillSz": size,
        "fillPnl": "10",
        "fee": "-0.05",
        "feeCcy": "USDT",
        "execType": "T",
        "fillTime": str(int((NOW - timedelta(hours=1)).timestamp() * 1000)),
        "ts": str(int((NOW - timedelta(hours=1)).timestamp() * 1000)),
    }


@pytest.fixture
async def repair(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/repair.db")
    await store.initialize()
    manager = OrderManager(store)
    await store.append("orders", entry(), symbol=SYMBOL, reference_id="entry")
    await manager.restore()
    client = AsyncMock()
    client.fills_history_window.return_value = [fill()]
    client.order_by_id.return_value = [
        {
            "instId": SYMBOL,
            "ordId": "exit-order",
            "side": "sell",
            "posSide": "net",
            "state": "filled",
            "accFillSz": "1",
            "reduceOnly": "true",
            "algoClOrdId": "known-stop",
            "clOrdId": "",
        }
    ]
    client.algo_order.return_value = [
        {
            "instId": SYMBOL,
            "algoClOrdId": "known-stop",
            "algoId": "remote-algo",
            "ordIdList": ["exit-order"],
            "side": "sell",
            "posSide": "net",
            "state": "effective",
            "failCode": "0",
        }
    ]
    service = LedgerRepairService(client, manager, store)
    try:
        yield service, client, manager, store
    finally:
        await store.close()


async def test_current_incident_missing_known_protective_fill_repaired(repair):
    service, client, manager, store = repair
    result = await service.repair({SYMBOL})
    assert result.fills_added == 1 and result.orders_added == 1
    assert result.evidence_complete and not result.repaired  # full runtime reconcile still required
    exit_order = manager.orders["protective-exit-order"]
    assert exit_order["reduce_only"] is True and exit_order["filled"] == "1"
    assert exit_order["state"] == "FILLED"
    assert (
        sum(
            Decimal(r["filled"]) * (1 if r["direction"] == "LONG" else -1)
            for r in manager.orders.values()
        )
        == 0
    )
    rows = await store.latest("fills")
    assert rows[0]["fee"] == "-0.05" and rows[0]["fillPnl"] == "10"
    assert rows[0]["recovered"] and rows[0]["recovery_source"] == "okx_fills_history"
    assert await store.latest("order_events")
    second = await service.repair({SYMBOL})
    assert second.fills_added == second.orders_added == 0
    assert len(await store.latest("fills")) == 1
    assert (await store.latest("orders"))[-1]["filled"] == "1"  # original entry unchanged


@pytest.mark.parametrize(
    "case,reason",
    [
        ("unknown", "ledger repair ownership unverified"),
        ("symbol", "ledger fill conflict"),
        ("side", "ledger fill conflict"),
        ("exceeds", "ledger fill conflict"),
        ("malformed", "ledger repair evidence incomplete"),
        ("conflict", "ledger fill conflict"),
        ("local_over", "ledger fill conflict"),
    ],
)
async def test_conflicting_or_unproven_evidence_never_changes_ledger(repair, case, reason):
    service, client, manager, store = repair
    row = fill()
    if case == "unknown":
        client.order_by_id.return_value[0]["algoClOrdId"] = ""
        client.algo_order.return_value = []
    elif case == "symbol":
        row["clOrdId"] = "entry"
        row["instId"] = "ETH-USDT-SWAP"
    elif case == "side":
        row["clOrdId"] = "entry"  # known LONG cannot have sell fill
    elif case == "exceeds":
        row["fillSz"] = "2"
        client.order_by_id.return_value[0]["accFillSz"] = "2"
    elif case == "malformed":
        row["fillSz"] = "NaN"
    elif case == "conflict":
        await store.append(
            "fills", {**row, "fillSz": "2"}, symbol=SYMBOL, reference_id=row["tradeId"]
        )
    elif case == "local_over":
        manager.orders["entry"]["filled"] = "2"
        row.update(clOrdId="entry", ordId="entry-order", side="buy")
        client.order_by_id.return_value[0].update(
            ordId="entry-order", side="buy", reduceOnly="false"
        )
    client.fills_history_window.return_value = [row]
    before = await store.latest("orders")
    events = await store.latest("order_events")
    result = await service.repair({SYMBOL})
    assert result.reason == reason and not result.evidence_complete
    assert await store.latest("orders") == before
    assert await store.latest("order_events") == events


async def test_three_partial_exit_fills_aggregate_and_partial_existing_history(repair):
    service, client, manager, store = repair
    # Known two-contract entry; old historical record is never rewritten.
    await store.append(
        "order_events",
        {**entry(), "filled": "2", "approved_contracts": "2"},
        symbol=SYMBOL,
        reference_id="entry",
    )
    await manager.restore()
    rows = [fill("A", "0.7"), fill("B", "0.8"), fill("C", "0.5")]
    client.fills_history_window.return_value = rows
    client.order_by_id.return_value[0]["accFillSz"] = "2"
    await store.append("fills", rows[0], symbol=SYMBOL, reference_id="A")
    await manager.restore()
    result = await service.repair({SYMBOL})
    assert result.evidence_complete and result.fills_added == 2
    assert manager.orders["protective-exit-order"]["filled"] == "2.0"
    assert len(await store.latest("fills")) == 3
    again = await service.repair({SYMBOL})
    assert again.fills_added == again.orders_added == 0
    assert len(await store.latest("order_events")) == 2


async def test_existing_partial_exit_order_only_appends_missing_fills(repair):
    service, client, manager, store = repair
    rows = [fill("A", "0.3"), fill("B", "0.2"), fill("C", "0.5")]
    await store.append(
        "orders",
        {
            **entry(),
            "clOrdId": "protective-exit-order",
            "order_id": "exit-order",
            "direction": "SHORT",
            "reduce_only": True,
            "protective_algo_id": "",
            "state": "PARTIALLY_FILLED",
            "filled": "0.3",
        },
        symbol=SYMBOL,
        reference_id="protective-exit-order",
    )
    await store.append("fills", rows[0], symbol=SYMBOL, reference_id="A")
    await manager.restore()
    client.fills_history_window.return_value = rows
    result = await service.repair({SYMBOL})
    assert result.fills_added == 2 and result.orders_added == 0
    assert manager.orders["protective-exit-order"]["filled"] == "1.0"
    assert (await store.latest("orders"))[0]["filled"] == "0.3"


@pytest.mark.parametrize(
    "field,value",
    [
        ("fillSz", "Infinity"),
        ("fillPx", "0"),
        ("fee", "NaN"),
        ("fillPnl", "oops"),
        ("fillTime", ""),
        ("ts", "bad"),
        ("tradeId", ""),
        ("ordId", None),
        ("side", "unknown"),
        ("posSide", "long"),
    ],
)
async def test_malformed_fill_fails_closed(repair, field, value):
    service, client, manager, store = repair
    client.fills_history_window.return_value = [{**fill(), field: value}]
    result = await service.repair({SYMBOL})
    assert result.reason == "ledger repair evidence incomplete"
    assert not await store.latest("fills")
    assert len(manager.orders) == 1


async def test_foreign_fill_nearby_prevents_ownership_guess(repair):
    service, client, manager, store = repair
    client.fills_history_window.return_value = [fill(), fill("foreign", order="foreign-order")]
    result = await service.repair({SYMBOL})
    assert not result.evidence_complete
    assert not await store.latest("fills")
    assert len(manager.orders) == 1


async def test_remote_unknown_position_never_creates_entry(repair):
    service, client, manager, store = repair
    manager.orders.clear()
    result = await service.repair({SYMBOL})
    assert not result.evidence_complete
    client.fills_history_window.assert_not_awaited()
    assert not await store.latest("fills")


@pytest.mark.parametrize("old_or_missing", ["old", "missing", "naive"])
async def test_unbounded_query_window_is_rejected(repair, old_or_missing):
    service, client, manager, store = repair
    if old_or_missing == "old":
        manager.orders["entry"]["created_at"] = (NOW - timedelta(days=100)).isoformat()
    elif old_or_missing == "naive":
        manager.orders["entry"]["created_at"] = "2026-10-01T00:00:00"
    else:
        manager.orders["entry"].pop("created_at")
    result = await service.repair({SYMBOL})
    assert result.reason == "ledger repair evidence incomplete"
    client.fills_history_window.assert_not_awaited()


async def test_per_fill_fee_comparison_ignores_ws_cumulative_fee(repair):
    service, client, manager, store = repair
    r = fill()
    await store.append(
        "fills", {**r, "fee": "-0.10", "fillFee": "-0.05"}, symbol=SYMBOL, reference_id=r["tradeId"]
    )
    result = await service.repair({SYMBOL})
    assert result.evidence_complete and result.fills_added == 0 and result.orders_added == 1
    assert (await store.latest("fills"))[0]["fee"] == "-0.10"


async def test_daily_review_includes_recovery_on_actual_fill_day(repair):
    from app.daily_review import build_daily_review

    service, client, manager, store = repair
    await service.repair({SYMBOL})
    day = datetime.fromtimestamp(int(fill()["fillTime"]) / 1000, UTC).date()
    report = await build_daily_review(store, day)
    assert report["fills"] == 1 and Decimal(report["fees"]) == Decimal("-0.05")
    assert Decimal(report["realized_pnl_after_fees"]) == Decimal("9.95")


async def runtime_incident(tmp_path, *, emergency=False):
    from app.config import Settings
    from app.models import GovernorState
    from app.runtime import TradingRuntime
    from tests.test_core import instrument
    from tests.test_production_safety import Exchange

    runtime = TradingRuntime(
        Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/runtime.db")
    )
    await runtime.store.initialize()
    await runtime.store.append("orders", entry(), symbol=SYMBOL, reference_id="entry")
    await runtime.order_manager.restore()
    runtime.instruments[SYMBOL] = instrument()
    client = Exchange()
    client.fills_history_window = AsyncMock(return_value=[fill()])
    client.order_by_id = AsyncMock(
        return_value=[
            {
                "instId": SYMBOL,
                "ordId": "exit-order",
                "clOrdId": "",
                "side": "sell",
                "posSide": "net",
                "reduceOnly": "true",
                "state": "filled",
                "accFillSz": "1",
                "algoClOrdId": "known-stop",
            }
        ]
    )
    client.algo_order = AsyncMock(
        return_value=[
            {
                "instId": SYMBOL,
                "algoClOrdId": "known-stop",
                "ordIdList": ["exit-order"],
                "side": "sell",
                "posSide": "net",
                "state": "effective",
                "failCode": "0",
                "algoId": "remote-algo",
            }
        ]
    )
    await runtime.client.close()
    client.close = AsyncMock()
    runtime.client = client
    if emergency:
        runtime.governor.halt("startup position mismatch", emergency=True)
        assert runtime.governor.state == GovernorState.EMERGENCY
    return runtime, client


@pytest.mark.parametrize("emergency", [False, True])
async def test_startup_backfill_full_reconcile_never_auto_resumes(tmp_path, emergency):
    from app.models import GovernorState

    runtime, client = await runtime_incident(tmp_path, emergency=emergency)
    try:
        await runtime.reconcile()
        result = runtime.ledger_repair_result
        assert result.repaired and result.fills_added == result.orders_added == 1
        assert runtime.portfolio.synchronized and not runtime.portfolio.positions
        assert {k: v for k, v in runtime._audited_positions().items() if v} == {}
        assert runtime.governor.state == (
            GovernorState.EMERGENCY if emergency else GovernorState.HALT
        )
        assert runtime.governor.reason == "ledger repaired; manual resume required"
        assert runtime._auto_recovery_forbidden
        await runtime._auto_recovery_check()
        assert not runtime.auto_recovery_status()["eligible"]
        hold = await runtime.store.ledger_repair_hold()
        assert hold["emergency"] == emergency
        before = await runtime.store.latest("fills")
        await runtime.reconcile()
        assert await runtime.store.latest("fills") == before
        assert runtime.governor.state != GovernorState.NORMAL
    finally:
        await runtime.close()


async def test_full_reconciliation_failure_cannot_mark_repaired(tmp_path):
    runtime, client = await runtime_incident(tmp_path)
    client.pending_orders = AsyncMock(side_effect=[[], RuntimeError("snapshot failed")])
    try:
        await runtime.reconcile()
        assert runtime.ledger_repair_result.fills_added == 1
        assert not runtime.ledger_repair_result.repaired
        assert not runtime.reconciliation_healthy
        assert not runtime.portfolio.synchronized
        assert runtime.governor.state.value == "HALT"
    finally:
        await runtime.close()


@pytest.mark.parametrize("error", [TimeoutError(), ConnectionError(), RuntimeError("unavailable")])
async def test_history_failure_leaves_runtime_halted(tmp_path, error):
    runtime, client = await runtime_incident(tmp_path)
    client.fills_history_window.side_effect = error
    try:
        await runtime.reconcile()
        assert not runtime.ledger_repair_result.repaired
        assert runtime.governor.state.value == "HALT"
        assert not runtime.portfolio.synchronized
        assert not await runtime.store.latest("fills")
        await runtime.reconcile()
        client.fills_history_window.assert_awaited_once()  # once per incident, no hot retry loop
    finally:
        await runtime.close()


async def test_runtime_position_mismatch_uses_same_service(tmp_path):
    runtime, client = await runtime_incident(tmp_path)
    runtime.portfolio.synchronized = True
    runtime.portfolio.positions = {SYMBOL: Decimal("1")}
    try:
        await runtime.reconcile()
        assert runtime.ledger_repair_result.repaired
        assert runtime.governor.state.value == "HALT"
    finally:
        await runtime.close()


@pytest.mark.parametrize("mode", ["auth_error", "transient"])
async def test_okx_api_failure_keeps_halt(tmp_path, mode):
    from app.okx import OkxError

    runtime, client = await runtime_incident(tmp_path)
    client.fills_history_window.side_effect = OkxError(
        "safe error",
        code="50113" if mode == "auth_error" else "50026",
        retryable=mode == "transient",
    )
    try:
        await runtime.reconcile()
        assert runtime.governor.state.value == "HALT"
        assert not await runtime.store.latest("fills")
    finally:
        await runtime.close()


@pytest.mark.parametrize("owner,verified", [(False, True), (True, False), (True, True)])
async def test_live_repair_requires_verified_identity_and_current_writer(repair, owner, verified):
    from tests.test_live_acceptance import live_settings

    service, client, manager, store = repair
    client.settings = live_settings()
    client.live_identity_verified = lambda: verified
    client.live_writer_guard = AsyncMock(return_value=owner)
    result = await service.repair({SYMBOL})
    assert result.evidence_complete == (owner and verified)
    assert len(await store.latest("fills")) == int(owner and verified)
    if not (owner and verified):
        client.fills_history_window.assert_not_awaited()


async def test_live_ownership_loss_during_evidence_collection_prevents_commit(repair):
    from tests.test_live_acceptance import live_settings

    service, client, manager, store = repair
    client.settings = live_settings()
    client.live_identity_verified = lambda: True
    client.live_writer_guard = AsyncMock(side_effect=[True, False])
    result = await service.repair({SYMBOL})
    assert not result.evidence_complete
    assert not await store.latest("fills") and len(manager.orders) == 1


async def test_post_commit_process_failure_converges_on_restart(repair):
    service, client, manager, store = repair
    restore = manager.restore
    manager.restore = AsyncMock(side_effect=RuntimeError("process terminated after commit"))
    result = await service.repair({SYMBOL})
    assert not result.evidence_complete
    assert len(await store.latest("fills")) == 1  # durable transaction survived
    assert await store.ledger_repair_hold()  # cannot auto start trading after crash
    manager.restore = restore
    await manager.restore()
    again = await service.repair({SYMBOL})
    assert again.evidence_complete and again.fills_added == again.orders_added == 0
    assert manager.orders["protective-exit-order"]["filled"] == "1"


async def test_database_transaction_failure_does_not_leave_partial_recovery(repair):
    service, client, manager, store = repair
    snapshots = {
        t: await store.ledger_snapshot(t, {SYMBOL}) for t in ("orders", "order_events", "fills")
    }
    records = [
        ("orders", {**entry(), "clOrdId": "recovered"}, "recovered"),
        ("bad-table", {}, "bad"),
    ]
    with pytest.raises(ValueError):
        await store.append_ledger_recovery(records, snapshots, {SYMBOL})
    assert len(await store.latest("orders")) == 1
    assert not await store.latest("fills") and not await store.latest("order_events")


async def test_concurrent_ledger_change_invalidates_recovery_plan(repair):
    service, client, manager, store = repair
    snapshots = {
        t: await store.ledger_snapshot(t, {SYMBOL}) for t in ("orders", "order_events", "fills")
    }
    await store.append("order_events", entry(), symbol=SYMBOL, reference_id="entry")
    with pytest.raises(ValueError, match="ledger changed"):
        await store.append_ledger_recovery([("fills", fill(), "exit-001")], snapshots, {SYMBOL})
    assert not await store.latest("fills")


async def test_startup_manual_hold_survives_restart(tmp_path, monkeypatch):
    from app.runtime import TradingRuntime
    from tests.test_core import instrument

    runtime, client = await runtime_incident(tmp_path, emergency=True)
    await runtime.reconcile()
    new = TradingRuntime(runtime.settings)
    await new.client.close()
    new.client = client
    new.instruments[SYMBOL] = instrument()
    # Exercise production initialize(), bypassing only Redis and candle network preload.
    redis = AsyncMock()
    monkeypatch.setattr("app.runtime.Redis.from_url", lambda *a, **kw: redis)
    client.instruments = AsyncMock(return_value={SYMBOL: instrument()})
    client.candles = AsyncMock(return_value=[])
    try:
        await new.initialize()
        assert new._ledger_repair_manual_hold and new._auto_recovery_forbidden
        assert new.governor.state.value == "EMERGENCY"
        assert new.governor.reason == "ledger repaired; manual resume required"
    finally:
        await new.close()
        await runtime.close()


async def test_real_oct6_protective_order_response_shape(repair):
    service, client, manager, store = repair
    # Redacted shape from the actual 2026-10-06 BTC close; no account identifiers/secrets.
    await store.append(
        "order_events",
        {**entry(), "filled": "1.17", "approved_contracts": "1.17"},
        symbol=SYMBOL,
        reference_id="entry",
    )
    await manager.restore()
    r = fill(size="1.17", client="O-generated-exit")
    r.update(fillPx="85595.01", fillPnl="5.59143", fee="-0.5007308085")
    client.fills_history_window.return_value = [r]
    client.order_by_id.return_value[0].update(
        clOrdId="O-generated-exit", accFillSz="1.17", algoId="remote-algo"
    )
    client.algo_order.return_value[0].update(ordId="exit-order", reduceOnly="true")
    result = await service.repair({SYMBOL})
    assert result.evidence_complete and result.fills_added == result.orders_added == 1
    assert manager.orders["protective-exit-order"]["filled"] == "1.17"
    assert (
        sum(
            Decimal(o["filled"]) * (1 if o["direction"] == "LONG" else -1)
            for o in manager.orders.values()
        )
        == 0
    )


@pytest.mark.parametrize("mismatch", ["algo_id", "spawned_shape", "spawned_id"])
async def test_protective_proof_must_exactly_link_spawned_order(repair, mismatch):
    service, client, manager, store = repair
    if mismatch == "algo_id":
        client.order_by_id.return_value[0]["algoId"] = "other-algo"
    elif mismatch == "spawned_shape":
        client.algo_order.return_value[0]["ordIdList"] = "exit-order"
    else:
        client.algo_order.return_value[0]["ordIdList"] = ["exit-order-unrelated"]
    result = await service.repair({SYMBOL})
    assert not result.evidence_complete and not await store.latest("fills")


async def test_only_explicit_successful_resume_releases_persistent_hold(tmp_path):
    runtime, client = await runtime_incident(tmp_path)
    try:
        await runtime.reconcile()
        runtime._reconcile_and_assess = AsyncMock(return_value=(False, ["websockets"]))
        assert not await runtime.resume()
        assert await runtime.store.ledger_repair_hold()
        runtime._reconcile_and_assess = AsyncMock(return_value=(True, []))
        assert await runtime.resume()
        assert not await runtime.store.ledger_repair_hold()
        assert runtime.governor.state.value == "NORMAL"
        assert not runtime._ledger_repair_attempted
    finally:
        await runtime.close()


async def test_repair_notification_failure_does_not_change_reconciliation_result(tmp_path):
    runtime, client = await runtime_incident(tmp_path)
    runtime.notifications.publish = AsyncMock(side_effect=RuntimeError("notification unavailable"))
    try:
        await runtime.reconcile()
        assert runtime.ledger_repair_result.repaired
        assert runtime.portfolio.synchronized and runtime.reconciliation_healthy
        assert runtime.governor.state.value == "HALT"
    finally:
        await runtime.close()


async def test_already_complete_history_does_not_require_old_order_detail(repair):
    service, client, manager, store = repair
    await service.repair({SYMBOL})
    client.order_by_id.reset_mock()
    client.order_by_id.side_effect = RuntimeError("order detail retention expired")
    result = await service.repair({SYMBOL})
    assert result.evidence_complete and result.fills_added == 0
    client.order_by_id.assert_not_awaited()


async def test_history_network_does_not_hold_order_ingestion_lock(repair):
    import asyncio

    service, client, manager, store = repair
    entered = asyncio.Event()
    release = asyncio.Event()

    async def history(*args, **kwargs):
        entered.set()
        await release.wait()
        return [fill()]

    client.fills_history_window.side_effect = history
    task = asyncio.create_task(service.repair({SYMBOL}))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        event = {
            **fill("concurrent-entry", order="entry-order", side="buy", client="entry"),
            "state": "filled",
            "accFillSz": "1",
        }
        await asyncio.wait_for(manager.ingest(event), 1)
        release.set()
        result = await asyncio.wait_for(task, 2)
        assert not result.evidence_complete  # concurrent ledger append invalidates the old plan
        assert [r["tradeId"] for r in await store.latest("fills")] == ["concurrent-entry"]
        assert "protective-exit-order" not in manager.orders
    finally:
        release.set()
        await task


async def test_ledger_repair_status_is_safe_and_read_only(tmp_path, monkeypatch):
    import httpx

    from app.api import create_app

    runtime, client = await runtime_incident(tmp_path)
    try:
        await runtime.reconcile()
        monkeypatch.setattr("app.api.TradingRuntime", lambda _: runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(runtime.settings)),
            base_url="http://test",
        ) as api:
            status = (await api.get("/status")).json()["ledger_repair"]
        assert status["manual_resume_required"]
        assert status["last_attempt"]["repaired"]
        assert status["last_attempt"]["fills_added"] == 1
        assert not any(k in str(status) for k in ["api_key", "uid", "account_digest", "password"])
    finally:
        await runtime.close()


async def test_evidence_deadline_is_bounded_and_never_commits_partial_plan(repair, monkeypatch):
    import asyncio

    service, client, manager, store = repair
    original = asyncio.timeout
    seconds = []

    def immediate_timeout(value):
        seconds.append(value)
        return original(0)

    async def unavailable(*args, **kwargs):
        await asyncio.Event().wait()

    client.fills_history_window.side_effect = unavailable
    monkeypatch.setattr("app.ledger_repair.asyncio.timeout", immediate_timeout)
    result = await service.repair({SYMBOL})
    assert seconds == [30]
    assert not result.evidence_complete and not await store.latest("fills")


async def test_previous_known_protective_algo_ownership_survives_replacement(repair):
    service, client, manager, store = repair
    from app.models import OrderState

    await manager.transition(
        "entry",
        OrderState.FILLED,
        protective_algo_id="replacement-stop",
    )
    assert manager.orders["entry"]["protective_algo_id"] == "replacement-stop"
    result = await service.repair({SYMBOL})
    assert result.evidence_complete and result.fills_added == 1
    assert manager.orders["protective-exit-order"]["protective_algo_id"] == "known-stop"
