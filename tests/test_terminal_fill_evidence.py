from unittest.mock import AsyncMock

import pytest

from app.config import Settings
from app.models import GovernorState
from app.okx import OkxError
from app.runtime import TradingRuntime
from tests.test_ledger_repair import entry, fill


@pytest.fixture
async def snapshot_runtime(tmp_path):
    runtime = TradingRuntime(Settings(
        _env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path}/snapshot.db"
    ))
    await runtime.store.initialize()
    local = {**entry(), "state": "ACKNOWLEDGED", "filled": "0", "reconciled_filled": "0",
             "approved_contracts": "3.93"}
    await runtime.store.append("orders", local, symbol=local["symbol"], reference_id="entry")
    await runtime.order_manager.restore()
    runtime.portfolio.synchronized = True
    detail = dict(clOrdId="entry", ordId="entry-order", instId=local["symbol"], side="buy",
                  posSide="net", reduceOnly="false", sz="3.93", accFillSz="3.93", state="filled",
                  tradeId="last-fill", fillSz="3.79", fillPx="110", fillTime=fill()["fillTime"],
                  fee="-0.200696454", feeCcy="USDT")
    canonical = {**fill("last-fill", "3.79", "entry-order", "buy", "entry"),
                 "fee": "-0.193546962", "fillPnl": "0"}
    client = AsyncMock()
    client.order.return_value = [detail]
    client.fills_history.return_value = [canonical]
    for name in ("account", "positions", "pending_orders", "pending_algos"):
        getattr(client, name).return_value = []
    original = runtime.client
    runtime.client = client
    try:
        yield runtime, client, detail, canonical
    finally:
        await original.close()
        await runtime.notifications.close()
        await runtime.store.close()


async def snapshot(runtime):
    return await runtime._refresh_terminal_order_snapshot([], [], [], [])


async def test_rest_cumulative_fee_is_not_written_as_individual_fill_fee(snapshot_runtime):
    runtime, client, detail, canonical = snapshot_runtime
    await snapshot(runtime)
    rows = await runtime.store.latest("fills")
    assert len(rows) == 1
    assert rows[0]["fee"] == canonical["fee"] != detail["fee"]
    assert rows[0]["fillPnl"] == "0"
    assert rows[0]["fill_evidence_source"] == "okx_fills_history"
    assert rows[0]["order_fee"] == detail["fee"]
    client.fills_history.assert_awaited_once_with(detail["instId"], limit=100)


async def test_rest_then_ws_fill_with_individual_fee_is_idempotent(snapshot_runtime):
    runtime, _, detail, canonical = snapshot_runtime
    await snapshot(runtime)
    before = await runtime.store.latest("fills")
    await runtime.order_manager.ingest({**detail, "fillFee": canonical["fee"], "fillPnl": "0"})
    assert await runtime.store.latest("fills") == before
    assert runtime.order_manager.orders["entry"]["filled"] == "3.93"
    assert runtime.order_manager.seen_trade_ids == {(detail["instId"], "last-fill")}


async def test_late_earlier_partial_fill_keeps_terminal_size_and_its_own_fee(snapshot_runtime):
    runtime, _, detail, canonical = snapshot_runtime
    await snapshot(runtime)
    await runtime.order_manager.ingest({**canonical, "tradeId": "earlier-fill", "fillSz": "0.14",
        "fee": "-0.007149492", "fillFee": "-0.007149492", "state": "partially_filled",
        "accFillSz": "0.14"})
    row = runtime.order_manager.orders["entry"]
    assert row["state"] == "FILLED" and row["filled"] == "3.93"
    rows = await runtime.store.latest("fills")
    assert len(rows) == 2
    assert {r["tradeId"] for r in rows} == {"last-fill", "earlier-fill"}


@pytest.mark.parametrize("change", [{"ordId": "foreign"}, {"clOrdId": "foreign"},
                                   {"instId": "ETH-USDT-SWAP"}, {"side": "sell"},
                                   {"fillSz": "1"}, {"fillPx": "111"},
                                   {"fillTime": "1"}, {"fee": "NaN"}])
async def test_unverified_terminal_fill_evidence_never_mutates_ledger(snapshot_runtime, change):
    runtime, client, _, canonical = snapshot_runtime
    client.fills_history.return_value = [{**canonical, **change}]
    with pytest.raises((OkxError, ValueError)):
        await snapshot(runtime)
    assert await runtime.store.latest("fills") == []
    assert await runtime.store.latest("order_events") == []
    assert runtime.order_manager.orders["entry"]["filled"] == "0"


async def test_history_unavailable_keeps_owned_order_unmodified(snapshot_runtime):
    runtime, client, _, _ = snapshot_runtime
    client.fills_history.side_effect = OkxError("read unavailable")
    with pytest.raises(OkxError):
        await snapshot(runtime)
    assert await runtime.store.latest("fills") == []
    assert runtime.order_manager.orders["entry"]["state"] == "ACKNOWLEDGED"


@pytest.mark.parametrize("rows", [[], "malformed", [None]])
async def test_missing_or_malformed_history_fails_closed(snapshot_runtime, rows):
    runtime, client, _, _ = snapshot_runtime
    client.fills_history.return_value = rows
    with pytest.raises((OkxError, ValueError)):
        await snapshot(runtime)
    assert await runtime.store.latest("fills") == []


async def test_existing_fill_conflict_is_still_rejected_and_never_overwritten(snapshot_runtime):
    runtime, _, detail, canonical = snapshot_runtime
    event = {**detail, **canonical, "state": "partially_filled", "accFillSz": "3.79",
             "fee": "-0.01"}
    await runtime.order_manager.ingest(event)
    before = await runtime.store.latest("fills")
    with pytest.raises(OkxError, match="ledger fill conflict"):
        await snapshot(runtime)
    assert await runtime.store.latest("fills") == before


async def test_private_order_failure_logs_type_and_stable_code_not_payload(snapshot_runtime, caplog):
    runtime, _, detail, _ = snapshot_runtime
    runtime.order_manager.ingest = AsyncMock(side_effect=OkxError("ledger fill conflict"))
    runtime.reconcile = AsyncMock()
    runtime.entry_controller.cancel_all = AsyncMock()
    await runtime._on_private({"arg": {"channel": "orders"}, "data": [detail]})
    assert runtime.governor.state == GovernorState.EMERGENCY
    records = [r for r in caplog.records if r.getMessage() == "private order processing failed"]
    assert len(records) == 1
    assert records[0].exception_type == "OkxError"
    assert records[0].error_code == "ledger_fill_conflict"


async def test_duplicate_history_identity_is_ambiguous_and_not_ingested(snapshot_runtime):
    runtime, client, _, canonical = snapshot_runtime
    client.fills_history.return_value = [canonical, canonical]
    with pytest.raises(OkxError, match="evidence incomplete"):
        await snapshot(runtime)
    assert await runtime.store.latest("fills") == []


async def test_terminal_history_read_is_bounded_to_one_page(snapshot_runtime):
    runtime, client, _, canonical = snapshot_runtime
    client.fills_history.return_value = [canonical] * 101
    with pytest.raises(OkxError, match="evidence incomplete"):
        await snapshot(runtime)
    assert client.fills_history.await_count == 1
    assert await runtime.store.latest("fills") == []


@pytest.mark.parametrize("error", [ConnectionError("offline"), OkxError("authentication denied")])
async def test_full_reconciliation_history_failure_keeps_halt_and_never_places_order(tmp_path, error):
    from tests.test_reconciliation_incident import exit_reconciliation_runtime

    runtime, exchange, row, detail = await exit_reconciliation_runtime(tmp_path)
    evidence = fill("exit-history", "1", "exit-id", "sell", row["clOrdId"])
    detail.update(tradeId=evidence["tradeId"], fillSz="1", fillPx=evidence["fillPx"],
                  fillTime=evidence["fillTime"], fee="-1")
    exchange.fills_history = AsyncMock(side_effect=error)
    try:
        await runtime.reconcile()
        assert not runtime.reconciliation_healthy and not runtime._last_reconcile_safe
        assert runtime.governor.state != GovernorState.NORMAL
        assert not exchange.placed
        assert await runtime.store.latest("fills") == []
    finally:
        await runtime.notifications.close()
        await runtime.store.close()
