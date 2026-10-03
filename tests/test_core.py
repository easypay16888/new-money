from decimal import Decimal

import httpx
import pytest

from app.config import Mode, Settings
from app.execution import ExecutionEngine, OrderManager
from app.market import MarketDataEngine
from app.models import (
    GovernorState,
    Instrument,
    OrderState,
    PortfolioState,
    Side,
    TradeIntent,
    utcnow,
)
from app.okx import OkxError, OkxRestClient, OkxWebSocket, signature
from app.risk import RiskEngine, RiskGovernor
from app.storage import Store


def intent() -> TradeIntent:
    return TradeIntent(
        symbol="BTC-USDT-SWAP",
        direction=Side.LONG,
        confidence=0.8,
        signal_ids=("s1",),
        strategies=("trend",),
        entry_reference=Decimal("50000"),
        stop_price=Decimal("49500"),
        take_profit_reference=Decimal("51000"),
    )


def instrument() -> Instrument:
    return Instrument(
        symbol="BTC-USDT-SWAP",
        contract_value=Decimal("0.01"),
        contract_currency="BTC",
        lot_size=Decimal("0.01"),
        min_size=Decimal("0.01"),
        tick_size=Decimal("0.1"),
    )


def portfolio() -> PortfolioState:
    return PortfolioState(
        equity=Decimal("10000"), available_balance=Decimal("10000"), synchronized=True
    )


def ready_risk() -> RiskEngine:
    governor = RiskGovernor()
    assert governor.resume(synchronized=True, healthy=True)
    return RiskEngine(Settings(), governor)


def evaluate(engine: RiskEngine, state: PortfolioState | None = None):
    return engine.evaluate(
        intent(), state or portfolio(), instrument(), data_fresh=True, infrastructure_healthy=True
    )


def test_default_is_paper_and_live_requires_triple_gate():
    assert Settings(_env_file=None).mode == Mode.PAPER
    with pytest.raises(ValueError):
        Settings(_env_file=None, mode=Mode.LIVE)
    with pytest.raises(ValueError):
        Settings(_env_file=None, mode=Mode.LIVE, live_trading_enabled=True)


def test_position_sizing_respects_risk_and_contract_precision():
    decision = evaluate(ready_risk())
    assert decision.approved
    assert decision.approved_contracts == Decimal("3.99")
    assert decision.approved_notional == Decimal("1995.0000")
    assert ExecutionEngine.from_risk(decision, instrument()).price == Decimal("50000")


def test_kill_switch_daily_loss_and_stale_data():
    engine = ready_risk()
    losing = portfolio().model_copy(update={"daily_pnl": Decimal("-150")})
    assert evaluate(engine, losing).status == "HALT"
    assert engine.governor.state == GovernorState.HALT
    engine = ready_risk()
    decision = engine.evaluate(
        intent(), portfolio(), instrument(), data_fresh=False, infrastructure_healthy=True
    )
    assert not decision.approved and engine.governor.state == GovernorState.HALT


def test_weekly_drawdown_total_risk_and_position_count():
    weekly = portfolio().model_copy(update={"weekly_drawdown": Decimal("0.04")})
    assert evaluate(ready_risk(), weekly).status == "HALT"
    total = portfolio().model_copy(update={"open_risk": Decimal("90")})
    assert not evaluate(ready_risk(), total).approved
    crowded = portfolio().model_copy(
        update={
            "positions": {
                "ETH-USDT-SWAP": Decimal("1"),
                "SOL-USDT-SWAP": Decimal("1"),
                "XRP-USDT-SWAP": Decimal("1"),
            }
        }
    )
    assert evaluate(ready_risk(), crowded).reason == "max positions"


def test_max_leverage_margin_and_duplicate_position():
    engine = ready_risk()
    assert not evaluate(
        engine, portfolio().model_copy(update={"margin_used": Decimal("2500")})
    ).approved
    assert not evaluate(
        engine, portfolio().model_copy(update={"positions": {"BTC-USDT-SWAP": Decimal("1")}})
    ).approved
    with pytest.raises(ValueError):
        Settings(_env_file=None, leverage=4)


def test_existing_exposure_limits_new_notional():
    engine = ready_risk()
    state = portfolio().model_copy(update={"position_notional": Decimal("29000")})
    decision = evaluate(engine, state)
    assert decision.approved
    assert decision.status == "REDUCED"
    assert decision.approved_notional <= Decimal("1000")


def test_emergency_cannot_resume_without_health():
    governor = RiskGovernor()
    governor.halt("manual", emergency=True)
    assert not governor.resume(synchronized=False, healthy=True)
    assert governor.state == GovernorState.EMERGENCY


@pytest.mark.asyncio
async def test_order_timeout_queries_same_client_id_and_never_retries(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/orders.db")
    await store.initialize()
    manager = OrderManager(store)

    class FakeClient:
        placements = 0
        queries = 0

        async def place_order(self, body):
            self.placements += 1
            raise OkxError("timeout")

        async def order(self, symbol, client_order_id):
            self.queries += 1
            assert client_order_id.startswith("q")
            return [{"ordId": "123"}]

    client = FakeClient()
    execution = ExecutionEngine(client, manager)
    request = ExecutionEngine.from_risk(evaluate(ready_risk()), instrument())
    await execution.submit(request)
    assert client.placements == client.queries == 1
    assert manager.orders[request.client_order_id]["state"] == OrderState.ACKNOWLEDGED
    with pytest.raises(ValueError):
        await execution.submit(request)
    assert client.placements == 1
    await store.close()


@pytest.mark.asyncio
async def test_partial_fill_duplicate_event_is_idempotent(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/fills.db")
    await store.initialize()
    manager = OrderManager(store)
    request = ExecutionEngine.from_risk(evaluate(ready_risk()), instrument())
    await manager.create(request)
    event = {
        "clOrdId": request.client_order_id,
        "state": "partially_filled",
        "accFillSz": "0.5",
        "ordId": "123",
        "instId": request.symbol,
        "fillSz": "0.5",
        "tradeId": "trade-1",
    }
    await manager.ingest(event)
    await manager.ingest(event)
    assert len(await store.latest("order_events")) == 1
    assert len(await store.latest("fills")) == 1
    await manager.ingest(
        {"clOrdId": request.client_order_id, "state": "live", "accFillSz": "0", "ordId": "123"}
    )
    assert manager.orders[request.client_order_id]["state"] == OrderState.PARTIALLY_FILLED
    await store.close()


@pytest.mark.asyncio
async def test_attached_protective_order_is_recognized(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/protective.db")
    await store.initialize()
    manager = OrderManager(store)
    request = ExecutionEngine.from_risk(evaluate(ready_risk()), instrument())
    await manager.create(request)
    protective_id = manager.orders[request.client_order_id]["protective_algo_id"]
    await manager.ingest(
        {
            "clOrdId": "",
            "algoClOrdId": protective_id,
            "ordId": "protective-123",
            "instId": request.symbol,
            "state": "filled",
            "accFillSz": "0.5",
            "fillSz": "0.5",
            "tradeId": "protective-trade",
        }
    )
    assert manager.orders["protective-protective-123"]["reduce_only"] is True
    assert manager.orders["protective-protective-123"]["state"] == OrderState.FILLED
    await store.close()


def test_websocket_sequence_gap_is_fail_closed():
    async def handle(_):
        pass

    ws = OkxWebSocket("wss://example.invalid", [], handle, Settings())
    ws._check_sequence({"arg": {"channel": "books", "instId": "BTC"}, "data": [{"seqId": 2}]})
    with pytest.raises(OkxError):
        ws._check_sequence(
            {"arg": {"channel": "books", "instId": "BTC"}, "data": [{"prevSeqId": 1, "seqId": 3}]}
        )
    assert not ws.connected


@pytest.mark.asyncio
async def test_market_uses_confirmed_candles_only(tmp_path):
    store = Store(f"sqlite+aiosqlite:///{tmp_path}/market.db")
    await store.initialize()
    market = MarketDataEngine(store)
    timestamp = str(int(utcnow().timestamp() * 1000))
    row = [timestamp, "100", "102", "99", "101", "10", "10", "10", "0"]
    await market.handle({"arg": {"channel": "candle15m", "instId": "BTC-USDT-SWAP"}, "data": [row]})
    assert not await store.latest("market_candles")
    row[-1] = "1"
    await market.handle({"arg": {"channel": "candle15m", "instId": "BTC-USDT-SWAP"}, "data": [row]})
    assert len(await store.latest("market_candles")) == 1
    await store.close()


@pytest.mark.asyncio
async def test_rest_demo_header_and_no_creds():
    def handler(request: httpx.Request):
        assert request.headers["x-simulated-trading"] == "1"
        return httpx.Response(200, json={"code": "0", "data": []})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://openapi.okx.com") as client:
        rest = OkxRestClient(Settings(_env_file=None), client)
        assert await rest.request("GET", "/api/v5/public/instruments") == []
        with pytest.raises(OkxError):
            await rest.account()


def test_signature_known_vector():
    assert signature("secret", "message") == "i19IcCmVwVmMVz2x4hhmqbgl1KeU0WnXBgoDYFeWNgs="
