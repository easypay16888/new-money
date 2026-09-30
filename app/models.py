from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


def utcnow() -> datetime:
    return datetime.now(UTC)


class Side(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


class Regime(StrEnum):
    TREND_UP = "TREND_UP"
    TREND_DOWN = "TREND_DOWN"
    RANGE = "RANGE"
    HIGH_VOL = "HIGH_VOL"
    LOW_VOL = "LOW_VOL"
    PANIC = "PANIC"
    UNKNOWN = "UNKNOWN"


class GovernorState(StrEnum):
    NORMAL = "NORMAL"
    CAUTION = "CAUTION"
    REDUCE = "REDUCE"
    HALT = "HALT"
    EMERGENCY = "EMERGENCY"


class NotificationLevel(StrEnum):
    INFO = "INFO"
    TRADE = "TRADE"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class NotificationCategory(StrEnum):
    SYSTEM = "SYSTEM"
    TRADE = "TRADE"
    RISK = "RISK"
    INFRASTRUCTURE = "INFRASTRUCTURE"
    DAILY_REPORT = "DAILY_REPORT"
    HEARTBEAT = "HEARTBEAT"


class NotificationPriority(StrEnum):
    PASSIVE = "PASSIVE"
    ACTIVE = "ACTIVE"
    TIME_SENSITIVE = "TIME_SENSITIVE"
    CRITICAL = "CRITICAL"


class NotificationEvent(BaseModel):
    id: str = Field(default_factory=lambda: uuid4().hex)
    event_code: str = ""
    timestamp: datetime = Field(default_factory=utcnow)
    level: NotificationLevel
    category: NotificationCategory
    title: str
    message: str
    symbol: str | None = None
    dedup_key: str | None = None
    priority: NotificationPriority = NotificationPriority.ACTIVE
    metadata: dict[str, Any] = Field(default_factory=dict)


class OrderState(StrEnum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


class MarketTick(BaseModel):
    symbol: str
    timestamp: datetime
    last: Decimal
    bid: Decimal | None = None
    ask: Decimal | None = None


class Candle(BaseModel):
    symbol: str
    timeframe: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    confirmed: bool = False


class Trade(BaseModel):
    symbol: str
    timestamp: datetime
    trade_id: str
    price: Decimal
    size: Decimal
    side: str


class OrderBook(BaseModel):
    symbol: str
    timestamp: datetime
    bids: list[tuple[Decimal, Decimal]]
    asks: list[tuple[Decimal, Decimal]]
    sequence: int | None = None


class FundingInfo(BaseModel):
    symbol: str
    timestamp: datetime
    rate: Decimal


class OpenInterest(BaseModel):
    symbol: str
    timestamp: datetime
    contracts: Decimal


class DerivativeObservation(BaseModel):
    symbol: str
    timestamp: datetime
    kind: Literal["mark", "index", "funding", "oi"]
    value: Decimal


class Instrument(BaseModel):
    symbol: str
    contract_value: Decimal
    contract_currency: str
    lot_size: Decimal
    min_size: Decimal
    tick_size: Decimal


class MarketStateVector(BaseModel):
    symbol: str
    timestamp: datetime
    timeframe: str
    close: Decimal
    returns: float | None = None
    log_returns: float | None = None
    high_low_range: float | None = None
    ema20: float | None = None
    ema50: float | None = None
    ema200: float | None = None
    ema_slope: float | None = None
    rsi: float | None = None
    roc: float | None = None
    atr: float | None = None
    adx: float | None = None
    realized_vol: float | None = None
    volume_ratio: float | None = None
    volume_ma: float | None = None
    donchian_upper: float | None = None
    donchian_lower: float | None = None
    bollinger_upper: float | None = None
    bollinger_lower: float | None = None
    bollinger_bandwidth: float | None = None
    price_zscore: float | None = None
    vwap_deviation: float | None = None
    atr_contraction_ratio: float | None = None
    spread: float | None = None
    orderbook_imbalance: float | None = None
    funding_rate: float | None = None
    funding_zscore: float | None = None
    open_interest: float | None = None
    oi_change: float | None = None
    mark_index_premium: float | None = None
    trade_imbalance: float | None = None
    trend_score: float = 0
    momentum_score: float = 0
    volatility_score: float = 0
    volume_score: float = 0


class MarketRegime(BaseModel):
    symbol: str
    timestamp: datetime
    regime: Regime
    confidence: float
    reason: dict[str, Any] = Field(default_factory=dict)


class Signal(BaseModel):
    id: str = Field(default_factory=lambda: uuid4().hex)
    timestamp: datetime = Field(default_factory=utcnow)
    symbol: str
    strategy: str
    side: Side
    confidence: float = Field(ge=0, le=1)
    entry_reference: Decimal
    stop_price: Decimal
    take_profit_reference: Decimal | None = None
    expected_rr: float
    regime: Regime
    features_snapshot: dict[str, Any] = Field(default_factory=dict)
    reason: dict[str, Any] = Field(default_factory=dict)
    expires_at: datetime


class TradeIntent(BaseModel):
    id: str = Field(default_factory=lambda: uuid4().hex)
    symbol: str
    direction: Side
    confidence: float
    signal_ids: tuple[str, ...]
    strategies: tuple[str, ...]
    entry_reference: Decimal
    stop_price: Decimal
    take_profit_reference: Decimal | None
    timestamp: datetime = Field(default_factory=utcnow)
    expires_at: datetime | None = None


class PortfolioState(BaseModel):
    equity: Decimal
    available_balance: Decimal
    margin_used: Decimal = Decimal(0)
    position_notional: Decimal = Decimal(0)
    effective_leverage: Decimal = Decimal(0)
    unrealized_pnl: Decimal = Decimal(0)
    realized_pnl: Decimal = Decimal(0)
    directional_exposure: Decimal = Decimal(0)
    correlation_exposure: Decimal = Decimal(0)
    liquidation_distance: Decimal | None = None
    daily_pnl: Decimal = Decimal(0)
    weekly_drawdown: Decimal = Decimal(0)
    open_risk: Decimal = Decimal(0)
    positions: dict[str, Decimal] = Field(default_factory=dict)
    synchronized: bool = False


class RiskDecision(BaseModel):
    approved: bool
    status: str
    reason: str
    intent_id: str
    symbol: str
    direction: Side
    approved_contracts: Decimal = Decimal(0)
    approved_notional: Decimal = Decimal(0)
    leverage: int = 1
    stop_price: Decimal | None = None
    entry_reference: Decimal | None = None
    take_profit_reference: Decimal | None = None
    signal_expires_at: datetime | None = None


class ExecutionRequest(BaseModel):
    risk_decision: RiskDecision
    client_order_id: str
    order_type: str = "limit"
    price: Decimal | None = None
    reduce_only: bool = False
    signal_expires_at: datetime | None = None

    @property
    def symbol(self) -> str:
        return self.risk_decision.symbol
