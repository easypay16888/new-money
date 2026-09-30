from __future__ import annotations

from enum import StrEnum
from functools import lru_cache

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Mode(StrEnum):
    BACKTEST = "BACKTEST"
    PAPER = "PAPER"
    LIVE = "LIVE"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mode: Mode = Mode.PAPER
    live_trading_enabled: bool = False
    confirm_live_account_id: str = ""
    okx_api_key: str = ""
    okx_secret_key: str = ""
    okx_passphrase: str = ""
    okx_rest_url: str = "https://openapi.okx.com"
    okx_paper_ws_url: str = "wss://wspap.okx.com:8443/ws/v5"
    okx_live_ws_url: str = "wss://ws.okx.com:8443/ws/v5"
    database_url: str = "sqlite+aiosqlite:///./data/trading.db"
    redis_url: str = "redis://localhost:6379/0"
    symbols: tuple[str, ...] = ("BTC-USDT-SWAP", "ETH-USDT-SWAP")
    timeframes: tuple[str, ...] = ("1m", "5m", "15m", "1H", "4H")
    risk_per_trade: float = Field(default=0.0025, gt=0)
    max_risk_per_trade: float = Field(default=0.0035, gt=0)
    max_total_open_risk: float = Field(default=0.01, gt=0)
    max_daily_loss: float = Field(default=0.015, gt=0)
    max_weekly_drawdown: float = Field(default=0.04, gt=0)
    max_margin_usage: float = Field(default=0.25, gt=0, le=1)
    min_margin_ratio: float = Field(default=1.5, gt=0)
    max_open_positions: int = Field(default=3, ge=1)
    max_leverage: int = Field(default=3, ge=1, le=3)
    leverage: int = Field(default=1, ge=1, le=3)
    stale_timeout_seconds: float = Field(default=20, gt=0)
    orderbook_snapshot_interval_seconds: float = Field(default=5, gt=0)
    request_timeout_seconds: float = Field(default=8, gt=0)
    reconcile_interval_seconds: float = Field(default=30, gt=0)
    entry_cancel_confirm_seconds: float = Field(default=15, gt=0)
    protection_confirm_seconds: float = Field(default=10, gt=0)
    ws_backoff_max_seconds: float = Field(default=30, gt=0)
    cancel_all_after_seconds: int = Field(default=60, ge=10, le=120)
    cancel_all_after_refresh_seconds: int = Field(default=20, ge=1)
    min_signal_confidence: float = Field(default=0.65, ge=0, le=1)
    maker_fee: float = 0.0002
    taker_fee: float = 0.0005
    slippage_bps: float = 2.0
    backtest_spread_bps: float = Field(default=1.0, ge=0)
    backtest_funding_rate: float = 0.0
    backtest_fill_ratio: float = Field(default=1.0, gt=0, le=1)
    max_spread_ratio: float = Field(default=0.005, gt=0)
    trend_min_adx: float = Field(default=20, ge=0)
    trend_min_volume_ratio: float = Field(default=1, gt=0)
    breakout_min_volume_ratio: float = Field(default=1.5, gt=0)
    breakout_max_atr_contraction: float = Field(default=0.8, gt=0)
    breakout_max_bandwidth: float = Field(default=0.06, gt=0)
    mean_reversion_rsi_low: float = Field(default=30, ge=0, le=100)
    mean_reversion_rsi_high: float = Field(default=70, ge=0, le=100)
    mean_reversion_min_zscore: float = Field(default=1.5, gt=0)
    regime_panic_volatility: float = Field(default=0.85, gt=0)
    regime_panic_volume: float = Field(default=0.8, gt=0)
    regime_high_volatility: float = Field(default=0.65, gt=0)
    regime_low_volatility: float = Field(default=0.15, ge=0)
    strategy_weights: dict[str, float] = {"trend": 1.0, "breakout": 1.0, "mean_reversion": 0.8}
    api_token: str = ""
    alert_webhook_url: str = ""
    bark_enabled: bool = False
    bark_server: str = "https://api.day.app"
    bark_device_key: SecretStr = SecretStr("")
    bark_group: str = "OKX Quant"
    bark_timeout_seconds: float = Field(default=5, gt=0, le=30)
    bark_heartbeat_hours: float = Field(default=6, gt=0)
    bark_dedup_seconds: float = Field(default=60, ge=0)
    bark_heartbeat_enabled: bool = False
    bark_notify_entry_submitted: bool = False
    bark_notify_system_stopping: bool = False
    bark_notify_fast_recovery: bool = False
    bark_infra_alert_delay_seconds: float = Field(default=60, ge=0)
    bark_incident_merge_window_seconds: float = Field(default=300, ge=0)
    bark_incident_retry_initial_seconds: float = Field(default=30, gt=0)
    bark_incident_retry_max_seconds: float = Field(default=300, gt=0)
    bark_trade_notifications: bool = True
    bark_risk_notifications: bool = True
    bark_daily_report: bool = True
    webhook_notifications_verbose: bool = True
    bark_sound: str = ""
    bark_critical_sound: str = "alarm"
    bark_critical_volume: int = Field(default=5, ge=0, le=10)
    auto_recovery_enabled: bool = True
    auto_recovery_success_threshold: int = Field(default=3, ge=1)
    auto_recovery_check_seconds: float = Field(default=30, gt=0)
    auto_recovery_min_halt_seconds: float = Field(default=30, ge=0)
    auto_recovery_stability_seconds: float = Field(default=60, ge=0)
    auto_recovery_max_resumes_per_hour: int = Field(default=3, ge=1)

    @model_validator(mode="after")
    def validate_live(self) -> Settings:
        if self.mode == Mode.LIVE and not (
            self.live_trading_enabled and self.confirm_live_account_id
        ):
            raise ValueError("LIVE requires LIVE_TRADING_ENABLED and CONFIRM_LIVE_ACCOUNT_ID")
        if self.leverage > self.max_leverage:
            raise ValueError("leverage exceeds configured maximum")
        if self.risk_per_trade > self.max_risk_per_trade:
            raise ValueError("risk per trade exceeds configured maximum")
        return self

    @property
    def ws_base(self) -> str:
        return self.okx_live_ws_url if self.mode == Mode.LIVE else self.okx_paper_ws_url

    @property
    def has_credentials(self) -> bool:
        return bool(self.okx_api_key and self.okx_secret_key and self.okx_passphrase)


@lru_cache
def get_settings() -> Settings:
    return Settings()
