from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP")
MAX_INPUT_BYTES = 262144
MAX_EVIDENCE_BYTES = 8 * 1024 * 1024


def timestamp_input(value: Any) -> Any:
    if not isinstance(value, (datetime, str)):
        raise ValueError("explicit timezone-aware timestamp required")
    return value


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()


def aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone required")
    if value.year < 2020 or value.year > 2100:
        raise ValueError("timestamp outside research bounds")
    return value.astimezone(UTC)


def safe_text(value: str) -> str:
    if any(ord(c) < 32 and c not in "\n\t" or ord(c) == 127 for c in value):
        raise ValueError("control characters forbidden")
    # Source references and prose are inert, never rendered as HTML or logged.
    return value


class FrozenModel(BaseModel):
    model_config = ConfigDict(
        frozen=True, extra="forbid", allow_inf_nan=False, hide_input_in_errors=True
    )


class Stance(StrEnum):
    STRONG_BULLISH = "STRONG_BULLISH"
    BULLISH = "BULLISH"
    NEUTRAL = "NEUTRAL"
    BEARISH = "BEARISH"
    STRONG_BEARISH = "STRONG_BEARISH"


class Horizon(StrEnum):
    H1 = "1H"
    H4 = "4H"
    H12 = "12H"
    H24 = "24H"
    H72 = "72H"

    @property
    def hours(self) -> int:
        return int(self.value[:-1])


class AdvisoryInput(FrozenModel):
    provider: Literal["manual_okx_ai"] = "manual_okx_ai"
    provider_version: str = Field(default="structured-v2", min_length=1, max_length=64)
    symbol: Literal["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    generated_at: datetime
    horizon: Horizon
    stance: Stance
    confidence: float | None = Field(default=None, ge=0, le=1, strict=True)
    summary: str = Field(default="", max_length=8192)
    reasoning_points: tuple[str, ...] = Field(default=(), max_length=32)
    support_levels: tuple[float, ...] = Field(default=(), max_length=32)
    resistance_levels: tuple[float, ...] = Field(default=(), max_length=32)
    expected_range_low: float | None = Field(default=None, gt=0)
    expected_range_high: float | None = Field(default=None, gt=0)
    invalidation_level: float | None = Field(default=None, gt=0)
    market_structure: str | None = Field(default=None, max_length=2048)
    momentum_view: str | None = Field(default=None, max_length=2048)
    volatility_view: str | None = Field(default=None, max_length=2048)
    liquidity_view: str | None = Field(default=None, max_length=2048)
    source_reference: str | None = Field(default=None, max_length=256)
    supersedes_id: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    source_timestamp_trusted: bool = Field(default=False, strict=True)
    schema_version: Literal[2] = 2

    _time_input = field_validator("generated_at", mode="before")(timestamp_input)
    _time = field_validator("generated_at")(aware)

    @field_validator(
        "summary",
        "provider_version",
        "market_structure",
        "momentum_view",
        "volatility_view",
        "liquidity_view",
        "source_reference",
    )
    @classmethod
    def text_fields(cls, value: str | None) -> str | None:
        if value is not None:
            safe_text(value)
        return value

    @field_validator("reasoning_points")
    @classmethod
    def reasons(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        for value in values:
            if len(value) > 2048:
                raise ValueError("reason too long")
            safe_text(value)
        return values

    @field_validator("support_levels", "resistance_levels")
    @classmethod
    def levels(cls, values: tuple[float, ...]) -> tuple[float, ...]:
        if any(not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError("invalid level")
        return values

    @field_validator("source_reference")
    @classmethod
    def reference(cls, value: str | None) -> str | None:
        # Only human labels, not URLs, file paths, cookies, or bearer credentials.
        if value and (
            not re.fullmatch(r"[\w .:#-]{1,256}", value)
            or re.search(r"(?i)(token|secret|password|cookie|api.?key|bearer)", value)
        ):
            raise ValueError("source reference must be a non-secret label")
        return value

    @model_validator(mode="after")
    def range_pair(self) -> AdvisoryInput:
        low, high = self.expected_range_low, self.expected_range_high
        if (low is None) != (high is None) or (
            low is not None and high is not None and low >= high
        ):
            raise ValueError("invalid expected range")
        return self


class ExternalAdvisory(AdvisoryInput):
    advisory_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    ingested_at: datetime
    raw_source_digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    late_import: bool
    _ingest_input = field_validator("ingested_at", mode="before")(timestamp_input)
    _ingest_time = field_validator("ingested_at")(aware)


class ResearchPolicy(FrozenModel):
    # Research thresholds only; no Settings or risk configuration references.
    direction_threshold_pct: float = Field(default=0.1, ge=0, le=100)
    level_tolerance_bps: float = Field(default=1, ge=0, le=100)
    future_tolerance_seconds: int = Field(default=60, ge=0, le=300)
    late_import_seconds: int = Field(default=300, ge=0, le=86400)
    context_max_age_seconds: int = Field(default=1800, ge=1, le=86400)
    max_price_gap_seconds: int = Field(default=900, ge=1, le=3600)
    minimum_sample: int = Field(default=30, ge=10, le=10000)
    bootstrap_samples: int = Field(default=500, ge=100, le=2000)
    version: Literal["research-v2"] = "research-v2"


class InternalEvidence(FrozenModel):
    symbol: Literal["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    kind: Literal["features", "regime", "signals", "derivatives", "book", "position"]
    observed_at: datetime
    available_at: datetime
    # A serialized immutable JSON object avoids nested mutable model dictionaries.
    content_json: str = Field(max_length=32768)
    source_reference: str = Field(min_length=1, max_length=256)
    _time_inputs = field_validator("observed_at", "available_at", mode="before")(timestamp_input)
    _times = field_validator("observed_at", "available_at")(aware)

    @model_validator(mode="after")
    def validate_evidence(self) -> InternalEvidence:
        if self.available_at < self.observed_at:
            raise ValueError("availability precedes observation")
        parsed = bounded_json(self.content_json)
        if not isinstance(parsed, dict):
            raise ValueError("object evidence required")
        # A later embedded feature/signal timestamp cannot hide behind an older row timestamp.
        validate_embedded_times(parsed, self.observed_at)
        if parsed.get("symbol", self.symbol) != self.symbol:
            raise ValueError("evidence symbol mismatch")
        if self.kind == "regime" and parsed.get("regime") not in {
            "TREND_UP",
            "TREND_DOWN",
            "RANGE",
            "HIGH_VOL",
            "LOW_VOL",
            "PANIC",
            "UNKNOWN",
        }:
            raise ValueError("invalid recorded regime")
        AdvisoryInput.reference(self.source_reference)
        return self


class PriceObservation(FrozenModel):
    symbol: Literal["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    observed_at: datetime
    available_at: datetime
    price: float = Field(gt=0, strict=True)
    _time_inputs = field_validator("observed_at", "available_at", mode="before")(timestamp_input)
    _times = field_validator("observed_at", "available_at")(aware)

    @model_validator(mode="after")
    def times(self) -> PriceObservation:
        if self.available_at < self.observed_at:
            raise ValueError("availability precedes observation")
        return self


class EvidenceBundle(FrozenModel):
    internal: tuple[InternalEvidence, ...] = Field(default=(), max_length=5000)
    prices: tuple[PriceObservation, ...] = Field(default=(), max_length=20000)


def validate_embedded_times(value: Any, ceiling: datetime) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"timestamp", "observed_at", "available_at", "snapshot_at", "generated_at"}:
                if not isinstance(item, str) or aware(datetime.fromisoformat(item)) > ceiling:
                    raise ValueError("future embedded evidence")
            else:
                validate_embedded_times(item, ceiling)
    elif isinstance(value, list):
        for item in value:
            validate_embedded_times(item, ceiling)


def bounded_json(raw: str | bytes, max_bytes: int = MAX_INPUT_BYTES) -> Any:
    encoded = raw.encode() if isinstance(raw, str) else raw
    if len(encoded) > max_bytes:
        raise ValueError("input too large")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def bad_constant(_: str) -> None:
        raise ValueError("non-finite JSON")

    def validate(value: Any, depth: int = 0) -> None:
        if depth > 32:
            raise ValueError("JSON nesting too deep")
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("non-finite JSON")
        if isinstance(value, dict):
            for key, item in value.items():
                safe_text(key)
                validate(item, depth + 1)
        elif isinstance(value, list):
            for item in value:
                validate(item, depth + 1)
        elif isinstance(value, str):
            safe_text(value)

    try:
        result = json.loads(encoded, object_pairs_hook=pairs, parse_constant=bad_constant)
        validate(result)
        return result
    except (ValueError, RecursionError, UnicodeError):
        raise ValueError("invalid bounded JSON") from None
