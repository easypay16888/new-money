from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from app.advisory.models import (
    EvidenceBundle,
    ExternalAdvisory,
    PriceObservation,
    ResearchPolicy,
    Stance,
    aware,
    digest,
)


def external_direction(stance: Stance) -> int:
    return 1 if "BULLISH" in stance else -1 if "BEARISH" in stance else 0


def internal_direction(regime: str | None) -> int | None:
    # Volatility, panic, and unknown regimes have no directional interpretation.
    return {"TREND_UP": 1, "TREND_DOWN": -1, "RANGE": 0}.get(regime or "")


def agreement(regime: str | None, stance: Stance) -> str:
    internal, external = internal_direction(regime), external_direction(stance)
    if internal is None:
        return "UNCLASSIFIED"
    if internal and external and internal != external:
        return "DIRECT_CONFLICT"
    if internal == external:
        return {1: "AGREE_BULLISH", -1: "AGREE_BEARISH", 0: "BOTH_NEUTRAL"}[internal]
    if internal == 0:
        return f"EXTERNAL_{'BULLISH' if external > 0 else 'BEARISH'}_INTERNAL_NEUTRAL"
    return f"INTERNAL_{'BULLISH' if internal > 0 else 'BEARISH'}_EXTERNAL_NEUTRAL"


def snapshot(
    advisory: ExternalAdvisory, bundle: EvidenceBundle, policy: ResearchPolicy
) -> dict[str, Any]:
    t = advisory.generated_at
    candidates = [
        row
        for row in bundle.internal
        if row.symbol == advisory.symbol
        and row.observed_at <= t
        and row.available_at <= t
        and (t - row.observed_at).total_seconds() <= policy.context_max_age_seconds
    ]
    chosen: dict[str, Any] = {}
    for kind in sorted({row.kind for row in candidates}):
        rows = sorted(
            (r for r in candidates if r.kind == kind),
            key=lambda r: (r.observed_at, r.available_at, digest(r.model_dump(mode="json"))),
        )
        row = rows[-1]
        # Two different records with identical time keys are ambiguous, not ordered by opinion.
        if any(
            r.content_json != row.content_json
            for r in rows
            if (r.observed_at, r.available_at) == (row.observed_at, row.available_at)
        ):
            raise ValueError("conflicting internal evidence")
        chosen[kind] = row.model_dump(mode="json")
    regime = (
        json.loads(chosen["regime"]["content_json"]).get("regime") if "regime" in chosen else None
    )
    prices = available_prices(advisory, bundle, t)
    before = [
        p
        for p in prices
        if p.observed_at <= t
        and (t - p.observed_at).total_seconds() <= policy.max_price_gap_seconds
    ]
    start = before[-1] if before else None
    return {
        "advisory_id": advisory.advisory_id,
        "symbol": advisory.symbol,
        "snapshot_at": t.isoformat(),
        "status": "AVAILABLE"
        if "features" in chosen and regime is not None and start
        else "INTERNAL_CONTEXT_UNAVAILABLE",
        "evidence": chosen,
        "regime": regime,
        "agreement": agreement(regime, advisory.stance),
        "reference_price": start.price if start else None,
        "reference_observed_at": start.observed_at.isoformat() if start else None,
    }


def available_prices(
    advisory: ExternalAdvisory, bundle: EvidenceBundle, as_of: datetime
) -> list[PriceObservation]:
    by_time: dict[datetime, PriceObservation] = {}
    for row in bundle.prices:
        if row.symbol != advisory.symbol or row.available_at > as_of or row.observed_at > as_of:
            continue
        old = by_time.get(row.observed_at)
        if old and old.price != row.price:
            raise ValueError("conflicting price evidence")
        if old is None or row.available_at < old.available_at:
            by_time[row.observed_at] = row
    return sorted(by_time.values(), key=lambda p: p.observed_at)


def correct(direction: int, return_pct: float, threshold: float) -> bool:
    if direction > 0:
        return return_pct > threshold
    if direction < 0:
        return return_pct < -threshold
    return abs(return_pct) <= threshold


def outcome(
    advisory: ExternalAdvisory, bundle: EvidenceBundle, policy: ResearchPolicy, *, as_of: datetime
) -> dict[str, Any]:
    as_of = aware(as_of)
    start_at = advisory.generated_at
    end_at = start_at + timedelta(hours=advisory.horizon.hours)
    base: dict[str, Any] = {
        "advisory_id": advisory.advisory_id,
        "symbol": advisory.symbol,
        "horizon": advisory.horizon.value,
        "status": "PENDING",
        "window_start": start_at.isoformat(),
        "window_end": end_at.isoformat(),
        "policy": policy.model_dump(mode="json"),
    }
    if advisory.late_import and not advisory.source_timestamp_trusted:
        return dict(base, pending_reason="UNTRUSTED_LATE_SOURCE")
    if as_of < end_at:
        return dict(base, pending_reason="WINDOW_INCOMPLETE")
    prices = available_prices(advisory, bundle, as_of)
    # Reference price was actually available at T, not just timestamped before T.
    starts = [
        p
        for p in prices
        if p.observed_at <= start_at
        and p.available_at <= start_at
        and (start_at - p.observed_at).total_seconds() <= policy.max_price_gap_seconds
    ]
    future = [p for p in prices if start_at < p.observed_at <= end_at]
    if not starts or not future or future[-1].observed_at != end_at:
        return dict(base, pending_reason="PRICE_WINDOW_INCOMPLETE")
    points = [starts[-1], *future]
    if any(
        (b.observed_at - a.observed_at).total_seconds() > policy.max_price_gap_seconds
        for a, b in zip(points, points[1:], strict=False)
    ):
        return dict(base, pending_reason="PRICE_GAP")
    start, end = points[0].price, points[-1].price
    # Extrema include T reference; after-T data only determines future evolution.
    high, low = max(p.price for p in points), min(p.price for p in points)
    ret = (end / start - 1) * 100
    direction = external_direction(advisory.stance)
    upside, downside = (high / start - 1) * 100, (1 - low / start) * 100
    result = dict(
        base,
        status="LABELED",
        start_price=start,
        end_price=end,
        return_pct=ret,
        high=high,
        low=low,
        realized_range_pct=(high - low) / start * 100,
        max_favorable_excursion=upside if direction > 0 else downside if direction < 0 else None,
        max_adverse_excursion=downside if direction > 0 else upside if direction < 0 else None,
        direction_correct=correct(direction, ret, policy.direction_threshold_pct),
        observations=len(future),
        price_evidence_digest=digest([p.model_dump(mode="json") for p in points]),
        extrema_scope="OBSERVED_PRICES_ONLY",
    )
    lo, hi = advisory.expected_range_low, advisory.expected_range_high
    if lo is not None and hi is not None:
        breaks = [p for p in future if not lo <= p.price <= hi]
        result["range"] = {
            "range_containment": not breaks,
            "fraction_observations_inside": sum(lo <= p.price <= hi for p in future) / len(future),
            "upper_break": any(p.price > hi for p in future),
            "lower_break": any(p.price < lo for p in future),
            "first_breakout_direction": ("UP" if breaks[0].price > hi else "DOWN")
            if breaks
            else None,
            "first_breakout_at": breaks[0].observed_at.isoformat() if breaks else None,
            "maximum_overshoot": max([0.0, *[max(p.price - hi, lo - p.price, 0) for p in future]]),
            "close_inside": lo <= end <= hi,
        }
    result["levels"] = [
        level_result(level, kind, points, start_at, policy.level_tolerance_bps)
        for kind, levels in (
            ("support", advisory.support_levels),
            ("resistance", advisory.resistance_levels),
        )
        for level in levels
    ]
    return result


def level_result(
    level: float,
    kind: str,
    points: list[PriceObservation],
    start_at: datetime,
    tolerance_bps: float,
) -> dict[str, Any]:
    tolerance = level * tolerance_bps / 10000
    future = points[1:]
    # Discrete crossing implies a touch; exact tick sampling need not hit the level.
    touch = next(
        (
            b
            for a, b in zip(points, future, strict=False)
            if abs(b.price - level) <= tolerance
            or min(a.price, b.price) <= level <= max(a.price, b.price)
        ),
        None,
    )
    broken = (
        [p for p in future if p.price < level - tolerance]
        if kind == "support"
        else [p for p in future if p.price > level + tolerance]
    )
    reclaimed = bool(
        broken
        and any(
            p.observed_at > broken[0].observed_at
            and (
                p.price >= level + tolerance if kind == "support" else p.price <= level - tolerance
            )
            for p in future
        )
    )
    return {
        "kind": kind,
        "level": level,
        "tolerance_bps": tolerance_bps,
        "level_touched": touch is not None,
        "level_broken": bool(broken),
        "level_reclaimed": reclaimed,
        "seconds_to_first_touch": (touch.observed_at - start_at).total_seconds() if touch else None,
        "max_penetration": max(
            [0.0, *[(level - p.price if kind == "support" else p.price - level) for p in future]]
        ),
    }
