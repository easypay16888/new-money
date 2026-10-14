from __future__ import annotations

import random
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Any

from app.advisory.evaluation import correct, internal_direction
from app.advisory.models import ResearchPolicy


def statistics_summary(values: list[float], policy: ResearchPolicy) -> dict[str, Any]:
    n = len(values)
    result: dict[str, Any] = {
        "n": n,
        "status": "SUFFICIENT_SAMPLE" if n >= policy.minimum_sample else "INSUFFICIENT_SAMPLE",
        "minimum_sample": policy.minimum_sample,
        "mean": statistics.mean(values) if values else None,
        "median": statistics.median(values) if values else None,
        "standard_deviation": statistics.stdev(values) if n > 1 else None,
        "standard_error": statistics.stdev(values) / n**0.5 if n > 1 else None,
        "bootstrap_95_ci": None,
    }
    if n >= policy.minimum_sample:
        rng = random.Random(0)
        means = sorted(
            statistics.mean(rng.choices(values, k=n)) for _ in range(policy.bootstrap_samples)
        )
        result["bootstrap_95_ci"] = [means[int(0.025 * len(means))], means[int(0.975 * len(means))]]
    return result


def build_report(
    rows: list[dict[str, Any]], policy: ResearchPolicy, *, include_late: bool = True
) -> dict[str, Any]:
    # One selected evaluation per immutable advisory; no duplicate versions in statistics.
    rows = [r for r in rows if include_late or not r["advisory"]["late_import"]]
    providers: dict[str, list[dict[str, Any]]] = defaultdict(list)
    cohorts: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        a, s, o = row["advisory"], row["snapshot"], row["outcome"]
        providers[a["provider"]].append(row)
        if o["status"] != "LABELED":
            continue
        t = datetime.fromisoformat(a["generated_at"])
        features = s["evidence"].get("features", {}).get("content_json")
        import json

        values = json.loads(features) if features else {}
        vol = values.get("realized_vol")
        funding = values.get("funding_rate")
        vol_bucket = (
            "UNKNOWN"
            if vol is None
            else "LOW"
            if vol < 0.005
            else "MEDIUM"
            if vol < 0.02
            else "HIGH"
        )
        funding_bucket = (
            "UNKNOWN"
            if funding is None
            else "POSITIVE"
            if funding > 0
            else "NEGATIVE"
            if funding < 0
            else "ZERO"
        )
        groups = [
            "baseline:EXTERNAL",
            "agreement:" + s["agreement"],
            "regime:" + str(s["regime"]),
            "symbol:" + a["symbol"],
            "horizon:" + a["horizon"],
            "stance_horizon:" + a["stance"] + ":" + a["horizon"],
            "matrix:" + str(s["regime"]) + ":" + a["stance"],
            "volatility:" + vol_bucket,
            "funding:" + funding_bucket,
            "hour_utc:" + str(t.hour),
            "weekday_utc:" + str(t.weekday()),
        ]
        if internal_direction(s["regime"]) is not None and s["status"] == "AVAILABLE":
            groups.append("baseline:INTERNAL")
        if s["agreement"].startswith("AGREE_"):
            groups.append("baseline:AGREEMENT")
        elif s["agreement"] == "DIRECT_CONFLICT":
            groups.append("baseline:CONFLICT")
        else:
            groups.append("baseline:NEUTRAL_MIXED")
        if a["confidence"] is not None:
            groups.append("confidence_bucket:" + str(min(9, int(a["confidence"] * 10))))
        for group in groups:
            cohorts[group].append(row)
    summaries = {}
    for name, group in sorted(cohorts.items()):
        outcomes = [r["outcome"] for r in group]
        accuracy = [
            int(
                correct(
                    internal_direction(r["snapshot"]["regime"]) or 0,
                    r["outcome"]["return_pct"],
                    policy.direction_threshold_pct,
                )
            )
            if name == "baseline:INTERNAL"
            else int(r["outcome"]["direction_correct"])
            for r in group
        ]
        mfes, maes = [], []
        for r in group:
            o = r["outcome"]
            if name == "baseline:INTERNAL":
                direction = internal_direction(r["snapshot"]["regime"])
                if direction:
                    up = (o["high"] / o["start_price"] - 1) * 100
                    down = (1 - o["low"] / o["start_price"]) * 100
                    mfes.append(up if direction > 0 else down)
                    maes.append(down if direction > 0 else up)
            elif o["max_favorable_excursion"] is not None:
                mfes.append(o["max_favorable_excursion"])
                maes.append(o["max_adverse_excursion"])
        summaries[name] = {
            "returns_pct": statistics_summary([o["return_pct"] for o in outcomes], policy),
            "direction_accuracy": statistics.mean(accuracy),
            "mean_MFE_pct": statistics.mean(mfes) if mfes else None,
            "mean_MAE_pct": statistics.mean(maes) if maes else None,
            "adverse_excursion_pct": statistics_summary(maes, policy),
        }
    ranges = [r["outcome"]["range"] for r in rows if r["outcome"].get("range")]
    revisions = []
    by_id = {r["advisory"]["advisory_id"]: r["advisory"] for r in rows}
    for row in rows:
        a = row["advisory"]
        prior = by_id.get(a.get("supersedes_id"))
        if prior:
            revisions.append(
                {
                    "advisory_id": a["advisory_id"],
                    "supersedes_id": prior["advisory_id"],
                    "stance_changed": a["stance"] != prior["stance"],
                    "confidence_changed": a["confidence"] != prior["confidence"],
                    "range_changed": (a["expected_range_low"], a["expected_range_high"])
                    != (prior["expected_range_low"], prior["expected_range_high"]),
                    "seconds_between": (
                        datetime.fromisoformat(a["generated_at"])
                        - datetime.fromisoformat(prior["generated_at"])
                    ).total_seconds(),
                }
            )
    return {
        "research_only": True,
        "trading_authority": False,
        "policy": policy.model_dump(mode="json"),
        "observations": len(rows),
        "pending": sum(r["outcome"]["status"] == "PENDING" for r in rows),
        "providers": {
            provider: {
                "n": len(group),
                "symbols": sorted({r["advisory"]["symbol"] for r in group}),
                "horizons": sorted({r["advisory"]["horizon"] for r in group}),
                "date_start": min(r["advisory"]["generated_at"] for r in group),
                "date_end": max(r["advisory"]["generated_at"] for r in group),
            }
            for provider, group in providers.items()
        },
        "cohorts": summaries,
        "ranges": {
            "n": len(ranges),
            "status": "SUFFICIENT_SAMPLE"
            if len(ranges) >= policy.minimum_sample
            else "INSUFFICIENT_SAMPLE",
            "containment_rate": statistics.mean(r["range_containment"] for r in ranges)
            if ranges
            else None,
            "breakout_rate": statistics.mean(not r["range_containment"] for r in ranges)
            if ranges
            else None,
            "upper_breaks": sum(r["upper_break"] for r in ranges),
            "lower_breaks": sum(r["lower_break"] for r in ranges),
            "mean_overshoot": statistics.mean(r["maximum_overshoot"] for r in ranges)
            if ranges
            else None,
        },
        "revisions": revisions,
        "limitations": [
            "Observed-price resolution; extrema between observations are unknown.",
            "Advisory-aligned internal baseline, not an exhaustive trading backtest.",
            "Overlapping horizons and revisions are correlated; IID bootstrap is descriptive only.",
            "No significance or causal incremental-value claim.",
        ],
    }
