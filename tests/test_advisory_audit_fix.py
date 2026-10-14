"""Targeted independent-audit regressions: endpoint, provenance, health isolation."""

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError

import app.advisory.service as service_module
from app.advisory.evaluation import outcome
from app.advisory.models import (
    EvidenceBundle,
    HistoricalVerification,
    PriceObservation,
    ResearchPolicy,
)
from app.advisory.providers import ManualOkxAIProvider
from app.advisory.reports import build_report
from app.advisory.service import AdvisoryResearch, ResearchError
from tests.test_advisory import T, advisory, bundle, raw
from tests.test_advisory import research_store as research_store


def metric(service, name, labels=None):
    return service.registry.get_sample_value("quant_external_advisory_" + name, labels or {})


def proof(a, *, at=T + timedelta(hours=2)):
    return HistoricalVerification(
        advisory_id=a.advisory_id,
        evidence_reference="archived source 01",
        reviewed_by="reviewer 01",
        verified_at=at,
    )


def endpoint_bundle(age):
    b = bundle()
    at = T + timedelta(hours=1) - timedelta(seconds=age)
    point = b.prices[-1].model_copy(update={"observed_at": at, "available_at": at})
    return b.model_copy(update={"prices": (*b.prices[:-1], point)})


@pytest.mark.parametrize("age", [0, 0.1, 37, 60])
async def test_endpoint_exact_or_pre_end_within_tolerance(age):
    o = outcome(
        await advisory(), endpoint_bundle(age), ResearchPolicy(), as_of=T + timedelta(hours=1)
    )
    assert o["status"] == "LABELED"
    assert o["requested_end_at"] == o["window_end"] == (T + timedelta(hours=1)).isoformat()
    assert (
        o["actual_end_observed_at"] == (T + timedelta(hours=1) - timedelta(seconds=age)).isoformat()
    )
    assert o["endpoint_age_seconds"] == pytest.approx(age)
    assert o["policy"]["endpoint_max_age_seconds"] == 60
    assert o["start_price"] == 100 and o["end_price"] == pytest.approx(100.4)
    assert o["price_evidence_digest"]


@pytest.mark.parametrize("age", [60.001, 137])
async def test_endpoint_beyond_tolerance_is_pending(age):
    o = outcome(
        await advisory(), endpoint_bundle(age), ResearchPolicy(), as_of=T + timedelta(hours=1)
    )
    assert o["status"] == "PENDING" and o["pending_reason"] == "PRICE_WINDOW_INCOMPLETE"


async def test_post_end_price_never_selected_even_when_closer():
    b = endpoint_bundle(0.1)
    post = PriceObservation(
        symbol="BTC-USDT-SWAP",
        observed_at=T + timedelta(hours=1, milliseconds=50),
        available_at=T + timedelta(hours=1, milliseconds=50),
        price=10000,
    )
    o = outcome(
        await advisory(),
        b.model_copy(update={"prices": (*b.prices, post)}),
        ResearchPolicy(),
        as_of=T + timedelta(hours=1, seconds=1),
    )
    assert o["status"] == "LABELED" and o["end_price"] == pytest.approx(100.4)
    assert o["endpoint_age_seconds"] == 0.1 and o["high"] < 101


async def test_only_post_end_candidate_does_not_complete_window():
    b = bundle()
    post = b.prices[-1].model_copy(
        update={
            "observed_at": T + timedelta(hours=1, milliseconds=100),
            "available_at": T + timedelta(hours=1, milliseconds=100),
        }
    )
    o = outcome(
        await advisory(),
        b.model_copy(update={"prices": (*b.prices[:-1], post)}),
        ResearchPolicy(),
        as_of=T + timedelta(hours=2),
    )
    assert o["status"] == "PENDING"


@pytest.mark.parametrize("horizon", ["1H", "4H", "12H", "24H", "72H"])
async def test_irregular_non_grid_generated_time_labels_without_lookahead(horizon):
    start = T + timedelta(hours=12, minutes=3, seconds=17, milliseconds=321)
    a = await ManualOkxAIProvider().fetch_or_parse(
        raw(generated_at=start.isoformat(), horizon=horizon), now=start, policy=ResearchPolicy()
    )
    duration = a.horizon.hours * 3600
    offsets = [0, *[i + 0.123 for i in range(600, duration, 600)], duration - 0.219]
    points = tuple(
        PriceObservation(
            symbol=a.symbol,
            observed_at=start + timedelta(seconds=i),
            available_at=start + timedelta(seconds=i),
            price=100 + n / 100,
        )
        for n, i in enumerate(offsets)
    )
    o = outcome(
        a,
        EvidenceBundle(prices=points),
        ResearchPolicy(),
        as_of=start + timedelta(seconds=duration),
    )
    assert o["status"] == "LABELED" and o["endpoint_age_seconds"] == 0.219
    assert o["actual_end_observed_at"] < o["requested_end_at"]


async def test_valid_endpoint_never_bypasses_internal_gap():
    b = endpoint_bundle(0.1)
    b = b.model_copy(update={"prices": tuple(p for i, p in enumerate(b.prices) if i != 2)})
    o = outcome(await advisory(), b, ResearchPolicy(), as_of=T + timedelta(hours=1))
    assert o["status"] == "PENDING" and o["pending_reason"] == "PRICE_GAP"


async def test_endpoint_availability_and_start_integrity_remain_required():
    a, b = await advisory(), endpoint_bundle(0.1)
    later = b.prices[-1].model_copy(update={"available_at": T + timedelta(hours=1, seconds=1)})
    b = b.model_copy(update={"prices": (*b.prices[:-1], later)})
    assert outcome(a, b, ResearchPolicy(), as_of=T + timedelta(hours=1))["status"] == "PENDING"
    assert (
        outcome(a, b, ResearchPolicy(), as_of=T + timedelta(hours=1, seconds=1))["status"]
        == "LABELED"
    )
    bad_start = b.prices[0].model_copy(update={"available_at": T + timedelta(seconds=1)})
    b = b.model_copy(update={"prices": (bad_start, *b.prices[1:])})
    assert outcome(a, b, ResearchPolicy(), as_of=T + timedelta(hours=2))["status"] == "PENDING"


@pytest.mark.parametrize("value", [-1, 301, float("inf"), float("nan"), True, 60.1, "60"])
def test_endpoint_policy_bounds_strict(value):
    with pytest.raises(ValidationError):
        ResearchPolicy(endpoint_max_age_seconds=value)


async def test_zero_endpoint_tolerance_retains_exact_mode():
    policy = ResearchPolicy(endpoint_max_age_seconds=0)
    assert (
        outcome(await advisory(), endpoint_bundle(0), policy, as_of=T + timedelta(hours=1))[
            "status"
        ]
        == "LABELED"
    )
    assert (
        outcome(await advisory(), endpoint_bundle(0.1), policy, as_of=T + timedelta(hours=1))[
            "status"
        ]
        == "PENDING"
    )


@pytest.mark.parametrize("trusted", [False, True])
async def test_late_self_declared_trust_never_enters_primary(research_store, trusted):
    service = AdvisoryResearch(research_store)
    a, _ = await service.ingest(raw(source_timestamp_trusted=trusted), now=T + timedelta(hours=2))
    r = await service.evaluate(a.advisory_id, bundle(), as_of=T + timedelta(hours=2))
    assert r["provenance"] == "UNVERIFIED_HISTORICAL" and r["outcome"]["status"] == "PENDING"
    report = await service.report()
    assert report["observations"] == 1 and report["primary_observations"] == 0
    assert report["cohorts"] == {} and report["ranges"]["n"] == 0
    assert report["diagnostics"]["UNVERIFIED_HISTORICAL"]["n"] == 1


async def test_realtime_and_independently_verified_history_are_primary(research_store):
    service = AdvisoryResearch(research_store)
    realtime, _ = await service.ingest(raw(), now=T)
    late, _ = await service.ingest(
        raw(summary="retrospective", confidence=0.8), now=T + timedelta(hours=2)
    )
    await service.evaluate(realtime.advisory_id, bundle(), as_of=T + timedelta(hours=2))
    await service.evaluate(
        late.advisory_id, bundle(), as_of=T + timedelta(hours=2), verification=proof(late)
    )
    report = await service.report()
    assert report["primary_observations"] == 2
    assert report["provenance_counts"] == {
        "REALTIME_INGEST": 1,
        "VERIFIED_HISTORICAL": 1,
        "UNVERIFIED_HISTORICAL": 0,
    }
    assert report["cohorts"]["baseline:EXTERNAL"]["returns_pct"]["n"] == 2
    assert "provenance:VERIFIED_HISTORICAL" in report["cohorts"]
    assert (await service.report(include_late=False))["observations"] == 1


async def test_verified_re_evaluation_keeps_unverified_records_append_only(research_store):
    service = AdvisoryResearch(research_store)
    a, _ = await service.ingest(raw(), now=T + timedelta(hours=2))
    before = await service.evaluate(a.advisory_id, bundle(), as_of=T + timedelta(hours=2))
    original_a = await research_store.records("external_advisories")
    verified = await service.evaluate(
        a.advisory_id, bundle(), as_of=T + timedelta(hours=2), verification=proof(a)
    )
    await service.evaluate(
        a.advisory_id, bundle(), as_of=T + timedelta(hours=3), verification=proof(a)
    )
    await service.evaluate(a.advisory_id, bundle(), as_of=T + timedelta(hours=4))
    assert await research_store.records("external_advisories") == original_a
    outcomes = await research_store.records("external_advisory_outcomes")
    assert len(outcomes) == 2 and before["outcome"] in outcomes and verified["outcome"] in outcomes
    assert (await service.report())["provenance_counts"]["VERIFIED_HISTORICAL"] == 1


@pytest.mark.parametrize("case", ["wrong_id", "future_review", "before_prediction"])
async def test_independent_verification_must_match_and_be_time_valid(research_store, case):
    service = AdvisoryResearch(research_store)
    a, _ = await service.ingest(raw(), now=T + timedelta(hours=2))
    v = proof(a)
    v = (
        v.model_copy(update={"advisory_id": "a" * 64})
        if case == "wrong_id"
        else v.model_copy(
            update={
                "verified_at": T + timedelta(hours=3)
                if case == "future_review"
                else T - timedelta(seconds=1)
            }
        )
    )
    with pytest.raises(ResearchError):
        await service.evaluate(
            a.advisory_id, bundle(), as_of=T + timedelta(hours=2), verification=v
        )
    assert not await research_store.records("external_advisory_outcomes")


async def test_provider_source_cannot_supply_independent_verification(research_store):
    with pytest.raises(ResearchError):
        await AdvisoryResearch(research_store).ingest(
            raw(verification={"reviewed_by": "self"}), now=T
        )


async def test_legacy_self_trusted_label_is_quarantined_from_performance():
    a = await ManualOkxAIProvider().fetch_or_parse(
        raw(), now=T + timedelta(hours=2), policy=ResearchPolicy()
    )
    # Simulate an immutable old label that was admitted by the source's own trust flag.
    o = outcome(
        a.model_copy(update={"late_import": False, "ingested_at": T}),
        bundle(),
        ResearchPolicy(),
        as_of=T + timedelta(hours=2),
    )
    row = {
        "advisory": a.model_dump(mode="json"),
        "snapshot": {
            "regime": "TREND_UP",
            "evidence": {},
            "agreement": "AGREE_BULLISH",
            "status": "AVAILABLE",
        },
        "outcome": o,
        "provenance": "VERIFIED_HISTORICAL",
    }
    report = build_report([row], ResearchPolicy())
    assert report["primary_observations"] == 0 and report["cohorts"] == {}
    assert report["diagnostics"]["UNVERIFIED_HISTORICAL"]["labeled"] == 1


async def test_provider_errors_only_change_provider_and_service_health(research_store):
    broken = AsyncMock()
    broken.fetch_or_parse.side_effect = TimeoutError("do not log secret")
    service = AdvisoryResearch(research_store, provider=broken)
    with pytest.raises(ResearchError):
        await service.ingest(raw(), now=T)
    assert metric(service, "provider_health", {"provider": "manual_okx_ai"}) == 0
    assert metric(service, "storage_health") == 1 and metric(service, "service_health") == 0
    service.provider = ManualOkxAIProvider()
    await service.ingest(raw(), now=T)
    assert metric(service, "provider_health", {"provider": "manual_okx_ai"}) == 1
    assert metric(service, "service_health") == 1


async def test_parser_failure_does_not_mark_storage_unhealthy(research_store):
    service = AdvisoryResearch(research_store)
    with pytest.raises(ResearchError):
        await service.ingest(b"{bad JSON", now=T)
    assert metric(service, "provider_health", {"provider": "manual_okx_ai"}) == 0
    assert metric(service, "storage_health") == 1


@pytest.mark.parametrize("stage", ["import", "evaluate", "report"])
async def test_database_error_never_changes_provider_health(research_store, monkeypatch, stage):
    service = AdvisoryResearch(research_store)
    a, _ = await service.ingest(raw(), now=T)
    with monkeypatch.context() as m:
        method = "put" if stage == "import" else "get" if stage == "evaluate" else "records"
        m.setattr(
            research_store,
            method,
            AsyncMock(side_effect=OperationalError("redacted", {}, RuntimeError())),
        )
        with pytest.raises(ResearchError):
            if stage == "import":
                await service.ingest(raw(), now=T)
            elif stage == "evaluate":
                await service.evaluate(a.advisory_id, bundle(), as_of=T)
            else:
                await service.report()
    assert metric(service, "provider_health", {"provider": "manual_okx_ai"}) == 1
    assert metric(service, "storage_health") == 0 and metric(service, "service_health") == 0
    await service.report()
    assert metric(service, "storage_health") == 1 and metric(service, "service_health") == 1


@pytest.mark.parametrize("stage", ["evaluate", "report"])
async def test_computation_failure_preserves_provider_and_storage_health(
    research_store, monkeypatch, stage
):
    service = AdvisoryResearch(research_store)
    a, _ = await service.ingest(raw(), now=T)
    m = monkeypatch
    m.setattr(
        service_module,
        "outcome" if stage == "evaluate" else "build_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("computation failure")),
    )
    with pytest.raises(ResearchError):
        if stage == "evaluate":
            await service.evaluate(a.advisory_id, bundle(), as_of=T)
        else:
            await service.report()
    assert metric(service, "provider_health", {"provider": "manual_okx_ai"}) == 1
    assert metric(service, "storage_health") == 1 and metric(service, "service_health") == 0


async def test_successful_report_does_not_clear_existing_provider_failure(research_store):
    service = AdvisoryResearch(research_store)
    with pytest.raises(ResearchError):
        await service.ingest(b"bad JSON", now=T)
    await service.report()
    assert metric(service, "provider_health", {"provider": "manual_okx_ai"}) == 0
    assert metric(service, "storage_health") == 1 and metric(service, "service_health") == 1


async def test_new_endpoint_policy_keeps_old_evaluation_immutable(research_store):
    from app.advisory.evaluation import snapshot
    from app.advisory.models import digest

    service = AdvisoryResearch(research_store)
    a, _ = await service.ingest(raw(), now=T)
    old = outcome(a, bundle(), service.policy, as_of=T + timedelta(hours=1))
    old["policy"].pop("endpoint_max_age_seconds")
    old["policy"]["version"] = "research-v2"
    for field in (
        "requested_end_at",
        "actual_end_observed_at",
        "endpoint_age_seconds",
        "provenance",
        "verification",
    ):
        old.pop(field)
    old_id = digest({"advisory_id": a.advisory_id, "policy": old["policy"], "status": "LABELED"})
    await research_store.put("external_advisory_outcomes", old_id, a.advisory_id, old, now=T)
    old_eval = {
        "advisory": a.model_dump(mode="json"),
        "snapshot": snapshot(a, bundle(), service.policy),
        "outcome": old,
        "outcome_id": old_id,
        "snapshot_id": "legacy",
    }
    await research_store.put(
        "external_advisory_evaluations", digest(old_eval), a.advisory_id, old_eval, now=T
    )
    assert (await service.report())["pending"] == 1
    new = await service.evaluate(a.advisory_id, endpoint_bundle(0.1), as_of=T + timedelta(hours=1))
    assert new["outcome"]["policy"]["version"] == "research-v2.1"
    assert new["outcome"]["policy"] != old["policy"]
    assert await research_store.get("external_advisory_outcomes", old_id) == old
    assert len(await research_store.records("external_advisory_outcomes")) == 2
    assert (await service.report())["pending"] == 0


async def test_self_trusted_source_never_rewrites_first_import_classification(research_store):
    service = AdvisoryResearch(research_store)
    a, _ = await service.ingest(raw(), now=T + timedelta(hours=2))
    duplicate, added = await service.ingest(raw(), now=T + timedelta(hours=3))
    assert not added and duplicate == a
    assert (await service.report())["provenance_counts"]["UNVERIFIED_HISTORICAL"] == 1


async def test_provider_success_before_db_failure_restores_provider_only(
    research_store, monkeypatch
):
    service = AdvisoryResearch(research_store)
    with pytest.raises(ResearchError):
        await service.ingest(b"bad JSON", now=T)
    assert metric(service, "provider_health", {"provider": "manual_okx_ai"}) == 0
    monkeypatch.setattr(
        research_store, "put", AsyncMock(side_effect=RuntimeError("database offline"))
    )
    with pytest.raises(ResearchError):
        await service.ingest(raw(), now=T)
    assert metric(service, "provider_health", {"provider": "manual_okx_ai"}) == 1
    assert metric(service, "storage_health") == metric(service, "service_health") == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("evidence_reference", " "),
        ("reviewed_by", " "),
        ("evidence_reference", "https://private.test/token"),
    ],
)
def test_verification_references_are_bounded_non_secret_labels(field, value):
    with pytest.raises(ValueError):
        HistoricalVerification(
            advisory_id="a" * 64,
            evidence_reference=value if field == "evidence_reference" else "archive 01",
            reviewed_by=value if field == "reviewed_by" else "reviewer",
            verified_at=T,
        )


async def test_review_change_cannot_bypass_one_complete_outcome_per_policy(research_store):
    service = AdvisoryResearch(research_store)
    a, _ = await service.ingest(raw(), now=T + timedelta(hours=2))
    v = proof(a)
    first = await service.evaluate(
        a.advisory_id, bundle(), as_of=T + timedelta(hours=2), verification=v
    )
    changed = v.model_copy(update={"evidence_reference": "different archive"})
    with pytest.raises(ResearchError):
        await service.evaluate(
            a.advisory_id,
            bundle(prices=[100, 110, 120, 130, 140]),
            as_of=T + timedelta(hours=3),
            verification=changed,
        )
    assert await research_store.records("external_advisory_outcomes") == [first["outcome"]]
    assert len(await research_store.records("external_advisory_evaluations")) == 1


async def test_verified_history_does_not_require_self_trusted_payload(research_store):
    service = AdvisoryResearch(research_store)
    a, _ = await service.ingest(raw(source_timestamp_trusted=False), now=T + timedelta(hours=2))
    r = await service.evaluate(
        a.advisory_id, bundle(), as_of=T + timedelta(hours=2), verification=proof(a)
    )
    assert r["outcome"]["status"] == "LABELED" and r["provenance"] == "VERIFIED_HISTORICAL"
    assert (await service.report())["primary_observations"] == 1
