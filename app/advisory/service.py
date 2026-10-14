from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable
from datetime import datetime
from typing import Any, TypeVar

from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest

from app.advisory.evaluation import advisory_provenance, outcome, snapshot
from app.advisory.models import (
    MAX_EVIDENCE_BYTES,
    MAX_INPUT_BYTES,
    EvidenceBundle,
    ExternalAdvisory,
    HistoricalVerification,
    Provenance,
    ResearchPolicy,
    aware,
    bounded_json,
    digest,
)
from app.advisory.providers import ExternalAdvisoryProvider, ManualOkxAIProvider
from app.advisory.reports import build_report
from app.advisory.storage import ResearchStore

logger = logging.getLogger(__name__)
T = TypeVar("T")


class ResearchError(RuntimeError):
    pass


class AdvisoryResearch:
    """Explicit offline calls only. No trading object references, queues, or background tasks."""

    def __init__(
        self,
        store: ResearchStore,
        policy: ResearchPolicy | None = None,
        provider: ExternalAdvisoryProvider | None = None,
        *,
        provider_timeout: float = 5,
    ) -> None:
        if not 0 < provider_timeout <= 30:
            raise ValueError("invalid provider timeout")
        self.store, self.policy = store, policy or ResearchPolicy()
        self.provider = provider or ManualOkxAIProvider()
        self.provider_timeout = provider_timeout
        self.healthy = True
        self.registry = CollectorRegistry()
        self.ingested = Counter(
            "quant_external_advisory_ingested_total",
            "Unique imports",
            ["provider", "symbol", "horizon"],
            registry=self.registry,
        )
        self.rejected = Counter(
            "quant_external_advisory_rejected_total",
            "Research errors",
            ["reason"],
            registry=self.registry,
        )
        self.pending = Gauge(
            "quant_external_advisory_pending_outcomes",
            "Latest pending outcomes",
            registry=self.registry,
        )
        self.labeled = Counter(
            "quant_external_advisory_labeled_total",
            "Unique outcome versions",
            ["horizon"],
            registry=self.registry,
        )
        self.provider_health = Gauge(
            "quant_external_advisory_provider_health",
            "Research provider health",
            ["provider"],
            registry=self.registry,
        )
        self.provider_health.labels("manual_okx_ai").set(1)
        self.storage_health = Gauge(
            "quant_external_advisory_storage_health",
            "Research storage health",
            registry=self.registry,
        )
        self.service_health = Gauge(
            "quant_external_advisory_service_health",
            "Research service health",
            registry=self.registry,
        )
        self.storage_health.set(int(store.healthy))
        self.service_health.set(1)

    def failure(self, stage: str, exc: Exception, *, component: str = "service") -> ResearchError:
        self.healthy = False
        self.rejected.labels(stage).inc()
        self.service_health.set(0)
        if component == "provider":
            self.provider_health.labels("manual_okx_ai").set(0)
        elif component == "storage":
            self.storage_health.set(0)
        logger.warning("advisory research failed stage=%s error_type=%s", stage, type(exc).__name__)
        return ResearchError("research operation failed: " + stage)

    async def _storage(self, operation: Awaitable[T]) -> T:
        try:
            result = await operation
        except ValueError as exc:
            # Bounds / immutable-content conflicts are service errors, not DB outages.
            raise self.failure("storage_validation", exc) from None
        except Exception as exc:
            raise self.failure("storage", exc, component="storage") from None
        self.storage_health.set(1)
        return result

    def _success(self) -> None:
        self.healthy = True
        self.service_health.set(1)

    async def ingest(self, raw: bytes, *, now: datetime) -> tuple[ExternalAdvisory, bool]:
        stage = "provider"
        try:
            if len(raw) > MAX_INPUT_BYTES:
                raise ValueError("input too large")
            advisory = await asyncio.wait_for(
                self.provider.fetch_or_parse(raw, now=now, policy=self.policy),
                timeout=self.provider_timeout,
            )
            # Validate even future providers; the protocol is not a validation bypass.
            advisory = ExternalAdvisory.model_validate(advisory.model_dump(mode="json"))
            from app.advisory.models import AdvisoryInput

            identity = AdvisoryInput.model_validate(
                {
                    k: v
                    for k, v in advisory.model_dump(mode="json").items()
                    if k not in {"advisory_id", "ingested_at", "raw_source_digest", "late_import"}
                }
            )
            if digest(identity.model_dump(mode="json")) != advisory.advisory_id:
                raise ValueError("invalid advisory identity")
            if (
                advisory.generated_at - aware(now)
            ).total_seconds() > self.policy.future_tolerance_seconds:
                raise ValueError("future advisory")
            if advisory.ingested_at != aware(now) or advisory.late_import != (
                (aware(now) - advisory.generated_at).total_seconds()
                > self.policy.late_import_seconds
            ):
                raise ValueError("invalid provider timestamps")
            self.provider_health.labels("manual_okx_ai").set(1)
            stage = "import"
            if advisory.supersedes_id:
                prior = await self._storage(
                    self.store.get("external_advisories", advisory.supersedes_id)
                )
                if (
                    prior is None
                    or any(
                        prior[k] != getattr(advisory, k) for k in ("symbol", "provider", "horizon")
                    )
                    or datetime.fromisoformat(prior["generated_at"]) > advisory.generated_at
                ):
                    raise ValueError("invalid advisory revision")
            added = await self._storage(
                self.store.put(
                    "external_advisories",
                    advisory.advisory_id,
                    advisory.advisory_id,
                    advisory.model_dump(mode="json"),
                    now=aware(now),
                )
            )
            original = await self._storage(
                self.store.get("external_advisories", advisory.advisory_id)
            )
            assert original is not None
            if added:
                self.ingested.labels(
                    advisory.provider, advisory.symbol, advisory.horizon.value
                ).inc()
            self._success()
            return ExternalAdvisory.model_validate(original), added
        except ResearchError:
            raise
        except Exception as exc:
            raise self.failure(
                stage, exc, component="provider" if stage == "provider" else "service"
            ) from None

    async def evaluate(
        self,
        advisory_id: str,
        bundle: EvidenceBundle,
        *,
        as_of: datetime,
        verification: HistoricalVerification | None = None,
    ) -> dict[str, Any]:
        try:
            raw = await self._storage(self.store.get("external_advisories", advisory_id))
            if raw is None:
                raise ValueError("unknown advisory")
            a = ExternalAdvisory.model_validate(raw)
            if aware(as_of) < a.generated_at:
                raise ValueError("future advisory cannot match an earlier decision")
            # Revalidate serialized evidence to reject model_construct / mutable external callers.
            bundle = EvidenceBundle.model_validate(
                bounded_json(bundle.model_dump_json(), MAX_EVIDENCE_BYTES)
            )
            s, o = (
                snapshot(a, bundle, self.policy),
                outcome(a, bundle, self.policy, as_of=as_of, verification=verification),
            )
            sid = digest(s)
            # One complete label per advisory/policy, atomically fenced by the DB PK.
            # Pending observations remain separate immutable content versions.
            oid = (
                digest(
                    {
                        "advisory_id": advisory_id,
                        "policy": o["policy"],
                        "status": "LABELED",
                    }
                )
                if o["status"] == "LABELED"
                else digest(o)
            )
            previous = await self._storage(self.store.get("external_advisory_outcomes", oid))
            if previous is not None and previous != o:
                raise ValueError("conflicting outcome evidence")
            await self._storage(
                self.store.put("external_advisory_snapshots", sid, advisory_id, s, now=as_of)
            )
            added = await self._storage(
                self.store.put("external_advisory_outcomes", oid, advisory_id, o, now=as_of)
            )
            result = {
                "advisory": raw,
                "snapshot": s,
                "outcome": o,
                "snapshot_id": sid,
                "outcome_id": oid,
                "provenance": o["provenance"],
            }
            await self._storage(
                self.store.put(
                    "external_advisory_evaluations", digest(result), advisory_id, result, now=as_of
                )
            )
            if added and o["status"] == "LABELED":
                self.labeled.labels(a.horizon.value).inc()
            self._success()
            return result
        except ResearchError:
            raise
        except Exception as exc:
            raise self.failure("evaluation", exc) from None

    async def report(self, *, include_late: bool = True) -> dict[str, Any]:
        try:
            selected: dict[str, dict[str, Any]] = {}
            for row in await self._storage(self.store.records("external_advisory_evaluations")):
                # Filter policy versions rather than pooling measurements with different thresholds.
                if row["outcome"]["policy"] == self.policy.model_dump(mode="json"):
                    identity = row["advisory"]["advisory_id"]
                    prior = selected.get(identity)
                    if (
                        prior is None
                        or (
                            row["outcome"]["status"] == "LABELED"
                            and (
                                prior["outcome"]["status"] != "LABELED"
                                or row["provenance"] == Provenance.VERIFIED_HISTORICAL.value
                                or prior["provenance"] != Provenance.VERIFIED_HISTORICAL.value
                            )
                        )
                        or prior["outcome"]["status"] != "LABELED"
                    ):
                        selected[identity] = row
            for a in await self._storage(self.store.records("external_advisories")):
                if a["advisory_id"] not in selected:
                    selected[a["advisory_id"]] = {
                        "advisory": a,
                        "provenance": advisory_provenance(
                            ExternalAdvisory.model_validate(a),
                            self.policy,
                            as_of=datetime.fromisoformat(a["ingested_at"]),
                        ).value,
                        "snapshot": {
                            "status": "INTERNAL_CONTEXT_UNAVAILABLE",
                            "regime": None,
                            "agreement": "UNCLASSIFIED",
                            "evidence": {},
                        },
                        "outcome": {"status": "PENDING", "pending_reason": "NOT_EVALUATED"},
                    }
            self.pending.set(sum(r["outcome"]["status"] == "PENDING" for r in selected.values()))
            result = build_report(list(selected.values()), self.policy, include_late=include_late)
            self._success()
            return result
        except ResearchError:
            raise
        except Exception as exc:
            raise self.failure("report", exc) from None

    def metrics(self) -> bytes:
        return generate_latest(self.registry)
