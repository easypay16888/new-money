from __future__ import annotations

from datetime import datetime
from typing import Protocol

from app.advisory.models import (
    AdvisoryInput,
    ExternalAdvisory,
    ResearchPolicy,
    aware,
    bounded_json,
    digest,
)


class ExternalAdvisoryProvider(Protocol):
    async def fetch_or_parse(
        self, raw: bytes, *, now: datetime, policy: ResearchPolicy
    ) -> ExternalAdvisory: ...


class ManualOkxAIProvider:
    """No network, credentials, free-form parser, or trading interfaces."""

    async def fetch_or_parse(
        self, raw: bytes, *, now: datetime, policy: ResearchPolicy
    ) -> ExternalAdvisory:
        now = aware(now)
        data = AdvisoryInput.model_validate(bounded_json(raw))
        if (data.generated_at - now).total_seconds() > policy.future_tolerance_seconds:
            raise ValueError("future advisory timestamp")
        normalized = data.model_dump(mode="json")
        return ExternalAdvisory(
            **normalized,
            advisory_id=digest(normalized),
            ingested_at=now,
            raw_source_digest=digest(bounded_json(raw)),
            late_import=(now - data.generated_at).total_seconds() > policy.late_import_seconds,
        )
