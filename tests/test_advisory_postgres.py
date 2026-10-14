"""Real PostgreSQL 16, in a unique throwaway research DB; never the ledger DB."""

import asyncio
import os
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from app.advisory.service import AdvisoryResearch
from app.advisory.storage import ResearchStore
from tests.test_advisory import T, bundle, raw


@pytest.fixture
async def pg_research_store():
    url = os.environ.get("TEST_POSTGRES_URL")
    assert url, "real PostgreSQL research tests must not skip"
    parsed = make_url(url)
    assert parsed.drivername == "postgresql+asyncpg"
    assert parsed.database == "quant_live_acceptance_test"
    assert parsed.username == "quant_test" and parsed.host in {"127.0.0.1", "localhost"}
    db_name = "quant_advisory_" + uuid4().hex + "_test"
    admin = create_async_engine(parsed, isolation_level="AUTOCOMMIT", hide_parameters=True)
    async with admin.connect() as conn:
        assert 160000 <= int(await conn.scalar(text("SHOW server_version_num"))) < 170000
        await conn.execute(text(f"CREATE DATABASE {db_name}"))
    store = ResearchStore(parsed.set(database=db_name).render_as_string(hide_password=False))
    try:
        await store.initialize()
        yield store
    finally:
        await store.close()
        async with admin.connect() as conn:
            # Exact UUID-created test DB only; never a production or shared ledger cleanup.
            await conn.execute(text(f"DROP DATABASE {db_name}"))
        await admin.dispose()


async def test_pg16_advisory_concurrent_import_idempotent(pg_research_store):
    a, b = AdvisoryResearch(pg_research_store), AdvisoryResearch(pg_research_store)
    results = await asyncio.gather(a.ingest(raw(), now=T), b.ingest(raw(), now=T))
    assert sum(added for _, added in results) == 1
    assert len(await pg_research_store.records("external_advisories")) == 1


async def test_pg16_immutable_research_and_restart(pg_research_store):
    service = AdvisoryResearch(pg_research_store)
    a, _ = await service.ingest(raw(), now=T)
    await service.evaluate(a.advisory_id, bundle(), as_of=T)
    for table in (
        "external_advisories",
        "external_advisory_snapshots",
        "external_advisory_outcomes",
        "external_advisory_evaluations",
    ):
        for operation in (f"UPDATE {table} SET payload = payload", f"DELETE FROM {table}"):
            with pytest.raises(Exception, match="append only"):
                async with pg_research_store.engine.begin() as conn:
                    await conn.execute(text(operation))
    await pg_research_store.initialize()
    restarted = ResearchStore(pg_research_store.engine.url.render_as_string(hide_password=False))
    try:
        assert len(await restarted.records("external_advisories")) == 1
    finally:
        await restarted.close()


async def test_pg16_revisions_and_outcome_idempotency(pg_research_store):
    from datetime import timedelta

    service = AdvisoryResearch(pg_research_store)
    a, _ = await service.ingest(raw(), now=T)
    for _ in range(2):
        await service.evaluate(a.advisory_id, bundle(), as_of=T + timedelta(hours=1))
    assert len(await pg_research_store.records("external_advisory_outcomes")) == 1
    await service.ingest(raw(stance="BEARISH", supersedes_id=a.advisory_id), now=T)
    assert len(await pg_research_store.records("external_advisories")) == 2


async def test_pg16_non_research_database_rejected_without_mutation(pg_research_store):
    async with pg_research_store.engine.begin() as conn:
        await conn.execute(text("CREATE TABLE orders (id INTEGER)"))
    with pytest.raises(ValueError, match="dedicated research"):
        await pg_research_store.initialize()
    async with pg_research_store.engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM orders")) == 0


async def test_pg16_concurrent_conflicting_complete_outcomes(pg_research_store):
    from datetime import timedelta

    from app.advisory.service import ResearchError

    a, _ = await AdvisoryResearch(pg_research_store).ingest(raw(), now=T)
    first, second = AdvisoryResearch(pg_research_store), AdvisoryResearch(pg_research_store)
    results = await asyncio.gather(
        first.evaluate(a.advisory_id, bundle(), as_of=T + timedelta(hours=1)),
        second.evaluate(
            a.advisory_id, bundle(prices=[100, 110, 120, 130, 140]), as_of=T + timedelta(hours=1)
        ),
        return_exceptions=True,
    )
    assert sum(isinstance(r, ResearchError) for r in results) == 1
    assert len(await pg_research_store.records("external_advisory_outcomes")) == 1
    assert len(await pg_research_store.records("external_advisory_evaluations")) == 1


async def test_pg16_audit_endpoint_re_evaluation_append_only(pg_research_store):
    from datetime import timedelta

    from app.advisory.models import ResearchPolicy
    from tests.test_advisory_audit_fix import endpoint_bundle

    strict = AdvisoryResearch(pg_research_store, ResearchPolicy(endpoint_max_age_seconds=0))
    a, _ = await strict.ingest(raw(), now=T)
    pending = await strict.evaluate(
        a.advisory_id, endpoint_bundle(0.1), as_of=T + timedelta(hours=1)
    )
    assert pending["outcome"]["status"] == "PENDING"
    relaxed = AdvisoryResearch(pg_research_store)
    labeled = await relaxed.evaluate(
        a.advisory_id, endpoint_bundle(0.1), as_of=T + timedelta(hours=1)
    )
    assert (
        labeled["outcome"]["status"] == "LABELED"
        and labeled["outcome"]["endpoint_age_seconds"] == 0.1
    )
    rows = await pg_research_store.records("external_advisory_outcomes")
    assert len(rows) == 2 and pending["outcome"] in rows and labeled["outcome"] in rows


async def test_pg16_audit_provenance_promoted_by_separate_evidence_only(pg_research_store):
    from datetime import timedelta

    from tests.test_advisory_audit_fix import proof

    service = AdvisoryResearch(pg_research_store)
    a, _ = await service.ingest(raw(source_timestamp_trusted=True), now=T + timedelta(hours=2))
    pending = await service.evaluate(a.advisory_id, bundle(), as_of=T + timedelta(hours=2))
    assert pending["outcome"]["status"] == "PENDING"
    assert (await service.report())["primary_observations"] == 0
    verified = await service.evaluate(
        a.advisory_id, bundle(), as_of=T + timedelta(hours=2), verification=proof(a)
    )
    assert verified["provenance"] == "VERIFIED_HISTORICAL"
    report = await service.report()
    assert (
        report["primary_observations"] == 1
        and report["provenance_counts"]["VERIFIED_HISTORICAL"] == 1
    )
    assert len(await pg_research_store.records("external_advisory_outcomes")) == 2
    assert len(await pg_research_store.records("external_advisories")) == 1
