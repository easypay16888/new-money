"""Explicit offline CLI; does not load .env, Settings, Runtime, or OKX clients."""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.advisory.models import (
    MAX_EVIDENCE_BYTES,
    MAX_INPUT_BYTES,
    EvidenceBundle,
    HistoricalVerification,
    ResearchPolicy,
    bounded_json,
)
from app.advisory.service import AdvisoryResearch
from app.advisory.storage import ResearchStore


def read_input(path: str, maximum: int) -> bytes:
    with Path(path).open("rb") as source:
        raw = source.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("research input too large")
    return raw


def csv_cell(value: object) -> str:
    cell = json.dumps(value, ensure_ascii=False, allow_nan=False)
    # JSON encoding adds quotes to strings; spreadsheet formula injection is neutralized explicitly.
    if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@")):
        cell = "'" + cell
    return cell


async def execute(args: argparse.Namespace) -> None:
    url = os.environ.get("RESEARCH_DATABASE_URL")
    if not url:
        raise ValueError("RESEARCH_DATABASE_URL required; use a dedicated research database")
    store = ResearchStore(url)
    try:
        await store.initialize()
        policy = (
            ResearchPolicy.model_validate(bounded_json(read_input(args.policy, MAX_INPUT_BYTES)))
            if args.policy
            else ResearchPolicy()
        )
        research = AdvisoryResearch(store, policy)
        now = datetime.now(UTC)
        output: dict[str, Any]
        if args.action == "import":
            advisory, added = await research.ingest(
                read_input(args.input, MAX_INPUT_BYTES), now=now
            )
            output = {"advisory_id": advisory.advisory_id, "added": added, "research_only": True}
        elif args.action == "evaluate":
            bundle = EvidenceBundle.model_validate(
                bounded_json(read_input(args.input, MAX_EVIDENCE_BYTES), MAX_EVIDENCE_BYTES)
            )
            verification = (
                HistoricalVerification.model_validate(
                    bounded_json(read_input(args.verification, MAX_INPUT_BYTES))
                )
                if args.verification
                else None
            )
            output = await research.evaluate(
                args.advisory_id, bundle, as_of=now, verification=verification
            )
        elif args.action == "report":
            output = await research.report(include_late=not args.exclude_late)
        elif args.action == "export":
            data = {
                table: await store.records(table)
                for table in (
                    "external_advisories",
                    "external_advisory_snapshots",
                    "external_advisory_outcomes",
                    "external_advisory_evaluations",
                )
            }
            if args.format == "csv":
                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(["table", "immutable_record_json"])
                for table, rows in data.items():
                    for row in rows:
                        writer.writerow([table, csv_cell(row)])
                sys.stdout.write(buffer.getvalue())
                return
            output = data
        else:
            await research.report()
            sys.stdout.write(research.metrics().decode())
            return
        print(json.dumps(output, ensure_ascii=False, allow_nan=False, sort_keys=True))
    finally:
        await store.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Offline advisory research. Zero trading authority."
    )
    parser.add_argument(
        "--policy", help="Optional bounded research policy JSON; never trading Settings"
    )
    sub = parser.add_subparsers(dest="action", required=True)
    for action in ("import", "evaluate"):
        command = sub.add_parser(action)
        command.add_argument("--input", required=True)
        if action == "evaluate":
            command.add_argument("--advisory-id", required=True)
            command.add_argument(
                "--verification", help="Separate operator-reviewed historical provenance JSON"
            )
    command = sub.add_parser("report")
    command.add_argument("--exclude-late", action="store_true")
    command = sub.add_parser("export")
    command.add_argument("--format", choices=("json", "csv"), default="json")
    sub.add_parser("metrics")
    args = parser.parse_args()
    try:
        asyncio.run(execute(args))
        return 0
    except Exception as exc:
        # Never echo payloads, paths, URLs, validation values, or exception details.
        print(
            json.dumps({"research_only": True, "error_type": type(exc).__name__}), file=sys.stderr
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
