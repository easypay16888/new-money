# External advisory / shadow validation V2

**Research only. Zero trading authority. Independent audit required before merge.**

Baseline: `810c0b9b38cf1d778429f61bb480c40b1157ec1f`.
Branch: `feature/okx-ai-advisory-v2`. No deployment, server access, `.env` changes,
Demo database access, strategy changes, or trading readiness changes are involved.

## Provider and authority boundary

Reviewed official documentation on 2026-10-14:
[OKX Agent Trade Kit](https://www.okx.com/docs-v5/agent_en/) documents market,
account and trading tools; [official toolkit](https://github.com/okx/agent-trade-kit)
provides the current tool list. We found no documented AI market-analysis response
API suitable for this feature. This is a finding from the reviewed documentation,
not a claim that no such service can ever exist.

The initial provider is `manual_okx_ai`, implementing
`ExternalAdvisoryProvider.fetch_or_parse`. It validates structured JSON only. There
is no network call, scraping, LLM parser, browser cookie, MCP connection, or OKX key.
Future providers must be explicitly registered and schema versioned in the research
package; adding one does not require changes to any trading module.

```
Operator's structured analysis + exported historical evidence
                 ↓ explicit offline CLI / Python service
          Dedicated research database
                 ↓ deterministic evaluation
               JSON / CSV reports
```

Trading modules do not import the package. The package does not import Settings,
Store, Runtime, strategies, risk, execution, OKX clients or continuity. There is no
observer hook in the runtime: historical context must be supplied as an offline
evidence export. No HTTP API, background worker, queue, auto-start, `/status` change,
`/health` change, trading metrics registration, or readiness dependency is added.
A research job can fail or stop without reaching the trading process.

## Offline commands

Use a **separate, disposable/local research database**, never the Demo or production
ledger, and run in a separate process. The URL is read only from
`RESEARCH_DATABASE_URL`; `.env` is never loaded. The initialized database must
contain only the four research tables; any other table causes rejection before DDL.
The feature does not automatically export or open the live trading database.

```sh
export RESEARCH_DATABASE_URL=sqlite+aiosqlite:////tmp/advisory-research.db
uv run python -m app.advisory import --input analysis.json
uv run python -m app.advisory evaluate --advisory-id <returned-id> --input evidence.json
uv run python -m app.advisory report
uv run python -m app.advisory report --exclude-late
uv run python -m app.advisory export --format json > research-export.json
uv run python -m app.advisory export --format csv > research-export.csv
uv run python -m app.advisory metrics
```

PostgreSQL uses `postgresql+asyncpg` with a dedicated research DB and its own role.
Supply DB credentials privately through the environment, not command arguments or
source control. No database URL is printed in errors. The operator chooses input
paths on the command line; advisory content cannot choose paths or commands.

Optional research-only policy JSON precedes the subcommand:

```sh
uv run python -m app.advisory --policy research-policy.json report
```

No persistent/automatic worker is necessary in V2. Explicit jobs are bounded; an
unexpected task failure ends the research job. It cannot kill or HALT the trading
runtime. Host CPU/disk still require normal operational isolation if many offline
jobs are run concurrently; this release does not claim kernel-level resource quotas.

## Import / immutable schema

```json
{
  "provider": "manual_okx_ai",
  "provider_version": "structured-v2",
  "symbol": "BTC-USDT-SWAP",
  "generated_at": "2026-10-14T00:00:00Z",
  "horizon": "24H",
  "stance": "BULLISH",
  "confidence": null,
  "summary": "Operator-transcribed analysis, stored as inert text",
  "reasoning_points": [],
  "support_levels": [60000],
  "resistance_levels": [70000],
  "expected_range_low": null,
  "expected_range_high": null,
  "source_reference": "screenshot 20261014 001",
  "source_timestamp_trusted": true
}
```

Canonical immutable model also carries `advisory_id`, `ingested_at`,
`raw_source_digest`, `late_import`, `schema_version=2`, optional
`invalidation_level`, `market_structure`, `momentum_view`, `volatility_view`,
`liquidity_view`, and `supersedes_id`. Lists become tuples. Nested historical evidence
is retained as bounded immutable JSON strings, rather than mutable model dictionaries.

Only BTC-USDT-SWAP and ETH-USDT-SWAP are accepted for research, matching the existing
universe; no trading symbol config is altered. Stances are STRONG_BULLISH, BULLISH,
NEUTRAL, BEARISH, STRONG_BEARISH. Horizons are 1H, 4H, 12H, 24H, 72H.
**One immutable record per horizon**, split a multi-horizon source into separate
imports. No confidence is inferred from prose or from STRONG stance.

SHA-256 identity includes normalized provider/version, symbol, generated time,
horizon and content. Formatting/key-order changes do not duplicate an observation;
time, horizon, content or revision changes create distinct records. The first
`ingested_at`, original source digest and late-import flag remain unchanged on retry.
`raw_source_digest` hashes parsed original JSON (not byte formatting). No raw text is
logged. Revisions must reference an existing same-provider/symbol/horizon predecessor
at an equal or earlier generated time. Revisions never replace the prior view.

`source_timestamp_trusted` is an **operator attestation**, not a cryptographic proof.
Never set it for unverifiable screenshots, revised pages or invented timestamps.
Late imports without it stay PENDING, excluded from performance scoring. Timely
imports still require the operator to preserve accurate original source time.

## Storage / audit / failure isolation

Four independently defined research tables:

- `external_advisories`: validated analysis and ingestion audit.
- `external_advisory_snapshots`: immutable historical evidence and comparison.
- `external_advisory_outcomes`: PENDING and complete labeled outcome versions.
- `external_advisory_evaluations`: links to snapshot/outcome IDs and their content.

Each row has a unique deterministic record ID, advisory ID, creation time and JSON
payload. Inserts use SQLite/PostgreSQL atomic `ON CONFLICT DO NOTHING`, then verify
identical content. Concurrent identical imports produce one row. Conflicting
same-ID content raises a research error. Complete outcome IDs are scoped to advisory
and research policy; their unique DB key also fences concurrently conflicting labels. Database triggers reject UPDATE/DELETE on
all four tables. No SQL update/delete, retention job, compaction or VACUUM API exists.
Administrative DB owners can of course alter schema; they are outside the application
append-only guarantee.

An interrupted evaluation may have inserted a snapshot but no outcome/evaluation.
A repeat converges through content IDs; reports read completed evaluation links,
never partially persisted results. Conflicting complete outcomes for the same
advisory and policy are rejected, preserved for investigation without overwriting.
An incomplete subsequent evidence bundle cannot downgrade a completed label.

Provider calls have a bounded research-only timeout (default 5s, maximum 30s).
Errors log only operation stage and exception type. No payload, URL, traceback,
credentials or governor reference is logged. Failures set research health/metrics
only. Exceptions do not cross into trading because there is no call path between
the two subsystems. No retries, trading writes or governor transitions are attempted.

## Historical evidence and time alignment

`evaluate` accepts a separately exported `EvidenceBundle`:

```json
{
  "internal": [
    {
      "symbol": "BTC-USDT-SWAP",
      "kind": "regime",
      "observed_at": "2026-10-14T00:00:00Z",
      "available_at": "2026-10-14T00:00:00Z",
      "content_json": "{\"regime\":\"TREND_UP\"}",
      "source_reference": "durable regime event 123"
    }
  ],
  "prices": [
    {
      "symbol": "BTC-USDT-SWAP",
      "observed_at": "2026-10-14T00:00:00Z",
      "available_at": "2026-10-14T00:00:00Z",
      "price": 65000
    }
  ]
}
```

For every item distinguish event time from **first provable availability time**.
Never use candle opening time as the time its completed OHLC/features were known.
Export already-persisted features/regime/signals; do not recalculate them using
current strategies. If durable records cannot establish their availability at T,
do not invent it: omit the context and retain INTERNAL_CONTEXT_UNAVAILABLE.
The tool does not verify an operator's external provenance assertions automatically.

Internal kinds: features, regime, signals, derivatives, book, position. Export actual
feature values such as ATR, trend, momentum, realized volatility, spread/imbalance,
funding/OI/change/premium, and existing signals only if durably generated at that time.
Latest eligible evidence per kind requires **observed_at <= T AND available_at <= T**
and age <=30min by default. Embedded observation/feature timestamps also cannot exceed
the record time. Ambiguous same-time evidence fails rather than guessing. Missing
features/regime/reference price means INTERNAL_CONTEXT_UNAVAILABLE; no strategy rerun.

Snapshot reference price must also have been available by T. An advisory timestamped
in the future cannot be evaluated against an earlier decision. Outcomes use prices
strictly after T through T+H, and only those available by the explicit analysis time.
Default CLI analysis time is current UTC. Late ingestion does not move prediction time.
T-1/T/T+1 and availability-time tests cover the boundaries.

## Deterministic evaluation definitions

All research policy is independent of trading Settings:

- `direction_threshold_pct=0.1`: an explicit ±0.1% research neutral band, configurable
  before analysis; a measurement convention, not a tuned signal or trading cost model.
- `level_tolerance_bps=1`: documented ±1bp research touch/break tolerance; configurable.
- future timestamp tolerance60s; late import tolerance300s.
- maximum internal age1800s; maximum observed-price gap900s.
- minimum sample30 (allowed minimum10); deterministic bootstrap500 resamples.

Policies are saved with outcomes; reports never pool different policies. Do not search
for a best threshold or optimize these against outcomes in V2.

Internal direction mapping:

| Internal regime | Direction |
| --- | --- |
| TREND_UP | bullish |
| TREND_DOWN | bearish |
| RANGE | neutral |
| HIGH_VOL / LOW_VOL / PANIC / UNKNOWN / unavailable | unclassified |

STRONG stances have the same directional interpretation as their ordinary variants,
without invented confidence. Nine agreement classes distinguish directional agreement,
direct conflict, both neutral, four directional/neutral mixes, and UNCLASSIFIED.

Outcome is PENDING until T+H is reached, the endpoint has an exact timestamped price,
start price was available by T, and the sampled window has no gap >900s. Do not
interpolate missing endpoint data or use a T+H+1 price. A timestamp conflict rejects
the evidence. This conservative rule may leave irregular observations PENDING.

Returns/range/MFE/MAE are percentage points, **not fractions**:

```
return_pct = 100 × (end / start - 1)
upside = 100 × (high / start - 1)
downside = 100 × (1 - low / start)
realized_range_pct = 100 × (high - low) / start
bullish MFE/MAE = upside / downside
bearish MFE/MAE = downside / upside
neutral MFE/MAE = null (no directional assumption)
```

Extrema include the reference price; raw high/low are retained. They are extrema of
**observed price samples**, not guaranteed true intraperiod extrema. Price gaps,
endpoint requirement and sample count are exposed. Direction correctness:
bullish return >band; bearish return <-band; neutral absolute return <=band.

Range reports use after-T samples: containment, fraction observations inside,
upper/lower break, first direction/time, maximum overshoot in price units, close
inside/outside. Fraction of samples is not a claim of elapsed time inside the range.
Support/resistance evaluation records discrete touch (within tolerance or crossed),
broken side, later reclaim, seconds to first touch and maximum price penetration.
An observed jump across a level implies crossing, not a precisely observed execution.
Invalidation levels are retained metadata; no order/stop behavior is derived from them.

## Reports, calibration and exports

Provider summary includes n, symbols, horizons and generated time range. Unevaluated
advisories remain visible as PENDING. Outputs include stance×horizon performance,
agreement matrix, agreement/conflict/mixed cohorts, internal regime, symbol, horizon,
UTC hour/weekday, volatility and funding buckets. Volatility uses exported
`realized_vol`: LOW<0.005, MEDIUM<0.02, HIGH otherwise; funding buckets use sign.
Missing values have UNKNOWN buckets.

Baseline groups compare internal, external, agreement and conflict. Internal direction
accuracy and MFE/MAE are recalculated from the recorded internal regime and raw outcome
extrema, not the external stance. It is an **advisory-aligned internal baseline**,
not all strategy trades or an exhaustive counterfactual trading backtest.

Confidence calibration uses numeric source confidence only, in fixed10% buckets
(1.0 in the last bucket); absent confidence is excluded. Reported returns include
n/minimum n, mean, median, SD, SE and deterministic bootstrap95% CI when n is adequate.
Small samples explicitly say INSUFFICIENT_SAMPLE; their descriptive values remain
visible but are not meaningful proof. Overlapping windows, horizons and revisions
are correlated; IID bootstrap is descriptive, **not statistical significance** or
proof of incremental predictive value. No parameter optimization, autoweighting or
advisory-to-trading recommendation exists.

Reports show revision stance/confidence/range changes and elapsed time. JSON export
preserves all normalized records and their immutable evidence. CSV is a table label
plus an escaped JSON record cell for independent reconstruction, not a lossy summary.
Formula-leading text is neutralized; no spreadsheet dependency is added.

## Bounds, metrics and security

Import <=256KiB; evidence export <=8MiB; JSON nesting<=32. Strict known provider,
symbols, horizons, timezone-aware dates2020–2100, finite numeric values and valid ranges.
Summary<=8192chars;32 reasoning points<=2048chars each;32 levels per type.
Unknown fields, duplicate JSON keys, NaN/Infinity, excessive futures, unsafe controls,
secret-like/path/URL source references are rejected. Nested structured evidence also
rejects recognized credential, UID/account digest and continuity-ID fields before
persistence or export. Free-form prose must still be checked by the operator. Source prose may contain HTML,
SQL, hostile prompts or commands: it stays inert JSON text, is never executed,
rendered as HTML, interpreted as instructions, or passed to a model or trading API.
Operators must not paste credentials into prose; no system can identify every
arbitrary secret string automatically.

Evidence bounded to5000 context items/20000 price samples. Reads/exports fail explicitly
above10000records per table by default rather than silently truncate. There is no
aggressive deletion; partition/archive research datasets manually under a separate
policy. At2 symbols×5 horizons daily, about3650analyses/year, up to4records per completed
analysis plus pending/revision versions. Payload size drives disk requirements.

Metrics use an **independent CollectorRegistry**, never imported by trading monitoring:

- `quant_external_advisory_ingested_total{provider,symbol,horizon}`
- `quant_external_advisory_rejected_total{reason}` (fixed operation stages)
- `quant_external_advisory_pending_outcomes`
- `quant_external_advisory_labeled_total{horizon}`
- `quant_external_advisory_provider_health{provider}`

Counters are per research service process and reset on CLI invocation; pending is
reconstructed during report. The metrics CLI exports the standalone registry; it
does not extend the production Prometheus HTTP endpoint. No arbitrary text/time/ID
labels, token, UID, private account data or continuity IDs are accepted as labels.

## Validation and audit boundary

Dedicated tests exercise schema/faults, atomic inserts and immutable triggers on
SQLite and **real PostgreSQL16** in a unique generated test DB. TEST_POSTGRES_URL is
required; tests fail rather than skip if missing. Research tests never clean shared
ledger tables. Existing safety suites retain their own isolated CI ledger database.

The non-interference test uses real DecisionPipeline (including higher/entry timeframe confirmation)
→classify→TrendStrategy→meta→RiskEngine→
ExecutionEngine.from_risk→submit against a mock exchange. It freezes clocks/UUIDs
only to compare every resulting field, including actual outgoing order payload.
No-advisory, bullish, bearish, corrupt, unavailable provider, failed research DB,
hostile prompt and absent research-service cases give identical regime, signal,
TradeIntent, RiskDecision, contract size, stop/entry, ExecutionRequest, HTTP write
and Governor NORMAL. It does not prove real OKX field execution or hardware
resource isolation; those require separate acceptance. Structural import tests
also enforce that no trading module imports advisory and no advisory module imports
any trading authority. All frozen core files must remain byte-identical to baseline.

Known pre-existing P2s remain untouched: `/status` SQLite writer reservation,
market batch memory-before-durable-commit, SQLite retention/compaction.

Q1 order creation authority: **NO**.
Q2 governor/HALT/resume authority: **NO**.
Q3 provider outage blocks normal trading: **NO**.
Q4 BULLISH→BEARISH changes trading outputs: **NO**.
Q5 undocumented/private OKX AI endpoints: **NO**.

This branch is code for independent audit, not deployment or LIVE readiness.

## Local verification at feature completion

- 137 new research tests passed, including5 real PostgreSQL16 tests and the CLI.
- Requested targeted safety/regression set:659 passed before the final16 added checks.
- Final full suite:1225 collected /1225 passed /0 failed /0 skipped /0 warnings
  with `uv run pytest -W error` (local PostgreSQL16.14).
- `uv run ruff check app tests`:PASS.
- `uv run mypy app`:PASS,48 source files.
- Baseline1088 regressions retained; no tests removed.
- Frozen runtime/risk/decision/execution/OKX/continuity/strategy/features/config/storage/
  Emergency/lease files, Docker/compose and CI workflow are unchanged.
- The feature branch must also pass its exact-SHA GitHub Actions run; do not substitute
  baseline/main CI. Audit delivery report links that run separately.
