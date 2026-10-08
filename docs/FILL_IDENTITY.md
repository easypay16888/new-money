# Instrument-scoped OKX fill identity

## Invariant

OKX `tradeId` is unique within `instId`, not globally. Canonical identity is exactly
`FillKey = tuple[str, str]`, `(instId, tradeId)`. Order IDs and client order IDs remain
ownership evidence; they are not part of the fill key.

`app/fill_identity.py` provides one shared validating `fill_key()` and immutable
fill comparison. Identifiers must be non-empty strings without surrounding whitespace;
invalid input fails closed without exposing identifiers in exceptions.

## Ingestion, restore and repair

- OrderManager rebuilds `seen_trade_ids` from persisted `(symbol, reference_id)`.
- WS ingestion checks the instrument-scoped durable record before any order event is
  appended. Repeated identical evidence is idempotent. Same-key conflicting size,
  price, order ID or other immutable evidence raises `ledger fill conflict`; runtime
  retains its existing fail-closed HALT/EMERGENCY handling.
- BTC/123 and ETH/123 are two fills. `len(seen_trade_ids)` continues to count fills.
- LedgerRepairService's remote and existing maps, conflict checks and missing-fill
  checks all use FillKey. Transactional recovery checks the same instrument before
  testing tradeId existence. ETH/555 does not prevent a proven BTC/555 protective exit.
- Ownership proof, bounded history fetch, append-only recovery, manual resume hold,
  full reconciliation and EMERGENCY rules are unchanged.
- Restore refuses a fills ledger above the existing 100,000-record bound instead of
  silently rebuilding incomplete dedup state. No historical rows are deleted.

## Transactional migration

Store.initialize migrates both SQLite and PostgreSQL automatically, before runtime restore.
Production upgrades still require the documented safe shutdown and backup process.

1. SQLite acquires `BEGIN IMMEDIATE`. PostgreSQL serializes initialization with a
   transaction advisory lock and locks the fills table against concurrent writes.
2. Validate same `(symbol, reference_id)` duplicates and row/payload identities.
   Validation uses bounded 5,000-row keyset batches covering every row. Malformed
   identities or duplicate rows fail initialization; identical duplicate rows also
   require manual investigation, since migration cannot silently remove them.
3. Create and verify `fills_instrument_reference_unique`:

   ```sql
   CREATE UNIQUE INDEX fills_instrument_reference_unique
   ON fills (symbol, reference_id)
   WHERE reference_id IS NOT NULL;
   ```

4. Drop the legacy `fills_reference_unique` global index only after the composite
   invariant is validated. All DDL is in the same transaction, so failed migration
   preserves the old schema and historical records.
5. Repeated or concurrent initialization converges to one composite index. It does
   not mutate fill payloads, IDs or timestamps. Valid cross-instrument duplicate
   tradeIds survive migration. A wrong pre-existing composite index definition is
   rejected rather than trusted by name.

No raw database URL, account identity or credentials are included in migration errors.
PostgreSQL tests exclusively use `quant_live_acceptance_test` / `quant_test` on localhost;
intentional test-data corruption cleanup cannot target a production database.

## 10/6 incident

Global tradeId dedup was a valid possible loss path: a previously seen ETH tradeId
could suppress a legitimate BTC fill; the global database index could reject it;
repair could misclassify a cross-symbol match as a conflict.

**The 10/6 incident root cause remains unproven.** Regression fixtures demonstrate
the defect and its fix, but no actual historical evidence confirms a collision in
that incident. Missing fill rows alone do not prove why its protective order event
was absent. Do not relabel the incident as a proven collision.

## Verification

This patch adds 41 cases, including both database constraints, legacy migration,
rollback, restart/concurrent initialization, validation beyond the first batch,
WS cross-symbol ingestion/restore/partial fills, conflict fail-closed handling and
cross-symbol protective repair/idempotency. Existing tests are retained; the Demo
ledger gate fixture now uses a valid instrument-scoped fill row.

After both independent patches: **776 passed / 0 skipped / 0 warnings** locally
with `uv run pytest -q -W error`, including real PostgreSQL integration. Ruff PASS;
mypy PASS (35 source files). GitHub Actions is checked separately against each commit.
Strategies, risk limits, instruments, CAA ownership, LIVE lease and LIVE defaults are
unchanged. Passing code checks does not substitute for controlled OKX Demo field drills.
