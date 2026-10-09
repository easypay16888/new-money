# Complete Terminal Fill Reconstruction

## Contract verified on 2026-10-09

Sources: [OKX fills-history](https://my.okx.com/docs-v5/en/#order-book-trading-trade-get-transaction-details-last-3-months), [OKX order details](https://my.okx.com/docs-v5/en/#order-book-trading-trade-get-order-details), [OKX fill/position reconciliation](https://www.okx.com/docs-v5/trick_en/#reconciliation-between-fill-and-positions).

| Field / behavior | Verified contract |
| --- | --- |
| Read endpoint | GET `/api/v5/trade/fills-history`, `instType` required; SWAP supported |
| Order filters | Both `instId` and `ordId` supported; use both |
| after / before | Records earlier / newer than the supplied **billId** |
| Cursor | billId, never tradeId or fillTime |
| limit | Default / maximum 100 |
| Retention | Last three months |
| tradeId identity | Unique within instId; canonical `(instId, tradeId)` |
| fillSz / fillPx | Individual executed quantity / price; SWAP quantity in contracts |
| fee / feeCcy | Individual fee/rebate and currency; negative deduction, positive rebate |
| fillPnl | Individual closing fill realised PnL; zero for opening fills |
| fillTime / ts | Actual execution time / record generation time, Unix UTC milliseconds |
| billId | Bill identifier, distinct from tradeId |
| Ordering | Cursor traversal requests older bills; the endpoint section does not promise chronological fillTime ordering. Do not depend on page array order; use smallest billId for the next older query, and sort completed evidence by fillTime for accounting. |

## Completeness and persistence

1. Existing exact owned terminal order verification remains required.
2. Query only that instrument/order; validate every returned fill.
3. Exhaust the cursor stream to an **empty page**. A short page or accumulated size alone is not completion proof.
4. Require unique canonical keys, valid progressing bill cursors, complete fee/PnL, and `Decimal(sum(fillSz)) == Decimal(accFillSz)`.
5. If REST provides a last trade, verify its exact identity, size, price and time. Positive cumulative fills with no last-fill fields still require complete history.
6. Network reads occur outside the ingestion lock. Under `ledger_lock`, revalidate ownership, order identity/size/state and immutable existing evidence. Missing or conflicting evidence aborts before append.
7. Reuse `Store.append_ledger_recovery()` solely as the existing durable transaction primitive. Append missing fills and terminal order event atomically. Preserve old records and the in-memory `reconciled_filled` baseline. Update memory only after commit.
8. Production runtime cancellation confirmation with nonzero terminal fills and ambiguous-placement terminal GET share the same evidence-only callback. No repeated placement, Governor control or change to cancel/protection/Emergency decisions. The ordinary zero-fill cancellation path remains unchanged.
9. Run the unchanged complete reconciliation; accounting success neither resumes trading nor overrides any safety gate.

Bounds: **100 rows/page, 51 GETs including exhaustion probe, 5,000 rows, 30 seconds total**. Full pages at the record cap still require the empty exhaustion probe. Any bound, malformed result, repeated identity/cursor, unavailable history or authentication failure fails closed. No write retry or remote-position-only repair. Zero-filled cancelled orders need no invented history/fill; partially cancelled orders compare to accumulated executed size.

## Evidence and reporting

All recovered fills retain official per-fill fee, feeCcy, fillPnl and execution time, with `fill_evidence_source=okx_fills_history`. Order aggregate fee is audit-only `order_fee` on the terminal order event. Daily review uses the existing fills table and actual trade-day timestamps; all partial fills are visible once. Existing conflicting cumulative-fee records remain untouched for manual audit.

Diagnostics use stable `terminal_fill_evidence_incomplete`, `terminal_fill_evidence_conflict`, `terminal_fill_size_mismatch`, `terminal_fill_identity_mismatch`, `terminal_fill_history_unavailable`. Do not log raw private responses, authentication material, URLs with credentials, or arbitrary remote exception bodies.

## Validation and release boundary

Regression coverage includes two fills 0.14+3.79, existing first fill, late WS duplicate, WS concurrent with REST, per-instrument identity, missing/excess cumulative size, ownership/last-fill mismatches, malformed evidence, >100 fills, pagination bounds/repetition, history deadline/cancellation, cancelled partial/zero fills, atomic rollback/restart, full daily fees/PnL, and incomplete accounting preventing reconciliation success. SQLite and real isolated PostgreSQL tests run; existing safety regressions remain required.

This patch is for independent audit only. **No server deployment, no Demo resume, no LIVE enablement.** Strategies, risk limits, CAA, Emergency policy, WebSocket architecture and LIVE fencing remain unchanged. Passing tests is not OKX field acceptance. Retention/availability/completeness failures require manual investigation; never silently rewrite old fees or infer missing fills from a position snapshot.

Local validation: **884 passed, 0 failed, 0 skipped, 0 warnings** with `-W error`; 77 additional cases over the 807-test parent baseline. Ruff passed; mypy passed for 36 source files. Real isolated PostgreSQL tests include terminal transaction commit/rollback and all existing identity/lease/binding regressions. GitHub CI is checked separately after push.
