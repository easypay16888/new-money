# Append-only PAPER accounting corrections

This manual maintenance tool addresses one proven legacy defect: a terminal REST
order snapshot stored the whole order fee as the last partial fill fee and
omitted that fill's realized PnL. It does not resolve arbitrary ledger conflicts.

Original fills remain immutable. `fill_accounting_corrections` stores one receipt
per `(instId, tradeId)`, original row ID/SHA-256, complete authoritative fill set,
terminal order evidence and evidence SHA-256. Only effective fee/fillFee/fillPnl
change. Size, price, direction, symbol, order identity, fill time, order state,
fill count and audited positions never change.

## Proof and fail-closed rules

- An existing fully filled non-reduce-only order must exactly match the terminal
  order's symbol, clOrdId, ordId, side, requested and accumulated size.
- Reuse bounded `read_terminal_fills`: exhaust exact order history, validate each
  fill and ownership, cross-check last fill, prove sum(fillSz) == accFillSz.
- Every authoritative fill key must already exist locally, without extra keys.
  Missing fills belong to the existing reconstruction flow, not this tool.
- The legacy row must have no fillFee, cumulative fee equal to the whole order
  fee, accFillSz greater than its own fillSz, and no conflicting fillPnl.
  Unknown identity, quantity, price, time or accounting differences remain errors.
- PostgreSQL table locks / SQLite BEGIN IMMEDIATE serialize proof and append.
  Receipt, manual hold and audit commit atomically. Duplicate application adds zero.
- Altered originals/receipts, changed remote evidence and orphan receipts fail
  closed. Only allowlisted order/fill response fields enter receipts, no secrets.
- Store fill reads and daily review use validated effective evidence. Raw SQL
  and `ledger_snapshot` retain original rows for forensic and transaction checks.
  Ledger repair compares effective evidence but validates raw transaction snapshots.
- No automatic correction, trading API write, resume, strategy/risk change or
  LIVE support. Current position snapshots never constitute accounting evidence.

## Controlled maintenance

Use an exclusive operator window: nobody may start/resume another engine or
mutate the ledger while this tool runs. Preserve the verified consistent backup.
Call existing `POST /system/stop`, require HTTP 200/running=false, and pause
watchdog for planned maintenance. Keep the app container/API available, with its
trading runtime stopped and HALT/EMERGENCY. Never start a second engine.

Run inside that app container, with its unchanged PAPER credentials/database:

```bash
.venv/bin/python -m app.accounting_correction \
  --symbol ETH-USDT-SWAP --client-order-id OWNED_CLIENT_ORDER_ID
```

Default is dry run. Review complete evidence and proposed receipts, then apply:

```bash
.venv/bin/python -m app.accounting_correction \
  --symbol ETH-USDT-SWAP --client-order-id OWNED_CLIENT_ORDER_ID --apply
```

Both steps check local `/status` before history reads and immediately before the
transaction. Running=true, NORMAL, non-PAPER, LIVE enabled, endpoint failure or
unproven evidence rejects maintenance. No trust flag, SQL, alternate status URL
or caller-supplied authoritative fill payload is accepted by the CLI.

Restart the existing app with its manual EMERGENCY hold and restart watchdog.
Compare original ledger rows to backup, prove effective evidence matches OKX,
complete reconciliation, verify protection/WS/CAA/DB/Redis, zero emergency targets
and foreign orders. Only existing human-authorized `/system/resume` releases the
persisted manual hold; a receipt alone is not acceptance.

Old saved daily reports remain historical snapshots. Rebuilding reads corrected
per-fill accounting exactly once and never overwrites the old report. Direct SQL
consumers must explicitly join validated receipts instead of reading raw fees.

The 10/6 BTC missing-exit root cause remains unproven. This patch addresses the
separately proven cumulative-fee defect without claiming to explain BTC fill loss.
