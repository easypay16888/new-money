# Same-engine SPOT + SWAP Demo

The existing runtime can optionally manage USDT cash spot pairs alongside the
existing USDT perpetual swaps. One Governor, portfolio, entry action lock,
reconciliation loop, notification manager and account-wide Cancel-All-After
remain in use. There is no second trading process or independent risk budget.

## Configuration and rollout

Spot is disabled by default. The current running swap deployment is unchanged.
After controlled field acceptance, an operator can explicitly configure:

```dotenv
MODE=PAPER
LIVE_TRADING_ENABLED=false
SPOT_SYMBOLS=["BTC-USDT","ETH-USDT"]
SPOT_FEE_RATE=0.001
```

`SYMBOLS` continues to select swaps. `SPOT_SYMBOLS` adds cash spot pairs; it does
not replace swaps. Existing strategy rules, signal thresholds, leverage and risk
hard limits remain unchanged. Spot leverage is always one. Configuring spot in
LIVE is rejected, even when the existing LIVE account gates otherwise pass.

**Do not enable this on an account with unowned balances in the selected base
currencies.** Demo accounts may include preloaded coins. Startup compares the
complete cash balance to the fee-adjusted owned fill ledger; it never adopts,
spends, sells, or silently offsets pre-existing coins. A mismatch HALTs the
shared engine. Use an account whose selected base balances are attributable to
this ledger. The operator must arrange any external asset segregation separately;
the trading program does not transfer or automatically sell those balances.

The 2026-10-31 read-only deployment check found 1 BTC and 1 ETH outside the current
ledger. Spot was therefore **not enabled** in the operational `.env`.

## Execution and risk

- Spot entry is a limit **buy** with `tdMode=cash`; `sz` is base currency quantity.
  Attached SL/TP uses `last` prices. Market buys in base-currency units with
  attached protection are rejected locally because OKX does not support them.
- A short strategy signal cannot open a spot position. Existing SL/TP and
  Emergency cash sells close owned inventory; the strategy definitions are shared.
- Spot never sends derivative `posSide`, `reduceOnly`, leverage or borrowing
  parameters. There are no borrowing/transfer endpoints in this feature.
- Emergency market sells explicitly use `tgtCcy=base_ccy` and `banAmend=true`.
  The last REST gate requires a matching owned order and enough fresh available
  base balance. It never relies on `reduceOnly` for cash inventory.
- Before every spot buy, the REST gate checks its persisted approved decision,
  exact price/size, attached stop, current Governor/market/account state, current
  cash balance and owned inventory. Concurrent unsettled entries across both
  products are fenced until a complete reconciliation verifies their result.
- Both products share total open risk, position count, daily/weekly loss limits
  and equity. Sizing reserves existing swap margin **plus** cash spot notional
  against the existing conservative capital target. Cash cannot use swap leverage.
  Existing runtime 25% hard margin HALT remains based on derivative margin.
- Failed or ambiguous writes follow the existing same-clOrdId query path;
  orders/cancels are never blindly replayed. Cash protective/emergency sells use
  the independent safety REST pool, and CAA keeps its independent pool.

## Balances, fills and protection

Spot balances are reconstructed from the same complete account read used for
equity, rather than a separate derivative position endpoint. Negative balances,
base liabilities, malformed/missing valuation, missing fees and unknown ownership
fail closed. Cash inventory plus swaps contributes to shared exposure. USD balance
valuations are converted to USDT using that same snapshot's USDT eqUsd/eq ratio.

Every fill retains `(instId, tradeId)` identity. Base-currency commissions change
owned coin inventory; quote commissions do not. Fees are restored from immutable
effective fills, including after terminal/partial reconstruction. Order cumulative
fees are never treated as a single fill fee. Ledger repair remains append-only and
requires exact ownership evidence; success does not release a manual hold.

Cash protection requires an active owned cash sell with the exact net quantity,
valid last-price trigger and non-failed algo state (`failCode="0"` remains valid).
An attached gross-size stop that does not match received net coins is not accepted
as valid protection. Existing emergency/replacement processing remains fail closed.
Flat cash inventory with a remaining algo blocks further entries. Below-minimum
or below-lot residual inventory is never rounded up, discarded, or reported flat;
it keeps the relevant safety hold for operator handling.

Daily review reports raw commissions by currency; base fees are converted at the
authoritative fill price for the USDT fee total. Missing spot fillPnl remains
unknown, not a fabricated realized PnL. Backtests use the configurable spot fee
assumption charged in quote currency and apply no spot funding or short positions;
they are not a complete simulation of exchange fee denomination or partial fills.

Public cash feeds require tickers/books5/trades. Cash recovery warm-up requires
100 unique new trade IDs and fresh post-gap tick/book; derivative mark/index/OI/
funding are not invented for spot. Swap freshness requirements are unchanged.
`/status.trading_products`, order market type/base units and Bark titles distinguish
cash spot from swaps.

## Required field acceptance (not automatically passed)

Record timestamp, commit, initial/final coin balances, owned orders/fills, fees,
algo sizes/triggers, Governor state, reconciliation and Bark evidence for each:

1. Verify the selected base balances are fully explained by the ledger.
2. Start with both products subscribed; check preload, cash feeds and all workers.
3. Observe a natural cash limit buy, all partial fills and exact base-fee accounting.
4. Confirm the real attached stop's net quantity, or the verified replacement and
   expected emergency hold. Do not accept a gross-size stop on net inventory.
5. Observe SL/TP cash sell, no borrowing/reversal, exact terminal fills and fees.
6. Restart with a protected spot holding and a simultaneous swap holding.
7. Cancel a partial entry, including unconfirmed cancellation and fee evidence failure.
8. Exercise controlled Emergency sell, ambiguous result query and below-minimum dust.
9. Verify an unrelated unchanged swap position survives a cash exit reconciliation.
10. Verify orphan/foreign cash orders and unowned coins HALT without being adopted.
11. Verify safe shutdown, CAA and watchdog using the existing maintenance procedure.

Code tests and CI do not replace this field acceptance. Keep LIVE disabled.

Protocol references: [OKX API](https://www.okx.com/docs-v5/en/), specifically cash
`tdMode`, SPOT market `tgtCcy`/`banAmend`, attached TP/SL restrictions, account balance
and instrument lot/minimum size definitions.
