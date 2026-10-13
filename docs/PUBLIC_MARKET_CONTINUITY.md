# Public market continuity safety

This is a recovery safety gate. It does not change strategy formulas, risk limits, queue capacities, heartbeat/freshness budgets, database pragmas, public batch transaction design, private WS or candle cadence.

| Condition | Outcome |
|---|---|
| High-water wait succeeds and every frame is admitted | Continue normally; no hold, reset, HALT or critical notification |
| Public frame rejected by QueueFull/backpressure timeout | Immediate HALT and continuity hold |
| Observed pre-ACK public frames discarded on timeout/close | Same hold; original transport exception remains available for diagnostics |
| Public handler/worker failure, protocol rejection or unexpected run-task death | Continuity uncertain; same manual-only hold; WS retains its original machine failure code |
| Controlled shutdown abandons accepted, unfinished public frames | Record a hold before cancelling those tasks |
| Ordinary reconnect without rejected/unapplied-data evidence | Existing transient recovery policy remains available |

The explicit reason is **Public market continuity lost**, code **public_market_continuity_lost**. It classifies as `SAFETY_OR_MANUAL`. The existing transient whitelist is unchanged; the public fault is translated before it can use that whitelist. Auto-recovery eligibility also checks the independent continuity state.

## Immediate fencing and durable evidence

The synchronous fault callback fences entries and forbids automatic recovery before any awaited I/O. It increments the public epoch and invalidates all configured public symbols on the shared socket:

- latest trades, ticker and book;
- derivative observations/context;
- last persisted-book sampling timestamp.

Candles, owned orders, fills, positions, stop algos and durable market history remain intact. The existing durable `system_events` audit receives `public_market_continuity_hold` with a random incident identifier, fixed code/cause, socket, generation, UTC timestamp and cumulative loss count. No account UID, key or credentials are recorded. One audit writer and one latest pending state bound resources during DB outages; intermediate generations may be coalesced into the latest cumulative hold. No trading writes are retried by this mechanism.

Hold write failure keeps HALT and the logical hold active. A session guard (`public_market_session_open`) is committed before receiver tasks start. Clean shutdown appends a matching `public_market_session_close` only when any needed hold is durable. An unverified restart cannot be marked clean merely by calling stop/start: that guard remains open until explicit manual recovery, or until a durable hold independently protects restart. Consequently an unclean restart is manual-only even if the process crashed before its gap audit reached storage. This intentionally means an ordinary process crash also needs manual review; ordinary in-process WS reconnects do not.

## Epoch isolation and warm-up

The public queue carries the original receive time and connection generation as internal metadata. Old accepted frames can still be written as audit observations, but cannot refill volatile state or initiate cache publication after invalidation. A Redis request already in flight is not cancelled; trading readiness uses the verified in-memory state, never those cache entries. Receive/generation filtering remains in effect after manual release. In-flight batches check the market epoch as well. No warm-up is reconstructed from historical DB market rows.

After restart/gap, readiness requires **every configured symbol** to have:

- 100 distinct post-epoch trade IDs in the bounded 100-trade buffer, with source timestamps after the epoch and not in the future;
- a valid fresh post-epoch ticker and nonempty books5 snapshot;
- fresh post-epoch mark and index observations;
- healthy transport, live run/worker tasks, applied-feed freshness, no processing fault, and an idle business worker/empty queue so uncommitted input cannot satisfy readiness.

Mark/index are mandatory because they are already critical public feeds. Funding and OI are optional; their pre-gap observations are discarded, and absent context stays absent (`None` in features). No low-frequency funding window is required before manual resume. Derivative reconstruction still only uses observations available at its decision timestamp.

Warm-up does not change Governor state. Even 100 complete samples plus successful account reconciliation do not permit automatic recovery. Normal evaluation, execution entry permission, LIVE entry guard and pending evaluation epochs independently fence risk increases. An evaluation/request started before the gap cannot execute after a later manual recovery using its old epoch. Reduce-only safety actions and CAA ownership rules retain their existing permissions.

## Manual resume and crash safety

`/system/resume` remains protected by the existing control authorization and PAPER maintenance fence. The existing full reconciliation, portfolio, DB, Redis, CAA, private WS, pending-entry and emergency checks still apply. An incomplete public warm-up returns the existing fail-closed HTTP 409.

The manual path holds the public audit lock, checks the incident/epoch and durable evidence, and permits a provisional Governor resume while the **independent continuity entry gate remains closed**. Only a successful append-only `public_market_continuity_release` for the current hold clears it. Release failure/cancellation immediately re-HALTs; a crash before commit restores the hold. A new gap during the release cannot clear the new epoch/hold. An ambiguous committed release is verified through its durable matching receipt on a subsequent explicit manual attempt; no duplicate release is blindly appended. Session-guard-only recovery writes `public_market_session_manual_resume` before clearing its logical restart fence.

A completed release does not restore its old hold on restart. An unclosed session still requires fresh startup warm-up/manual review; it is a separate `restart_unverified` state. Audit corruption, mismatched releases or missing release evidence fail closed. No historical row is deleted or rewritten.

## Observability

`/status.public_market_continuity` reports hold active, restart unverified, current state (`NORMAL`, `WARMING_UP`, `READY_FOR_MANUAL_RESUME`), stable reason code, readiness, and per-symbol ticker/book/mark/index readiness plus trade count/requirement. Incident/session IDs are not exposed.

Added metrics:

- `quant_public_market_continuity_loss_total`;
- `quant_public_market_warmup_ready`;
- `quant_public_market_trade_warmup_samples{symbol}` (configured symbols only).

Existing receive, backpressure, batch-stage, queue-depth/wait and handler latency metrics retain their semantics. A critical notification is queued once per unreleased incident using existing dedup machinery. Successful backpressure produces no continuity warning. Notification failure cannot release a hold or control trading.

This safety patch needs independent audit. Automated checks do not replace later controlled Demo field acceptance and do not authorize LIVE.
