# WS runtime recovery: October 14 Demo incident

This patch is isolated from the unmerged AI advisory branch. Strategy, symbols,
risk limits, CAA ownership, LIVE gates, fill ownership and ledger repair are
unchanged. Queue capacity, heartbeat/staleness budgets, the 250 ms public
backpressure allowance and the transient recovery whitelist are unchanged.

## Evidence and limits

The deployed `810c0b9` Demo logged public backpressure failures at
2026-10-14 08:08 and 10:04 UTC. Following OKX maintenance notices, public/private
sockets also logged `InvalidStatus` as protocol failures, without the HTTP status.
The actual historical HTTP status is therefore **unproven**. A low queue depth
in the later session-end log does not prove when the consumer caught up.

At 12:06 UTC, read-only authoritative history checks found 33 local and 33 OKX
fills, with matching instrument-scoped keys and effective payloads, no missing
fills or conflicts, matching flat positions, no pending orders/algos and no
emergency targets. Earlier ledger repair had recorded incomplete evidence;
this patch does not erase its manual hold or change ledger repair behavior.

Sampled Prometheus DB-stage latency does not prove a SQLite writer bottleneck.
The confirmed redundant work is daily-baseline reconciliation loading up to
10,000 ORM snapshot rows to use just the first one. Requesting `limit=1` preserves
the same baseline and leaves the rolling weekly peak's SQL aggregation intact.
Regression coverage uses more than 20,000 snapshots.

## HTTP upgrade failures

HTTP 500/502/503/504 upgrade rejection is a temporary transport failure, following
[websockets' documented retry classification](https://websockets.readthedocs.io/en/14.1/reference/asyncio/client.html).
It immediately HALTs entries under the existing transport policy, reconnects
with the existing bounded backoff, and does not claim an invalid received market
frame. Existing health/reconciliation/manual holds still gate recovery.

Other upgrade statuses, authentication failures and unknown protocol errors
remain fail closed. A public protocol failure retains the durable continuity
hold. No new Governor auto-recovery reason is added.

Logs expose only numeric `handshake_http_status`; status exposes
`last_handshake_http_status`, and Bark includes the same numeric diagnostic.
Response bodies, headers, reason phrases, credentials and complete URLs are
never included. Healthy current state and last-failure diagnostics remain
separate. HTTP retries here apply only to WebSocket connection attempts, never
to trading writes.

## Backpressure deadline competition

When a public receiver waits at the queue high-water mark, the consumer records
the first monotonic timestamp at which queue depth falls below that mark. If a
timeout callback wins scheduling against the receiver wake-up, admission succeeds
only with proof of relief within the **original** deadline, healthy processing,
available queue space and an unexpired pending-pong deadline.

Late or unproven relief still rejects the frame and creates a continuity hold.
Worker failure and heartbeat expiry take precedence. The queue remains bounded,
FIFO and lossless on success; no frame drops, extra producer tasks or longer wait
allowance are introduced. Timestamp state is cleared after every wait.

## Deployment gate

Automated regression tests do not prove the production incident's timing or
that normal server bursts will no longer exceed the unchanged budget. This fix
requires independent audit before deployment, then controlled Demo observation.
Preserve any existing ledger/continuity hold. Verify backup, exact revision,
authoritative ledger/accounting equality, full reconciliation, fresh public
warm-up, all WS workers, DB/Redis/CAA and no unsafe exposure before explicit
manual PAPER resume. No LIVE authorization is provided.
