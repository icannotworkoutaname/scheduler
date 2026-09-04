# ADR-001: Polling over a shared index, not an in-memory timing wheel

Status: accepted

## Context

Firing "whatever is due" is naturally a timer-wheel problem, and that's the
first design most engineers reach for: bucket tasks by `fire_at`, advance a
wheel, fire the bucket that comes due. Rejected in favor of a plain poll
against a Postgres index. This ADR is the case for that, backed by numbers —
not an excuse for skipping the harder design.

## Decision

Each node runs a 200ms `@Scheduled` poll: `claimDueTasks` does
`SELECT ... WHERE shard = ANY(:ownedShards) AND state='pending' AND fire_at <= now() FOR UPDATE SKIP LOCKED`,
backed by `tasks_shard_state_fire_at_idx (shard, state, fire_at)`. No in-memory
schedule exists anywhere; "what's due" is answered fresh from Postgres every
poll.

A timing wheel would have looked strictly faster — sub-millisecond in-memory
lookup instead of a round-trip per node per 200ms. It was rejected because it
adds three failure surfaces the polling design doesn't have:

1. **Cascade correctness** — a wheel with coarse+fine buckets needs a cascade
   step (re-bucketing as the coarse pointer advances); a bug there is silent
   and only shows up as a task firing late by exactly one wheel cycle.
2. **Rebuild on restart/rebalance** — the wheel is derived state. Every node
   start and every shard rebalance (this system has no leader, so rebalances
   are routine, not exceptional — ADR-003) means re-deriving the in-memory
   wheel from the DB anyway. The wheel would be a cache that must always be
   provably consistent with Postgres, for a workload where "provably
   consistent with Postgres" is already the whole job.
3. **Dual-write consistency** — a fired task's wheel entry and its DB row must
   agree on state, or a crash between the two produces exactly the double-fire
   /lost-task bugs this project spends most of its chaos scenarios ruling out.
   Polling against the row *is* the state; there is no second copy to drift.

## Evidence

The bet was that a plain poll would still meet the SLOs at real scale. It did:

| | empty DB | 1 M pending rows |
|---|---|---|
| poll cycle, idle path, P50/P99 | 3.4 ms / 19 ms | 3.0 ms / 7.9 ms |

No growth from empty to a million rows — `tasks_shard_state_fire_at_idx` seeks
directly to each shard's due slice regardless of table size. Under the 100k/1s
peak-load burst, `claimDueTasks` itself runs in 0.78 ms (Bitmap Index Scan on
the same index); the reaper's expiry sweep runs 0.9 ms via a second partial
index (`tasks_firing_lease_expires_idx`), degrading to a 10.8 ms Seq Scan only
in the degenerate case where `firing` rows are most of the table — a state
backpressure (`fireCapacity()` bounding claims to the executor's free
capacity, 8/27) prevents in practice.

## Consequences

- No wheel, no cascade, no rebuild-on-rebalance, no dual-write. The tradeoff is
  a DB round-trip every 200ms per node instead of an in-memory pointer bump —
  measured to cost nothing that matters at the SLO's scale.
- If the SLO ever needed sub-millisecond precision or the poll interval itself
  became the bottleneck, this decision would need revisiting; at 200ms/1M rows
  it isn't close.
