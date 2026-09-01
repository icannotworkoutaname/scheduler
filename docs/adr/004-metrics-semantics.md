# ADR-004: Metrics semantics (requirements.md §9)

Status: accepted (8/24)

Block 5 shifts from "prove the system is correct" to "make its correctness
externally visible". Four of the seven §9 metrics carry a semantic decision
that, if wrong, makes the 8/28 README charts lie. This ADR records those.

## Decision 1 — `trigger_delay_seconds` is measured on the database clock

`trigger_delay = actual_trigger_instant − fire_at`. **Both** timestamps come
from Postgres, never a node's `Instant.now()` / injected `Clock`.

Mechanism: the claim query is `UPDATE tasks SET state='firing' ... RETURNING *,
now() AS db_fired_at`. The delay is `db_fired_at − fire_at`, computed in
`ClaimedTask.triggerDelay`, recorded in `PollingLoop.pollAndClaim` at claim
time. No node clock is read anywhere on this path.

Why: chaos scenario 7 (8/23) showed a clock-skewed node still *triggers*
correctly because every `fire_at <= now()` / `lease_expires_at < now()`
comparison runs in SQL. But a delay computed as `nodeClock.now() − fire_at`
would be wrong by exactly the skew — and skew is near-certain in a real
deployment. This metric is the last place node-clock correctness could
regress; `TriggerDelayMetricTest` pins it (a `+300s` node reports ~2s, not
~302s).

## Decision 2 — `duplicate_trigger_total` records absorption, not prevention

A single node cannot know one of its fires is a duplicate: from its view every
fire it makes is legitimate (it claimed the task, its lease was valid). The
honest thing to count is the *aftermath*:

> a node finished firing a task (sink call made, bytes delivered downstream)
> and its conditional state-update — `markSucceeded` **or** `markFailed` — hit
> **0 rows because the task was already `succeeded`**.

Those 0 rows are a late duplicate delivery being absorbed by the optimistic
lock. Implemented in `PollingLoop.handle`: on any 0-row update, `findById`; if
the state is now `SUCCEEDED`, `duplicateTriggerAbsorbed()`. Any other state
(reaper put it back to `pending`, etc.) is a plain version race, not a
duplicate — not counted.

Note this fires on scenario 6's actual path: the un-frozen node's HttpClient
threw ("Request cancelled"), so it went through `markFailed` → 0 rows → task
already `succeeded` → counted. The receiver's `/stats` `duplicate_triggers`
(used on 8/21) stays as an out-of-band cross-check; this is the in-band one.

Name is `duplicate_trigger_total`, help text says "absorbed", not "prevented".

## Decision 3 — `tasks_pending` is a sampled approximation, not a live value

Per-shard gauge (`tag: shard`, 64 series). A gauge that ran
`SELECT count(*) ... GROUP BY shard` on every Prometheus scrape would be a
load source in its own right at 8/26's million-row scale, competing with the
very thing 8/26 measures.

So: one background sample every **15s** (`SchedulerMetrics`, on its own
single daemon thread — never the shared `@Scheduled` poll thread), writing
into `AtomicInteger[64]` that the gauges read. Help text says so explicitly:
"sampled every 15s — a coarse observability value, NOT read per scrape and NOT
exact real-time". The 8/28 "load per shard" chart is a trend, not a
dashboard needle.

## Decision 4 — `trigger_delay_seconds` uses explicit histogram buckets

SLO is P50 ≤ 200ms, P99 ≤ 1s. Micrometer's default buckets are too coarse
there — P99 would be computed with poor resolution. Explicit
`serviceLevelObjectives`: **50ms, 100ms, 200ms, 500ms, 1s, 2s, 5s**. Both SLO
points (200ms, 1s) have a boundary on each side, so `histogram_quantile()` has
resolution exactly where the 8/28 precision histogram and the P99 number need
it. `sink_call_duration_seconds` and `poll_duration_seconds` get explicit
buckets too, sized to their own ranges.

## The other three metrics (no decision needed)

- `tasks_dead_total` — counter, incremented in `PollingLoop.handle` when a
  `markFailed` lands in the dead branch (retries exhausted).
- `sink_call_duration_seconds` — timer around `HttpSink.fire`, tag
  `outcome=success|failure`, measured with `System.nanoTime()` (elapsed real
  time, immune to clock skew — not the injected `Clock`).
- `poll_duration_seconds` — timer around the whole `pollAndClaim` body,
  **including the no-op path**. 8/26 uses its idle value to show that polling
  with nothing due is a cheap bounded index scan.
- `lease_takeover_total` — already wired into `ShardHeartbeat` (8/12), left
  there rather than moved.
