# ADR-004: Metrics semantics (requirements.md §9)

Status: accepted (8/24); decisions 5–7 added 9/5

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

## Decision 3 — `chronos.tasks.firing` (per-shard) — sampled, and live-load not backlog

Per-shard gauge (`tag: shard`, 64 series). Two things keep it from being a load
source in its own right at a million rows:

1. **Sampled, not per-scrape.** One background sample every **15s**
   (`SchedulerMetrics`, on its own single daemon thread — never the shared
   `@Scheduled` poll thread), written into `AtomicInteger[64]` that the gauges
   read.
2. **Live firing count, not backlog** (8/27). The original counted every
   pending/retrying row — a 91ms parallel full-table Seq Scan at 1M rows
   (`state` matches ~everything, no `shard =` to seek on). Bounding it to "due
   in the next 5 min" made it *worse* (300ms) — under a real drain that window
   holds tens of thousands of rows and an exact count is O(matches). What is
   both cheap and the right question: **how many rows each shard is firing
   right now.** `state = 'firing'` is selective (backpressure caps total firing
   at a few thousand), the partial index `tasks_firing_lease_expires_idx`
   covers it, and it is the live per-shard work distribution — 0 everywhere
   when idle, which correctly reads as "no load". ~0.9ms at rest, ~15ms
   mid-burst. Renamed `tasks_pending` → `tasks_firing`. `docs/performance.md`
   has the before/after plan numbers.

## Decision 4 — `trigger_delay_seconds` uses explicit histogram buckets

SLO is P50 ≤ 200ms, P99 ≤ 1s. Micrometer's default buckets are too coarse
there — P99 would be computed with poor resolution. Explicit
`serviceLevelObjectives`: **50ms, 100ms, 200ms, 500ms, 1s, 2s, 5s**. Both SLO
points (200ms, 1s) have a boundary on each side, so `histogram_quantile()` has
resolution exactly where the 8/28 precision histogram and the P99 number need
it. `sink_call_duration_seconds` and `poll_duration_seconds` get explicit
buckets too, sized to their own ranges.

## Decision 5 — scenario 6 recovery is dominated by the shard lease, not the task lease

Both leases are nominally 30s, so a naive estimate adds them: task lease
expires, reaper takes ≤5s to notice, shard lease also needs to expire, another
node's heartbeat takes ≤10s to notice — plan.md's original ~50s blind-sleep
estimate summed them. That's wrong: the two clocks run **in parallel**, not in
series, so the recovery time is `max(...)` of the two chains, not their sum.

```
shard lease expiry (≤30s) + heartbeat discovery (≤10s)  → survivor eligible, ≤40s
task lease expiry (30s)   + reaper sweep (≤5s)           → row back to pending, ≤35s  (runs concurrently with the above)
                                                          → survivor's poll picks it up, ≤0.2s
                                                          + whatever the downstream itself takes
```

Worst case is `max(40, 35) + 0.2 + downstream`, not `40 + 35 + downstream`.
`scripts/scenario6_freeze_double_fire.sh` derives this in comment form and
waits on the fact ("task reached `succeeded`") instead of a blind sleep for
exactly this reason. Measured (9/4 rerun, 8s artificial downstream delay,
otherwise production 30s/10s/5s timings): **41s** frozen-to-recovered, inside
the ~48s derived worst case.

Why this belongs here and not only in the shard-lease ADR: it's the reason
`trigger_delay` and `duplicate_trigger_total` behave the way they do during a
freeze — both are gated by whichever clock is slower, and a metrics reader
who assumes the two TTLs add will misjudge how long a real freeze-recovery
takes.

## Decision 6 — scenario 4 (cancel-vs-fire) proves outcome consistency, not race arbitration

What chaos scenario 4 demonstrates: whichever of "cancel" or "fire" wins, the
HTTP response, the DB row's terminal state, and the receiver's `/seen` record
all agree with each other. It does **not** demonstrate winning a true
millisecond-scale simultaneous collision — the test constructs a straddle (one
half of the cases pre-positioned to land just before `fire_at`, the other
half just after) rather than firing both branches at the exact same instant.

The actual race guarantee doesn't come from timing a test precisely enough to
hit a window — it comes from `UPDATE tasks SET state='cancelled' WHERE
state='pending' AND version=:v` (and the equivalent claim-then-fire path)
being unconditionally correct by construction: only one of the two conditional
updates can ever match a given row, regardless of how close in time they run.
The test's job is to sample both sides of the straddle and check the three
systems agree, not to prove the arbitration itself — that's Postgres row
locking, not application code.

**Don't write "we tested the actual race" anywhere reader-facing** — the
honest claim is "outcome consistency verified across the boundary; the
mutual-exclusion guarantee is `WHERE version=?`, proven by construction."

## Decision 7 — two different re-fires must not be conflated

`duplicate_trigger_total` counting up during a chaos run can mean two very
different things, and a reader who can't tell them apart will misread a
healthy peak-load run as broken:

- **Failure re-fire** (scenario 6): a node froze/died past its lease; another
  node correctly re-fires its in-flight task. This is the system working —
  every increment here is one duplicate correctly absorbed instead of a task
  getting lost.
- **Self-induced re-fire** (found during 8/27 peak-load tuning): under load,
  claiming due tasks faster than the worker pool can fire them lets `firing`
  rows pile up past their own lease TTL — the reaper then hands a task that
  was never actually stuck to a second claim, purely because the first claim
  hadn't finished yet. This is a capacity problem wearing the same counter.
  Backpressure (`fireCapacity()` bounding claims to the executor's free
  capacity) took this from ~2,000 re-fires down to 0
  in the 8/27 peak run, with zero code changes to the failure path above.

Same metric, same absorption mechanism, unrelated causes. A dashboard or
README that shows `duplicate_trigger_total` rising must say which one it's
demonstrating — flat-at-zero except during a named chaos scenario is the
signature of the first; any nonzero rate during ordinary peak load is the
second, and a bug to chase, not a feature to show off.

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
