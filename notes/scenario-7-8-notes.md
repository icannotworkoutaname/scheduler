# Scenario 7 / 8 working notes

Kept as the derivation behind ADR-005 and ADR-004 decision 1. Both scenarios are
implemented; this file records why they are shaped the way they are.

## Constraint on `trigger_delay_seconds`

The metric's time source must be the database clock, not the node's injected
`Clock`:

```
trigger_delay = (DB now() when the task was claimed) - task.fire_at
```

Both terms come from Postgres. Computed from `clock.instant()` on the node, a
node with a skewed or drifting clock (scenarios 7 and 8) would report a garbage
delay while its triggering was on time — the metric would misreport a working
system.

`TriggerDelayMetricTest` pins this: a node started with
`--chronos.clock.offset-seconds=300` fires a task and the reported
`trigger_delay` is within a second or two of the real value, not ~300 s.

This is the cash-in of the earlier risk-register line "when `Clock` is injected,
pin the time source per call site". Triggering correctness was already pinned to
the database clock — all `fire_at <= now()` and `lease_expires_at < now()`
comparisons run in SQL — and the metric was the one remaining place that could
regress it.

Scenario 7 itself (`ClockOffsetTest`, `scripts/scenario7_clock_offset.sh`) covers
the behavioral half: a `+5 min` node still fires its shards on time, no early
fires, both nodes fire their share.

## Scenario 8 — the collapse coefficient

The deliverable is one number: the drift rate at which the design breaks.

`ShardHeartbeat` derives its renewal cadence from the injected `Clock`: it polls
often via `@Scheduled`, but renews only once `clock.instant()` has advanced a
full `chronos.heartbeat.period-seconds`. The shard lease TTL is still
`now() + interval` in Postgres — the database clock, unaffected by node drift.

For a node running at rate `r` (its wall clock advances at `r` × real time):

| quantity | in real seconds |
|---|---|
| its renewal interval | `period / r` |
| the shard lease TTL | `ttl` (fixed) |

The lease survives iff `period / r < ttl`, i.e. `r > period / ttl`.

Production values are `period = 10 s`, `ttl = 30 s`, so the design tolerates a
node clock as slow as `r = 1/3` real speed. At exactly 1/3 the renewal interval
equals the TTL: the shard is lost and reclaimed every cycle, and by the same
arithmetic the 10 s sink timeout stretches to 30 s — the task lease TTL — so a
merely slow (not failed) downstream now causes a takeover, i.e. scenario 6's
double fire triggered by drift instead of by a freeze.

`ClockDriftTest` confirms the crossover with `period=2s`, `ttl=6s` (the same 1/3
ratio, a fraction of the wall time): at `r=0.5` the drifted node holds its 32
shards for the whole observation window; at `r=0.2` it drops below 32 as the
normal node reclaims its expired leases. `scripts/scenario8_clock_drift.sh` runs
the same two points end to end.

## Convergence bug exposed while building scenario 8

Making heartbeat renewal `Clock`-driven exposed a latent convergence bug: a node
that lost the startup race and found every shard already leased used to announce
with zero shards, which made it invisible to `countDistinctActiveOwners()`. The
over-holding peer then kept `softCap = 64` indefinitely and never rebalanced.
Fixed with:

- `ShardBootstrap` phase 1 force-claims one shard when the normal announce gets
  nothing (`ShardLeaseRepository.forceClaimOneShard`), guaranteeing visibility.
- `ShardHeartbeat` also rebalances downward: a node holding more than its fair
  share releases the excess (`releaseExcessShards`), so a transient imbalance
  self-heals within a heartbeat or two instead of persisting.

`ShardRebalanceConvergenceTest` guards both.
