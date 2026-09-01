# Scenario 7 / 8 notes (8/23)

## 8/24 hard constraint — `trigger_delay_seconds`

When the `trigger_delay` metric gets wired to Micrometer on 8/24, its time
source **must be the database clock**, not the node's injected `Clock`:

```
trigger_delay = (DB now() when the task was claimed) - task.fire_at
```

Both terms come from Postgres. If the delay were computed from
`clock.instant()` on the node, a node with a skewed or drifting clock
(scenarios 7 and 8) would report a garbage delay while its actual triggering
was perfectly on time — the metric would lie about a system that is working.

Ship a unit test alongside the metric: a node started with
`--chronos.clock.offset-seconds=300` fires a task and the reported
`trigger_delay` is still within a second or two of the real value, not ~300s.

This is the cash-in of the 8/7 risk-register line "when Clock is injected,
pin the time source per call site". Triggering correctness was already pinned
to the DB clock (all `fire_at <= now()` / `lease_expires_at < now()` run in
SQL); the metric is the one remaining place that could regress it.

Scenario 7 today (`ClockOffsetTest`, `scripts/scenario7_clock_offset.sh`)
only does the **behavior** half: a +5min node still fires its shards on time,
no early fires, both nodes fire their share. The metric half waits for 8/24.

## Scenario 8 — the collapse coefficient

The deliverable is one number: the drift rate at which the design breaks.

Setup after 8/23: `ShardHeartbeat` derives its renewal cadence from the
injected `Clock` (it polls often via `@Scheduled`, but only renews once
`clock.instant()` has advanced a full `chronos.heartbeat.period-seconds`).
The shard lease TTL is still `now() + interval` in Postgres — the **database**
clock, unaffected by node drift.

So a node running at rate `r` (wall clock advances at `r` × real time):

| quantity                    | in real seconds |
|-----------------------------|-----------------|
| its renewal interval        | `period / r`    |
| the shard lease TTL         | `ttl` (fixed)   |

The lease survives iff `period / r < ttl`, i.e.

```
r > period / ttl
```

Production values are `period = 10s`, `ttl = 30s`, so the design tolerates a
node clock as slow as **r = 1/3 real speed**. At exactly 1/3 the renewal
interval equals the TTL: the shard is lost and reclaimed every cycle (thrash),
and by the same arithmetic the 10s sink timeout stretches to 30s = the task
lease TTL, so a merely-slow (not failed) downstream now causes a takeover —
i.e. scenario 6's double fire, triggered by drift instead of a freeze.

`ClockDriftTest` confirms the crossover with `period=2s`, `ttl=6s` (same 1/3
ratio, a fraction of the wall time): at `r=0.5` the drifted node holds its 32
shards for the whole observation window; at `r=0.2` it drops below 32 as the
normal node reclaims its expired leases. `scripts/scenario8_clock_drift.sh`
runs the same two points end to end.

## Side effect of getting scenario 8 to be a real drift repro

Making the heartbeat renewal `Clock`-driven exposed a latent convergence bug
(the 8/20 one, never fully fixed): a node that loses the startup race and
finds **every** shard already leased used to announce with zero shards, which
made it invisible to `countDistinctActiveOwners()`. The over-holding peer then
kept `softCap = 64` forever and never rebalanced. Fixed here with:

- `ShardBootstrap` Phase 1 force-claims one shard if the normal announce got
  nothing (`ShardLeaseRepository.forceClaimOneShard`), guaranteeing visibility.
- `ShardHeartbeat` now also rebalances **down** — a node holding more than its
  fair share releases the excess (`releaseExcessShards`), so any transient
  imbalance self-heals within a heartbeat or two instead of being permanent.
