# ADR-003: Lease at the shard level, not the task level, and no leader

Status: accepted

## Context

Coordination has to happen somewhere: something must decide which node fires
which task, and what happens when a node disappears. Two designs were on the
table — lease every task individually as it's claimed, or lease one of 64
fixed shards per node and let shard ownership imply task ownership. Also
rejected: a leader node that assigns work, which would need its own failover
story on top of whatever else is built.

## Decision

64 fixed logical shards (`shard = uuid.bytes[15] & 0x3F`), each a row in
`shards` with `lease_owner` / `lease_expires_at`. A node polls only tasks whose
shard it currently owns. No leader: the `shards` table *is* the coordination
primitive, contended with optimistic locks (`version` column) and
`FOR UPDATE SKIP LOCKED` on claim.

Rebalancing is symmetric and leaderless, in `ShardHeartbeat`/`ShardBootstrap`:

- A node under its fair share (`activeOwners` recomputed each heartbeat, soft
  cap = `64 / activeOwners`) renews what it has.
- A node that finds every shard already leased **force-claims one** anyway
  (`forceClaimOneShard`) so it stays visible in `countDistinctActiveOwners` —
  otherwise a node that loses the startup race never gets counted and the
  system can't tell it exists.
- A node over its share **releases the excess** (`releaseExcessShards`,
  highest shard IDs first) so a newly-visible node has something to claim next
  heartbeat.

`ShardRebalanceConvergenceTest` proves this doesn't oscillate: 3 nodes settle
to `[22,22,20]` in exactly 3 release events, then the shard→owner map stays
byte-identical for ~8 more heartbeat periods. The arithmetic backs it up too —
released ≤ wanted, since `64 − softCap×N ≤ 0` for any N ≥ 1 — so a release
event can never overshoot into triggering a counter-release.

Task-level leasing was rejected because the shard table already answers "is
this task mine" for free — a node's `WHERE shard = ANY(:ownedShards)` filter
in `claimDueTasks` is the same check a per-task lease would make one row at a
time, except it costs one join instead of N lease acquisitions per poll.

## Evidence

The K8s rolling-restart on 9/4 is the sharpest real-environment proof: a
`replicas:2` Deployment restart briefly ran 4 pods (2 old draining, 2 new
starting). `activeOwners` in the heartbeat logs went 4 → 3 → 2 across three
consecutive rebalance events as old pods drained, converging back to the
steady `[32, 32]` split with **zero pod restarts** and no leader election
anywhere in the process — the same leaderless mechanic built for node death
handled an orchestrator-driven churn event it was never written for.

Chaos scenario 6 exercises the death half of the same mechanic: freezing a
node past its shard lease TTL, the survivor takes over the frozen node's
shards and re-fires its in-flight task (measured recovery: 41s, against a
derived worst case of ~48s — see ADR-004).

## Consequences

- Rebalancing is a routine, continuous background process, not a rare
  failover path — which is exactly why ADR-001 rejected caching a wheel on
  top of it (the "steady state" the wheel would assume doesn't really exist).
- The design has no single point that decides shard assignment, so there's
  nothing to fail over *to* — the property chaos scenario 6 and the K8s
  rollout both exercise, from different triggers.
