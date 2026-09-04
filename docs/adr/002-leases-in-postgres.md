# ADR-002: Leases live in Postgres, not Redis

Status: accepted

## Context

A shard/task lease needs an atomic "acquire and check" — exactly what a Redis
`SET NX EX` gives cheaply. The obvious design splits storage: Postgres holds
task rows, Redis holds lease ownership, because Redis is the conventional
choice for anything lease-shaped. Rejected in favor of leases as ordinary rows
in the same database as the task state they gate.

## Decision

`shards.lease_owner` / `lease_expires_at` / `version` are columns in Postgres,
updated by the same conditional `UPDATE ... WHERE version = ?` pattern used
everywhere else in the system (ADR-003). No second datastore is on the
coordination path.

Splitting lease ownership (Redis) from task state (Postgres) creates a
dual-write inconsistency window: a crash between "Redis says I own shard 9"
and "Postgres row updated to reflect it" leaves the two stores disagreeing
about who owns what, with no transaction spanning both to make that
impossible. Every other correctness argument in this system (ADR-004's
optimistic-lock absorption, ADR-003's leaderless rebalance) depends on a
single conditional statement being the entire state transition — introducing
a second store for leases specifically would reopen the exact class of bug
the rest of the design spends its effort closing.

Lease acquisition is not on the request-latency path — it happens once per
heartbeat period, not once per fired task — so the throughput Redis would have
bought was never the constraint.

## Evidence

No coordination-store benchmark was needed because the SLO it would have
served — failover within 10s — is already met using Postgres alone: the K8s
rolling-restart and chaos scenario 6 both converge within one to three
heartbeat periods (ADR-003), comfortably inside 10s, with lease reads/writes
riding the same connection pool as everything else.

The requirements doc (§7.6) originally planned a narrow Redis role even
without it holding leases — a dedup-window cache and dashboard hot counters.
Neither survived into the implementation: the demo receiver's dedup is a
plain in-process `set()`, and Prometheus/Micrometer already serve the
counters. There is no Redis anywhere in `docker-compose.yml`, the Gradle
build, or the K8s manifest — this decision ended up broader in practice than
originally scoped.

## Consequences

- One fewer moving part in `make demo` / the K8s manifest: no Redis at all,
  no second connection pool, no second failure mode to reason about during a
  node freeze or partition.
- If lease-renewal volume ever became a real bottleneck (many more shards, or
  a much shorter heartbeat period), this decision would need revisiting —
  today's numbers are nowhere near that.
