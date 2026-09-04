# Distributed Delayed Task Scheduler — Requirements

Status: revised after external design review, 2026-08-05
Owner: Yuanyu Zhang

## 1. Problem statement

A distributed scheduler that accepts "execute this action at a future time" requests
and reliably fires each task at its scheduled moment. It must support millions of
pending tasks, survive node crashes and node freezes without losing tasks, and scale
horizontally without silently firing the same task twice.

Real-world equivalents: order auto-cancellation after 30 minutes unpaid, delayed
notifications, retry queues, scheduled reminders.

## 2. Scope

### In scope

- Task submission with a future fire time
- Task cancellation and rescheduling (pending state only)
- Reliable at-point firing to an HTTP webhook
- Retry with exponential backoff and a dead-letter terminal state
- Horizontal scaling via shard-level leases, with defined assignment and rebalance
- Crash recovery: tasks owned by a dead node are taken over within 10s
- **Freeze recovery**: a node stalled beyond its lease TTL loses ownership, and the
  resulting duplicate fire is observable and harmless

### Non-goals

| Not doing | Why |
|---|---|
| Cron / recurring tasks | Different problem domain, dilutes the core narrative |
| Task priorities, DAG dependencies | Scope explosion; that is a workflow engine |
| Multi-tenant quotas and isolation | Pure implementation volume, no design interest |
| Web console UI | curl + Grafana is sufficient for demo |
| Millisecond precision | Second-level precision covers ~99% of real use cases |
| Cancelling a task already firing | Distributed cancellation; real systems do not promise it |
| Executing the business action itself | The scheduler triggers; downstream owns semantics |
| In-memory hierarchical time wheel | See ADR-001; polling meets the SLO with fewer failure modes |
| A second sink implementation (Kafka) | An unused abstraction proves nothing; interface only |

## 3. Public API

| Endpoint | Description |
|---|---|
| `POST /tasks` | Submit. Body: `payload`, `fireAt`, `idempotencyKey`, `callbackUrl`, optional `retryPolicy` |
| `PATCH /tasks/{id}` | Reschedule. Body: `fireAt`, `version`. 409 if not pending |
| `DELETE /tasks/{id}` | Cancel. 409 if not pending |
| `GET /tasks/{id}` | Inspect state (debug and demo) |

### Two layers of idempotency — do not conflate

**1. Submit-side — `idempotencyKey`, supplied by the client.**
Retried submissions caused by client-side network failure must not create duplicate
tasks. Enforced by a unique index; a conflicting insert returns the existing task.

**2. Fire-side — `triggerId`, derived by the scheduler.**

> **`triggerId` is deterministic and STABLE across every firing attempt of a task,
> including retries after downstream failure and re-fires after lease takeover.**
> It is derived from `taskId`, not generated per attempt.

This is load-bearing. If `triggerId` were unique per attempt, a node freeze followed
by takeover would send two different ids for the same logical action, downstream
deduplication would fail to match them, and the business action would execute twice —
defeating the entire consistency story.

The attempt counter travels in a separate header for observability only. It must never
participate in deduplication.

## 4. Delivery semantics

- **The system does NOT promise exactly-once.** It promises **at-least-once firing with
  a stable `triggerId`**, delegating deduplication to the downstream consumer.
  End-to-end the effect is effectively-once.
- The system does **not attempt to prevent** duplicate fires caused by node stalls
  exceeding the lease TTL. A GC pause or SIGSTOP longer than the TTL is undetectable
  from inside the stalled process; any scheme claiming otherwise is lying.
  The design instead makes duplicates **cheap** (stable `triggerId`) and **visible**
  (`duplicate_trigger_total`).
- Duplicate trigger rate under normal operation: **< 0.01%**, measured, not asserted.

## 5. Service level objectives

| Dimension | Target |
|---|---|
| Pending task capacity | 1,000,000 |
| Firing precision vs `fireAt` | P50 <= 200ms, P99 <= 1s |
| Delay range | 1 second to 30 days |
| Submit throughput | 1,000 TPS |
| Peak fire rate | 5,000 /s |
| Failover | Any single node dies, its tasks resume within 10s |
| Sink call timeout | 10s, strictly below the 30s lease TTL |

## 6. Task state machine

```
pending ──> firing ──> succeeded
   │           │  ^
   │           │  └── retrying (backoff, max 5 attempts)
   │           │            │
   │           │            └──> dead (manual replay only)
   │
   └──> cancelled
```

- `pending` — persisted, awaiting its fire time
- `firing` — a node holds the lease and is calling downstream
- `succeeded` — downstream acked
- `retrying` — call failed, awaiting exponential backoff
- `dead` — retry budget exhausted, requires manual replay
- `cancelled` — terminal, will never fire

`cancel` and `reschedule` are legal only from `pending`. Any other state returns 409.

A **reaper** sweeps rows stuck in `firing` past `lease_expires_at` and returns them to
`pending` for another node to claim. This is the mechanism that turns a node freeze
into a recoverable duplicate rather than a lost task.

## 7. Key design decisions

### 7.1 Claim and transition in one statement

Selecting due tasks and acquiring their lease is a single statement, leaving no
intermediate state a crash can land in:

```sql
UPDATE tasks
   SET state = 'firing',
       lease_owner = ?,
       lease_expires_at = now() + interval '30 seconds',
       version = version + 1
 WHERE id IN (
   SELECT id FROM tasks
    WHERE shard = ANY(?)
      AND state = 'pending'
      AND fire_at <= now()
    ORDER BY fire_at
    FOR UPDATE SKIP LOCKED
    LIMIT 500
 )
RETURNING *;
```

`FOR UPDATE SKIP LOCKED` lets multiple nodes poll the same shard set concurrently
without blocking each other — a node that would contend simply skips to the next row.

### 7.2 Leases live in PostgreSQL, not Redis

Lease acquisition and state transition must be atomic. Splitting them across Redis and
PostgreSQL creates a dual-write inconsistency window: a crash between the two writes
leaves Redis claiming ownership while PostgreSQL still shows `pending`.

Lease acquisition is not on a latency-critical path, so the cost over Redis is
acceptable.

### 7.3 Leases are per-shard, not per-task

Tasks map to a fixed set of 64 logical shards via `hash(taskId) % 64`. A node acquires
leases on whole shards and only polls within its own shards.

Per-task locking would mean a million lock acquisitions for a million tasks. Per-shard
leasing bounds contention to 64 regardless of task count.

### 7.4 Shard assignment and rebalance

- A `shards` table holds 64 rows: `shard_id, lease_owner, lease_expires_at, version`
- On startup a node greedily claims any shard whose lease is null or expired, up to a
  soft cap of `ceil(64 / active_nodes)`
- Leases renew every 10s against a 30s TTL
- A node that fails to renew (partition, stall) loses its shards to the next claimer
- A node that discovers it has lost a shard stops polling it immediately and does not
  fire tasks in it — verified by the conditional update in 7.1, which will affect zero
  rows for tasks whose shard it no longer owns

There is no coordinator, no leader election, and no ZooKeeper. Contention is resolved
by the same optimistic-locking primitive used everywhere else in the system.

### 7.5 Duplicates are made cheap, not prevented

Sink timeout (10s) is set strictly below lease TTL (30s) so that ordinary slow
downstreams never cause takeover. A stall exceeding 30s — long GC, SIGSTOP,
host freeze — will cause a duplicate fire, and that is accepted by design.

The stable `triggerId` makes the duplicate harmless downstream, and
`duplicate_trigger_total` makes it visible. Chaos scenario 6 demonstrates the whole
path end to end.

### 7.6 Redis was dropped entirely, not just narrowed

**Superseded.** The two narrow roles planned for Redis here were each absorbed
by something already in the design, so no Redis instance exists anywhere in
the running system (not in `docker-compose.yml`, not a Gradle dependency, not
in the demo receiver):

1. `triggerId` deduplication window — the demo receiver (`scripts/receiver.py`)
   does this with a plain in-process `set()`. Fine for a demo; a real
   downstream would own its own dedup store regardless of what this system
   provides, per §1's non-goal ("executing the business action itself").
2. Hot counters for dashboards — Prometheus + Micrometer counters/gauges
   (§9) serve this directly; there was never a second counter store.

See ADR-002 for why leases specifically never went to Redis; this section is
the broader point that the whole datastore was never needed for anything.

## 8. Storage

- **PostgreSQL** — source of truth for tasks, state, shard leases, version.
  The only datastore in the running system — see §7.6.
- No in-process scheduling structure. Nodes poll their shards every 200ms.

Required index: `(shard, state, fire_at)`. With it, a poll that finds nothing due is a
bounded index range scan and stays cheap at a million pending rows.

## 9. Required observability

| Metric | Purpose |
|---|---|
| `trigger_delay_seconds` (histogram) | Precision SLO, P50/P99 |
| `duplicate_trigger_total` | Proves the at-least-once claim is measured |
| `tasks_firing` (gauge, by shard) | Live per-shard load, rebalancing evidence — renamed from `tasks_pending` (ADR-004 decision 3): an exact backlog count is a full-table scan at 1M rows, live firing count is not |
| `tasks_dead_total` | Dead-letter accumulation |
| `lease_takeover_total` | Failover actually happening |
| `sink_call_duration_seconds` | Downstream latency |
| `poll_duration_seconds` | Cost of the polling loop under load |

## 10. Required chaos scenarios

Each must have a scripted, repeatable demonstration plus a consistency report.

1. **Kill a node mid-firing** — tasks taken over within 10s, none lost
2. **Downstream returns 500** — retries with backoff, eventually dead-letters
3. **Duplicate submission with the same `idempotencyKey`** — one task created
4. **Cancel racing with fire** — one side wins cleanly, never both
5. **Network partition** — partitioned node loses shards, and on recovery discovers
   this and stops firing rather than continuing
6. **Node freeze (`kill -STOP`) exceeding lease TTL** — the centrepiece.
   Lease expires, another node takes over and re-fires with the *same* `triggerId`,
   the frozen node resumes (`kill -CONT`) and also fires,
   `duplicate_trigger_total` increments, and the downstream receiver deduplicates so
   the business action still occurs exactly once.

7. **Clock offset** — a node whose wall clock is minutes ahead or behind still behaves
   correctly, because every correctness comparison (`fire_at <= now()`,
   `lease_expires_at < now()`) is evaluated by PostgreSQL. The scenario must also
   verify that `trigger_delay_seconds` is computed from the database clock, not the
   node clock — otherwise the headline precision number is measured wrong on any
   skewed host.

   **Fulfilled 8/24** (deferred one day from the 8/23 scenario itself, since the
   metric didn't exist yet): `ClockOffsetTest` covers the behavioral half,
   `TriggerDelayMetricTest` covers the metric half — a `+300s` skewed node
   reports a delay of ~2s, not ~302s. See ADR-004 decision 1.
8. **Clock rate drift** — a node whose clock runs slow measures node-local *durations*
   wrong, and two of those are load-bearing:
   - the 10s heartbeat interval against a 30s lease TTL, and
   - the 10s sink timeout, which is the safety margin keeping ordinary slow
     downstreams from triggering takeover.

   A node running at 1/3 speed renews every 30s wall-clock and loses its shards
   spuriously, and its 10s sink timeout becomes 30s wall-clock — reproducing the
   scenario 6 double fire from drift rather than from freeze. This scenario establishes
   the drift factor at which the design breaks.

   **Fulfilled 8/23, crossover confirmed at `rate = period/ttl` (= 1/3 in
   production).** One caveat carried into ADR-005: `ClockDriftTest` actually
   instruments the heartbeat-renewal collapse (measured, both sides of the
   crossover); the equivalent sink-timeout collapse follows the same formula
   but was never independently instrumented — `DriftingClock` has no seam into
   `HttpClient`'s timer. Cite the heartbeat number as measured, the
   sink-timeout number as derived; they are not the same kind of evidence.

Both are exercised by injecting `java.time.Clock` at the node boundary rather than by
faking time at the container level: offset via `Clock.offset`, drift via a custom
rate-scaled `Clock`. This keeps the tests deterministic and inside the Testcontainers
suite.

## 11. ADRs

All five accepted and written (9/5) — see [`docs/adr/`](adr):

- [ADR-001](adr/001-polling-not-timing-wheel.md) — Why polling instead of a hierarchical time wheel
- [ADR-002](adr/002-leases-in-postgres.md) — Why leases live in PostgreSQL rather than Redis
- [ADR-003](adr/003-shard-level-lease.md) — Why shard-level rather than task-level leases
- [ADR-004](adr/004-metrics-semantics.md) — Why at-least-once, and why duplicates are measured rather than prevented
- [ADR-005](adr/005-clock-model.md) — The clock model: which comparisons are database-authoritative, which
  durations are node-local, and the measured drift factor at which the design breaks
