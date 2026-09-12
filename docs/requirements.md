# Distributed Delayed Task Scheduler — Requirements

Status: revised after external design review, 2026-08-05; §5 and §7.1 corrected
against the implementation, 2026-09-05
Owner: Yuanyu Zhang

## 1. Problem statement

A distributed scheduler that accepts "execute this action at a future time"
requests and fires each task at its scheduled moment. It must hold millions of
pending tasks, survive node crashes and node freezes without losing tasks, and
scale horizontally without silently firing the same task twice.

Equivalent production use cases: order auto-cancellation after 30 minutes
unpaid, delayed notifications, retry queues, scheduled reminders.

## 2. Scope

### In scope

- Task submission with a future fire time
- Task cancellation and rescheduling (pending state only)
- Firing to an HTTP webhook at the scheduled time
- Retry with exponential backoff and a dead-letter terminal state
- Horizontal scaling via shard-level leases, with defined assignment and rebalance
- Crash recovery: tasks owned by a dead node are taken over within one lease
  cycle (§5)
- **Freeze recovery**: a node stalled beyond its lease TTL loses ownership, and
  the resulting duplicate fire is observable and harmless

### Non-goals

| Not doing | Why |
|---|---|
| Cron / recurring tasks | Different problem domain |
| Task priorities, DAG dependencies | That is a workflow engine |
| Multi-tenant quotas and isolation | Implementation volume, no design interest |
| Web console UI | curl + Grafana is sufficient here |
| Millisecond precision | Second-level precision covers the target use cases |
| Cancelling a task already firing | Distributed cancellation; real systems do not promise it |
| Executing the business action itself | The scheduler triggers; downstream owns semantics |
| In-memory hierarchical time wheel | See ADR-001; polling meets the SLO with fewer failure modes |
| A second sink implementation (Kafka) | Interface only; an unused abstraction proves nothing |

## 3. Public API

| Endpoint | Description |
|---|---|
| `POST /tasks` | Submit. Body: `payload`, `fireAt`, `idempotencyKey`, `callbackUrl` |
| `PATCH /tasks/{id}` | Reschedule. Body: `fireAt`, `version`. 409 if not pending or if `version` is stale |
| `DELETE /tasks/{id}` | Cancel. 409 if not pending |
| `GET /tasks/{id}` | Inspect state |

`fireAt` must be between 1 second and 30 days in the future (§5); otherwise 400.

The retry policy is fixed rather than per-task: 5 attempts, backoff `2^attempt`
seconds (`RetryPolicy`). A per-task `retryPolicy` field was scoped out — it adds
request surface without exercising any new failure mode.

### Two layers of idempotency

**1. Submit-side — `idempotencyKey`, supplied by the client.**
Retried submissions caused by client-side network failure must not create
duplicate tasks. Enforced by a unique index; a conflicting insert returns the
existing task.

**2. Fire-side — `triggerId`, derived by the scheduler.**

> `triggerId` is deterministic and stable across every firing attempt of a task,
> including retries after downstream failure and re-fires after lease takeover.
> It is derived from `taskId`, not generated per attempt.

If `triggerId` were unique per attempt, a node freeze followed by takeover would
send two different ids for the same logical action, downstream deduplication
would not match them, and the business action would execute twice.

The attempt counter travels in a separate header for observability only. It must
never participate in deduplication.

## 4. Delivery semantics

- The system does not promise exactly-once. It promises **at-least-once firing
  with a stable `triggerId`**, delegating deduplication to the downstream
  consumer. End to end the effect is effectively-once.
- The system does not attempt to prevent duplicate fires caused by node stalls
  exceeding the lease TTL. A GC pause or SIGSTOP longer than the TTL is
  undetectable from inside the stalled process. The design instead makes
  duplicates cheap (stable `triggerId`) and visible (`duplicate_trigger_total`).
- Duplicate trigger rate under normal operation: **< 0.01%**, measured.

## 5. Service level objectives

| Dimension | Target | Result |
|---|---|---|
| Pending task capacity | 1,000,000 | met |
| Firing precision vs `fireAt` | P50 ≤ 200 ms, P99 ≤ 1 s | ~120 ms / ~480 ms |
| Delay range | 1 second to 30 days | enforced in `TaskValidation` |
| Submit throughput | 1,000 TPS | 2,309 TPS |
| Peak fire rate | 5,000 /s | 5,118 /s |
| Sink call timeout | 10 s, strictly below the 30 s lease TTL | `HttpSinkConfig` |
| Failover | see below | 41 s measured |

**Failover bound (revised).** The original target — "any single node dies, its
tasks resume within 10 s" — is not achievable with a 30 s lease TTL, and no part
of the implementation ever met it. Recovery is bounded by lease expiry, and the
shard and task chains run concurrently, not in series:

```
shard lease TTL (30 s) + heartbeat discovery (<= 10 s)  -> survivor owns the shard,  <= 40 s
task lease TTL (30 s)  + reaper sweep (<= 5 s)          -> row returned to pending,  <= 35 s
                                                        -> survivor's poll claims it, <= 0.2 s
```

Worst case is `max(40, 35) + 0.2 s` plus the downstream call, not the sum.
Measured in scenario 6: **41 s** frozen-to-recovered at production timings. See
ADR-004 decision 5. Reaching a 10 s bound would mean shortening the lease TTL,
which trades directly against tolerance for slow downstreams and GC pauses
(§7.5); that trade was not made.

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

`cancel` and `reschedule` are legal only from `pending`. Any other state returns
409.

The claim query treats `pending` and `retrying` identically: a `retrying` row
carries its backoff in `fire_at`, so it becomes claimable once the backoff has
elapsed.

A **reaper** sweeps rows stuck in `firing` past `lease_expires_at` and returns
them to `pending` for another node to claim. This is what turns a node freeze
into a recoverable duplicate rather than a lost task.

## 7. Key design decisions

### 7.1 Claim and transition in one statement

Selecting due tasks and acquiring their lease is a single statement, leaving no
intermediate state a crash can land in (`TaskRepository.claimDueTasks`):

```sql
UPDATE tasks
   SET state = 'firing',
       lease_owner = :leaseOwner,
       lease_expires_at = now() + (:ttlSeconds * interval '1 second'),
       version = version + 1,
       attempt_count = attempt_count + 1
 WHERE id IN (
   SELECT id FROM tasks
    WHERE shard IN (:shards)
      AND state IN ('pending', 'retrying')
      AND fire_at <= now()
    ORDER BY fire_at
    FOR UPDATE SKIP LOCKED
    LIMIT :limit
 )
RETURNING *, now() AS db_fired_at;
```

`:shards` is the node's currently owned shard set, re-read from the `shards`
table on every poll. `:limit` is `min(chronos.poll.claim-limit, free capacity in
the firing pool)` — see §7.7. `db_fired_at` is the database timestamp the
trigger-delay metric is computed from (§9, ADR-004 decision 1).

`FOR UPDATE SKIP LOCKED` lets multiple nodes poll overlapping shard sets without
blocking each other.

### 7.2 Leases live in PostgreSQL, not Redis

Lease acquisition and state transition must be atomic. Splitting them across
Redis and PostgreSQL creates a dual-write inconsistency window: a crash between
the two writes leaves Redis claiming ownership while PostgreSQL still shows
`pending`.

Lease acquisition is not on a latency-critical path, so the cost over Redis is
acceptable. See ADR-002.

### 7.3 Leases are per-shard, not per-task

Tasks map to a fixed set of 64 logical shards, computed once at insert time from
the task id's low bits (`ShardCalculator`). A node acquires leases on whole
shards and only polls within its own shards.

Per-task locking would mean a million lock acquisitions for a million tasks.
Per-shard leasing bounds contention to 64 regardless of task count.

### 7.4 Shard assignment and rebalance

- A `shards` table holds 64 rows: `shard_id, lease_owner, lease_expires_at, version`
- On startup a node claims one shard immediately so that peers can see it, waits
  out a settle window, then claims up to a soft cap of
  `ceil(64 / active_owners)`
- If every shard is already leased at startup, the node force-claims one anyway;
  otherwise it stays invisible to `countDistinctActiveOwners()` and the
  over-holding peer never recomputes its cap
- Leases renew every 10 s against a 30 s TTL
- A node holding more than the soft cap releases the excess on its next heartbeat
- A node that fails to renew (partition, stall) loses its shards to the next
  claimer

A node stops firing in a shard it has lost because the owned-shard set is re-read
from the database on every poll (`ownedShardsFor` filters on
`lease_expires_at >= now()`), so a lost shard is absent from `:shards` in §7.1.
The task-level `version` guard is a second, independent check: a stale completion
write for a task another node has since driven to `succeeded` affects zero rows.

There is no coordinator, no leader election, and no ZooKeeper. Contention is
resolved by the same conditional-update primitive used everywhere else. See
ADR-003.

### 7.5 Duplicates are made cheap, not prevented

The sink timeout (10 s) is set strictly below the lease TTL (30 s), so ordinary
slow downstreams never cause takeover. A stall exceeding 30 s — long GC, SIGSTOP,
host freeze — will cause a duplicate fire, and that is accepted.

The stable `triggerId` makes the duplicate harmless downstream, and
`duplicate_trigger_total` makes it visible. Chaos scenario 6 exercises the whole
path.

### 7.6 Redis was dropped entirely, not just narrowed

**Superseded.** The two narrow roles planned for Redis were each absorbed by
something already in the design, so no Redis instance exists anywhere in the
running system (not in `docker-compose.yml`, not a Gradle dependency, not in the
K8s manifest):

1. `triggerId` deduplication window — the demo receiver (`scripts/receiver.py`)
   uses an in-process `set()`. A real downstream owns its own dedup store
   regardless, per §2's non-goal.
2. Hot counters for dashboards — Prometheus and Micrometer counters/gauges (§9)
   serve this directly; there was never a second counter store.

ADR-002 covers why leases specifically never went to Redis; this section is the
broader point that the datastore was not needed at all.

### 7.7 Claim rate is bounded by firing capacity

The poll loop claims `min(claim-limit, idle threads + free queue slots)` against
a bounded `ThreadPoolExecutor`. Without that bound, a burst lets `firing` rows
accumulate past their own 30 s lease and the reaper re-issues tasks that were
never actually stuck — a capacity problem that registers on the same counter as a
genuine failure re-fire. Added after the peak-load measurement; see
`docs/performance.md` §2 and ADR-004 decision 7.

## 8. Storage

- **PostgreSQL** — source of truth for tasks, state, shard leases, version. The
  only datastore in the running system (§7.6).
- No in-process scheduling structure. Nodes poll their shards every 200 ms.

Indexes: `tasks_shard_state_fire_at_idx (shard, state, fire_at)` for the claim
query, and the partial index
`tasks_firing_lease_expires_idx WHERE state = 'firing'` for the reaper sweep and
the per-shard gauge. With the first, a poll that finds nothing due is a bounded
index range scan and stays cheap at a million pending rows.

## 9. Observability

Micrometer names below; the Prometheus endpoint exposes them with `.` replaced by
`_` (`chronos_trigger_delay_seconds` and so on).

| Metric | Purpose |
|---|---|
| `chronos.trigger.delay.seconds` (histogram) | Precision SLO, P50/P99 |
| `chronos.duplicate.trigger.total` | Makes the at-least-once claim measurable |
| `chronos.tasks.firing` (gauge, by shard) | Live per-shard load, rebalance evidence — renamed from `tasks_pending` (ADR-004 decision 3): an exact backlog count is a full-table scan at 1 M rows, a live firing count is not |
| `chronos.tasks.dead.total` | Dead-letter accumulation |
| `chronos.lease.takeover.total` | Failover occurring |
| `chronos.sink.call.duration.seconds` | Downstream latency |
| `chronos.poll.duration.seconds` | Cost of the polling loop under load |

## 10. Chaos scenarios

Each has a scripted, repeatable demonstration plus a consistency report.

1. **Kill a node mid-firing** — tasks taken over within the §5 bound, none lost
2. **Downstream returns 500** — retries with backoff, eventually dead-letters
3. **Duplicate submission with the same `idempotencyKey`** — one task created
4. **Cancel racing with fire** — one side wins cleanly, never both. What this
   demonstrates is outcome consistency across the HTTP response, the DB row, and
   the receiver record. Mutual exclusion itself comes from
   `WHERE state='pending' AND version=:v`, not from the test's timing — ADR-004
   decision 6.
5. **Network partition** — the partitioned node loses its shards and, on
   recovery, discovers this and stops firing rather than continuing
6. **Node freeze (`kill -STOP`) exceeding lease TTL.** The lease expires, another
   node takes over and re-fires with the same `triggerId`, the frozen node
   resumes (`kill -CONT`) and also fires, `duplicate_trigger_total` increments,
   and the downstream receiver deduplicates so the business action still occurs
   once.
7. **Clock offset** — a node whose wall clock is minutes ahead or behind still
   behaves correctly, because every correctness comparison (`fire_at <= now()`,
   `lease_expires_at < now()`) is evaluated by PostgreSQL. The scenario also
   verifies that trigger delay is computed from the database clock, not the node
   clock.

   Covered by `ClockOffsetTest` (behavior) and `TriggerDelayMetricTest` (metric:
   a `+300 s` skewed node reports ~2 s, not ~302 s). See ADR-004 decision 1.
8. **Clock rate drift** — a node whose clock runs slow measures node-local
   *durations* wrong, and two of those are load-bearing: the 10 s heartbeat
   interval against a 30 s lease TTL, and the 10 s sink timeout that keeps
   ordinary slow downstreams from triggering takeover.

   A node at 1/3 speed renews every 30 s of real time and loses its shards, and
   its 10 s sink timeout stretches to 30 s of real time — reproducing scenario
   6's double fire from drift rather than from freeze. Crossover confirmed at
   `rate = period / ttl` (1/3 at production values).

   Caveat carried into ADR-005: `ClockDriftTest` instruments the
   heartbeat-renewal collapse from both sides; the sink-timeout collapse follows
   the same formula but was never independently instrumented, because
   `DriftingClock` has no seam into `HttpClient`'s timer. Cite the heartbeat
   number as measured, the sink-timeout number as derived.

Both clock scenarios inject `java.time.Clock` at the node boundary rather than
faking time at the container level: offset via `Clock.offset`, drift via a
rate-scaled `Clock`. This keeps the tests deterministic and inside the
Testcontainers suite.

## 11. ADRs

All five accepted — see [`docs/adr/`](adr):

- [ADR-001](adr/001-polling-not-timing-wheel.md) — polling instead of a hierarchical time wheel
- [ADR-002](adr/002-leases-in-postgres.md) — leases in PostgreSQL rather than Redis
- [ADR-003](adr/003-shard-level-lease.md) — shard-level rather than task-level leases, and no leader
- [ADR-004](adr/004-metrics-semantics.md) — metric semantics: why duplicates are measured rather than prevented
- [ADR-005](adr/005-clock-model.md) — the clock model: which comparisons are database-authoritative, which durations are node-local, and the measured drift factor at which the design breaks
