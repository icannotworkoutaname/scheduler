# Chronos

A distributed delayed-task scheduler: N identical nodes over one Postgres, no
coordinator. At-least-once firing with a deterministic `triggerId`; a node
dying mid-flight costs a duplicate delivery, never a lost or double-executed
task.

![failover](docs/img/failover.png)

**Node kill under load.** The survivor claims the dead node's 32 shards in
~30–40 s — the 30 s shard lease TTL plus one heartbeat. Throughput drops while
those shards are unowned, then recovers, with a spike as the backlog drains.
No in-flight task is lost.

![duplicate](docs/img/duplicate.png)

**Node freeze past the lease TTL** (`kill -STOP`). The other node re-fires the
task with the same `triggerId`. When the frozen node resumes, its write hits a
row already `succeeded`, and the conditional update is a no-op:
`duplicate_trigger_total` goes 0 → 1, the downstream deduplicates on
`triggerId`, the action ran once.

![precision](docs/img/precision.png)

**At 1,000,000 pending rows**, `trigger_delay` (measured on the database
clock) holds P50 ≈ 120 ms / P99 ≈ 480 ms against SLOs of 200 ms / 1 s.

Peak **5,118 triggers/s** against a `return 200` downstream — a slow
downstream makes the scheduler throttle claiming to match rather than pile up.
Submit **2,309 TPS**. Idle poll cost flat at **3.0 ms**, empty database through
1 M pending rows.

---

## What it is

Chronos fires a callback (`POST` to a `callbackUrl`) at a given `fireAt`. It
runs as N identical nodes against one Postgres. Work is split across 64 logical
shards; a node leases a share of them and only fires tasks whose
`shard = uuid.leastSignificantBits & 63` it currently owns. There is no leader
— the `shards` table is the coordination primitive, contended with conditional
updates.

- **Submit** — `POST /tasks` with an `idempotencyKey`; a unique constraint makes
  a retried submit return the existing task rather than a second one.
- **Fire** — a 200 ms poll claims due tasks with `FOR UPDATE SKIP LOCKED`, hands
  them to a bounded worker pool, and `POST`s the callback. `triggerId` is derived
  from the task id, so it is identical across every retry and every re-fire.
- **Recover** — task and shard leases have a TTL. A node that dies stops renewing;
  its shards and in-flight tasks are picked up by whoever is still polling. No
  node detects the death — lease expiry does the work.
- **Retry / dead-letter** — failed callbacks retry with exponential backoff for up
  to 5 attempts, then move to `dead`.

Requirements and state machine: [`docs/requirements.md`](docs/requirements.md).
The ADRs below record the decisions behind the implementation.

## Running it

```bash
docker compose up -d                 # postgres + prometheus + grafana + renderer
./gradlew bootJar
java -jar build/libs/scheduler-0.0.1-SNAPSHOT.jar --server.port=8080 &
java -jar build/libs/scheduler-0.0.1-SNAPSHOT.jar --server.port=8081 &
```

- API: `http://localhost:8080`
- Grafana (anonymous): `http://localhost:3000/d/chronos-overview` — the panels
  above are provisioned; `grafana/README.md` has the screenshot procedure.
- On WSL2 run `scripts/monitoring-up.sh` instead of `docker compose up`: it pins
  the Prometheus scrape target to the distro IP.

### One-command demo

```bash
make demo         # accelerated: 8 s lease TTL, scenario runs in ~30 s
make demo-slow    # production timings: 30 s lease TTL, ~90 s
```

Brings up Postgres, builds the jar, starts a downstream receiver and two nodes,
waits for the shards to split, runs the node-freeze scenario, and prints a
result table (`chronos_duplicate_trigger_total`, business actions, tasks
lost/stuck). A `trap` cleans up every process on any exit path.
`make demo-down` stops the compose stack.

<video src="docs/img/demo.mp4" controls width="700"></video>

`make demo` running end to end: the terminal's `[4/6]`/`[5/6]` narration and the
`duplicate_trigger_total` panel stepping 0 → 1 land in the same beat — the
absorbed duplicate the terminal describes is the line the dashboard draws.

## Kubernetes

`k8s/chronos.yaml` runs the scheduler as a plain `replicas: 2` Deployment plus
an ephemeral Postgres — no StatefulSet, no leader election. Build steps are in
the file header. The manifest demonstrates orchestrability; the chaos scenarios
are validated in the local compose environment (`make demo`), not here.

## Design decisions

ADRs under [`docs/adr/`](docs/adr):

- **[001 — polling, not a timing wheel](docs/adr/001-polling-not-timing-wheel.md)**
  — an in-memory wheel would be faster in principle but adds a cascade step, a
  rebuild on every rebalance, and a second copy of state to keep consistent with
  Postgres. A poll against an index costs 3.0 ms idle, unchanged from an empty
  database to 1 M rows.
- **[002 — leases in Postgres, not Redis](docs/adr/002-leases-in-postgres.md)** —
  splitting lease ownership from task state reopens a dual-write window that the
  rest of the design exists to close; lease acquisition is not on a
  latency-critical path.
- **[003 — shard-level lease, no leader](docs/adr/003-shard-level-lease.md)** —
  64 fixed shards, leaderless coordination through the `shards` table. Converges
  without oscillation in both a chaos test and a K8s rolling restart
  (4 owners → 2, zero pod restarts).
- **[004 — metrics semantics](docs/adr/004-metrics-semantics.md)** — how each of
  the seven metrics is measured and why: trigger delay on the *database* clock,
  so a skewed node cannot distort it; `duplicate_trigger` counting a duplicate
  *absorbed* rather than prevented, and what that counter means during a freeze
  versus under peak load; the per-shard gauge as live firing count, not backlog.
- **[005 — clock injection for scenarios 7/8](docs/adr/005-clock-model.md)** —
  `java.time.Clock` as the only injection seam, and why the heartbeat's 1/3
  drift-collapse coefficient is measured while the equivalent sink-timeout
  coefficient is only derived.

## Chaos scenarios

Eight scripted failure reproductions under [`scripts/`](scripts): node kill,
persistent downstream failure, concurrent duplicate submit, cancel-vs-fire race,
network partition, node freeze, clock skew, clock drift. Scenarios 1–4, 7 and 8
also run in CI as Testcontainers tests (`.github/workflows/ci.yml`); 5 and 6
remain dev scripts because they need `kill -STOP` and a TCP proxy.

Two results the tests pin down:

- **Clock drift** — the design tolerates a node clock as slow as **1/3 real
  speed**; at exactly `heartbeat_period / lease_ttl` both safety margins collapse
  against the TTL at once. `ClockDriftTest` confirms the coefficient from both
  sides.
- **Shard rebalance** — a node that loses the startup race and finds every shard
  leased force-claims one to stay visible, then rebalances downward.
  `ShardRebalanceConvergenceTest` asserts it reaches a fixed point and stops
  moving.

## Load test

Numbers, methods, query plans, and the measure-then-tune before/after are in
[`docs/performance.md`](docs/performance.md). Re-run with
`scripts/loadtest.sh {plans|capacity|peak|submit}`.
