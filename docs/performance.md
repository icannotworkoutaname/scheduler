# Performance — load test (Block 5, "8/26")

**Discipline for this pass: measure, record, root-cause. No tuning.** Fixes and
before/after numbers are 8/27's job. When this file is edited then, add an
`after` column — do not overwrite the `before` numbers.

## Environment

| | |
|---|---|
| Run date | 2026-09-01 |
| Host | WSL2 (Ubuntu) on Windows 11, 20 vCPU, 15 GB RAM |
| Postgres | 16.14 (alpine) in Docker Desktop — **stock config**: shared_buffers 128 MB, work_mem 4 MB, effective_cache_size 4 GB, random_page_cost 4, jit on, max_parallel_workers_per_gather 2 |
| App | JDK 21, Spring Boot 4.1, 2 nodes as `java -jar --server.port=8080/8081` on the host |
| Sink | `scripts/receiver.py` (Python `ThreadingHTTPServer`) on :9000, fail-rate 0 |
| Defaults | heartbeat 10 s, poll 200 ms, reaper 5 s, shard-lease TTL 30 s, claim `LIMIT 500`, `taskExecutor` = fixed pool of 16 with an unbounded queue, HikariCP pool = 10 |
| Re-run | `scripts/loadtest.sh {plans\|capacity\|peak\|submit}` |

## SLOs under test (requirements.md §5)

| SLO | target | result | |
|---|---|---|---|
| trigger delay P50 | ≤ 200 ms | **~120 ms** (steady 278/s) | ✅ |
| trigger delay P99 | ≤ 1 s | **~480 ms** (steady 278/s) | ✅ |
| peak trigger rate | ≥ 5,000 / s | **~4,400 / s** | ❌ −12 % |
| submission throughput | ≥ 1,000 TPS | **~1,450 TPS** | ✅ (client-limited; true ceiling higher) |
| idle poll cost, empty → 1 M | no growth | 3.0 ms P50 / 7.9 ms P99 at 1 M pending | ✅ |

Three **separate** loads — the numbers do not share a run.

- **Capacity + precision** — 1,000,000 tasks, `fire_at` uniform over the next
  hour (~278/s average). Bulk-loaded via `COPY` (11 s), `shard` precomputed as
  `uuid.bytes[15] & 0x3F` (= `ShardCalculator.shardFor`, cross-checked against
  8 API-submitted tasks). `fire_at` offset +300 s so there is a clean idle
  window.
- **Peak** — 100,000 tasks, `fire_at` all at `now + 50 s`. Bulk-loaded.
- **Submission TPS** — 50,000 tasks via `POST /tasks`, 96 concurrent workers
  (`scripts/loadtest_submit.py`).

---

## §0 — Query plans on 1 M rows (`EXPLAIN ANALYZE`)

| query | plan | exec time | buffers |
|---|---|---|---|
| **reaper** — `state='firing' AND lease_expires_at < now()` (0 firing rows) | **Index Scan** on `tasks_firing_lease_expires_idx` | 0.9 ms | 1 |
| **reaper** — same, with **100 k firing rows in a 100 k table** | **Seq Scan** (planner drops the partial index once `state='firing'` matches ~everything) | 10.8 ms | 2,849 |
| **poll claim** — `shard IN (0..31) AND state IN (pending,retrying) AND fire_at ≤ now() ORDER BY fire_at LIMIT 500` | **Bitmap Index Scan** on `tasks_shard_state_fire_at_idx` → sort → limit | 0.78 ms | 204 |
| **poll claim** — same, nothing due | same shape | 0.65 ms | 204 |
| **`tasks_pending` gauge** — `state IN (pending,retrying) GROUP BY shard` | **Parallel Seq Scan** of the whole 142 MB table, 2 workers | **91 ms** | **18,206** |

The reaper prediction ("百万行下极可能全表扫描") is **not borne out under normal
load** — the partial index `tasks_firing_lease_expires_idx` (V1) is exactly
shaped for the reaper's `WHERE`, and it is chosen (0.9 ms). It degrades to a
Seq Scan only when `firing` rows are a large fraction of the table, which the
peak burst produced but a steady million-pending table does not.

The full-table scan that *does* show up at scale is the **8/24 pending-count
gauge** — see Finding 1.

---

## §1 — Capacity + precision (1 M, steady ~278/s)

12-minute observation while ~200 k of the 1 M drained.

| minute | triggers/s | trigger P50 | trigger P99 | poll P99 |
|---|---|---|---|---|
| 3 | 172 | 107 ms | 453 ms | 24 ms |
| 6 | 278 | 116 ms | 479 ms | 99 ms |
| 9 | 277 | 136 ms | 489 ms | 100 ms |
| 12 | 279 | 112 ms | 473 ms | 99 ms |

- Throughput tracked the arrival rate exactly (278/s in = 278/s fired). Pending
  drained monotonically. **0 duplicate triggers, 0 dead-letters, 0 takeovers.**
- **trigger delay P50 ≈ 120 ms, P99 ≈ 480 ms — both well inside SLO** at a
  million pending rows.
- **Sink call P99 ≈ 8.6 s, with ~6.6 % of calls > 1 s.** This is `receiver.py`,
  not the scheduler: the scheduler-side latency (trigger delay = claim) stayed
  flat at 480 ms P99 while the *outbound HTTP* was slow. A Python
  `ThreadingHTTPServer` at 278 req/s stalls ~1 request in 15 for seconds (GIL /
  thread scheduling). See Finding 6.

## §2 — Peak (100 k, `fire_at` in one instant)

3-second sampling across the burst:

| t (from due) | claim/s (10 s rate) | firing rows | succeeded |
|---|---|---|---|
| +3 s | 1,994 | 14,118 | 6,451 |
| +9 s | **4,400** | 32,155 | 16,418 |
| +15 s | 4,300 | 49,990 | 27,597 |
| +18 s | 3,300 | **60,785** | 39,295 |
| +21 s | 0 (all claimed) | 54,926 | 45,161 |
| +54 s | — | 3 | 99,997 |

- **Peak claim rate 4,400/s** — short of the 5,000/s SLO by 12 %. Ceiling is
  `LIMIT 500` × 5 polls/s × 2 nodes = 5,000/s *theoretical*; real poll cycles
  take > 0 ms so the effective rate is lower. Finding 2.
- **Pipeline throughput (claim → fire → mark) ≈ 1,950/s** across both nodes.
  Claim outran it 2.3 : 1, so `firing` rows piled up to **60,785** before the
  claim side ran out of pending work. Finding 3.
- **trigger delay P99 hit the 5 s top bucket** (tail ~20 s) for the ~80 s the
  burst took to drain — SLO violated for the duration, which is inherent to a
  burst larger than the drain rate.
- **~2,000 duplicate deliveries.** Tasks claimed early had their 30 s lease
  expire while their handler sat in the executor queue; the reaper reclaimed
  them, another node re-fired. The receiver deduped all 2,000. **Every task
  ended `succeeded` — 0 lost, 0 dead, `lease_takeover_total` = 0.** Correctness
  held under 4,400/s + a 60 k firing backlog.
- **App `duplicate_trigger_total` = 7, receiver `/stats` = 2,000.** The app
  metric only fires when the late handler's 0-row update coincides with
  `state = succeeded`; under a re-fire storm most late deliveries land while the
  task is `firing` or `pending`. Finding 4.

## §3 — Submission TPS (50 k via `POST /tasks`)

| run | workers | result |
|---|---|---|
| 1 | 96 | **1,460 TPS** (50,000 × 201) |
| 2 | 160 | 1,417 TPS |

- **SLO ≥ 1,000 TPS met.** Flat across 96 → 160 workers.
- Hikari (pool 10) during the run: mean acquire 0.35 ms, mean hold 3.0 ms, max
  acquire 148 ms, **0 timeouts**. Low mean acquire + the plateau regardless of
  client concurrency ⇒ the **Python `urllib` + threads client is the likely
  co-limiter**, not the pool or the server. True server ceiling is ≥ 1,450 and
  **not precisely characterised** — 8/27 needs a keepalive tool (wrk/hey) for
  the real number. Finding 7.

## §4 — Idle poll cost: empty vs 1 M pending

| | `poll_duration_seconds` P50 | P99 |
|---|---|---|
| empty DB (8/24) | 3.4 ms | 19 ms |
| **1 M pending rows** | **3.0 ms** | **7.9 ms** |

**No growth from empty to a million pending rows.** The claim query walks
`tasks_shard_state_fire_at_idx` only as far as the `fire_at ≤ now()` boundary
(~204 buffers, all cache hits) and stops; table size never enters. This is the
concrete evidence for ADR-001 (polling over a timer wheel): the idle poll is a
bounded index probe, not a scan.

---

## Findings — no code changed (priority order for 8/27)

1. **`tasks_pending` gauge is a 91 ms parallel full-table Seq Scan every 15 s
   at 1 M rows.** `state IN (pending,retrying)` matches ~every row, so no index
   can help. Not catastrophic — 0.6 % duty cycle, and PG's bulk-read ring
   buffer keeps it out of `shared_buffers` — but it is a standing CPU + IO cost
   that scales with the table. Options: sample every 60 s not 15 s; drop the
   per-shard breakdown for a single total; or maintain the count incrementally
   in the app instead of querying.

2. **Peak claim ceiling 4,400/s < 5,000/s SLO.** Raise `LIMIT` (500 → e.g.
   2,000), and/or shorten the poll interval (200 ms → 100 ms), and/or add a
   third node. Any of these gives a clean before/after against the 5,000/s SLO.

3. **Pipeline throughput ~1,950/s** — `taskExecutor` (16 threads, *unbounded*
   queue) + sink latency. Under a burst, claim outpaces it 2 : 1 → firing
   backlog → lease-expiry re-fires. Size the executor to the target rate, bound
   the queue, and/or gate claiming to the processing rate so the scheduler
   doesn't claim work it can't service within the lease TTL. This is the fix
   that also removes the peak-burst duplicate storm.

4. **`duplicate_trigger_total` undercounts under load** (7 vs 2,000 real
   deliveries). Correct per ADR-004 decision 2's scope (`state = succeeded`),
   but it means the 8/28 duplicate figure should be sourced from the receiver
   `/stats`, and the metric's help text / README caption should read
   "≥ N duplicates absorbed", a lower bound.

5. **Reaper Seq-scans when `firing` dominates the table** (10.8 ms at
   100 k-of-100 k). A realistic 1 M table with a burst keeps `firing` a
   minority and the partial index is still chosen; low priority. A
   `(state, lease_expires_at)` composite would not help the degenerate case.

6. **Sink P99 ≈ 8.6 s / 6.6 % > 1 s is `receiver.py`.** 8/27's load test needs
   a compiled or nginx (`return 200`) sink to measure the scheduler's true
   sink-facing capacity; the current tail is the Python server, not Chronos.

7. **Submission TPS not cleanly characterised** — the `urllib`/threads client
   plateaus at ~1,450 regardless of concurrency while Hikari shows headroom.
   Needs a proper HTTP load tool with connection reuse.

### What this means for 8/27's priority

- The **capacity** P50/P99 SLOs are already comfortably met — no tuning needed
  there.
- The **peak** SLO (5,000/s) is the real gap, and Findings 2 + 3 are the same
  fix surface (claim faster *and* drain faster, in balance).
- The reaper is **not** the problem that was predicted; the analogous
  "full scan at scale" issue is the pending-count gauge (Finding 1).
