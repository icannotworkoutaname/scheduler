# Performance — load test and tuning

Two passes: a **measure pass** (2026-09-01) that recorded the system as built,
and a **tune pass** (2026-09-02) that changed one thing at a time, re-ran the
affected phase, and recorded the number before moving on, so each gain is
attributable to a specific change. Items deliberately left unchanged are recorded
too.

## Environment

| | |
|---|---|
| Run dates | 2026-09-01 (measure), 2026-09-02 (tune) |
| Host | WSL2 (Ubuntu) on Windows 11, 20 vCPU, 15 GB RAM |
| Postgres | 16.14 (alpine) in Docker Desktop, stock config |
| App | JDK 21, Spring Boot 4.1, 2 nodes as `java -jar --server.port=8080/8081` on the host |
| Sink (measure) | `scripts/receiver.py` (Python `ThreadingHTTPServer`) |
| Sink (tune) | nginx `return 200` container — Python's GIL made the measure-pass sink the bottleneck, see §2 |
| Load client (measure) | `scripts/loadtest_submit.py` (urllib + threads) |
| Load client (tune) | `wrk` with a Lua script issuing unique `idempotencyKey`s |
| Re-run | `scripts/loadtest.sh {plans\|capacity\|peak\|submit}` |

## SLO scorecard (requirements.md §5)

| SLO | target | measure | tune | |
|---|---|---|---|---|
| trigger delay P50 | ≤ 200 ms | ~120 ms | ~120 ms | met (untouched) |
| trigger delay P99 | ≤ 1 s | ~480 ms | ~480 ms | met (untouched) |
| **peak trigger rate** | ≥ 5,000 /s | ~4,400 /s — missed | **~5,118 /s** | met |
| **submission throughput** | ≥ 1,000 TPS | ~1,450 TPS (client-limited) | **~2,309 TPS** | met |
| idle poll cost, empty → 1 M | no growth | 3.0 ms P50 | 3.0 ms P50 | met (untouched) |

---

## ① `tasks_pending` gauge full-table scan

**Change:** the gauge now counts what each shard is firing right now
(`SELECT shard, count(*) FROM tasks WHERE state='firing' GROUP BY shard`), served
by the partial index `tasks_firing_lease_expires_idx`. Renamed
`chronos.tasks.pending` → `chronos.tasks.firing`.

| | before | after |
|---|---|---|
| query | `state IN (pending,retrying) GROUP BY shard` | `state='firing' GROUP BY shard` |
| plan at 1 M rows | Parallel Seq Scan, whole 142 MB table | Index Scan on `tasks_firing_lease_expires_idx` |
| `EXPLAIN ANALYZE` | **94 ms** | **0.9 ms** idle, ~15 ms mid-burst |
| buffers | ~18,200 (~142 MB) | ~190 |

Two rejected intermediates: (a) bounding `fire_at` alone is still a Seq Scan,
because `(shard, state, fire_at)` leads with `shard` and PG 16 has no skip scan;
(b) a `generate_series(0,63)` correlated subquery to force per-shard index use ran
5 ms on a fresh load but 300 ms under a real drain, because "due in 5 min" then
holds ~80 k rows and an exact count is O(matches). `state='firing'` is both cheap
(backpressure keeps firing to a few thousand) and the right question for "are the
shards balanced": it is the live work distribution, 0 everywhere when idle.

## ② Peak trigger rate — locate the ceiling, then raise it

**Step 1 — where is the 1,950/s ceiling?** Swap the Python receiver for an nginx
`return 200` and re-run the 100 k burst, nothing else changed:

| sink | pipeline throughput (succeeded/s) | `firing` backlog peak | re-fires |
|---|---|---|---|
| `receiver.py` (measure) | ~1,950 /s | 60,785 | ~2,000 |
| nginx (tune) | **~4,400 /s** | 3,270 | **0** |

The ceiling was the test sink. Python's GIL serialises request handling in
`ThreadingHTTPServer` and caps it at ~1,950/s regardless of thread count. With a
faster downstream the pipeline runs twice as fast and the measure pass's re-fire
storm (leases expiring while handlers queued) does not occur — handlers keep up,
nothing expires. The measure pass's "pipeline throughput" and "duplicate
undercount" findings were both artifacts of the slow test sink.

**Step 2 — the real claim ceiling.** `LIMIT 500` × ~5 polls/s × 2 nodes ≈ 4,400/s
measured, short of 5,000. Raise `chronos.poll.claim-limit` 500 → 2,000:

| | claim/s peak | `firing` peak | re-fires |
|---|---|---|---|
| limit 500 (measure) | 4,400 | 60,785 | ~2,000 |
| limit 2,000, no backpressure | **10,219** | **71,214** | 0 |
| limit 2,000, with backpressure | **5,118** | **3,549** | 0 |

Raising the limit alone clears the SLO (10 k/s) but lets `firing` reach 71 k:
claiming now outruns the ~5,000/s pipeline by a wide margin.

**Step 3 — backpressure.** `taskExecutor` is a `ThreadPoolExecutor` with a bounded
queue (16 threads + 2,000 slots), and the poll loop claims
`min(claim-limit, idle threads + free queue slots)`. Claiming self-throttles to
what the pool can fire inside the 30 s lease.

- nginx sink: 5,118/s peak (SLO met), `firing` bounded at 3,549, 0 re-fires.
- Re-checked against the slow `receiver.py` sink:

| | before backpressure | after |
|---|---|---|
| `firing` peak | 60,785 | **3,969** |
| re-fires / reclaimed leases | ~2,000 / ~2,000 | **0 / 0** |
| claim rate | 4,400/s then stalls | tracks the downstream, ~1,820/s |

With a genuinely slow downstream the system now slows its claiming to match
instead of thrashing. Every task still reaches `succeeded`.

The 5,000/s SLO is met at 5,118/s with the nginx sink; no requirement revision.
Measurement condition stated: `return 200` downstream. A slow real downstream
caps the rate below the SLO rather than piling up.

## ③ Submission throughput — measure with a real client

The measure pass's 1,450 TPS was `urllib` + threads plateauing regardless of
concurrency: the client, not the server. Re-measured with `wrk` (unique keys per
request, `wrk-t{thread}-r{counter}`, rows verified ≈ requests):

| | TPS | mean Hikari acquire wait | latency P50 / P99 |
|---|---|---|---|
| measure, `urllib` client, pool 10 | 1,450 | 0.35 ms* | — |
| **tune, `wrk`, pool 10** | **2,309** | **32 ms** | 41 ms / 66 ms |
| tune, `wrk`, pool 32 (diagnostic) | 6,880 | — | 13 ms / 185 ms |

\* the measure-pass acquire number was low because the client could not contend
the pool — see ④.

The ≥ 1,000 TPS SLO is met at the default pool (2,309). No committed change.

## ④ Connection pool — measured, not changed

The measure pass read the pool as healthy (0.35 ms acquire, 0 timeouts), which was
an artifact of a client that could not push hard. Under `wrk` at pool 10 the mean
acquire wait is **32 ms** (96 connections contending for 10), and every request's
latency has the same ~41 ms floor with low stdev — the pool-wait signature. Pool
32 gives 3× the throughput.

Not changed: the 1,000 TPS SLO is met with margin at the default, and submission
throughput is not the binding constraint. Recorded here so that when it becomes
one, `spring.datasource.hikari.maximum-pool-size` is the knob, bounded by
`max_connections` (100) ÷ node count.

## Idle poll cost — re-confirmed, unchanged

| | P50 | P99 |
|---|---|---|
| empty DB | 3.4 ms | 19 ms |
| 1 M pending rows | 3.0 ms | 7.9 ms |

No growth from empty to a million pending rows — the ADR-001 evidence. Not
touched in the tune pass.

---

## §0 — query plans on 1 M rows (unchanged queries, for reference)

| query | plan | exec |
|---|---|---|
| reaper `state='firing' AND lease_expires_at < now()` (few firing rows) | Index Scan on `tasks_firing_lease_expires_idx` | 0.9 ms |
| reaper, 100 k firing rows in a 100 k table | Seq Scan (partial index dropped when `state='firing'` matches nearly all rows) | 10.8 ms |
| poll claim | Bitmap Index Scan on `tasks_shard_state_fire_at_idx` | 0.78 ms |

The measure pass predicted a reaper full scan at a million rows; it did not occur
under normal load, because the V1 partial index is shaped for that query. It
degrades only when `firing` rows dominate the table, which a realistic 1 M-row
table plus burst does not produce. The full scan that did appear at scale was ①,
now fixed.

## Open findings

- **Reaper Seq-scans when `firing` dominates the table.** Degenerate case, low
  priority.
- **`duplicate_trigger_total` undercounts under a re-fire storm** (7 vs ~2,000).
  Moot now that backpressure prevents the storm; the receiver `/stats` remains the
  source for the duplicate figure regardless.
- **Connection pool** is the next submission-throughput ceiling once that matters
  (④).
