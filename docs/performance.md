# Performance — load test & tuning (Block 5, "8/26" measure, "8/27" tune)

8/26 measured; 8/27 tuned. Each change was made alone, its phase re-run, and
the number recorded before moving to the next — so a gain is attributable to a
specific change. Items left unchanged are recorded too: "not changing it" is
also a result.

## Environment

| | |
|---|---|
| Run dates | 2026-09-01 (measure), 2026-09-02 (tune) |
| Host | WSL2 (Ubuntu) on Windows 11, 20 vCPU, 15 GB RAM |
| Postgres | 16.14 (alpine) in Docker Desktop — **stock config** |
| App | JDK 21, Spring Boot 4.1, 2 nodes as `java -jar --server.port=8080/8081` on the host |
| Sink (8/26) | `scripts/receiver.py` (Python `ThreadingHTTPServer`) |
| Sink (8/27) | nginx `return 200` container (Python's GIL made the 8/26 sink the bottleneck — see §2) |
| Load client (8/26) | `scripts/loadtest_submit.py` (urllib + threads) |
| Load client (8/27) | `wrk` with a Lua script issuing unique `idempotencyKey`s |
| Re-run | `scripts/loadtest.sh {plans\|capacity\|peak\|submit}` |

## SLO scorecard (requirements.md §5)

| SLO | target | 8/26 | 8/27 | |
|---|---|---|---|---|
| trigger delay P50 | ≤ 200 ms | ~120 ms | ~120 ms | ✅ (untouched) |
| trigger delay P99 | ≤ 1 s | ~480 ms | ~480 ms | ✅ (untouched) |
| **peak trigger rate** | ≥ 5,000 / s | ~4,400 / s ❌ | **~5,118 / s** | ✅ |
| **submission throughput** | ≥ 1,000 TPS | ~1,450 TPS (client-limited) | **~2,309 TPS** (measured properly) | ✅ |
| idle poll cost, empty → 1 M | no growth | 3.0 ms P50 | 3.0 ms P50 | ✅ (untouched) |

---

## ① `tasks_pending` gauge full-table scan

**Change:** the gauge now counts what each shard is **firing right now**
(`SELECT shard, count(*) FROM tasks WHERE state='firing' GROUP BY shard`),
served by the partial index `tasks_firing_lease_expires_idx`. Renamed
`chronos.tasks.pending` → `chronos.tasks.firing`.

| | before | after |
|---|---|---|
| query | `state IN (pending,retrying) GROUP BY shard` | `state='firing' GROUP BY shard` |
| plan @ 1 M rows | Parallel Seq Scan, whole 142 MB table | Index Scan on `tasks_firing_lease_expires_idx` |
| `EXPLAIN ANALYZE` | **94 ms** | **0.9 ms** (idle), ~15 ms mid-burst |
| buffers | ~18,200 (~142 MB) | ~190 |

Two dead ends on the way: (a) bounding `fire_at` alone — still a Seq Scan,
`(shard, state, fire_at)` leads with `shard` and PG 16 has no skip scan;
(b) a `generate_series(0,63)` correlated subquery to force per-shard index use —
5 ms on a *fresh* load but **300 ms** under a real drain, because "due in 5 min"
then holds ~80 k rows and an exact count is O(matches). `state='firing'` is the
question that's both cheap (backpressure keeps firing to a few thousand) and
the right one for "are the shards balanced": it's the live work distribution, 0
everywhere when idle.

## ② Peak trigger rate — locate the wall, then fix

**Step 1 — where is the 1,950/s ceiling?** Swap the Python receiver for an
nginx `return 200` and re-run the 100 k burst, nothing else changed:

| sink | pipeline throughput (succeeded/s) | `firing` backlog peak | re-fires |
|---|---|---|---|
| `receiver.py` (8/26) | ~1,950 / s | 60,785 | ~2,000 |
| nginx (8/27) | **~4,400 / s** | 3,270 | **0** |

**The wall was the test sink.** Python's GIL serialises request handling in
`ThreadingHTTPServer`; it caps at ~1,950/s regardless of how many threads. With
a real downstream the pipeline runs 2× faster and the 8/26 re-fire storm
(leases expiring while handlers queued) **vanishes** — handlers keep up, nothing
expires. 8/26's "pipeline throughput" and "duplicate undercount" findings were
both artifacts of the slow test sink.

**Step 2 — the real claim ceiling.** `LIMIT 500` × ~5 polls/s × 2 nodes ≈
4,400/s measured, short of 5,000. Raise `chronos.poll.claim-limit` 500 → 2,000:

| | claim/s peak | `firing` peak | re-fires |
|---|---|---|---|
| limit 500 (8/26) | 4,400 | 60,785 | ~2,000 |
| limit 2,000, no backpressure | **10,219** | **71,214** | 0 |
| limit 2,000, **+ backpressure** | **5,118** ✅ | **3,549** | 0 |

Raising the limit alone clears the SLO by miles (10 k/s) but lets `firing`
balloon to 71 k — claim now vastly outruns the ~5,000/s pipeline. So:

**Step 3 — backpressure.** `taskExecutor` is now a `ThreadPoolExecutor` with a
**bounded** queue (16 threads + 2,000 slots); the poll loop claims
`min(claim-limit, idle threads + free queue slots)`. Claim self-throttles to
what the pool can fire inside the 30 s lease.

- nginx sink: **5,118/s peak (SLO met), `firing` bounded at 3,549, 0 re-fires.**
- Defensive check with the **slow** `receiver.py` sink again:

| | 8/26 (no backpressure) | 8/27 (backpressure) |
|---|---|---|
| `firing` peak | 60,785 | **3,969** |
| re-fires / reclaimed leases | ~2,000 / ~2,000 | **0 / 0** |
| claim rate | 4,400/s then stalls | tracks the downstream: ~1,820/s |

With a genuinely slow downstream the system now slows its claiming to match,
instead of thrashing. Every task still reaches `succeeded`.

**SLO decision: 5,000/s is met (5,118/s) with the nginx sink — no requirement
revision.** Measurement condition stated: `return 200` downstream; a slow real
downstream will correctly cap the rate below SLO rather than pile up.

## ③ Submission throughput — measure with a real client

8/26's 1,450 TPS was `urllib` + threads plateauing regardless of concurrency —
the client, not the server. Re-measured with `wrk` (unique keys per request;
`wrk-t{thread}-r{counter}`, verified rows ≈ requests):

| | TPS | mean Hikari acquire wait | latency P50 / P99 |
|---|---|---|---|
| 8/26 `urllib` client, pool 10 | 1,450 | 0.35 ms* | — |
| **8/27 `wrk`, pool 10** | **2,309** | **32 ms** | 41 ms / 66 ms |
| 8/27 `wrk`, pool 32 (diagnostic) | 6,880 | — | 13 ms / 185 ms |

\* the 8/26 acquire number was low **because the weak client couldn't contend
the pool** — see ④.

**SLO (≥ 1,000 TPS) is met at the default pool (2,309).** No committed change.

## ④ Connection pool — measured, not changed

8/26 read the pool as healthy (0.35 ms acquire, 0 timeouts) — but that was an
artifact of a client that couldn't push hard. `wrk` shows the truth: at pool 10
the mean acquire wait is **32 ms** (96 connections contending 10), and every
request's latency has the same ~41 ms floor (low stdev) — the pool-wait
signature. Pool 32 → 3× the throughput.

**Not changed, because the 1,000 TPS SLO is met with margin at the default and
"submission throughput" is not currently the binding constraint.** Recorded
here so that when it *is* — or when a real client load reveals it in prod —
`spring.datasource.hikari.maximum-pool-size` is the knob, bounded by
`max_connections` (100) ÷ node count.

## Idle poll cost — unchanged, re-confirmed

| | P50 | P99 |
|---|---|---|
| empty DB (8/24) | 3.4 ms | 19 ms |
| 1 M pending rows (8/26) | 3.0 ms | 7.9 ms |

No growth from empty to a million pending rows — the ADR-001 evidence. Not
touched on 8/27.

---

## §0 — query plans on 1 M rows (unchanged queries, for reference)

| query | plan | exec |
|---|---|---|
| reaper `state='firing' AND lease_expires_at < now()` (few firing rows) | Index Scan on `tasks_firing_lease_expires_idx` | 0.9 ms |
| reaper, 100 k firing rows in a 100 k table | Seq Scan (partial index dropped when `state='firing'` matches ~all) | 10.8 ms |
| poll claim | Bitmap Index Scan on `tasks_shard_state_fire_at_idx` | 0.78 ms |

The 8/26 reaper prediction ("full scan at a million rows") did not hold under
normal load — the V1 partial index is shaped for it. It degrades only when
`firing` rows dominate the table, which a realistic 1 M-row table + burst does
not produce. The full scan that *did* show up at scale was ① — now fixed.

## Findings still open for later

- **Reaper Seq-scans when `firing` dominates the table** (degenerate; low priority).
- **`duplicate_trigger_total` undercounts under a re-fire storm** (7 vs 2,000).
  Moot now that backpressure prevents the storm; the receiver `/stats` stays
  the source for the 8/28 duplicate figure regardless.
- **Connection pool** is the next submission-throughput ceiling once that
  matters (④).
