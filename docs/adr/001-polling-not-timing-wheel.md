# ADR-001: Polling over a shared index, not an in-memory timing wheel

Status: accepted

## Context

"Fire whatever is due" is naturally a timer-wheel problem: bucket tasks by
`fire_at`, advance a wheel, fire the bucket that comes due. This ADR records why
that was rejected in favour of a poll against a Postgres index, and the numbers
the rejection rests on.

## Decision

Each node runs a 200 ms `@Scheduled` poll. `TaskRepository.claimDueTasks` is a
single `UPDATE ... WHERE id IN (SELECT ... WHERE shard IN (:shards) AND state IN
('pending','retrying') AND fire_at <= now() ORDER BY fire_at FOR UPDATE SKIP
LOCKED LIMIT :limit) RETURNING *`, backed by
`tasks_shard_state_fire_at_idx (shard, state, fire_at)`. No in-memory schedule
exists; "what is due" is answered from Postgres on every poll.

A timing wheel would be faster in isolation — an in-memory lookup instead of a
round trip per node per 200 ms. It was rejected because it adds three failure
surfaces the polling design does not have:

1. **Cascade correctness** — a wheel with coarse and fine buckets needs a cascade
   step as the coarse pointer advances. A bug there is silent and surfaces only
   as a task firing late by exactly one wheel cycle.
2. **Rebuild on restart and rebalance** — the wheel is derived state. Every node
   start and every shard rebalance means re-deriving it from the database.
   Because there is no leader, rebalances are routine rather than exceptional
   (ADR-003), so the wheel would be a cache that must be provably consistent with
   Postgres in a workload where that consistency is the entire job.
3. **Dual-write consistency** — a fired task's wheel entry and its database row
   must agree on state, or a crash between the two writes produces the
   double-fire and lost-task cases the chaos scenarios exist to rule out. Polling
   against the row makes the row the only copy of the state.

## Evidence

The assumption was that a poll would still meet the SLOs at target scale.

| | empty DB | 1 M pending rows |
|---|---|---|
| poll cycle, idle path, P50/P99 | 3.4 ms / 19 ms | 3.0 ms / 7.9 ms |

No growth from empty to a million rows: the index seeks directly to each shard's
due slice regardless of table size. Under the 100 k/1 s peak-load burst,
`claimDueTasks` runs in 0.78 ms (Bitmap Index Scan on the same index). The
reaper's expiry sweep runs in 0.9 ms via the partial index
`tasks_firing_lease_expires_idx`, degrading to a 10.8 ms Seq Scan only when
`firing` rows are most of the table — a state that the claim-side backpressure
(`fireCapacity()`) prevents in practice.

## Consequences

- No wheel, no cascade, no rebuild on rebalance, no dual write. The cost is a
  database round trip every 200 ms per node instead of an in-memory pointer
  advance, measured above.
- If the SLO required sub-millisecond precision, or the poll interval itself
  became the bottleneck, this decision would need revisiting. At 200 ms and 1 M
  rows it is not close.

---

*Chinese version below / 中文版如下*

# ADR-001：轮询共享索引，而非内存中的时间轮

状态：已采纳

## 背景

"触发所有到期任务"天然像是一个时间轮（timing wheel）问题：按 `fire_at`
把任务分桶，推进轮子，触发到期的桶。本 ADR 记录了为什么最终选择了针对
Postgres 索引的轮询方案而非时间轮，以及支撑这一决定的数据。

## 决定

每个节点运行一个 200 毫秒的 `@Scheduled` 轮询。`TaskRepository.claimDueTasks`
是一条单独的 `UPDATE ... WHERE id IN (SELECT ... WHERE shard IN (:shards) AND
state IN ('pending','retrying') AND fire_at <= now() ORDER BY fire_at FOR
UPDATE SKIP LOCKED LIMIT :limit) RETURNING *`，底层由
`tasks_shard_state_fire_at_idx (shard, state, fire_at)` 支撑。系统中不存在任何
内存态的调度表；"什么任务到期了"这个问题在每次轮询时都直接向 Postgres 提问。

单看性能，时间轮会更快——内存查找取代了每个节点每 200 毫秒一次的数据库
往返。但它被否决了，因为它引入了轮询方案没有的三类故障面：

1. **级联正确性** —— 粗、细两级分桶的时间轮需要在粗指针推进时执行级联步骤。
   这里的 bug 是静默的，只会表现为某个任务恰好晚了一个轮周期才触发。
2. **重启与再均衡时的重建** —— 时间轮是派生状态。每次节点启动、每次分片
   再均衡都意味着要从数据库重新推导它。由于系统没有 leader，再均衡是常态
   而非例外情况（见 ADR-003），因此时间轮将成为一个必须被证明与 Postgres
   保持一致的缓存——而在这个工作负载里，"保持一致"本身就是全部的工作。
3. **双写一致性** —— 一次触发既要写时间轮条目又要写数据库行，两者状态必须
   一致，否则两次写入之间的一次崩溃就会产生混沌测试场景本就是为了排除的
   "重复触发"和"任务丢失"问题。轮询直接对行本身操作，使该行成为状态的
   唯一副本。

## 证据

最初的假设是：轮询在目标规模下依然能满足 SLO。

| | 空数据库 | 100 万条待处理行 |
|---|---|---|
| 轮询周期，空闲路径，P50 / P99 | 3.4 ms / 19 ms | 3.0 ms / 7.9 ms |

从空表到百万行没有出现增长：索引会直接定位到每个分片到期的那一小段数据，
与表的总大小无关。在每秒 10 万请求的峰值压测下，`claimDueTasks`
（同一索引上的 Bitmap Index Scan）耗时 0.78 ms。回收器（reaper）的过期
扫描通过局部索引 `tasks_firing_lease_expires_idx` 耗时 0.9 ms，只有当
`firing` 状态的行占表的大多数时才会退化为 10.8 ms 的顺序扫描——而这种
情况在实践中被认领端的背压机制（`fireCapacity()`）所阻止。

## 后果

- 没有时间轮、没有级联、重启或再均衡时无需重建、也没有双写。代价是每个
  节点每 200 毫秒一次数据库往返，而不是一次内存指针推进——具体数字见上文。
- 如果 SLO 要求亚毫秒级精度，或者轮询间隔本身成为瓶颈，这个决定就需要
  重新评估。但在 200 毫秒间隔、百万行规模下，目前远未触及这个上限。
