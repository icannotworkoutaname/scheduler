# ADR-004: Metrics semantics (requirements.md §9)

Status: accepted (2026-08-24); decisions 5–7 added 2026-09-05

Seven metrics are required by §9. Four of them carry a semantic choice that, if
made wrong, would make the README charts misleading; decisions 5–7 record three
further reading hazards found during load testing and the K8s run. This ADR
records all of them.

## Decision 1 — `trigger_delay_seconds` is measured on the database clock

`trigger_delay = actual_trigger_instant − fire_at`. Both timestamps come from
Postgres, never from a node's `Instant.now()` or injected `Clock`.

Mechanism: the claim query is `UPDATE tasks SET state='firing' ... RETURNING *,
now() AS db_fired_at`. The delay is `db_fired_at − fire_at`, computed in
`ClaimedTask.triggerDelay` and recorded in `PollingLoop.pollAndClaim` at claim
time. No node clock is read on this path.

Rationale: chaos scenario 7 showed that a clock-skewed node still triggers
correctly, because every `fire_at <= now()` and `lease_expires_at < now()`
comparison runs in SQL. A delay computed as `nodeClock.now() − fire_at` would be
wrong by exactly the skew, and skew is likely in any real deployment. This metric
is the last place node-clock correctness could regress. `TriggerDelayMetricTest`
pins it: a `+300 s` node reports ~2 s, not ~302 s.

## Decision 2 — `duplicate_trigger_total` records absorption, not prevention

A single node cannot know that one of its fires is a duplicate: from its own view
every fire is legitimate, since it claimed the task and its lease was valid. What
is observable is the aftermath:

> a node finished firing a task (sink call made, bytes delivered downstream) and
> its conditional state update — `markSucceeded` or `markFailed` — affected
> **0 rows because the task was already `succeeded`**.

Those zero rows are a late duplicate delivery being absorbed by the conditional
update. Implemented in `PollingLoop.handle`: on any 0-row update, `findById`; if
the state is `SUCCEEDED`, `duplicateTriggerAbsorbed()`. Any other state (the
reaper returned the row to `pending`, for example) is a plain version race and is
not counted.

This is the path scenario 6 actually takes: the unfrozen node's HTTP client threw
("Request cancelled"), so it went through `markFailed` → 0 rows → task already
`succeeded` → counted. The receiver's `/stats` `duplicate_triggers` remains an
out-of-band cross-check; this is the in-band one.

The metric is named `duplicate_trigger_total` and its help text says "absorbed",
not "prevented".

## Decision 3 — `chronos.tasks.firing` is sampled, and measures live load not backlog

Per-shard gauge (`tag: shard`, 64 series). Two properties keep it from becoming a
load source at a million rows:

1. **Sampled, not per-scrape.** One background sample every 15 s
   (`SchedulerMetrics`, on its own single daemon thread, never the shared
   `@Scheduled` poll thread), written into an `AtomicInteger[64]` that the gauges
   read.
2. **Live firing count, not backlog.** The original counted every
   pending/retrying row: a 91 ms parallel full-table Seq Scan at 1 M rows, since
   `state` matches nearly everything and there is no `shard =` predicate to seek
   on. Bounding it to "due in the next 5 min" made it worse (300 ms) — under a
   real drain that window holds tens of thousands of rows, and an exact count is
   O(matches). Counting what each shard is firing right now is both cheap and the
   right question for "are the shards balanced": `state = 'firing'` is selective
   (backpressure caps total firing at a few thousand), the partial index
   `tasks_firing_lease_expires_idx` covers it, and the result is the live
   per-shard work distribution — 0 everywhere when idle. ~0.9 ms at rest, ~15 ms
   mid-burst. Renamed `tasks_pending` → `tasks_firing`; `docs/performance.md` has
   the before/after plans.

## Decision 4 — `trigger_delay_seconds` uses explicit histogram buckets

The SLO is P50 ≤ 200 ms, P99 ≤ 1 s, and Micrometer's default buckets are too
coarse there to compute P99 with useful resolution. Explicit
`serviceLevelObjectives`: 50 ms, 100 ms, 200 ms, 500 ms, 1 s, 2 s, 5 s. Both SLO
points have a boundary on each side, so `histogram_quantile()` has resolution
where the precision figure needs it. `sink_call_duration_seconds` and
`poll_duration_seconds` get explicit buckets sized to their own ranges.

## Decision 5 — scenario 6 recovery is bounded by the slower of two chains, not their sum

Both leases are nominally 30 s, so a naive estimate adds them: the task lease
expires, the reaper takes ≤ 5 s to notice, the shard lease also has to expire, and
another node's heartbeat takes ≤ 10 s to notice. The two chains run concurrently,
so recovery is the maximum, not the sum:

```
shard lease expiry (<= 30 s) + heartbeat discovery (<= 10 s)  -> survivor eligible,        <= 40 s
task lease expiry (30 s)     + reaper sweep (<= 5 s)          -> row back to pending,      <= 35 s  (concurrent)
                                                              -> survivor's poll claims it, <= 0.2 s
                                                              +  the downstream call itself
```

Worst case is `max(40, 35) + 0.2 s + downstream`.
`scripts/scenario6_freeze_double_fire.sh` derives this in comments and waits on
the fact (the task reached `succeeded`) rather than sleeping a fixed interval.
Measured on the 2026-09-04 rerun, with an 8 s artificial downstream delay and
otherwise production 30 s / 10 s / 5 s timings: **41 s** frozen-to-recovered.

This belongs in the metrics ADR because it is why `trigger_delay` and
`duplicate_trigger_total` behave as they do during a freeze: both are gated by
whichever clock is slower, and a reader who assumes the two TTLs add will
misjudge how long a freeze recovery takes. requirements.md §5 carries the same
correction to the failover SLO.

## Decision 6 — scenario 4 proves outcome consistency, not race arbitration

What chaos scenario 4 demonstrates: whichever of cancel or fire wins, the HTTP
response, the row's terminal state, and the receiver's `/seen` record agree. It
does not demonstrate winning a millisecond-scale simultaneous collision — the
test constructs a straddle, with half the cases positioned to land just before
`fire_at` and half just after, rather than firing both branches at the same
instant.

The mutual-exclusion guarantee does not come from timing a test precisely enough
to hit a window. It comes from `UPDATE tasks SET state='cancelled' WHERE
state='pending' AND version=:v` (and the equivalent claim-then-fire path) being
correct by construction: only one of the two conditional updates can match a
given row, regardless of how close in time they run. The test samples both sides
of the straddle and checks that the three views agree.

Reader-facing text should therefore claim outcome consistency across the
boundary, not that the race itself was reproduced.

## Decision 7 — two different re-fires must not be conflated

`duplicate_trigger_total` rising can mean two unrelated things:

- **Failure re-fire** (scenario 6): a node froze or died past its lease and
  another node correctly re-fired its in-flight task. Each increment is one
  duplicate absorbed instead of a task lost.
- **Self-induced re-fire** (found during peak-load tuning): under load, claiming
  due tasks faster than the worker pool can fire them lets `firing` rows pile up
  past their own lease TTL. The reaper then hands a task that was never stuck to
  a second claim, purely because the first claim had not finished. This is a
  capacity problem on the same counter. Backpressure (`fireCapacity()` bounding
  claims to the executor's free capacity) took this from ~2,000 re-fires to 0 in
  the peak run, with no change to the failure path above.

Same metric, same absorption mechanism, unrelated causes. A dashboard or README
showing `duplicate_trigger_total` rising must state which one it shows: flat at
zero except during a named chaos scenario is the first; any nonzero rate during
ordinary peak load is the second, and is a defect to investigate.

## The remaining metrics

- `tasks_dead_total` — counter, incremented in `PollingLoop.handle` when
  `markFailed` lands in the dead branch (retries exhausted).
- `sink_call_duration_seconds` — timer around `HttpSink.fire`, tag
  `outcome=success|failure`, measured with `System.nanoTime()`: elapsed real
  time, immune to clock skew, not the injected `Clock`.
- `poll_duration_seconds` — timer around the whole `pollAndClaim` body, including
  the no-op path. Its idle value is what shows that polling with nothing due is a
  bounded index scan.
- `lease_takeover_total` — wired into `ShardHeartbeat` rather than
  `SchedulerMetrics`, where the takeover is detected.

---

*Chinese version below / 中文版如下*

# ADR-004：指标语义（requirements.md §9）

状态：已采纳（2026-08-24）；决定 5–7 于 2026-09-05 补充

§9 要求了七个指标。其中四个涉及语义选择，一旦选错就会让 README 里的图表
产生误导；决定 5–7 记录了在压力测试和 K8s 运行过程中发现的另外三个易被
误读的地方。本 ADR 把它们全部记录下来。

## 决定 1 —— `trigger_delay_seconds` 以数据库时钟为准

`trigger_delay = actual_trigger_instant − fire_at`。两个时间戳都来自
Postgres，绝不来自某个节点的 `Instant.now()` 或注入的 `Clock`。

机制：认领查询是 `UPDATE tasks SET state='firing' ... RETURNING *, now() AS
db_fired_at`。延迟计算为 `db_fired_at − fire_at`，在 `ClaimedTask.
triggerDelay` 中计算，并在认领时由 `PollingLoop.pollAndClaim` 记录。这条
路径上不读取任何节点本地时钟。

依据：混沌测试场景 7 表明，一个时钟偏移的节点依然能正确触发，因为每一次
`fire_at <= now()` 和 `lease_expires_at < now()` 的比较都在 SQL 里执行。
如果延迟按 `nodeClock.now() − fire_at` 计算，就会精确地错上一个偏移量的
误差，而在真实部署中时钟偏移几乎必然存在。这个指标是节点本地时钟正确性
最后一个可能出问题的地方。`TriggerDelayMetricTest` 对此做了钉死式验证：
一个偏移 `+300 s` 的节点报告出的延迟约为 2 秒，而不是约 302 秒。

## 决定 2 —— `duplicate_trigger_total` 记录的是"被吸收"，而非"被阻止"

单个节点无法知道自己的某次触发是不是重复触发：从它自己的视角看，每次
触发都是合法的，因为它确实认领了任务，租约也确实有效。能被观察到的是
事后的结果：

> 一个节点完成了任务的触发（下游调用已发出、字节已送达），但它随后的
> 条件式状态更新——`markSucceeded` 或 `markFailed`——影响了 **0 行，
> 因为该任务已经处于 `succeeded` 状态**。

这种"0 行受影响"就是一次迟到的重复投递被条件更新吸收了。实现在
`PollingLoop.handle` 中：任何一次 0 行更新发生后都会执行 `findById`；
如果状态是 `SUCCEEDED`，就调用 `duplicateTriggerAbsorbed()`。其他任何
状态（比如回收器把该行重新置回了 `pending`）都只是一次普通的版本竞争，
不计入该指标。

场景 6 走的正是这条路径：未被冻结的节点的 HTTP 客户端抛出了异常
（"Request cancelled"），于是走到 `markFailed` → 0 行受影响 → 发现任务
已是 `succeeded` → 计数。receiver 的 `/stats` 中的 `duplicate_triggers`
仍然作为一个带外的交叉校验；这个指标则是带内的那一个。

该指标名为 `duplicate_trigger_total`，其帮助文本写的是"absorbed"（已吸收），
而不是"prevented"（已阻止）。

## 决定 3 —— `chronos.tasks.firing` 是抽样得到的，衡量的是当前负载而非积压量

按分片打标签的 gauge（`tag: shard`，共 64 个序列）。有两个特性使它在
百万行规模下也不会成为一个负载来源：

1. **抽样，而非每次抓取都计算。** 每 15 秒在后台采样一次
   （`SchedulerMetrics`，运行在自己独立的单一守护线程上，绝不占用共享的
   `@Scheduled` 轮询线程），写入一个 `AtomicInteger[64]` 供 gauge 读取。
2. **衡量的是实时触发数，而非积压量。** 最初的实现会统计每一行
   pending/retrying 状态的任务：在百万行规模下，这是一次 91 毫秒的并行
   全表顺序扫描，因为 `state` 几乎匹配所有行，而且没有 `shard =` 谓词可供
   索引定位。把统计范围限制在"未来 5 分钟内到期"反而更糟（300 毫秒）——
   在真实的排空过程中，这个窗口本身就会包含数以万计的行，而精确计数的
   代价与匹配行数成正比。相反，统计"每个分片当前正在触发什么"既便宜，
   又恰好是"分片是否均衡"这个问题真正需要的答案：`state = 'firing'`
   本身选择性很高（背压把总的 firing 数量限制在几千以内），局部索引
   `tasks_firing_lease_expires_idx` 覆盖了这次查询，结果就是实时的每分片
   工作量分布——空闲时处处为 0。静止状态下约 0.9 毫秒，压测中约 15 毫秒。
   该指标已从 `tasks_pending` 重命名为 `tasks_firing`；`docs/performance.md`
   中有改动前后的执行计划对比。

## 决定 4 —— `trigger_delay_seconds` 使用显式的直方图分桶

SLO 要求 P50 ≤ 200 毫秒、P99 ≤ 1 秒，而 Micrometer 的默认分桶在这个范围内
太粗，无法算出有实际参考价值的 P99。因此使用显式的
`serviceLevelObjectives`：50 ms、100 ms、200 ms、500 ms、1 s、2 s、5 s。
两个 SLO 关键点的两侧都各有一个分桶边界，因此 `histogram_quantile()`
恰好能在精度指标最需要分辨率的地方拥有分辨率。`sink_call_duration_seconds`
和 `poll_duration_seconds` 也各自配置了适合自身取值范围的显式分桶。

## 决定 5 —— 场景 6 的恢复时间由两条链路中较慢的一条决定，而非二者之和

两个租约名义上都是 30 秒，一种朴素的估算方法是把二者相加：任务租约到期，
回收器最多 5 秒后发现，分片租约也要到期，另一个节点的心跳最多 10 秒后
发现。但这两条链路是并发进行的，因此恢复时间取决于二者中较慢的那一条，
而不是它们的和：

```
分片租约到期(<= 30 s) + 心跳发现(<= 10 s)  -> 存活节点具备接管资格，<= 40 s
任务租约到期(30 s)   + 回收扫描(<= 5 s)    -> 该行被重置为 pending， <= 35 s（与上面并发）
                                            -> 存活节点的轮询认领该行，<= 0.2 s
                                            +  下游调用本身耗时
```

最坏情况是 `max(40, 35) + 0.2 s + 下游耗时`。
`scripts/scenario6_freeze_double_fire.sh` 在注释里推导了这个结果，并且是
等待事实发生（任务确实到达 `succeeded` 状态），而不是固定时长的 sleep。
在 2026-09-04 的重跑中，人为设置 8 秒的下游延迟，其余保持生产环境的
30 秒 / 10 秒 / 5 秒时序，实测**从冻结到恢复耗时 41 秒**。

之所以把这一条放进指标 ADR，是因为它正是 `trigger_delay` 和
`duplicate_trigger_total` 在一次冻结期间表现出特定行为的原因：两者都受限于
两条链路中较慢的那一个时钟，如果读者误以为两个 TTL 是相加关系，就会
错误判断一次冻结恢复所需的时间。requirements.md §5 中对故障转移 SLO
也做了同样的修正说明。

## 决定 6 —— 场景 4 证明的是结果一致性，而非竞争仲裁

混沌测试场景 4 所证明的是：无论"取消"还是"触发"哪一个赢，HTTP 响应、
该行的终态、以及 receiver 的 `/seen` 记录三者都会保持一致。它并不证明
系统赢得了一次毫秒级的真正同时竞争——该测试构造的是一种"跨界"场景，
一半用例被安排在恰好 `fire_at` 之前落地，另一半恰好在之后，而不是让
两个分支在同一瞬间真正同时触发。

这种互斥保证并非来自把测试的时机掐得足够准以命中某个窗口，而是来自
`UPDATE tasks SET state='cancelled' WHERE state='pending' AND version=:v`
（以及对应的"先认领后触发"路径）在构造上就是正确的：无论两个条件式
更新在时间上靠得多近，对同一行而言只可能有一个会命中。该测试对这个
跨界的两侧分别取样，并检查三方视图是否一致。

因此，面向读者的表述应当声称"边界两侧的结果保持一致"，而不是声称
"真正复现了那次竞争本身"。

## 决定 7 —— 两种不同的重新触发不能混为一谈

`duplicate_trigger_total` 的上升可能来自两种完全不相关的原因：

- **失败导致的重新触发**（场景 6）：一个节点冻结或死亡，超过了其租约
  期限，另一个节点正确地重新触发了它未完成的任务。每一次递增都意味着
  吸收了一次重复触发，而不是丢失了一个任务。
- **自我诱发的重新触发**（在峰值负载调优期间发现）：在高负载下，认领
  到期任务的速度超过了工作线程池实际触发任务的速度，导致 `firing`
  状态的行在自身的租约 TTL 到期前就已经堆积。回收器随后会把一个其实
  并没有卡住的任务交给第二次认领，原因仅仅是第一次认领还没完成。这是
  同一个计数器上的一个容量问题。背压机制（`fireCapacity()` 将认领数量
  限制在执行器的空闲容量以内）在峰值压测中把这类重新触发从约 2,000 次
  降到了 0 次，且没有影响上面的失败路径。

同一个指标、同一套吸收机制，但背后是两种互不相关的原因。任何展示
`duplicate_trigger_total` 的仪表盘或 README 都必须说明它展示的是哪一种：
除了在指定的混沌测试场景期间之外始终保持在零，属于第一种；而在正常
峰值负载下出现任何非零的速率，属于第二种，且是需要排查的缺陷。

## 其余指标

- `tasks_dead_total` —— 计数器，在 `PollingLoop.handle` 中当 `markFailed`
  落入"dead"分支（重试次数耗尽）时递增。
- `sink_call_duration_seconds` —— 围绕 `HttpSink.fire` 的计时器，带标签
  `outcome=success|failure`，用 `System.nanoTime()` 测量：真实流逝时间，
  不受时钟偏移影响，也不使用注入的 `Clock`。
- `poll_duration_seconds` —— 围绕整个 `pollAndClaim` 方法体的计时器，
  包括空转（无任务可认领）路径。它的空闲值正是"在无任务到期时，轮询
  本身是一次有界的索引扫描"这一说法的证据。
- `lease_takeover_total` —— 接入在 `ShardHeartbeat` 中而非
  `SchedulerMetrics` 中，因为接管动作正是在那里被检测到的。
