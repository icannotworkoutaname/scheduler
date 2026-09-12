# ADR-005: Clock injection for chaos scenarios 7 and 8

Status: accepted (2026-08-23)

## Context

Scenarios 7 (clock skew) and 8 (clock drift) need a node whose notion of "now"
diverges from reality, without root access (no `libfaketime` / `LD_PRELOAD` under
WSL2) and without moving the wall clock for the whole machine — only this node's
time-based decisions should move.

## Decision

`java.time.Clock` is the only injection seam. `ClockConfig` produces
`Clock.offset(...)` for a constant skew (scenario 7), optionally wrapped in
`DriftingClock` — `instant() = anchor + realElapsed × rate` — for a rate drift
(scenario 8).

`@Scheduled` itself stays on the system clock and acts as a cheap ticker; the
injected clock gates whether a tick actually renews. `ShardHeartbeat` checks
`clock.instant() >= nextRenewalDue` inside the method body, so a slow clock
genuinely renews less often rather than only appearing to.

The seam reaches one code path: the heartbeat's renewal cadence. Two things it
explicitly does not reach:

- **Postgres `now()`** — every TTL and expiry comparison
  (`lease_expires_at < now()`) runs in SQL against the database clock, by design.
  ADR-004 decision 1 applies the same principle to the delay metric.
- **The JDK HTTP client's read timeout** — `HttpSinkConfig` sets a 10 s connect
  and read timeout on the JVM's monotonic clock, which `DriftingClock` cannot
  drive without `libfaketime`.

Scenario 7's offset therefore has no effect on firing correctness at all; it only
shifts the ±1 s to ±30 d submission-validation window in `TaskValidation`.

## The crossover: measured on one path, derived on the other

A slow heartbeat and scenario 6's double fire hinge on the same ratio,
`rate = period / ttl` — 10 s / 30 s = 1/3 at production values. Above it the node
renews inside the TTL and keeps its shards; at or below it, renewal loses the
race.

- **Measured.** `ClockDriftTest` (period 2 s, ttl 6 s, the same 1/3 crossover)
  instruments the renewal path directly: at `r = 0.5` the drifted node holds all
  32 shards for the full sampling window; at `r = 0.2` it drops to ≤ 4 and stays
  below fair share for ≥ 75% of samples. This is a Testcontainers assertion, not
  arithmetic.
- **Derived.** The equivalent collapse of the sink-timeout path — a drifting
  node's 10 s read timeout effectively lengthening, contributing to scenario 6's
  double fire — follows the same `period / ttl` arithmetic but was never
  independently instrumented, because `DriftingClock` has no seam into the HTTP
  client's timer.

## Consequences

- A skewed or drifting node stays correct and never fires early: scenario 7's
  node at `+5 min` fired 9 of 20 tasks (the ones assigned to it) with
  `fired_early = 0`.
- Any writeup citing the 1/3 coefficient must state which side it refers to. The
  heartbeat-renewal crossover is measured; the sink-timeout crossover is the same
  formula applied by inference. They are not the same kind of evidence.

---

*Chinese version below / 中文版如下*

# ADR-005：为混沌测试场景 7 和 8 注入时钟

状态：已采纳（2026-08-23）

## 背景

场景 7（时钟偏移）和场景 8（时钟漂移）都需要一个节点，其对"现在"的
认知与真实时间不一致，但又不能要求 root 权限（WSL2 下没有
`libfaketime` / `LD_PRELOAD`），也不能拨动整台机器的系统时钟——只应该
让这一个节点的、与时间有关的决策发生偏移。

## 决定

`java.time.Clock` 是唯一的注入接口。`ClockConfig` 为恒定偏移（场景 7）
生成 `Clock.offset(...)`，也可以选择性地包一层 `DriftingClock`——
`instant() = anchor + realElapsed × rate`——用于速率漂移（场景 8）。

`@Scheduled` 本身仍然基于系统时钟，只是充当一个廉价的心跳节拍器；
真正决定一次心跳是否触发续期的，是被注入的时钟。`ShardHeartbeat`
在方法体内部检查 `clock.instant() >= nextRenewalDue`，因此一个变慢的
时钟会真正地更少续期，而不只是表面上看起来变慢。

这个注入接口只触及一条代码路径：心跳的续期节奏。它明确不触及以下两处：

- **Postgres 的 `now()`** —— 每一次 TTL 和过期时间的比较
  （`lease_expires_at < now()`）都在 SQL 中针对数据库时钟执行，这是刻意
  设计。ADR-004 决定 1 中对延迟指标应用的是同一原则。
- **JDK HTTP 客户端的读超时** —— `HttpSinkConfig` 为 JVM 设置了 10 秒的
  连接和读超时，运行在 JVM 的单调时钟上，`DriftingClock` 在没有
  `libfaketime` 的情况下无法驱动它。

因此场景 7 的时钟偏移对触发正确性完全没有影响；它只会把 `TaskValidation`
中 ±1 秒到 ±30 天的提交校验窗口整体平移。

## 交叉点：一条路径靠实测，另一条靠推导

心跳变慢和场景 6 的重复触发，本质上取决于同一个比值
`rate = period / ttl`——在生产参数下是 10 s / 30 s = 1/3。高于这个比值，
节点能在 TTL 内完成续期并保住自己的分片；等于或低于这个比值，续期就会
在竞争中落败。

- **实测。** `ClockDriftTest`（周期 2 秒、ttl 6 秒，同样是 1/3 的交叉点）
  直接对续期路径做了插桩：在 `r = 0.5` 时，发生漂移的节点在整个采样
  窗口内都持有全部 32 个分片；在 `r = 0.2` 时，它的持有量会降到 ≤ 4，
  并且在 ≥ 75% 的采样点上都低于公平份额。这是一个基于 Testcontainers
  的断言，不是算出来的推论。
- **推导。** sink 超时路径上的同一种崩溃——一个漂移中的节点的 10 秒
  读超时实际上被拉长了，从而助长场景 6 的重复触发——遵循的是同一个
  `period / ttl` 算式，但从未被单独用测试仪表化验证过，因为
  `DriftingClock` 没有接入 HTTP 客户端计时器的接口。

## 后果

- 一个时钟偏移或漂移的节点始终保持正确，也从不会提前触发：场景 7 中
  一个偏移 `+5 分钟` 的节点触发了分给它的 20 个任务中的 9 个，
  `fired_early = 0`。
- 任何引用这个 1/3 系数的文档都必须说明指的是哪一侧。心跳续期的交叉点
  是实测得到的；sink 超时的交叉点是用同一公式推导出来的。二者不是同一
  类别的证据，不能混为一谈。
