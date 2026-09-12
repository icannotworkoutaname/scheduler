# ADR-002: Leases live in Postgres, not Redis

Status: accepted

## Context

A shard or task lease needs an atomic acquire-and-check, which is what Redis
`SET NX EX` provides cheaply. The conventional split is therefore Postgres for
task rows, Redis for lease ownership. This ADR records why leases are ordinary
rows in the same database as the state they gate.

## Decision

`shards.lease_owner`, `lease_expires_at` and `version` are Postgres columns,
updated by the same conditional `UPDATE ... WHERE lease_owner = ?` /
`WHERE version = ?` pattern used everywhere else (ADR-003). No second datastore
is on the coordination path.

Splitting lease ownership (Redis) from task state (Postgres) creates a dual-write
inconsistency window: a crash between "Redis says I own shard 9" and "the
Postgres row reflects it" leaves the two stores disagreeing about ownership, with
no transaction spanning both. Every correctness argument in this system —
ADR-004's absorption of duplicates by the conditional update, ADR-003's
leaderless rebalance — depends on a single conditional statement being the entire
state transition. A second store for leases would reopen exactly the class of bug
the rest of the design closes.

Lease acquisition is not on the request-latency path: it happens once per
heartbeat period, not once per fired task. The throughput Redis would have bought
was never the constraint.

## Evidence

No coordination-store benchmark was run, because the SLO it would have served —
failover bounded by lease expiry (requirements.md §5) — is met with Postgres
alone: the K8s rolling restart and chaos scenario 6 both converge within one to
three heartbeat periods (ADR-003), with lease reads and writes on the same
connection pool as everything else.

The requirements doc (§7.6) originally planned a narrow Redis role even without
leases: a dedup-window cache and dashboard hot counters. Neither survived into
the implementation. The demo receiver's dedup is an in-process `set()`, and
Prometheus/Micrometer already serve the counters. There is no Redis in
`docker-compose.yml`, the Gradle build, or the K8s manifest.

## Consequences

- One fewer moving part in `make demo` and the K8s manifest: no Redis, no second
  connection pool, no second failure mode to reason about during a node freeze or
  partition.
- If lease-renewal volume became a bottleneck (many more shards, or a much
  shorter heartbeat period), this decision would need revisiting. Current volume
  is one renewal per node per 10 s.

---

*Chinese version below / 中文版如下*

# ADR-002：租约存放在 Postgres 中，而非 Redis

状态：已采纳

## 背景

分片或任务租约需要一个原子的"获取并检查"操作，而 Redis 的 `SET NX EX`
恰好能低成本地提供这种能力。因此常规做法是：Postgres 存任务行，Redis
管理租约归属。本 ADR 记录了为什么租约最终选择作为普通的行，存放在与
它所约束的状态相同的数据库里。

## 决定

`shards.lease_owner`、`lease_expires_at` 和 `version` 都是 Postgres 的列，
通过与系统其他地方相同的条件式 `UPDATE ... WHERE lease_owner = ?` /
`WHERE version = ?` 模式更新（见 ADR-003）。协调路径上没有第二个数据存储。

把租约归属（Redis）和任务状态（Postgres）分开会制造一个双写不一致的窗口：
"Redis 认为我拥有分片 9"和"Postgres 行反映了这一点"之间如果发生崩溃，
两个存储对归属权的认知就会不一致，而且没有跨越两者的事务来兜底。这个
系统里的每一条正确性论证——ADR-004 中依赖条件更新吸收重复触发、ADR-003
中的无 leader 再均衡——都依赖于"单条条件语句就是整个状态迁移"这一前提。
再引入一个租约存储，就等于重新打开了系统其余部分本已关闭的那类 bug。

租约获取并不在请求延迟路径上：它每个心跳周期发生一次，而不是每次任务
触发发生一次。Redis 本可能带来的吞吐提升，从一开始就不是这里的瓶颈。

## 证据

没有做过协调存储的专项基准测试，因为它本该服务的那个 SLO——故障恢复
时间受租约到期时间约束（见 requirements.md §5）——单靠 Postgres 就已经
满足了：无论是 K8s 滚动重启还是混沌测试场景 6，都能在一到三个心跳周期
内收敛（见 ADR-003），租约的读写与系统其他部分共用同一个连接池。

需求文档（§7.6）最初曾计划过一个更窄的 Redis 角色，甚至不涉及租约：
去重窗口缓存和仪表盘的热点计数器。这两者都没有进入最终实现。演示用的
receiver 的去重逻辑是进程内的一个 `set()`，而 Prometheus/Micrometer 早已
承担了计数器的职责。`docker-compose.yml`、Gradle 构建、K8s manifest 里
都没有 Redis 的身影。

## 后果

- `make demo` 和 K8s manifest 中少了一个活动部件：没有 Redis，没有第二个
  连接池，在节点冻结或分区时也就少了一种需要推理的故障模式。
- 如果租约续期的量级成为瓶颈（比如分片数大幅增加，或者心跳周期大幅
  缩短），这个决定就需要重新评估。目前的量级是每个节点每 10 秒续期一次。
