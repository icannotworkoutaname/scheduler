# ADR-003: Lease at the shard level, not the task level, and no leader

Status: accepted

## Context

Something must decide which node fires which task, and what happens when a node
disappears. Two designs were considered: lease every task individually as it is
claimed, or lease one of 64 fixed shards per node and let shard ownership imply
task ownership. A third — a leader node that assigns work — was rejected because
it needs its own failover story on top of everything else.

## Decision

64 fixed logical shards, assigned at insert time as
`shard = uuid.leastSignificantBits & 63` (`ShardCalculator`). Each shard is a row
in `shards` with `lease_owner` and `lease_expires_at`. A node polls only tasks
whose shard it currently owns. There is no leader: the `shards` table is the
coordination primitive, contended with a `version` column and
`FOR UPDATE SKIP LOCKED` on claim.

Rebalancing is symmetric and leaderless, in `ShardHeartbeat` and
`ShardBootstrap`:

- A node under its fair share renews what it holds and claims up to
  `softCap = ceil(64 / activeOwners)`, with `activeOwners` recomputed each
  heartbeat.
- A node that finds every shard already leased force-claims one
  (`forceClaimOneShard`) so that it appears in `countDistinctActiveOwners()`.
  Without this, a node that loses the startup race is never counted, and the
  over-holding peer keeps `softCap = 64` indefinitely.
- A node over its share releases the excess (`releaseExcessShards`, highest shard
  ids first), so a newly visible node has something to claim on the next
  heartbeat.

`ShardRebalanceConvergenceTest` covers oscillation: 3 nodes settle to
`[22, 22, 20]` in exactly 3 release events, after which the shard-to-owner map
stays byte-identical for roughly 8 further heartbeat periods. The arithmetic
agrees — released ≤ wanted, since `64 − softCap × N ≤ 0` for any `N ≥ 1` — so a
release can never overshoot into triggering a counter-release.

Task-level leasing was rejected because the shard table already answers "is this
task mine". A node's `WHERE shard IN (:shards)` filter in `claimDueTasks` is the
same check a per-task lease would make one row at a time, at the cost of one
predicate rather than N lease acquisitions per poll.

## Evidence

The K8s rolling restart on 2026-09-04: a `replicas: 2` Deployment restart briefly
ran 4 pods (2 old draining, 2 new starting). `activeOwners` in the heartbeat logs
went 4 → 3 → 2 across three consecutive rebalance events as the old pods drained,
converging back to a `[32, 32]` split with zero pod restarts and no leader
election. The same mechanism written for node death handled an
orchestrator-driven churn event.

Chaos scenario 6 exercises the death path: freezing a node past its shard lease
TTL, the survivor takes over the frozen node's shards and re-fires its in-flight
task. Measured recovery 41 s against a derived worst case of ~40 s plus the
downstream call — see ADR-004 decision 5.

## Consequences

- Rebalancing is a continuous background process, not a rare failover path, which
  is part of why ADR-001 rejected caching a wheel on top of it: the steady state
  such a cache would assume does not exist.
- Nothing in the system decides shard assignment centrally, so there is nothing
  to fail over to. Chaos scenario 6 and the K8s rollout exercise that property
  from different triggers.

---

*Chinese version below / 中文版如下*

# ADR-003：租约作用在分片层级而非任务层级，且不设 leader

状态：已采纳

## 背景

必须有某种机制来决定哪个节点触发哪个任务，以及节点消失后该怎么办。
考虑过两种设计：为每个任务在被认领时单独加租约，或者每个节点租下
64 个固定分片中的一部分，让分片归属隐含任务归属。第三种方案——由一个
leader 节点分配工作——被否决了，因为它需要在其他一切之上再单独设计一套
故障转移逻辑。

## 决定

64 个固定的逻辑分片，在插入时通过 `shard = uuid.leastSignificantBits & 63`
（`ShardCalculator`）分配。每个分片是 `shards` 表里的一行，带有
`lease_owner` 和 `lease_expires_at`。节点只轮询自己当前拥有的分片上的任务。
系统没有 leader：`shards` 表本身就是协调原语，通过 `version` 列和认领时的
`FOR UPDATE SKIP LOCKED` 来竞争。

再均衡是对称且无 leader 的，实现在 `ShardHeartbeat` 和 `ShardBootstrap` 中：

- 一个持有份额低于公平值的节点会续期自己已持有的分片，并认领至多
  `softCap = ceil(64 / activeOwners)` 个，`activeOwners` 在每次心跳时重新计算。
- 一个发现所有分片都已被租出的节点会强制认领一个（`forceClaimOneShard`），
  以便自己出现在 `countDistinctActiveOwners()` 的统计中。没有这一步，一个
  在启动竞争中落败的节点将永远不会被计入，而多占分片的那个节点会一直
  保持 `softCap = 64`。
- 一个持有份额超出公平值的节点会释放多余的部分（`releaseExcessShards`，
  优先释放编号最大的分片），这样一个刚刚变得可见的新节点在下一次心跳时
  就有分片可以认领。

`ShardRebalanceConvergenceTest` 覆盖了震荡场景：3 个节点经过恰好 3 次释放
事件后收敛到 `[22, 22, 20]`，此后分片到所有者的映射在大约再往后的 8 个
心跳周期内保持逐字节一致。这在算术上也说得通：由于对任意 `N ≥ 1` 都有
`64 − softCap × N ≤ 0`，释放量永远不会超过实际需要的量，因此一次释放
不可能反过来触发另一次释放。

任务层级的租约被否决了，因为分片表本身已经能回答"这个任务是不是我的"
这个问题。节点在 `claimDueTasks` 中的 `WHERE shard IN (:shards)` 过滤条件，
和逐个任务加租约要做的检查是同一件事，但只需要一个谓词，而不是每次
轮询要做 N 次租约获取。

## 证据

2026-09-04 的一次 K8s 滚动重启：一个 `replicas: 2` 的 Deployment 在重启期间
短暂运行了 4 个 pod（2 个旧的正在排空，2 个新的正在启动）。心跳日志中
`activeOwners` 在三次连续的再均衡事件中依次变为 4 → 3 → 2，随着旧 pod
排空完毕，最终收敛回 `[32, 32]` 的均分状态，期间没有任何 pod 重启，也
没有任何 leader 选举。同一套为节点死亡设计的机制，同样处理了一次由
编排器驱动的节点数变化事件。

混沌测试场景 6 则演练了死亡路径：冻结一个节点直到其分片租约过期，
存活节点接管被冻结节点的分片，并重新触发其正在进行中的任务。实测恢复
耗时 41 秒，对应推导出的最坏情况约为 40 秒加上下游调用本身的时间——
详见 ADR-004 决定 5。

## 后果

- 再均衡是一个持续运行的后台过程，而不是一条罕见的故障转移路径，这也是
  ADR-001 否决"在其上再缓存一个时间轮"的部分原因：这类缓存所假设的稳定
  状态在这里并不存在。
- 系统里没有任何东西集中决定分片分配，因此也就没有什么东西需要"故障转移
  到"。混沌测试场景 6 和 K8s 滚动升级从不同的触发方式验证了这一特性。
