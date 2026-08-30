package com.chronos.scheduler.shard

import com.chronos.scheduler.node.NodeIdentity
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Component

@Component
class ShardBootstrap(
    private val shardLeaseRepository: ShardLeaseRepository,
    private val shardAllocator: ShardAllocator,
    private val nodeIdentity: NodeIdentity,
    @Value("\${chronos.shard.settle-delay-seconds:3}")
    private val settleDelaySeconds: Long,
) : CommandLineRunner {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun run(vararg args: String) {
        // Phase 1 —— 报到:立刻抢 1 个 shard,不管多不管少，
        // 目的只是尽快让自己出现在 countDistinctActiveOwners() 里，
        // 让几乎同时启动的兄弟节点有机会"看见"自己。
        val announced = shardLeaseRepository.claimAvailableShards(nodeIdentity.nodeId, 1)
        log.info("node {} announced with shard(s) {}", nodeIdentity.nodeId, announced)

        // Phase 2 —— 等待:给兄弟节点留出时间也完成它们自己的 Phase 1。
        // 这次延迟放在报到之后，不是放在报到之前——上次的版本睡在最前面，
        // 睡的时候两边都还没在数据库里露面，等长的延迟不会缩小彼此的相对差距。
        Thread.sleep(settleDelaySeconds * 1000)

        // Phase 3 —— 按公平份额认领剩下的:这时候数据库里已经能看到所有
        // 在窗口内完成了 Phase 1 的兄弟节点，softCap 算出来才是准的。
        val activeOwners = shardLeaseRepository.countDistinctActiveOwners()
        val softCap = shardAllocator.softCapFor(activeOwners)
        val remaining = (softCap - announced.size).coerceAtLeast(0)
        val claimedRest = shardLeaseRepository.claimAvailableShards(nodeIdentity.nodeId, remaining)

        val total = announced + claimedRest
        log.info(
            "node {} holds {} shards total (activeOwners={}, softCap={}): {}",
            nodeIdentity.nodeId, total.size, activeOwners, softCap, total
        )
    }
}
