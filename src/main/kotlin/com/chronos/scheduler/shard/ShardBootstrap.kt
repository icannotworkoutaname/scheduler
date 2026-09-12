package com.chronos.scheduler.shard

import com.chronos.scheduler.node.NodeIdentity
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Component

@Component
class ShardBootstrap(
    private val shardLeaseRepository: ShardLeaseRepository,
    private val shardHeartbeat: ShardHeartbeat,
    private val nodeIdentity: NodeIdentity,
    @Value("\${chronos.shard.settle-delay-seconds:3}")
    private val settleDelaySeconds: Long,
) : CommandLineRunner {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun run(vararg args: String) {
        // Phase 1 — announce: claim one shard immediately, so this node appears
        // in countDistinctActiveOwners() before any peer computes a soft cap.
        val announced = shardLeaseRepository.claimAvailableShards(nodeIdentity.nodeId, 1)
        if (announced.isEmpty()) {
            // Every shard is leased: a peer won the startup race. Take one
            // anyway to become visible; the peer's next heartbeat rebalances
            // down to a fair share.
            val stolen = shardLeaseRepository.forceClaimOneShard(nodeIdentity.nodeId)
            log.info("node {} announced by force-claiming shard {} (all shards were leased)", nodeIdentity.nodeId, stolen)
        } else {
            log.info("node {} announced with shard(s) {}", nodeIdentity.nodeId, announced.map { it.shardId })
        }

        // Phase 2 — settle: give peers time to finish their own phase 1. The
        // delay is after the announce, not before it: sleeping first leaves both
        // nodes absent from the database, so the wait does not reduce the gap.
        Thread.sleep(settleDelaySeconds * 1000)

        // Phase 3 — claim up to fair share, by running the heartbeat itself
        // rather than duplicating its renew-and-claim logic. markReadyAndRun
        // both lifts the readiness gate and performs this node's first
        // heartbeat, so the scheduler's own first tick cannot race phase 3:
        // until the flag is set, renewAndRebalance() returns immediately.
        shardHeartbeat.markReadyAndRun()
    }
}
