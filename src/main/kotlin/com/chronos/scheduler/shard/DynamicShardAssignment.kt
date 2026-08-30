package com.chronos.scheduler.shard

import com.chronos.scheduler.node.NodeIdentity
import org.springframework.stereotype.Component

/**
 * Replaces StaticAllShardAssignment (8/7 placeholder). Queries the shards
 * table for whichever rows this node currently owns — the polling loop
 * (PollingLoop.kt) depends only on the ShardAssignment interface, so this
 * swap doesn't touch any polling code, exactly as planned when the interface
 * was introduced.
 */
@Component
class DynamicShardAssignment(
    private val shardLeaseRepository: ShardLeaseRepository,
    private val nodeIdentity: NodeIdentity,
) : ShardAssignment {
    override fun ownedShards(): List<Int> =
        shardLeaseRepository.ownedShardsFor(nodeIdentity.nodeId)
}
