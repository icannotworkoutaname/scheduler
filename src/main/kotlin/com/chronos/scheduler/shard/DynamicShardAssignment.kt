package com.chronos.scheduler.shard

import com.chronos.scheduler.node.NodeIdentity
import org.springframework.stereotype.Component

/**
 * Reads this node's owned shards from the shards table on every call, so a
 * lease lost between polls disappears from the set without any explicit
 * invalidation. See ShardLeaseRepository.ownedShardsFor for the expiry filter.
 */
@Component
class DynamicShardAssignment(
    private val shardLeaseRepository: ShardLeaseRepository,
    private val nodeIdentity: NodeIdentity,
) : ShardAssignment {
    override fun ownedShards(): List<Int> =
        shardLeaseRepository.ownedShardsFor(nodeIdentity.nodeId)
}
