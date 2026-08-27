package com.chronos.scheduler.shard

import com.chronos.scheduler.task.ShardCalculator
import org.springframework.stereotype.Component

/**
 * Placeholder until Block 2: assumes a single node owns every shard.
 * Real dynamic assignment via the `shards` lease table lands then. Exists so
 * the polling loop can depend on ShardAssignment instead of a hardcoded
 * 0..63 range — swapping the real implementation in later won't touch the
 * polling code at all.
 */
@Component
class StaticAllShardAssignment : ShardAssignment {
    override fun ownedShards(): List<Int> =
        (0 until ShardCalculator.SHARD_COUNT).toList()
}
