package com.chronos.scheduler.shard

interface ShardAssignment {
    /** Which shards this node currently owns and should poll. */
    fun ownedShards(): List<Int>
}
