package com.chronos.scheduler.task

import java.util.UUID

/**
 * Maps a task id to one of 64 logical shards (requirements.md §7.3).
 * Computed once at task-creation time from the id's own random bits.
 */
object ShardCalculator {
    const val SHARD_COUNT = 64

    fun shardFor(id: UUID): Int {
        return (id.leastSignificantBits and (SHARD_COUNT - 1).toLong()).toInt()
    }
}
