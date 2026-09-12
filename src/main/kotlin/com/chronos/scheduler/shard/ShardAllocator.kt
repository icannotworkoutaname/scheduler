package com.chronos.scheduler.shard

import org.springframework.stereotype.Component
import kotlin.math.ceil

@Component
class ShardAllocator {
    companion object {
        const val TOTAL_SHARDS = 64
    }

    /** activeOwnerCount includes the caller, provided it has already announced. */
    fun softCapFor(activeOwnerCount: Int): Int =
        ceil(TOTAL_SHARDS.toDouble() / activeOwnerCount.coerceAtLeast(1)).toInt()
}
