package com.chronos.scheduler.shard

import org.springframework.stereotype.Component
import kotlin.math.ceil

@Component
class ShardAllocator {
    companion object {
        const val TOTAL_SHARDS = 64
    }

    /** activeOwnerCount 应该已经反映真实情况——包括调用方自己,如果它已经报到过。 */
    fun softCapFor(activeOwnerCount: Int): Int =
        ceil(TOTAL_SHARDS.toDouble() / activeOwnerCount.coerceAtLeast(1)).toInt()
}
