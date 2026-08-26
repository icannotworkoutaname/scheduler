package com.chronos.scheduler.task

import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertTrue

class ShardCalculatorTest {

	@Test
	fun `shard distribution is uniform across 100k random UUIDs`() {
		val sampleSize = 100_000
		val counts = IntArray(ShardCalculator.SHARD_COUNT)

		repeat(sampleSize) {
			val shard = ShardCalculator.shardFor(UUID.randomUUID())
			counts[shard]++
		}

		val expected = sampleSize.toDouble() / ShardCalculator.SHARD_COUNT
		val tolerance = expected * 0.2

		counts.forEachIndexed { shard, count ->
			assertTrue(
				count > 0,
				"shard $shard received no tasks out of $sampleSize samples",
			)
			assertTrue(
				kotlin.math.abs(count - expected) <= tolerance,
				"shard $shard count $count deviates from expected $expected by more than $tolerance",
			)
		}
	}

}
