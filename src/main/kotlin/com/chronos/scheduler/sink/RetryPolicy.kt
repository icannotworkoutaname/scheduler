package com.chronos.scheduler.sink

import java.time.Duration

object RetryPolicy {
    const val MAX_ATTEMPTS = 5

    /**
     * attempt 是已经发生的尝试次数(claim 时递增过的 attempt_count)。
     * 例如第 1 次尝试失败(attempt=1)，退避到第 2 次尝试；
     * 第 5 次尝试失败(attempt=5)，达到上限，进 dead，不再退避。
     */
    fun shouldRetry(attempt: Int): Boolean = attempt < MAX_ATTEMPTS

    fun backoffFor(attempt: Int): Duration {
        val seconds = (1L shl attempt.coerceAtMost(6)) // 2, 4, 8, 16, 32...
        return Duration.ofSeconds(seconds)
    }
}
