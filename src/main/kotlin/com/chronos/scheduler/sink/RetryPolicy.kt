package com.chronos.scheduler.sink

import java.time.Duration

object RetryPolicy {
    const val MAX_ATTEMPTS = 5

    /**
     * `attempt` is the number of attempts already made — attempt_count, which the
     * claim query increments. attempt=1 means the first attempt just failed and a
     * second is due; attempt=MAX_ATTEMPTS means the budget is spent and the task
     * goes to dead.
     */
    fun shouldRetry(attempt: Int): Boolean = attempt < MAX_ATTEMPTS

    /** 2, 4, 8, 16 s for attempts 1..4; attempt 5 exhausts the budget instead. */
    fun backoffFor(attempt: Int): Duration =
        Duration.ofSeconds(1L shl attempt.coerceIn(1, MAX_ATTEMPTS))
}
