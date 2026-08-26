package com.chronos.scheduler.task

import java.time.Duration
import java.time.Instant

class InvalidFireAtException(message: String) : RuntimeException(message)

object TaskValidation {
    private val MIN_DELAY: Duration = Duration.ofSeconds(1)
    private val MAX_DELAY: Duration = Duration.ofDays(30)

    fun validateFireAt(fireAt: Instant, now: Instant) {
        val delay = Duration.between(now, fireAt)
        when {
            delay < MIN_DELAY ->
                throw InvalidFireAtException("fireAt must be at least 1 second in the future")
            delay > MAX_DELAY ->
                throw InvalidFireAtException("fireAt must be within 30 days")
        }
    }
}
