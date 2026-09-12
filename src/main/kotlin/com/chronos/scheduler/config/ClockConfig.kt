package com.chronos.scheduler.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock
import java.time.Duration

@Configuration
class ClockConfig {

    /**
     * The single source of "now" across the app. Every component that needs the
     * current time takes this as a constructor dependency rather than calling
     * Instant.now(), which is what lets scenarios 7 and 8 (requirements.md §10)
     * inject an offset or drifting Clock without touching business logic.
     *
     * offset-seconds: a fixed skew (scenario 7). Correctness does not depend on
     *   it, since fire_at and lease_expires_at comparisons run in Postgres; it
     *   only shifts the 1s..30d submission-validation window.
     * drift-rate: this clock advances at this multiple of real time (scenario
     *   8). It matters only where a duration is derived from this Clock, which
     *   today is ShardHeartbeat's renewal deadline.
     */
    @Bean
    fun clock(
        @Value("\${chronos.clock.offset-seconds:0}") offsetSeconds: Long,
        @Value("\${chronos.clock.drift-rate:1.0}") driftRate: Double,
    ): Clock {
        var clock: Clock = Clock.systemUTC()
        if (offsetSeconds != 0L) {
            clock = Clock.offset(clock, Duration.ofSeconds(offsetSeconds))
        }
        if (driftRate != 1.0) {
            clock = DriftingClock(clock, driftRate)
        }
        return clock
    }
}
