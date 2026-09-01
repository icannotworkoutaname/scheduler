package com.chronos.scheduler.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock
import java.time.Duration

@Configuration
class ClockConfig {

    /**
     * The single source of truth for "now" across the app. Every place that
     * needs the current time takes this as a constructor dependency instead of
     * calling Instant.now() directly — that's what lets chaos scenario 7/8
     * (requirements.md §10) inject an offset or drifting Clock at test time
     * without touching business logic.
     *
     * offset-seconds: a fixed skew (scenario 7). Correctness never depends on
     *   it — fire_at / lease_expires_at comparisons all run in Postgres — so
     *   this only shifts the ±1s..±30d submission-validation window.
     * drift-rate: wall-clock runs at this multiple of real time (scenario 8).
     *   Only meaningful once something derives a *duration* from this Clock;
     *   ShardHeartbeat's renewal deadline is that something.
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
