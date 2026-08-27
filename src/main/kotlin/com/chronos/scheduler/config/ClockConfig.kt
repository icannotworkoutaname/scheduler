package com.chronos.scheduler.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

@Configuration
class ClockConfig {

    /**
     * The single source of truth for "now" across the app. Every place that
     * needs the current time takes this as a constructor dependency instead of
     * calling Instant.now() directly — that's what lets chaos scenario 7/8
     * (requirements.md §10) inject an offset or drifting Clock at test time
     * without touching business logic.
     */
    @Bean
    fun clock(): Clock = Clock.systemUTC()
}
