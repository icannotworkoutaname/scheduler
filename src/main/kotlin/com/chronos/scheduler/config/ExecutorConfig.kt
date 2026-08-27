package com.chronos.scheduler.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Configuration
class ExecutorConfig {

    /**
     * Bounded pool that claimed tasks get handed off to. Sizing is a
     * placeholder for now — real tuning happens under load on 8/26. What
     * matters today is that it's bounded: a burst of 500 claimed tasks
     * (the claim query's LIMIT) can't spawn 500 unbounded threads.
     */
    @Bean(destroyMethod = "shutdown")
    fun taskExecutor(): ExecutorService = Executors.newFixedThreadPool(16)
}
