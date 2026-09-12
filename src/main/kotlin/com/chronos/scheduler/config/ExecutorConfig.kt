package com.chronos.scheduler.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Configuration
class ExecutorConfig {

    /**
     * The pool that claimed tasks are handed to for firing.
     *
     * The queue is bounded. With the earlier fixed pool and its unbounded queue,
     * the poll loop kept claiming 500 per poll per node regardless of whether
     * the pool could keep up: `firing` rows reached 60–70 k and, against a slow
     * downstream, their 30s leases expired while still queued, producing a
     * self-inflicted re-fire storm (docs/performance.md §2).
     *
     * Exposed as the concrete ThreadPoolExecutor so PollingLoop can read
     * queue.remainingCapacity() and claim only what there is room to fire.
     * CallerRunsPolicy covers the case where that estimate is wrong: the submit
     * runs on the poll thread, stalling claiming until the pool drains, so a due
     * task is never dropped.
     */
    @Bean(destroyMethod = "shutdown")
    fun taskExecutor(
        @Value("\${chronos.executor.threads:16}") threads: Int,
        @Value("\${chronos.executor.queue-capacity:2000}") queueCapacity: Int,
    ): ThreadPoolExecutor = ThreadPoolExecutor(
        threads, threads,
        0L, TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(queueCapacity),
        ThreadPoolExecutor.CallerRunsPolicy(),
    )
}
