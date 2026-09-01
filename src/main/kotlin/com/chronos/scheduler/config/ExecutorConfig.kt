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
     * 8/27: was `Executors.newFixedThreadPool(16)` — a fixed pool with an
     * *unbounded* queue. Under a burst the poll loop kept claiming 500/poll/node
     * regardless of whether the pool could keep up, so `firing` rows piled to
     * 60–70 k and (with a slow downstream) their 30 s leases expired mid-queue →
     * a self-inflicted re-fire storm.
     *
     * Now a `ThreadPoolExecutor` with a **bounded** queue, exposed as its
     * concrete type so PollingLoop can read `queue.remainingCapacity()` and
     * claim only what there is room to fire (`chronos.poll.claim-limit` becomes
     * a ceiling, not a fixed amount). `CallerRunsPolicy` is the belt-and-braces
     * case: if the estimate is ever wrong and a submit would overflow, it runs
     * on the poll thread, which stalls claiming until the pool drains — a due
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
