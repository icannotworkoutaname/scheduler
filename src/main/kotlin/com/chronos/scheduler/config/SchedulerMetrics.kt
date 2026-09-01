package com.chronos.scheduler.config

import com.chronos.scheduler.task.TaskRepository
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * requirements.md §9 — the instrumentation, in one place so the semantic
 * decisions (docs/adr/004-metrics-semantics.md) sit next to the code that
 * enforces them. `chronos.lease.takeover.total` is the exception: it was wired
 * into ShardHeartbeat on 8/12 and stays there.
 */
@Component
class SchedulerMetrics(
    private val registry: MeterRegistry,
    private val taskRepository: TaskRepository,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        const val PENDING_SAMPLE_SECONDS = 15L
        const val TOTAL_SHARDS = 64
    }

    /**
     * ADR-004 decision 1 + 4: delay = db_fired_at − fire_at, both from Postgres.
     * Explicit buckets, not Micrometer defaults — the SLO points P50 ≤ 200ms and
     * P99 ≤ 1s each get a bucket boundary on both sides so histogram_quantile
     * has resolution exactly where it matters.
     */
    private val triggerDelay: Timer = Timer.builder("chronos.trigger.delay.seconds")
        .description("time from a task's fire_at to the instant it was claimed; measured entirely on the database clock, so node clock skew/drift cannot distort it")
        .serviceLevelObjectives(
            Duration.ofMillis(50),
            Duration.ofMillis(100),
            Duration.ofMillis(200),
            Duration.ofMillis(500),
            Duration.ofSeconds(1),
            Duration.ofSeconds(2),
            Duration.ofSeconds(5),
        )
        .register(registry)

    /**
     * ADR-004 decision 2: this is NOT "duplicates I prevented". A single node
     * cannot know a fire is a duplicate — from its view every fire it makes is
     * legitimate. This counts the event "a node finished firing a task that
     * another node had already driven to 'succeeded', and its conditional
     * state-update touched 0 rows". Those 0 rows ARE a late duplicate delivery
     * being absorbed by the optimistic lock. The duplicate happened; this
     * records that it was harmless.
     */
    private val duplicateTrigger: Counter = Counter.builder("chronos.duplicate.trigger.total")
        .description("fires delivered downstream for a task another node had already completed; absorbed by the optimistic lock (0-row conditional update)")
        .register(registry)

    private val tasksDead: Counter = Counter.builder("chronos.tasks.dead.total")
        .description("tasks moved to the dead-letter state after exhausting retries")
        .register(registry)

    /** Covers the no-op path too — 8/26 uses the idle value to show polling is a cheap bounded index scan when nothing is due. */
    private val pollTimer: Timer = Timer.builder("chronos.poll.duration.seconds")
        .description("one poll-and-claim cycle, including the path where nothing is due")
        .serviceLevelObjectives(
            Duration.ofMillis(1),
            Duration.ofMillis(5),
            Duration.ofMillis(20),
            Duration.ofMillis(100),
            Duration.ofMillis(500),
        )
        .register(registry)

    private val sinkTimerSuccess: Timer = sinkTimer("success")
    private val sinkTimerFailure: Timer = sinkTimer("failure")

    private fun sinkTimer(outcome: String): Timer = Timer.builder("chronos.sink.call.duration.seconds")
        .description("downstream HTTP callback round trip")
        .tag("outcome", outcome)
        .serviceLevelObjectives(
            Duration.ofMillis(10),
            Duration.ofMillis(50),
            Duration.ofMillis(200),
            Duration.ofSeconds(1),
            Duration.ofSeconds(10),
        )
        .register(registry)

    // --- chronos.tasks.firing: per-shard live-load gauge, ADR-004 decision 3 -
    // 8/27: renamed from chronos.tasks.pending (never was "total backlog"). It
    // now counts what each shard is firing right now — cheap (state='firing' is
    // selective and covered by the partial index), and the live per-shard work
    // distribution, which is what "are the shards balanced" actually asks. 0 for
    // every shard at rest, correctly reading as "no load".
    private val firingByShard = Array(TOTAL_SHARDS) { AtomicInteger(0) }
    private var sampler: ScheduledExecutorService? = null

    @PostConstruct
    fun start() {
        for (shard in 0 until TOTAL_SHARDS) {
            Gauge.builder("chronos.tasks.firing", firingByShard[shard]) { it.get().toDouble() }
                .description(
                    "tasks this shard is firing right now — the live per-shard load. " +
                        "Sampled every ${PENDING_SAMPLE_SECONDS}s, not per scrape; 0 everywhere means idle."
                )
                .tag("shard", shard.toString())
                .register(registry)
        }
        // 独立单线程，绝不阻塞共享的 @Scheduled 轮询线程（心跳停顿排查里那条教训）。
        sampler = Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "firing-load-sampler").apply { isDaemon = true }
        }.also {
            it.scheduleWithFixedDelay(::sampleFiring, 0, PENDING_SAMPLE_SECONDS, TimeUnit.SECONDS)
        }
    }

    @PreDestroy
    fun stop() {
        sampler?.shutdownNow()
    }

    private fun sampleFiring() {
        try {
            val counts = taskRepository.firingCountByShard()
            for (shard in 0 until TOTAL_SHARDS) {
                firingByShard[shard].set(counts.getOrDefault(shard, 0))
            }
        } catch (e: Exception) {
            log.warn("firing-load sample failed: {}", e.message)
        }
    }

    // --- recording entry points --------------------------------------------

    fun recordTriggerDelay(delay: Duration) {
        // A negative delay (fire_at in the future) shouldn't be reachable — the
        // claim query filters fire_at <= now() — but clamp rather than let the
        // timer throw if the clocks ever disagree by a hair.
        triggerDelay.record(if (delay.isNegative) Duration.ZERO else delay)
    }

    fun duplicateTriggerAbsorbed() = duplicateTrigger.increment()

    fun taskDeadLettered() = tasksDead.increment()

    fun <T> timePoll(block: () -> T): T {
        val sample = Timer.start(registry)
        try {
            return block()
        } finally {
            sample.stop(pollTimer)
        }
    }

    fun recordSinkCall(elapsed: Duration, success: Boolean) {
        (if (success) sinkTimerSuccess else sinkTimerFailure).record(elapsed)
    }

    /** Current sampled pending count for a shard — test/diagnostic hook. */
    fun firingForShard(shard: Int): Int = firingByShard[shard].get()

    /** Force a firing-load sample now — test hook so a test doesn't wait 15s. */
    fun sampleNow() = sampleFiring()

    fun triggerDelayCount(): Long = triggerDelay.count()
    fun triggerDelayMeanSeconds(): Double = triggerDelay.mean(TimeUnit.SECONDS)
}
