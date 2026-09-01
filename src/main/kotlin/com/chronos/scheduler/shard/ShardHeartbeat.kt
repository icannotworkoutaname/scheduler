package com.chronos.scheduler.shard

import com.chronos.scheduler.node.NodeIdentity
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Every heartbeat period: renew whatever this node currently holds, then try
 * to claim more if it's below fair share. That second half is what makes
 * takeover happen — no separate "watch for dead nodes" mechanism exists. A
 * surviving node doesn't detect that another node died; it just keeps
 * periodically reaching for shards that satisfy claimAvailableShards'
 * condition (lease null or expired), and a dead node's former shards
 * eventually satisfy that condition on their own once the lease clock runs
 * out — nobody has to notice anything.
 *
 * The period is measured against the injected Clock, not Spring's @Scheduled
 * wall clock: @Scheduled just polls often (poll-ms), and the real renewal
 * only fires once `clock.instant()` has advanced a full period past the last
 * one. Under a normal Clock this is indistinguishable from the old
 * fixedDelay=10000. Under scenario 8's DriftingClock a slow node's renewals
 * stretch out in real time while the DB-side lease TTL does not — that
 * asymmetry is the whole point of the drift scenario (see DriftingClock).
 */
@Component
class ShardHeartbeat(
    private val shardLeaseRepository: ShardLeaseRepository,
    private val shardAllocator: ShardAllocator,
    private val nodeIdentity: NodeIdentity,
    private val clock: Clock,
    @Value("\${chronos.heartbeat.period-seconds:10}")
    private val periodSeconds: Long,
    meterRegistry: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val leaseTakeoverCounter = Counter.builder("chronos.lease.takeover.total")
        .description("shards this node claimed whose lease was previously held by a different, now-absent node")
        .register(meterRegistry)

    /**
     * Gates renewAndRebalance() until ShardBootstrap has finished its own
     * announce-then-settle protocol. Spring's @Scheduled fires its first
     * execution immediately on startup with no way to suppress that short of
     * an initialDelay — and any initialDelay is a guessed number that can
     * drift out of sync with settle-delay-seconds. A readiness flag set by
     * ShardBootstrap itself, once it's actually done, needs no timing
     * assumption at all.
     */
    private val ready = AtomicBoolean(false)

    /** Next moment, in injected-Clock time, that a renewal is allowed to run. */
    @Volatile
    private var nextRenewalDue: Instant = Instant.MIN

    /** Called by ShardBootstrap right after its settle window — this doubles as this node's first heartbeat, run deterministically rather than raced against the scheduler's own startup tick. */
    fun markReadyAndRun() {
        ready.set(true)
        nextRenewalDue = clock.instant()
        renewAndRebalance()
    }

    @Scheduled(fixedDelayString = "\${chronos.heartbeat.poll-ms:1000}")
    fun renewAndRebalance() {
        if (!ready.get()) return

        val now = clock.instant()
        if (now.isBefore(nextRenewalDue)) return
        nextRenewalDue = now.plusSeconds(periodSeconds)

        val renewed = shardLeaseRepository.renewOwnedShards(nodeIdentity.nodeId)

        val activeOwners = shardLeaseRepository.countDistinctActiveOwners()
        val softCap = shardAllocator.softCapFor(activeOwners)

        // Downward rebalance: hand back anything above fair share. Turns a
        // winner-takes-all start (or any transient over-hold) into something
        // that converges on the next heartbeat instead of staying broken.
        if (renewed.size > softCap) {
            val released = shardLeaseRepository.releaseExcessShards(nodeIdentity.nodeId, softCap)
            log.warn(
                "node {} released {} shard(s) — was holding {}, fair share is {}",
                nodeIdentity.nodeId, released.size, renewed.size, softCap
            )
        }

        val wanted = (softCap - renewed.size).coerceAtLeast(0)

        val newlyClaimed = if (wanted > 0) {
            shardLeaseRepository.claimAvailableShards(nodeIdentity.nodeId, wanted)
        } else {
            emptyList()
        }

        val takeovers = newlyClaimed.filter { it.previousOwner != null && it.previousOwner != nodeIdentity.nodeId }
        if (takeovers.isNotEmpty()) {
            leaseTakeoverCounter.increment(takeovers.size.toDouble())
            log.warn(
                "node {} took over {} shard(s) previously held by other node(s): {}",
                nodeIdentity.nodeId, takeovers.size, takeovers
            )
        }

        log.info(
            "node {} heartbeat: renewed={} activeOwners={} softCap={}",
            nodeIdentity.nodeId, renewed.size, activeOwners, softCap
        )
    }
}
