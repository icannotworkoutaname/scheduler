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
 * Every heartbeat period: renew what this node holds, then claim more if it is
 * below fair share. The second half is the takeover mechanism — there is no
 * separate death-detection path. A surviving node never detects that a peer
 * died; it keeps reaching for shards whose lease is null or expired, and a dead
 * node's shards satisfy that condition once its lease runs out.
 *
 * The period is measured against the injected Clock, not @Scheduled's wall
 * clock: @Scheduled polls frequently (poll-ms) and a renewal happens only once
 * clock.instant() has advanced a full period. Under a normal Clock this is
 * equivalent to fixedDelay=10000. Under scenario 8's DriftingClock a slow node's
 * renewals stretch out in real time while the database-side lease TTL does not,
 * which is what the drift scenario measures (see DriftingClock).
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
     * Gates renewAndRebalance() until ShardBootstrap has finished its
     * announce-then-settle protocol. @Scheduled fires its first execution
     * immediately, and the only built-in alternative is an initialDelay, which
     * would be a second timing constant to keep in sync with
     * settle-delay-seconds. A flag set by ShardBootstrap needs no such constant.
     */
    private val ready = AtomicBoolean(false)

    /** Next moment, in injected-Clock time, that a renewal is allowed to run. */
    @Volatile
    private var nextRenewalDue: Instant = Instant.MIN

    /** Called by ShardBootstrap after its settle window; doubles as this node's first heartbeat. */
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

        // Downward rebalance: hand back anything above fair share, so a
        // winner-takes-all start converges on the next heartbeat.
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
