package com.chronos.scheduler.shard

import com.chronos.scheduler.node.NodeIdentity
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Every 10s: renew whatever this node currently holds, then try to claim
 * more if it's below fair share. That second half is what makes takeover
 * happen — no separate "watch for dead nodes" mechanism exists. A surviving
 * node doesn't detect that another node died; it just keeps periodically
 * reaching for shards that satisfy claimAvailableShards' condition (lease
 * null or expired, from 8/10's ShardLeaseRepository), and a dead node's
 * former shards eventually satisfy that condition on their own once the
 * lease clock runs out — nobody has to notice anything.
 */
@Component
class ShardHeartbeat(
    private val shardLeaseRepository: ShardLeaseRepository,
    private val shardAllocator: ShardAllocator,
    private val nodeIdentity: NodeIdentity,
    meterRegistry: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val leaseTakeoverCounter = meterRegistry.counter("chronos.lease.takeover.total")

    /**
     * Gates renewAndRebalance() until ShardBootstrap has finished its own
     * announce-then-settle protocol. Spring's @Scheduled(fixedDelay = ...)
     * fires its first execution immediately on startup with no way to
     * suppress that short of an initialDelay — and any initialDelay is a
     * guessed number that can drift out of sync with settle-delay-seconds
     * (we hit exactly that: binding both to the same property just moved the
     * race from cross-node to intra-node). A readiness flag set by
     * ShardBootstrap itself, once it's actually done, needs no timing
     * assumption at all.
     */
    private val ready = AtomicBoolean(false)

    /** Called by ShardBootstrap right after its settle window — this doubles as this node's first heartbeat, run deterministically rather than raced against the scheduler's own startup tick. */
    fun markReadyAndRun() {
        ready.set(true)
        renewAndRebalance()
    }

    @Scheduled(fixedDelay = 10000)
    fun renewAndRebalance() {
        if (!ready.get()) return

        val renewed = shardLeaseRepository.renewOwnedShards(nodeIdentity.nodeId)

        val activeOwners = shardLeaseRepository.countDistinctActiveOwners()
        val softCap = shardAllocator.softCapFor(activeOwners)
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
