package com.chronos.scheduler.config

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

/**
 * A Clock whose observed time advances at `rate` × real time. rate < 1 means
 * this node's clock runs slow: its "10 seconds" takes 10/rate real seconds.
 *
 * chaos scenario 8 (8/23, clock drift). The heartbeat renewal cadence is
 * derived from the injected Clock (see ShardHeartbeat), but the shard lease
 * TTL is still measured by the database's own clock (ShardLeaseRepository's
 * `now() + interval`). So a node running at `rate` renews every
 * period/rate real seconds against a TTL that is still a fixed number of real
 * seconds. The design's safety margin — renew every 10s, lease lasts 30s, so
 * tolerate two missed renewals — collapses exactly when
 *
 *     period / rate  =  ttl        →  rate = period / ttl = 10 / 30 = 1/3
 *
 * At rate just above 1/3 the slow node keeps its shards; at 1/3 or below it
 * renews no faster than the lease expires and the other node takes over,
 * reproducing the shard thrash and (via the same arithmetic on the sink
 * timeout vs the task lease) scenario 6's double fire.
 */
class DriftingClock(
    private val base: Clock,
    private val rate: Double,
    private val anchor: Instant,
    private val zone: ZoneId,
) : Clock() {

    constructor(base: Clock, rate: Double) : this(base, rate, base.instant(), base.zone)

    override fun getZone(): ZoneId = zone

    override fun withZone(zone: ZoneId): Clock = DriftingClock(base, rate, anchor, zone)

    override fun instant(): Instant {
        val realElapsedMillis = Duration.between(anchor, base.instant()).toMillis()
        val scaledMillis = (realElapsedMillis * rate).toLong()
        return anchor.plusMillis(scaledMillis)
    }
}
