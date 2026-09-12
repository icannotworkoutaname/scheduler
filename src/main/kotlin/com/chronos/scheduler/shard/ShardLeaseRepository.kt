package com.chronos.scheduler.shard

import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository

data class ClaimedShard(val shardId: Int, val previousOwner: String?)

@Repository
class ShardLeaseRepository(
    private val jdbcClient: JdbcClient,
    /**
     * Shard lease lifetime, measured by the DATABASE clock (now() + this).
     * Default 30s (requirements.md §7.4). Configurable so chaos scenario 8
     * can shrink it — the drift crossover is rate = heartbeat-period / ttl,
     * independent of the absolute values, so a 6s TTL + 2s period tests the
     * exact same 1/3 coefficient in a fraction of the wall time.
     */
    @Value("\${chronos.shard.lease-ttl-seconds:30}")
    private val leaseTtlSeconds: Long,
) {

    /**
     * Claims up to softCap shards whose lease is null or expired. The CTE
     * captures the previous owner so callers can tell "claimed a shard nobody
     * held" from "took over a shard someone else held" — only the latter counts
     * toward lease_takeover_total (requirements.md §9).
     */
    fun claimAvailableShards(nodeId: String, softCap: Int): List<ClaimedShard> {
        if (softCap <= 0) return emptyList()

        return jdbcClient.sql(
            """
            WITH available AS (
                SELECT shard_id, lease_owner AS previous_owner
                  FROM shards
                 WHERE lease_owner IS NULL OR lease_expires_at < now()
                 ORDER BY shard_id
                 FOR UPDATE SKIP LOCKED
                 LIMIT :softCap
            )
            UPDATE shards
               SET lease_owner = :nodeId,
                   lease_expires_at = now() + make_interval(secs => :ttlSeconds),
                   version = version + 1
              FROM available
             WHERE shards.shard_id = available.shard_id
            RETURNING shards.shard_id, available.previous_owner
            """.trimIndent()
        )
            .param("nodeId", nodeId)
            .param("softCap", softCap)
            .param("ttlSeconds", leaseTtlSeconds.toDouble())
            .query { rs, _ ->
                ClaimedShard(
                    shardId = rs.getInt("shard_id"),
                    previousOwner = rs.getString("previous_owner"),
                )
            }
            .list()
    }

    /**
     * Take exactly one shard no matter what — an unowned one if there is any,
     * otherwise whichever live lease is closest to expiring. ShardBootstrap
     * falls back to this when its normal phase-1 announce claims nothing,
     * because a fast-starting peer already holds every shard. Phase 1 exists to
     * make this node visible in countDistinctActiveOwners() before phase 3
     * computes a soft cap; claiming zero shards leaves it invisible, the
     * over-holding peer keeps softCap = TOTAL_SHARDS, and nothing rebalances.
     * One shard is enough: the over-holder's next heartbeat sees two active
     * owners, recomputes the cap, and releaseExcessShards() hands the rest back.
     */
    fun forceClaimOneShard(nodeId: String): Int? {
        return jdbcClient.sql(
            """
            WITH victim AS (
                SELECT shard_id FROM shards
                 ORDER BY (lease_owner IS NULL) DESC,
                          lease_expires_at ASC NULLS FIRST,
                          shard_id DESC
                 LIMIT 1
                 FOR UPDATE SKIP LOCKED
            )
            UPDATE shards
               SET lease_owner = :nodeId,
                   lease_expires_at = now() + make_interval(secs => :ttlSeconds),
                   version = version + 1
              FROM victim
             WHERE shards.shard_id = victim.shard_id
            RETURNING shards.shard_id
            """.trimIndent()
        )
            .param("nodeId", nodeId)
            .param("ttlSeconds", leaseTtlSeconds.toDouble())
            .query { rs, _ -> rs.getInt("shard_id") }
            .optional()
            .orElse(null)
    }

    /**
     * Keeps the `keep` highest-numbered shards this node holds and releases the
     * rest. This is what makes an unbalanced start self-heal: a node that
     * claimed more than its fair share during the announce race (ran phase 3
     * before a slow-booting peer finished phase 1, saw only itself active,
     * computed softCap = TOTAL_SHARDS) sees renewed > softCap on its next
     * heartbeat and releases exactly the overage. Without this the imbalance is
     * permanent — the over-holder never drops below softCap on its own, and the
     * under-holder's claim query finds nothing available.
     *
     * It releases exactly `held - keep` and the heartbeat claims at most
     * `softCap - held`, so with N healthy nodes and Σ held = 64 ≤ N·softCap the
     * released shards never exceed what the under-holders want; the split
     * converges to a fixed point (ShardRebalanceConvergenceTest).
     *
     * Releasing is as safe as a takeover: shards carry no in-flight state, only
     * tasks do, and task leases are independently version-guarded.
     */
    fun releaseExcessShards(nodeId: String, keep: Int): List<Int> {
        return jdbcClient.sql(
            """
            UPDATE shards
               SET lease_owner = NULL,
                   lease_expires_at = NULL,
                   version = version + 1
             WHERE shard_id IN (
                 SELECT shard_id FROM shards
                  WHERE lease_owner = :nodeId
                  ORDER BY shard_id DESC
                  OFFSET :keep
             )
            RETURNING shard_id
            """.trimIndent()
        )
            .param("nodeId", nodeId)
            .param("keep", keep)
            .query { rs, _ -> rs.getInt("shard_id") }
            .list()
    }

    /** Active node count, used as the divisor in ShardAllocator.softCapFor. */
    fun countDistinctActiveOwners(): Int {
        return jdbcClient.sql(
            "SELECT count(DISTINCT lease_owner) FROM shards WHERE lease_expires_at >= now()"
        )
            .query(Int::class.java)
            .single()
    }

    fun ownedShardsFor(nodeId: String): List<Int> {
        return jdbcClient.sql(
            "SELECT shard_id FROM shards WHERE lease_owner = :nodeId AND lease_expires_at >= now()"
        )
            .param("nodeId", nodeId)
            .query { rs, _ -> rs.getInt("shard_id") }
            .list()
    }

    /**
     * Renews every shard this node currently holds. WHERE lease_owner = :nodeId
     * is the compare-and-set: if another node has already reclaimed one of these
     * rows (this node was slow or briefly partitioned), lease_owner no longer
     * matches and the UPDATE skips it, so the node's view of what it owns
     * self-corrects against the database on the next poll.
     */
    fun renewOwnedShards(nodeId: String): List<Int> {
        return jdbcClient.sql(
            """
            UPDATE shards
               SET lease_expires_at = now() + make_interval(secs => :ttlSeconds),
                   version = version + 1
             WHERE lease_owner = :nodeId
            RETURNING shard_id
            """.trimIndent()
        )
            .param("nodeId", nodeId)
            .param("ttlSeconds", leaseTtlSeconds.toDouble())
            .query { rs, _ -> rs.getInt("shard_id") }
            .list()
    }
}
