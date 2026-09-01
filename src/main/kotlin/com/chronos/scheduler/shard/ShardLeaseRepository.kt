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
     * Same SKIP LOCKED idiom as before, now captures the previous owner via a
     * CTE (same technique as TaskRepository.reclaimExpiredLeases, 8/9). This
     * lets callers tell "claimed a shard nobody held" apart from "took over a
     * shard someone else used to hold" — only the latter counts toward
     * lease_takeover_total (requirements.md §9).
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
     * falls back to this when its normal Phase-1 announce claims nothing
     * because a fast-starting peer already grabbed every shard. Phase 1's
     * whole job is to make this node visible in countDistinctActiveOwners()
     * before Phase 3 computes a soft cap; if it claims zero shards it stays
     * invisible, the over-holding peer keeps softCap = TOTAL_SHARDS forever,
     * and nothing ever rebalances (8/20). One stolen shard is enough — the
     * over-holder's next heartbeat then sees two active owners, recomputes a
     * fair softCap, and releaseExcessShards() hands the rest back.
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
     * Give up shards beyond `keep`, highest shard_id first. This is what makes
     * a bad start (or any transient imbalance) self-heal. If a node claimed
     * more than its fair share during the announce race — it ran Phase 3
     * before a slow-booting peer finished Phase 1, saw only itself as active,
     * and computed softCap = TOTAL_SHARDS — its next heartbeat sees
     * renewed > softCap and releases the excess here. Without this the
     * imbalance is permanent (8/20): the over-holder never drops below softCap
     * on its own, and the under-holder's claim query finds nothing available
     * because every shard still has a live lease.
     *
     * Releasing is as safe as a takeover: shards carry no in-flight state,
     * only tasks do, and task leases are independently version-guarded.
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

    /** Used to estimate how many nodes are currently active — see Step 3. */
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
     * is a CAS guard, not a defensive afterthought: if another node already
     * reclaimed one of these rows (this node was slow, or briefly partitioned),
     * that row's lease_owner no longer matches, and this UPDATE silently skips
     * it — the node's view of "what I own" self-corrects to match the database's
     * authoritative state on the very next poll, no special-case code needed.
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
