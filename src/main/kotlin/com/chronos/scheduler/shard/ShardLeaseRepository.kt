package com.chronos.scheduler.shard

import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository

data class ClaimedShard(val shardId: Int, val previousOwner: String?)

@Repository
class ShardLeaseRepository(private val jdbcClient: JdbcClient) {

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
                   lease_expires_at = now() + interval '30 seconds',
                   version = version + 1
              FROM available
             WHERE shards.shard_id = available.shard_id
            RETURNING shards.shard_id, available.previous_owner
            """.trimIndent()
        )
            .param("nodeId", nodeId)
            .param("softCap", softCap)
            .query { rs, _ ->
                ClaimedShard(
                    shardId = rs.getInt("shard_id"),
                    previousOwner = rs.getString("previous_owner"),
                )
            }
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
               SET lease_expires_at = now() + interval '30 seconds',
                   version = version + 1
             WHERE lease_owner = :nodeId
            RETURNING shard_id
            """.trimIndent()
        )
            .param("nodeId", nodeId)
            .query { rs, _ -> rs.getInt("shard_id") }
            .list()
    }
}
