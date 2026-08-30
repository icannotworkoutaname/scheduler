package com.chronos.scheduler.shard

import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository

@Repository
class ShardLeaseRepository(private val jdbcClient: JdbcClient) {

    /**
     * Greedily claims up to `softCap` shards whose lease is null or expired.
     * Same SKIP LOCKED idiom as TaskRepository.claimDueTasks (requirements.md
     * §7.1) — multiple nodes can call this concurrently against the same 64
     * rows without blocking each other, each just skips rows another node is
     * currently touching.
     */
    fun claimAvailableShards(nodeId: String, softCap: Int): List<Int> {
        if (softCap <= 0) return emptyList()

        return jdbcClient.sql(
            """
            UPDATE shards
               SET lease_owner = :nodeId,
                   lease_expires_at = now() + interval '30 seconds',
                   version = version + 1
             WHERE shard_id IN (
                 SELECT shard_id FROM shards
                  WHERE lease_owner IS NULL OR lease_expires_at < now()
                  ORDER BY shard_id
                  FOR UPDATE SKIP LOCKED
                  LIMIT :softCap
             )
            RETURNING shard_id
            """.trimIndent()
        )
            .param("nodeId", nodeId)
            .param("softCap", softCap)
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
