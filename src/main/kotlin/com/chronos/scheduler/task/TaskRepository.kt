package com.chronos.scheduler.task

import org.postgresql.util.PGobject
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

@Repository
class TaskRepository(private val jdbcClient: JdbcClient) {

    /**
     * Submit-side idempotency (requirements.md §3, layer 1): if idempotencyKey
     * already exists, this returns the EXISTING task rather than creating a new
     * one or throwing. The uniqueness guarantee lives in the database constraint
     * (tasks_idempotency_key_uq) — this method just makes the conflict path
     * ergonomic to call from the controller.
     */
    fun insertOrGetExisting(task: Task): Task {
        val inserted = jdbcClient.sql(
            """
            INSERT INTO tasks
                (id, idempotency_key, payload, callback_url, fire_at, state, shard)
            VALUES
                (:id, :idempotencyKey, :payload, :callbackUrl, :fireAt, :state, :shard)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING *
            """.trimIndent()
        )
            .param("id", task.id)
            .param("idempotencyKey", task.idempotencyKey)
            .param("payload", task.payload.toJsonb())
            .param("callbackUrl", task.callbackUrl)
            .param("fireAt", Timestamp.from(task.fireAt))
            .param("state", task.state.name.lowercase())
            .param("shard", task.shard)
            .query(::mapRow)
            .optional()

        // ON CONFLICT DO NOTHING means zero rows come back on a duplicate key —
        // that's the conflict path. Fetch and return what's already there instead
        // of treating it as an error.
        return inserted.orElseGet { findByIdempotencyKey(task.idempotencyKey) }
    }

    fun claimDueTasks(shards: List<Int>, leaseOwner: String, limit: Int = 500): List<Task> {
        if (shards.isEmpty()) return emptyList()

        return jdbcClient.sql(
            """
            UPDATE tasks
               SET state = 'firing',
                   lease_owner = :leaseOwner,
                   lease_expires_at = now() + interval '30 seconds',
                   version = version + 1
             WHERE id IN (
                 SELECT id FROM tasks
                  WHERE shard IN (:shards)
                    AND state = 'pending'
                    AND fire_at <= now()
                  ORDER BY fire_at
                  FOR UPDATE SKIP LOCKED
                  LIMIT :limit
             )
            RETURNING *
            """.trimIndent()
        )
            .param("leaseOwner", leaseOwner)
            .param("shards", shards)
            .param("limit", limit)
            .query(::mapRow)
            .list()
    }

    fun findById(id: UUID): Task? =
        jdbcClient.sql("SELECT * FROM tasks WHERE id = :id")
            .param("id", id)
            .query(::mapRow)
            .optional()
            .orElse(null)

    private fun findByIdempotencyKey(key: String): Task =
        jdbcClient.sql("SELECT * FROM tasks WHERE idempotency_key = :key")
            .param("key", key)
            .query(::mapRow)
            .single()

    private fun String.toJsonb(): PGobject =
        PGobject().apply {
            type = "jsonb"
            value = this@toJsonb
        }

    private fun mapRow(rs: java.sql.ResultSet, rowNum: Int): Task = Task(
        id = rs.getObject("id", UUID::class.java),
        idempotencyKey = rs.getString("idempotency_key"),
        payload = rs.getString("payload"),
        callbackUrl = rs.getString("callback_url"),
        fireAt = rs.getTimestamp("fire_at").toInstant(),
        state = TaskState.fromDb(rs.getString("state")),
        version = rs.getLong("version"),
        shard = rs.getInt("shard"),
        leaseOwner = rs.getString("lease_owner"),
        leaseExpiresAt = rs.getTimestamp("lease_expires_at")?.toInstant(),
        attemptCount = rs.getInt("attempt_count"),
        createdAt = rs.getTimestamp("created_at").toInstant(),
        updatedAt = rs.getTimestamp("updated_at").toInstant(),
    )
}
