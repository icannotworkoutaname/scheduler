package com.chronos.scheduler.task

import com.chronos.scheduler.sink.RetryPolicy
import org.postgresql.util.PGobject
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

data class ReclaimedLease(val taskId: UUID, val previousOwner: String?)

/**
 * A task the claim query just moved to 'firing', plus the database's own `now()`
 * from that same UPDATE ... RETURNING. requirements.md §9 / ADR-004 decision 1:
 * trigger delay is `dbFiredAt - task.fireAt` — both timestamps come from Postgres,
 * never a node's Instant.now(), so a node with a skewed or drifting clock still
 * reports a correct delay.
 */
data class ClaimedTask(val task: Task, val dbFiredAt: Instant) {
    val triggerDelay: java.time.Duration
        get() = java.time.Duration.between(task.fireAt, dbFiredAt)
}

@Repository
class TaskRepository(
    private val jdbcClient: JdbcClient,
    /**
     * How long a claimed task stays 'firing' before the reaper may reclaim it.
     * Default 30s (requirements.md §6). Configurable so `make demo` / the
     * accelerated tests can shrink the "wait for the lease to expire" step from
     * ~40s to a few seconds — the mechanism is identical, only the clock is
     * faster. Production stays 30s.
     */
    @Value("\${chronos.task.lease-ttl-seconds:30}")
    private val taskLeaseTtlSeconds: Long = 30,
) {

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

    fun claimDueTasks(shards: List<Int>, leaseOwner: String, limit: Int = 500): List<ClaimedTask> {
        if (shards.isEmpty()) return emptyList()

        return jdbcClient.sql(
            """
            UPDATE tasks
               SET state = 'firing',
                   lease_owner = :leaseOwner,
                   lease_expires_at = now() + (:ttlSeconds * interval '1 second'),
                   version = version + 1,
                   attempt_count = attempt_count + 1
             WHERE id IN (
                 SELECT id FROM tasks
                  WHERE shard IN (:shards)
                    AND state IN ('pending', 'retrying')
                    AND fire_at <= now()
                  ORDER BY fire_at
                  FOR UPDATE SKIP LOCKED
                  LIMIT :limit
             )
            RETURNING *, now() AS db_fired_at
            """.trimIndent()
        )
            .param("leaseOwner", leaseOwner)
            .param("shards", shards)
            .param("limit", limit)
            .param("ttlSeconds", taskLeaseTtlSeconds)
            .query { rs, rowNum ->
                ClaimedTask(mapRow(rs, rowNum), rs.getTimestamp("db_fired_at").toInstant())
            }
            .list()
    }

    /**
     * requirements.md §9 / ADR-004 decision 3: backing query for the per-shard
     * live-load gauge, every 15s from SchedulerMetrics' own thread.
     *
     * 8/27, two dead ends before this: the original
     * `WHERE state IN (pending,retrying) GROUP BY shard` is a 91ms parallel
     * full-table Seq Scan at 1M rows (`state` matches ~everything, no `shard =`
     * to seek on). Bounding it to "due in the next 5 min" only made it worse
     * (300ms) — under a real drain that window holds tens of thousands of rows
     * and an exact count is O(matches).
     *
     * What's actually cheap AND the right question: how many rows each shard is
     * firing *right now*. `state = 'firing'` is highly selective (backpressure
     * caps it at a few thousand total), the partial index
     * `tasks_firing_lease_expires_idx WHERE state = 'firing'` covers it, and the
     * count is the live per-shard work distribution — 0 for every shard when
     * idle, which correctly reads as "no load". ~0.9ms at rest, ~15ms mid-burst.
     */
    fun firingCountByShard(): Map<Int, Int> =
        jdbcClient.sql(
            "SELECT shard, count(*) c FROM tasks WHERE state = 'firing' GROUP BY shard"
        )
            .query { rs, _ -> rs.getInt("shard") to rs.getInt("c") }
            .list()
            .toMap()

    /**
     * firing -> succeeded, guarded by the version this node observed at claim
     * time. A 0-row update here would mean something else touched this row
     * between claim and completion — shouldn't happen in the single-node case
     * we're in today, but the guard costs nothing and stays correct once 8/10
     * introduces real multi-node contention.
     */
    fun markSucceeded(id: UUID, expectedVersion: Long): Boolean {
        val rows = jdbcClient.sql(
            """
            UPDATE tasks
               SET state = 'succeeded',
                   version = version + 1
             WHERE id = :id AND version = :expectedVersion
            """.trimIndent()
        )
            .param("id", id)
            .param("expectedVersion", expectedVersion)
            .update()
        return rows == 1
    }

    /**
     * firing -> retrying (with backoff) or firing -> dead, decided by whether
     * attempt has hit RetryPolicy.MAX_ATTEMPTS. Guarded by expectedVersion for
     * the same reason markSucceeded is — cheap correctness insurance.
     */
    fun markFailed(id: UUID, attempt: Int, expectedVersion: Long): Boolean {
        val rows = if (RetryPolicy.shouldRetry(attempt)) {
            val backoff = RetryPolicy.backoffFor(attempt)
            jdbcClient.sql(
                """
                UPDATE tasks
                   SET state = 'retrying',
                       fire_at = now() + make_interval(secs => :backoffSeconds),
                       version = version + 1
                 WHERE id = :id AND version = :expectedVersion
                """.trimIndent()
            )
                .param("id", id)
                .param("expectedVersion", expectedVersion)
                .param("backoffSeconds", backoff.seconds.toDouble())
                .update()
        } else {
            jdbcClient.sql(
                """
                UPDATE tasks
                   SET state = 'dead',
                       version = version + 1
                 WHERE id = :id AND version = :expectedVersion
                """.trimIndent()
            )
                .param("id", id)
                .param("expectedVersion", expectedVersion)
                .update()
        }
        return rows == 1
    }

    /**
     * requirements.md §6: sweeps rows stuck in 'firing' past their lease and
     * returns them to 'pending'. This is the mechanism that turns a node freeze
     * into a recoverable duplicate rather than a lost task — the entire premise
     * of chaos scenario 6 (8/21) depends on this working correctly.
     *
     * The CTE captures lease_owner BEFORE it gets wiped, purely for the log line
     * below — knowing which node died is useful during the freeze scenario.
     * Still a single atomic statement: no intermediate state a crash can land in,
     * same discipline as the claim query in §7.1.
     */
    fun reclaimExpiredLeases(limit: Int = 500): List<ReclaimedLease> {
        return jdbcClient.sql(
            """
            WITH expired AS (
                SELECT id, lease_owner AS previous_owner
                  FROM tasks
                 WHERE state = 'firing'
                   AND lease_expires_at < now()
                 FOR UPDATE SKIP LOCKED
                 LIMIT :limit
            )
            UPDATE tasks
               SET state = 'pending',
                   lease_owner = NULL,
                   lease_expires_at = NULL,
                   version = version + 1
              FROM expired
             WHERE tasks.id = expired.id
            RETURNING tasks.id, expired.previous_owner
            """.trimIndent()
        )
            .param("limit", limit)
            .query { rs, _ ->
                ReclaimedLease(
                    taskId = rs.getObject("id", UUID::class.java),
                    previousOwner = rs.getString("previous_owner"),
                )
            }
            .list()
    }

    /**
     * requirements.md §6: "cancel and reschedule are legal only from pending."
     * The WHERE clause IS that rule — a task in any other state just won't
     * match, 0 rows come back, no separate state-check-then-update needed.
     */
    fun cancelIfPending(id: UUID): Boolean {
        val rows = jdbcClient.sql(
            """
            UPDATE tasks
               SET state = 'cancelled', version = version + 1
             WHERE id = :id AND state = 'pending'
            """.trimIndent()
        )
            .param("id", id)
            .update()
        return rows == 1
    }

    /**
     * requirements.md §7: reschedule under the polling design is just an
     * UPDATE fire_at — no data structure to rebalance. version is the
     * client-supplied optimistic lock (requirements.md §3), guarding against
     * two callers racing to reschedule off the same stale read.
     */
    fun rescheduleIfPending(id: UUID, newFireAt: Instant, expectedVersion: Long): Boolean {
        val rows = jdbcClient.sql(
            """
            UPDATE tasks
               SET fire_at = :newFireAt, version = version + 1
             WHERE id = :id AND state = 'pending' AND version = :expectedVersion
            """.trimIndent()
        )
            .param("id", id)
            .param("newFireAt", Timestamp.from(newFireAt))
            .param("expectedVersion", expectedVersion)
            .update()
        return rows == 1
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
