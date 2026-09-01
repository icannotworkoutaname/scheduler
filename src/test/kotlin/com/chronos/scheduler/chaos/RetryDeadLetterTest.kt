package com.chronos.scheduler.chaos

import com.chronos.scheduler.task.TaskRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.DriverManager
import java.util.UUID

/**
 * Chaos scenario 2 (persistent downstream failure) — the fast, deterministic
 * version. The bash script (scripts/scenario2_persistent_failure.sh) waits out
 * the real 2+4+8+16s backoff against a live node; here we drive
 * TaskRepository's failure path directly, resetting fire_at to the past
 * between attempts, so the whole state machine — firing → retrying × 4 →
 * dead — runs in well under a second.
 *
 * What this pins: a task that fails every attempt is retried exactly
 * MAX_ATTEMPTS times and then dead-lettered, never lost, never stuck.
 */
@Testcontainers
class RetryDeadLetterTest {

    companion object {
        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")
            .withDatabaseName("scheduler")
            .withUsername("scheduler")
            .withPassword("scheduler")
    }

    private lateinit var repo: TaskRepository
    private lateinit var jdbc: JdbcClient

    @BeforeEach
    fun setUp() {
        val migration = javaClass.getResourceAsStream("/db/migration/V1__create_tasks_table.sql")!!
            .bufferedReader().readText()
        // 迁移脚本是多条语句 + $$ 美元引用，得走原始 Statement（PreparedStatement
        // 在 PG 下只吃单条），跟 ShardClaimRaceTest 一样。
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { c ->
            c.createStatement().execute(migration)
        }
        val ds = DriverManagerDataSource(postgres.jdbcUrl, postgres.username, postgres.password)
        jdbc = JdbcClient.create(ds)
        repo = TaskRepository(jdbc)
    }

    @Test
    fun `a task that fails every attempt is retried MAX_ATTEMPTS times then dead-lettered`() {
        val id = UUID.randomUUID()
        jdbc.sql(
            """
            INSERT INTO tasks (id, idempotency_key, payload, callback_url, fire_at, state, shard)
            VALUES (:id, :key, '{}'::jsonb, 'http://unused', now() - interval '1 second', 'pending', 7)
            """.trimIndent()
        ).param("id", id).param("key", "s2-$id").update()

        var claims = 0
        while (true) {
            val claimed = repo.claimDueTasks(listOf(7), "s2-node")
            if (claimed.isEmpty()) break
            claims++
            val task = claimed.single().task
            repo.markFailed(task.id, task.attemptCount, task.version)
            // markFailed on a retryable attempt pushes fire_at out by the
            // backoff; yank it back so the next claim picks it up immediately.
            jdbc.sql("UPDATE tasks SET fire_at = now() - interval '1 second' WHERE id = :id AND state = 'retrying'")
                .param("id", id).update()
            if (claims > 10) error("retry loop did not terminate")
        }

        val state = jdbc.sql("SELECT state FROM tasks WHERE id = :id").param("id", id)
            .query(String::class.java).single()
        val attempts = jdbc.sql("SELECT attempt_count FROM tasks WHERE id = :id").param("id", id)
            .query(Int::class.java).single()

        assertEquals(5, claims, "should have been claimed and fired exactly MAX_ATTEMPTS times")
        assertEquals(5, attempts, "attempt_count should record all 5 tries")
        assertEquals("dead", state, "after the 5th failure the task must be dead-lettered, not retrying or lost")
    }
}
