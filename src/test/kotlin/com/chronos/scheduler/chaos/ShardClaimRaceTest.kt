package com.chronos.scheduler.chaos

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.DriverManager
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * N concurrent racers all try to
 * claim the same batch of due tasks. Deliberately raw JDBC, not a full Spring
 * context — this test is about whether the SQL itself (FOR UPDATE SKIP
 * LOCKED) is safe under concurrency, which doesn't need the application
 * layer at all. Keeping it framework-free makes it fast and removes an
 * entire category of "was it my Spring wiring or the SQL" ambiguity.
 */
@Testcontainers
class ShardClaimRaceTest {

    companion object {
        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")
            .withDatabaseName("scheduler")
            .withUsername("scheduler")
            .withPassword("scheduler")
    }

    @BeforeEach
    fun setUpSchema() {
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
            val migration = javaClass.getResourceAsStream("/db/migration/V1__create_tasks_table.sql")!!
                .bufferedReader().readText()
            conn.createStatement().execute(migration)
        }
    }

    @Test
    fun `concurrent claimers never claim the same task twice`() {
        val taskCount = 50
        val racerCount = 10

        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
            conn.createStatement().use { st ->
                st.execute(
                    """
                    INSERT INTO tasks (id, idempotency_key, payload, callback_url, fire_at, state, shard)
                    SELECT gen_random_uuid(), 'race-' || g, '{}'::jsonb, 'http://x', now() - interval '1 second', 'pending', 10
                    FROM generate_series(1, $taskCount) g
                    """.trimIndent()
                )
            }
        }

        val claimSql = """
            UPDATE tasks
               SET state = 'firing', lease_owner = ?, lease_expires_at = now() + interval '30 seconds',
                   version = version + 1, attempt_count = attempt_count + 1
             WHERE id IN (
                 SELECT id FROM tasks WHERE shard IN (10) AND state IN ('pending','retrying') AND fire_at <= now()
                 ORDER BY fire_at FOR UPDATE SKIP LOCKED LIMIT $taskCount
             )
            RETURNING id
        """.trimIndent()

        val pool = Executors.newFixedThreadPool(racerCount)
        val results = (1..racerCount).map { racerId ->
            pool.submit<List<String>> {
                DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
                    conn.prepareStatement(claimSql).use { ps ->
                        ps.setString(1, "racer-$racerId")
                        val rs = ps.executeQuery()
                        val ids = mutableListOf<String>()
                        while (rs.next()) ids.add(rs.getString("id"))
                        ids
                    }
                }
            }
        }.map { it.get() }
        pool.shutdown()
        pool.awaitTermination(30, TimeUnit.SECONDS)

        val allClaimedIds = results.flatten()
        val uniqueIds = allClaimedIds.toSet()

        assertEquals(taskCount, allClaimedIds.size, "total claimed across all racers should equal task count exactly once each")
        assertEquals(uniqueIds.size, allClaimedIds.size, "no task id should appear in more than one racer's result — a duplicate here means SKIP LOCKED failed")
    }
}
