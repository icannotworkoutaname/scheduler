package com.chronos.scheduler.chaos

import com.chronos.scheduler.SchedulerApplication
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.UUID

/**
 * Chaos scenario 7 (8/23, clock offset) — behavior verification only.
 *
 * Node A's wall clock is pushed 5 minutes fast (Clock.offset). Its triggering
 * must be indistinguishable from a node with a correct clock, because every
 * correctness comparison — fire_at <= now(), lease_expires_at < now() — runs
 * in Postgres against the database clock. The node's own Clock only feeds
 * submission-time validation.
 *
 * (The "trigger_delay_seconds must be measured on the DB clock" half of the
 * 8/23 plan is deferred to 8/24, when that metric is actually wired to
 * Micrometer — see notes/scenario-7-8-notes.md.)
 */
@Testcontainers
class ClockOffsetTest {

    companion object {
        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")
            .withDatabaseName("scheduler")
            .withUsername("scheduler")
            .withPassword("scheduler")
    }

    private var ctxA: ConfigurableApplicationContext? = null
    private var ctxB: ConfigurableApplicationContext? = null
    private val receiver = FakeReceiver()

    @AfterEach
    fun tearDown() {
        ctxA?.close()
        ctxB?.close()
        receiver.stop()
    }

    private fun startNode(port: Int, vararg extra: String): ConfigurableApplicationContext =
        SpringApplicationBuilder(SchedulerApplication::class.java).run(
            "--server.port=$port",
            "--chronos.shard.settle-delay-seconds=5",
            "--spring.datasource.url=${postgres.jdbcUrl}",
            "--spring.datasource.username=${postgres.username}",
            "--spring.datasource.password=${postgres.password}",
            *extra,
        )

    private fun <T> queryOne(sql: String, map: (ResultSet) -> T): T =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { c ->
            c.createStatement().executeQuery(sql).use { rs ->
                rs.next()
                map(rs)
            }
        }

    private fun ownerCounts(): Map<String?, Int> =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { c ->
            val rs = c.createStatement().executeQuery(
                "SELECT lease_owner, count(*) k FROM shards WHERE lease_expires_at >= now() GROUP BY lease_owner"
            )
            buildMap { while (rs.next()) put(rs.getString("lease_owner"), rs.getInt("k")) }
        }

    private fun awaitEvenSplit() {
        val deadline = System.currentTimeMillis() + 35_000
        while (System.currentTimeMillis() < deadline) {
            val c = ownerCounts()
            if (c.size == 2 && c.values.all { it == 32 }) return
            Thread.sleep(2000)
        }
        fail<Unit>("nodes never converged to a 32/32 split: ${ownerCounts()}")
    }

    @Test
    fun `a node whose wall clock is 5 minutes fast still fires tasks on database time`() {
        // 串行启动 —— 见 ShardFailoverTest 里关于并发 run() 会撞 Spring Boot
        // 静态初始化抛 ConcurrentModificationException 的说明。
        ctxA = startNode(18080, "--chronos.clock.offset-seconds=300")
        ctxB = startNode(18081)
        awaitEvenSplit()

        val ids = (0 until 20).map { UUID.randomUUID() }
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { c ->
            c.prepareStatement(
                """
                INSERT INTO tasks (id, idempotency_key, payload, callback_url, fire_at, state, shard)
                VALUES (?, ?, '{}'::jsonb, ?, now() + interval '3 seconds', 'pending', ?)
                """.trimIndent()
            ).use { ps ->
                ids.forEachIndexed { i, id ->
                    ps.setObject(1, id)
                    ps.setString(2, "s7-$id")
                    ps.setString(3, receiver.hookUrl)
                    ps.setInt(4, (i * 3) % 64) // 铺开到 0..57，横跨两个节点各自那一半
                    ps.addBatch()
                }
                ps.executeBatch()
            }
        }

        val deadline = System.currentTimeMillis() + 20_000
        while (System.currentTimeMillis() < deadline) {
            val done = queryOne(
                "SELECT count(*) k FROM tasks WHERE idempotency_key LIKE 's7-%' AND state = 'succeeded'"
            ) { it.getInt("k") }
            if (done == 20) break
            Thread.sleep(1000)
        }

        val succeeded = queryOne(
            "SELECT count(*) k FROM tasks WHERE idempotency_key LIKE 's7-%' AND state = 'succeeded'"
        ) { it.getInt("k") }
        val firedEarly = queryOne(
            "SELECT count(*) k FROM tasks WHERE idempotency_key LIKE 's7-%' AND updated_at < fire_at"
        ) { it.getInt("k") }
        val worstDelaySeconds = queryOne(
            "SELECT coalesce(max(extract(epoch FROM (updated_at - fire_at))), 0) d FROM tasks WHERE idempotency_key LIKE 's7-%'"
        ) { it.getDouble("d") }
        val distinctFirers = queryOne(
            "SELECT count(DISTINCT lease_owner) k FROM tasks WHERE idempotency_key LIKE 's7-%'"
        ) { it.getInt("k") }

        assertEquals(20, succeeded, "all 20 tasks should have fired exactly once")
        assertEquals(
            0, firedEarly,
            "no task fired before its fire_at — a non-zero count would mean the node's +300s clock leaked into triggering"
        )
        assertTrue(
            worstDelaySeconds < 30,
            "worst fire delay was ${worstDelaySeconds}s; the +300s skew must not have shifted trigger timing"
        )
        assertEquals(
            2, distinctFirers,
            "both nodes — including the skewed one — should have fired their share of the 20"
        )
        ids.forEach { id ->
            assertTrue(receiver.hasSeen(id.toString()), "downstream never received trigger $id")
        }
    }
}
