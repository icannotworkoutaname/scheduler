package com.chronos.scheduler.chaos

import com.chronos.scheduler.SchedulerApplication
import com.chronos.scheduler.config.SchedulerMetrics
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.DriverManager
import java.util.UUID

/**
 * ADR-004 decision 1 — the metric half of chaos scenario 7.
 *
 * trigger_delay_seconds must be computed from the database clock (db_fired_at −
 * fire_at, both from the same UPDATE ... RETURNING). A node whose wall clock is
 * 5 minutes fast must still report a delay of a few seconds, not ~300s.
 */
@Testcontainers
class TriggerDelayMetricTest {

    companion object {
        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")
            .withDatabaseName("scheduler")
            .withUsername("scheduler")
            .withPassword("scheduler")
    }

    private var ctx: ConfigurableApplicationContext? = null
    private val receiver = FakeReceiver()

    @AfterEach
    fun tearDown() {
        ctx?.close()
        receiver.stop()
    }

    @Test
    fun `trigger delay is measured on the db clock, not the +5min node clock`() {
        ctx = SpringApplicationBuilder(SchedulerApplication::class.java).run(
            "--server.port=17080",
            "--chronos.shard.settle-delay-seconds=2",
            "--chronos.clock.offset-seconds=300", // wall clock 5 minutes fast
            "--spring.datasource.url=${postgres.jdbcUrl}",
            "--spring.datasource.username=${postgres.username}",
            "--spring.datasource.password=${postgres.password}",
        )
        Thread.sleep(4000) // single node — claims all 64 shards

        val id = UUID.randomUUID()
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { c ->
            c.prepareStatement(
                """
                INSERT INTO tasks (id, idempotency_key, payload, callback_url, fire_at, state, shard)
                VALUES (?, ?, '{}'::jsonb, ?, now() - interval '2 seconds', 'pending', 5)
                """.trimIndent()
            ).use { ps ->
                ps.setObject(1, id)
                ps.setString(2, "trigdelay-$id")
                ps.setString(3, receiver.hookUrl)
                ps.executeUpdate()
            }
        }

        val deadline = System.currentTimeMillis() + 15_000
        while (System.currentTimeMillis() < deadline && !receiver.hasSeen(id.toString())) {
            Thread.sleep(500)
        }
        assertTrue(receiver.hasSeen(id.toString()), "task never fired")

        val metrics = ctx!!.getBean(SchedulerMetrics::class.java)
        val count = metrics.triggerDelayCount()
        val meanSeconds = metrics.triggerDelayMeanSeconds()

        assertTrue(count >= 1, "trigger_delay recorded nothing")
        // Real delay: the task was 2s overdue + poll latency, so ~2-4s. If the
        // metric had used the node's Instant.now() it would read ~302s (skew
        // added) or clamp to 0 (skew subtracted); either fails this band.
        assertTrue(
            meanSeconds in 1.0..30.0,
            "trigger_delay mean was ${meanSeconds}s — the +300s node clock leaked into a metric that must be pure DB time"
        )
    }
}
