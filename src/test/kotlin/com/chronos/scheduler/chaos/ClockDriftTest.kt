package com.chronos.scheduler.chaos

import com.chronos.scheduler.SchedulerApplication
import com.chronos.scheduler.node.NodeIdentity
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.DriverManager

/**
 * Chaos scenario 8 (clock drift). The deliverable is a number: the
 * drift rate at which the design collapses.
 *
 * ShardHeartbeat derives its renewal cadence from the injected Clock, but the
 * shard lease TTL is still measured by Postgres' own clock. So a node running
 * at rate r renews every PERIOD/r real seconds against a TTL that is a fixed
 * number of real seconds. The safety margin — renew every PERIOD, lease lasts
 * TTL, tolerate (TTL/PERIOD − 1) missed renewals — is gone exactly when
 * PERIOD/r == TTL, i.e.
 *
 *     r = PERIOD / TTL
 *
 * This test uses PERIOD=2s, TTL=6s, so the crossover is r = 1/3 — the same
 * ratio as the production 10s / 30s. It checks both sides:
 *   r = 0.5 (> 1/3): renews every 4s  < 6s TTL  → drifted node holds its 32
 *   r = 0.2 (< 1/3): renews every 10s > 6s TTL  → drifted node can't keep any
 *
 * Nodes are started sequentially (normal node first, then the drifted one) —
 * two concurrent SpringApplication.run() calls race Spring Boot's static
 * init and throw ConcurrentModificationException, unrelated to the code
 * under test.
 */
@Testcontainers
class ClockDriftTest {

    companion object {
        const val PERIOD_SECONDS = 2L
        const val TTL_SECONDS = 6L

        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")
            .withDatabaseName("scheduler")
            .withUsername("scheduler")
            .withPassword("scheduler")
    }

    private var ctxNormal: ConfigurableApplicationContext? = null
    private var ctxDrift: ConfigurableApplicationContext? = null

    @BeforeEach
    fun clearLeases() {
        // 第一个测试跑之前 shards 表还不存在（要等 test 里起 context 触发 Flyway）；
        // 后续测试则需要清掉上一个测试残留的租约。42P01 = undefined_table，忽略即可。
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { c ->
            try {
                c.createStatement().execute("UPDATE shards SET lease_owner = NULL, lease_expires_at = NULL")
            } catch (e: org.postgresql.util.PSQLException) {
                if (e.sqlState != "42P01") throw e
            }
        }
    }

    @AfterEach
    fun tearDown() {
        ctxDrift?.close()
        ctxNormal?.close()
        ctxDrift = null
        ctxNormal = null
    }

    private fun startNode(port: Int, vararg extra: String): ConfigurableApplicationContext =
        SpringApplicationBuilder(SchedulerApplication::class.java).run(
            "--server.port=$port",
            "--chronos.shard.settle-delay-seconds=5",
            "--chronos.heartbeat.period-seconds=$PERIOD_SECONDS",
            "--chronos.shard.lease-ttl-seconds=$TTL_SECONDS",
            "--spring.datasource.url=${postgres.jdbcUrl}",
            "--spring.datasource.username=${postgres.username}",
            "--spring.datasource.password=${postgres.password}",
            *extra,
        )

    private fun shardCountFor(owner: String): Int =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { c ->
            c.createStatement().executeQuery(
                "SELECT count(*) k FROM shards WHERE lease_owner = '$owner' AND lease_expires_at >= now()"
            ).use { rs -> rs.next(); rs.getInt("k") }
        }

    private fun nodeIdOf(ctx: ConfigurableApplicationContext): String =
        ctx.getBean(NodeIdentity::class.java).nodeId

    /**
     * Normal node then drifted node, sequentially. Samples the drifted node's
     * live shard count once a second for [sampleSeconds] and returns the list.
     */
    private fun driftedNodeShardSamples(rate: Double, sampleSeconds: Int): List<Int> {
        ctxNormal = startNode(18081)
        ctxDrift = startNode(18080, "--chronos.clock.drift-rate=$rate")
        val drifted = nodeIdOf(ctxDrift!!)

        // 给再平衡一点时间：normal 节点释放多余的一半，drifted 节点认领。
        Thread.sleep(15_000)

        val samples = mutableListOf<Int>()
        repeat(sampleSeconds) {
            samples += shardCountFor(drifted)
            Thread.sleep(1000)
        }
        println("drift rate $rate — drifted node shard-count samples: $samples")
        return samples
    }

    @Test
    fun `above the crossover r=0_5 a drifting node holds its fair share`() {
        val samples = driftedNodeShardSamples(rate = 0.5, sampleSeconds = 20)
        assertEquals(
            32, samples.min(),
            "at r=0.5 the drifted node renews every ${PERIOD_SECONDS / 0.5}s, inside the ${TTL_SECONDS}s TTL — it should hold 32 without ever dropping one"
        )
    }

    @Test
    fun `below the crossover r=0_2 a drifting node cannot keep its shards`() {
        val samples = driftedNodeShardSamples(rate = 0.2, sampleSeconds = 25)
        assertTrue(
            samples.min() <= 4,
            "at r=0.2 the drifted node renews every ${PERIOD_SECONDS / 0.2}s, past the ${TTL_SECONDS}s TTL — its leases keep expiring and the normal node reclaims them (samples=$samples)"
        )
        assertTrue(
            samples.count { it < 32 } >= samples.size * 3 / 4,
            "at r=0.2 the drifted node should be below its fair share for most of the window (samples=$samples)"
        )
    }
}
