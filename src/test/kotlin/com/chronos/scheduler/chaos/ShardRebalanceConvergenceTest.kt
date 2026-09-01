package com.chronos.scheduler.chaos

import com.chronos.scheduler.SchedulerApplication
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.DriverManager

/**
 * Reviews the 8/23 rebalance fix (ShardBootstrap.forceClaimOneShard +
 * ShardHeartbeat's downward releaseExcessShards) for a symmetric failure it
 * could plausibly introduce.
 *
 * With 2 nodes the split is 32/32 and releaseExcessShards never trips — the
 * existing ShardFailoverTest doesn't exercise it at all. With 3+ nodes it
 * does: a node that force-claimed a single shard at startup is well under
 * fair share while the other two are over it. If the release rhythm and the
 * claim rhythm had no damping you could get overshoot — released too many →
 * laggard claims too many → laggard now over → it releases → … — i.e. a
 * permanent thrash instead of convergence.
 *
 * The arithmetic rules it out: each over-holder releases exactly its overage,
 * each under-holder claims at most (softCap − held), and since Σ held = 64 ≤
 * N·softCap the under-holders collectively want at least what is released, so
 * nothing overshoots and the system lands on a fixed point (two nodes at
 * ceil(64/N), the rest splitting the remainder). This test is the evidence
 * for that rather than the reasoning: it asserts the assignment reaches a
 * balanced state AND then stops changing for several heartbeat periods.
 */
@Testcontainers
class ShardRebalanceConvergenceTest {

    companion object {
        const val PERIOD_SECONDS = 3L

        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")
            .withDatabaseName("scheduler")
            .withUsername("scheduler")
            .withPassword("scheduler")
    }

    private val contexts = mutableListOf<ConfigurableApplicationContext>()

    @AfterEach
    fun tearDown() {
        contexts.forEach { it.close() }
        contexts.clear()
    }

    private fun startNode(port: Int): ConfigurableApplicationContext =
        SpringApplicationBuilder(SchedulerApplication::class.java).run(
            "--server.port=$port",
            "--chronos.shard.settle-delay-seconds=5",
            "--chronos.heartbeat.period-seconds=$PERIOD_SECONDS",
            "--spring.datasource.url=${postgres.jdbcUrl}",
            "--spring.datasource.username=${postgres.username}",
            "--spring.datasource.password=${postgres.password}",
        ).also { contexts += it }

    /** shard_id -> owner, for every shard with a live lease. */
    private fun assignment(): Map<Int, String?> =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { c ->
            val rs = c.createStatement().executeQuery(
                "SELECT shard_id, lease_owner FROM shards WHERE lease_expires_at >= now()"
            )
            buildMap { while (rs.next()) put(rs.getInt("shard_id"), rs.getString("lease_owner")) }
        }

    private fun counts(): Map<String?, Int> =
        assignment().values.groupingBy { it }.eachCount()

    @Test
    fun `three nodes converge to a balanced split and then stop moving`() {
        // 串行启动（并发 SpringApplication.run() 会撞 Spring Boot 静态初始化）。
        // 第 3 个节点起来时 64 个 shard 全被前两个租走 → Phase 1 强占 1 个。
        startNode(19091)
        startNode(19092)
        startNode(19093)

        // 收敛：3 个 owner，64 个 shard 全有主，彼此相差 ≤ 2（ceil(64/3)=22 →
        // 稳态是 [22,22,20] 或 [22,21,21]）。
        val deadline = System.currentTimeMillis() + 60_000
        var c = counts()
        while (System.currentTimeMillis() < deadline &&
            !(c.size == 3 && c.values.sum() == 64 && (c.values.max() - c.values.min()) <= 2)
        ) {
            Thread.sleep(2000)
            c = counts()
        }
        assertEquals(3, c.size, "expected 3 distinct owners, got $c")
        assertEquals(64, c.values.sum(), "expected all 64 shards owned, got $c")
        assertTrue(
            c.values.max() - c.values.min() <= 2,
            "expected a balanced spread (≤2 apart) once converged, got $c"
        )

        // 不动点：接下来 ~8 个心跳周期内，精确的 shard→owner 映射不得改变。
        // 采两次中间样本，让缓慢的震荡无法在两个相同端点之间藏起来。
        val settled = assignment()
        Thread.sleep(PERIOD_SECONDS * 1000 * 4)
        val mid = assignment()
        Thread.sleep(PERIOD_SECONDS * 1000 * 4)
        val end = assignment()

        assertEquals(
            settled, mid,
            "shard assignment changed after it should have settled — rebalance is still moving\n settled=$settled\n mid=$mid"
        )
        assertEquals(
            settled, end,
            "shard assignment changed after it should have settled — rebalance is oscillating, not converging\n settled=$settled\n end=$end"
        )
    }
}
