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

@Testcontainers
class ShardFailoverTest {

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

    @AfterEach
    fun tearDown() {
        ctxA?.close()
        ctxB?.close()
    }

    /**
     * @ServiceConnection only gets processed by Spring Test's container
     * customizer machinery, which is wired up via @SpringBootTest — it does
     * nothing for a context built by hand through SpringApplicationBuilder,
     * as this test does (two independent contexts, not one managed by the
     * JUnit Spring extension). Without any override, both nodes silently
     * fell back to application.properties' real dev database instead of the
     * ephemeral container, and Flyway happily reported "up to date" against
     * a database that already had these tables from manual testing.
     *
     * The first fix attempt — SpringApplicationBuilder.properties(map) —
     * looked right but still lost: that method adds to Spring Boot's
     * "default properties", which sit at the BOTTOM of the property source
     * precedence order, below application.properties on the classpath. The
     * file always won regardless of what was passed here. Command-line-style
     * "--key=value" arguments to run() land as a genuine PropertySource near
     * the TOP of that order, which is what actually overrides the file.
     */
    private fun startNode(port: Int): ConfigurableApplicationContext =
        SpringApplicationBuilder(SchedulerApplication::class.java)
            .run(
                "--server.port=$port",
                // settle-delay 必须够长，让慢启动的对等节点完成 Phase 1(报到)——
                // 否则先跑完 settle 的节点在 Phase 3 只看到自己一个 owner，softCap
                // 算成 64，独吞全部 shard，而心跳没有向下再平衡的逻辑(8/20)，之后
                // 永远收不回来。1 秒在并行启动两个 Spring 上下文的 CI/Testcontainers
                // 环境里不够，实测约 1/3 概率复现独吞；5 秒有充足余量。
                "--chronos.shard.settle-delay-seconds=5",
                "--spring.datasource.url=${postgres.jdbcUrl}",
                "--spring.datasource.username=${postgres.username}",
                "--spring.datasource.password=${postgres.password}",
            )

    private fun shardOwnerCounts(): Map<String?, Int> {
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
            val rs = conn.createStatement().executeQuery(
                "SELECT lease_owner, count(*) c FROM shards GROUP BY lease_owner"
            )
            val map = mutableMapOf<String?, Int>()
            while (rs.next()) map[rs.getString("lease_owner")] = rs.getInt("c")
            return map
        }
    }

    @Test
    fun `two nodes split shards evenly, then one takes over the other's shards on failure`() {
        // 串行启动，不并发。两个 SpringApplication.run() 同时在不同线程里跑会撞
        // Spring Boot 的共享静态初始化(日志系统等)，抛 ConcurrentModificationException
        // 打挂其中一个 context——这是"两节点挤一个 JVM"这个测试手法的固有脆弱点，
        // 跟被测代码无关。串行起完全没问题：node A 先独占 64，node B 起来后 Phase 1
        // 强占 1 个变可见，随后 A 的心跳做向下再平衡把多余的 32 个还回去，B 认领，
        // 收敛到 32/32。
        ctxA = startNode(18080)
        ctxB = startNode(18081)

        // 轮询等 32/32 收敛。settle 5s + node A 释放 + node B 认领，给 35 秒。
        val splitDeadline = System.currentTimeMillis() + 35_000
        var initialCounts = shardOwnerCounts()
        while (System.currentTimeMillis() < splitDeadline &&
            !(initialCounts.size == 2 && initialCounts.values.all { it == 32 })
        ) {
            Thread.sleep(2000)
            initialCounts = shardOwnerCounts()
        }
        assertEquals(2, initialCounts.size, "expected exactly two distinct owners, got: $initialCounts")
        assertTrue(initialCounts.values.all { it == 32 }, "expected an even 32/32 split, got: $initialCounts")

        // 模拟节点 A 崩溃：不走 Spring 的优雅关闭钩子，直接杀底层进程不现实
        // （两个节点在同一个 JVM 进程里），这里用 close() 模拟——我们目前的
        // 代码没有写任何"优雅释放 shard"的逻辑，所以 close() 造成的效果
        // 跟硬杀进程在这套系统里是等价的：A 的租约就是会自然过期，没有
        // 任何特殊清理路径可以抢跑。
        ctxA!!.close()
        ctxA = null

        // 最坏情况下需要 lease TTL(30s) + 心跳间隔(10s)，CI runner 上再留余量
        val deadline = System.currentTimeMillis() + 60_000
        var finalCounts = shardOwnerCounts()
        while (System.currentTimeMillis() < deadline && finalCounts.values.sum() != 64.let {
            finalCounts.values.maxOrNull() ?: 0
        }) {
            Thread.sleep(2000)
            finalCounts = shardOwnerCounts()
            if (finalCounts.values.any { it == 64 }) break
        }

        assertEquals(1, finalCounts.size, "expected only the surviving node's owner left, got: $finalCounts")
        assertEquals(64, finalCounts.values.first(), "surviving node should hold all 64 shards after takeover")
    }
}
