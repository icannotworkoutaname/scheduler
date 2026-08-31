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
                "--chronos.shard.settle-delay-seconds=1", // 测试里没必要等生产环境的 3 秒
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
        // 并发起两个节点，复刻 8/10 手工验证时靠时间窗口重叠触发公平抢占的做法
        val threadA = Thread { ctxA = startNode(18080) }
        val threadB = Thread { ctxB = startNode(18081) }
        threadA.start(); threadB.start()
        threadA.join(); threadB.join()

        Thread.sleep(3000) // 等三段式启动协议（报到-等待-认领）跑完

        val initialCounts = shardOwnerCounts()
        assertEquals(2, initialCounts.size, "expected exactly two distinct owners")
        assertTrue(initialCounts.values.all { it == 32 }, "expected an even 32/32 split, got: $initialCounts")

        // 模拟节点 A 崩溃：不走 Spring 的优雅关闭钩子，直接杀底层进程不现实
        // （两个节点在同一个 JVM 进程里），这里用 close() 模拟——我们目前的
        // 代码没有写任何"优雅释放 shard"的逻辑，所以 close() 造成的效果
        // 跟硬杀进程在这套系统里是等价的：A 的租约就是会自然过期，没有
        // 任何特殊清理路径可以抢跑。
        ctxA!!.close()
        ctxA = null

        // 最坏情况下需要 lease TTL(30s) + 心跳间隔(10s)，留够余量轮询等待
        val deadline = System.currentTimeMillis() + 45_000
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
