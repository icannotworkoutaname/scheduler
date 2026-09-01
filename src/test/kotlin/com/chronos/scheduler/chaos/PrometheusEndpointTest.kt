package com.chronos.scheduler.chaos

import com.chronos.scheduler.SchedulerApplication
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/**
 * requirements.md §9 acceptance for 8/24: /actuator/prometheus exposes all
 * seven metrics. They are all registered at startup (counters/timers show a
 * zero series before their first event), so this doesn't need to exercise
 * each one — just that the wiring and the endpoint are in place.
 */
@Testcontainers
class PrometheusEndpointTest {

    companion object {
        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")
            .withDatabaseName("scheduler")
            .withUsername("scheduler")
            .withPassword("scheduler")

        val EXPECTED = listOf(
            "chronos_trigger_delay_seconds",
            "chronos_duplicate_trigger_total",
            "chronos_tasks_firing",
            "chronos_tasks_dead_total",
            "chronos_lease_takeover_total",
            "chronos_sink_call_duration_seconds",
            "chronos_poll_duration_seconds",
        )
    }

    private var ctx: ConfigurableApplicationContext? = null

    @AfterEach
    fun tearDown() {
        ctx?.close()
    }

    @Test
    fun `all seven section-9 metrics are exposed at actuator prometheus`() {
        ctx = SpringApplicationBuilder(SchedulerApplication::class.java).run(
            "--server.port=17081",
            "--chronos.shard.settle-delay-seconds=2",
            "--spring.datasource.url=${postgres.jdbcUrl}",
            "--spring.datasource.username=${postgres.username}",
            "--spring.datasource.password=${postgres.password}",
        )
        Thread.sleep(3000) // let a few poll cycles record

        val body = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder(URI.create("http://localhost:17081/actuator/prometheus")).build(),
            HttpResponse.BodyHandlers.ofString(),
        ).body()

        val missing = EXPECTED.filter { !body.contains(it) }
        assertTrue(missing.isEmpty(), "missing from /actuator/prometheus: $missing")

        // trigger_delay must carry our explicit buckets, not Micrometer defaults
        assertTrue(
            body.contains("""chronos_trigger_delay_seconds_bucket{le="0.2"}"""),
            "trigger_delay is missing the 200ms SLO bucket — decision 4"
        )
        assertTrue(
            body.contains("""chronos_trigger_delay_seconds_bucket{le="1.0"}"""),
            "trigger_delay is missing the 1s SLO bucket — decision 4"
        )

        // the firing-load gauge must actually COUNT, not silently swallow a SQL
        // error and sit at 0 (8/27: an earlier form did exactly that)
        java.sql.DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { c ->
            c.prepareStatement(
                "INSERT INTO tasks (id, idempotency_key, payload, callback_url, fire_at, state, shard, lease_owner, lease_expires_at) " +
                    "VALUES (gen_random_uuid(), ?, '{}'::jsonb, 'http://x', now(), 'firing', 9, 'n', now() + interval '30 seconds')"
            ).use { ps -> repeat(5) { ps.setString(1, "fire-$it"); ps.executeUpdate() } }
        }
        val metrics = ctx!!.getBean(com.chronos.scheduler.config.SchedulerMetrics::class.java)
        metrics.sampleNow()
        assertTrue(
            metrics.firingForShard(9) == 5,
            "chronos_tasks_firing for shard 9 should be 5 after inserting 5 firing rows, got ${metrics.firingForShard(9)}"
        )
    }
}
