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
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.DriverManager
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.Future

/**
 * Chaos scenario 4 (cancel vs. fire race). Mirrors
 * scripts/scenario4_cancel_race.sh at the JVM level: half the tasks fire soon
 * (claimed and fired during a short wait), half fire far in the future (stay
 * pending), then every task gets a concurrent DELETE.
 *
 * The point is not the HTTP status on its own — it's that the status agrees
 * with the database state AND with the downstream's own record, so a
 * "client got 204 but the task actually fired" inconsistency can't hide:
 *
 *   cancel won : 204  ⇔  state = cancelled  ⇔  receiver never saw it
 *   fire won   : 409  ⇔  state = succeeded  ⇔  receiver saw it
 */
@Testcontainers
class CancelFireRaceTest {

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
    private val http = HttpClient.newHttpClient()
    private val base = "http://localhost:18080"

    @AfterEach
    fun tearDown() {
        ctx?.close()
        receiver.stop()
    }

    private fun submit(key: String, fireAt: Instant): String {
        val body = """{"payload":"{}","fireAt":"$fireAt","idempotencyKey":"$key","callbackUrl":"${receiver.hookUrl}"}"""
        val resp = http.send(
            HttpRequest.newBuilder(URI.create("$base/tasks"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        return Regex("\"taskId\":\"([0-9a-f-]+)\"").find(resp.body())!!.groupValues[1]
    }

    private fun dbState(taskId: String): String =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { c ->
            c.createStatement().executeQuery("SELECT state FROM tasks WHERE id = '$taskId'")
                .use { rs -> rs.next(); rs.getString("state") }
        }

    @Test
    fun `every cancel outcome agrees with the db state and the downstream record`() {
        ctx = SpringApplicationBuilder(SchedulerApplication::class.java).run(
            "--server.port=18080",
            "--chronos.shard.settle-delay-seconds=2",
            "--spring.datasource.url=${postgres.jdbcUrl}",
            "--spring.datasource.username=${postgres.username}",
            "--spring.datasource.password=${postgres.password}",
        )
        Thread.sleep(4000) // single node — settle + first heartbeat, it will hold all 64

        val near = (0 until 15).map { "s4-near-$it" to submit("s4-near-$it", Instant.now().plusSeconds(3)) }
        val far = (0 until 15).map { "s4-far-$it" to submit("s4-far-$it", Instant.now().plusSeconds(40)) }

        Thread.sleep(6000) // near half gets claimed and fired

        val pool = Executors.newFixedThreadPool(16)
        val deletes: List<Future<Pair<String, Int>>> = (near + far).map { (_, id) ->
            pool.submit<Pair<String, Int>> {
                val code = http.send(
                    HttpRequest.newBuilder(URI.create("$base/tasks/$id")).DELETE().build(),
                    HttpResponse.BodyHandlers.discarding(),
                ).statusCode()
                id to code
            }
        }
        val results = deletes.map { it.get() }
        pool.shutdown()

        Thread.sleep(4000) // let any fire-winners settle to succeeded

        var cancelWon = 0
        var fireWon = 0
        results.forEach { (id, code) ->
            val state = dbState(id)
            val seen = receiver.hasSeen(id)
            when (code) {
                204 -> {
                    cancelWon++
                    assertEquals("cancelled", state, "task $id returned 204 but DB state is $state")
                    assertTrue(!seen, "task $id returned 204 (cancel won) but the downstream saw it fire")
                }
                409 -> {
                    fireWon++
                    assertEquals("succeeded", state, "task $id returned 409 but DB state is $state")
                    assertTrue(seen, "task $id returned 409 (fire won) but the downstream never saw it")
                }
                else -> throw AssertionError("task $id got unexpected DELETE status $code")
            }
        }

        assertEquals(15, cancelWon, "the 15 far-future tasks should all have been cancellable")
        assertEquals(15, fireWon, "the 15 near tasks should all have fired before their DELETE landed")
    }
}
