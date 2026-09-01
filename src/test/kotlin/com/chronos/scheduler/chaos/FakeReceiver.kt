package com.chronos.scheduler.chaos

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

/**
 * JVM-level stand-in for scripts/receiver.py — a real HTTP endpoint that the
 * scheduler's HttpSink calls during Testcontainers chaos tests. Records every
 * triggerId it has executed so a test can cross-check "did this task actually
 * fire" against the database state, the same thing scenario 4's bash version
 * does through /seen. `failEverything` flips it to a 100%-500 downstream for
 * the persistent-failure scenario.
 */
class FakeReceiver {
    private val server: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
    private val seen = ConcurrentHashMap.newKeySet<String>()

    @Volatile
    var failEverything: Boolean = false

    val port: Int get() = server.address.port
    val hookUrl: String get() = "http://127.0.0.1:$port/hook"

    fun hasSeen(triggerId: String): Boolean = seen.contains(triggerId)
    fun seenCount(): Int = seen.size

    init {
        server.createContext("/hook") { exchange ->
            val triggerId = exchange.requestHeaders.getFirst("X-Trigger-Id") ?: ""
            exchange.requestBody.readAllBytes()
            val status = if (failEverything) {
                500
            } else {
                seen.add(triggerId)
                200
            }
            exchange.sendResponseHeaders(status, -1)
            exchange.close()
        }
        server.executor = Executors.newFixedThreadPool(8)
        server.start()
    }

    fun stop() = server.stop(0)
}
