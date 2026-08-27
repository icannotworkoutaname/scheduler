package com.chronos.scheduler.node

import org.springframework.stereotype.Component
import java.net.InetAddress
import java.util.UUID

/**
 * A stable-for-this-process identity used as lease_owner. Not persisted
 * anywhere as a registry — it only needs to be unique enough that two nodes
 * never collide, and readable enough that you can tell nodes apart in logs
 * during chaos scenario 6 (8/21) when node A freezes and node B takes over.
 */
@Component
class NodeIdentity {
    val nodeId: String = "${hostnameOrFallback()}-${UUID.randomUUID().toString().take(8)}"

    private fun hostnameOrFallback(): String =
        runCatching { InetAddress.getLocalHost().hostName }.getOrDefault("node")
}
