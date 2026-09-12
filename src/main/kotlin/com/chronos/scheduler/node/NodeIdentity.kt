package com.chronos.scheduler.node

import org.springframework.stereotype.Component
import java.net.InetAddress
import java.util.UUID

/**
 * Stable-for-this-process identity used as lease_owner. There is no registry:
 * it only needs to be unique enough that two nodes never collide, and readable
 * enough to tell nodes apart in logs during the failover scenarios.
 */
@Component
class NodeIdentity {
    val nodeId: String = "${hostnameOrFallback()}-${UUID.randomUUID().toString().take(8)}"

    private fun hostnameOrFallback(): String =
        runCatching { InetAddress.getLocalHost().hostName }.getOrDefault("node")
}
