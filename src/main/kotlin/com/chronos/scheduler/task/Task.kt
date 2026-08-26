package com.chronos.scheduler.task

import java.time.Instant
import java.util.UUID

data class Task(
    val id: UUID,
    val idempotencyKey: String,
    val payload: String,       // 先存 JSON 原始文本，序列化细节下一步再定
    val callbackUrl: String,
    val fireAt: Instant,
    val state: TaskState,
    val version: Long,
    val shard: Int,
    val leaseOwner: String?,
    val leaseExpiresAt: Instant?,
    val attemptCount: Int,
    val createdAt: Instant,
    val updatedAt: Instant,
) {
    companion object {
        fun newPending(
            idempotencyKey: String,
            payload: String,
            callbackUrl: String,
            fireAt: Instant,
            now: Instant,
        ): Task {
            val id = UUID.randomUUID()
            return Task(
                id = id,
                idempotencyKey = idempotencyKey,
                payload = payload,
                callbackUrl = callbackUrl,
                fireAt = fireAt,
                state = TaskState.PENDING,
                version = 0,
                shard = ShardCalculator.shardFor(id),   // 顺序在这里被强制正确
                leaseOwner = null,
                leaseExpiresAt = null,
                attemptCount = 0,
                createdAt = now,
                updatedAt = now,
            )
        }
    }
}
