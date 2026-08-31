package com.chronos.scheduler.task

import jakarta.validation.constraints.NotBlank
import java.time.Instant
import java.util.UUID

data class CreateTaskRequest(
    @field:NotBlank
    val payload: String,

    val fireAt: Instant,

    @field:NotBlank
    val idempotencyKey: String,

    @field:NotBlank
    val callbackUrl: String,
)

data class TaskResponse(
    val taskId: UUID,
    val state: String,
    val fireAt: Instant,
)

fun Task.toResponse() = TaskResponse(
    taskId = id,
    state = state.name.lowercase(),
    fireAt = fireAt,
)

data class RescheduleRequest(
    val fireAt: Instant,
    val version: Long,
)
