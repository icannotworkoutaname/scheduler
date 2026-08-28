package com.chronos.scheduler.sink

import com.chronos.scheduler.task.Task

data class SinkResult(
    val success: Boolean,
    val httpStatus: Int?,
)

interface TaskSink {
    /**
     * attempt is passed separately from triggerId on purpose — it travels in
     * its own header for observability only (requirements.md §3: "must never
     * participate in deduplication"). Keeping them as two distinct parameters
     * here, rather than one bundled object, makes it structurally awkward to
     * accidentally derive triggerId from attempt somewhere downstream.
     */
    fun fire(task: Task, triggerId: String, attempt: Int): SinkResult
}
