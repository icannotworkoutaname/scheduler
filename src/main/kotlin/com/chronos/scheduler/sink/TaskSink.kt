package com.chronos.scheduler.sink

import com.chronos.scheduler.task.Task

data class SinkResult(
    val success: Boolean,
    val httpStatus: Int?,
)

interface TaskSink {
    /**
     * attempt is separate from triggerId on purpose: it travels in its own
     * header for observability and must never participate in deduplication
     * (requirements.md §3). Keeping them as two parameters rather than one
     * object makes deriving triggerId from attempt awkward to do by accident.
     */
    fun fire(task: Task, triggerId: String, attempt: Int): SinkResult
}
