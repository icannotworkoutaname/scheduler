package com.chronos.scheduler.task

enum class TaskState {
    PENDING,
    FIRING,
    SUCCEEDED,
    RETRYING,
    DEAD,
    CANCELLED;

    companion object {
        fun fromDb(value: String): TaskState = valueOf(value.uppercase())
    }
}