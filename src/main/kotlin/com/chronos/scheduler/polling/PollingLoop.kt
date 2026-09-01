package com.chronos.scheduler.polling

import com.chronos.scheduler.config.SchedulerMetrics
import com.chronos.scheduler.node.NodeIdentity
import com.chronos.scheduler.shard.ShardAssignment
import com.chronos.scheduler.sink.RetryPolicy
import com.chronos.scheduler.sink.TaskSink
import com.chronos.scheduler.sink.triggerIdFor
import com.chronos.scheduler.task.Task
import com.chronos.scheduler.task.TaskRepository
import com.chronos.scheduler.task.TaskState
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.ExecutorService

@Component
class PollingLoop(
    private val taskRepository: TaskRepository,
    private val shardAssignment: ShardAssignment,
    private val nodeIdentity: NodeIdentity,
    private val taskSink: TaskSink,
    private val taskExecutor: ExecutorService,
    private val metrics: SchedulerMetrics,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelay = 200)
    fun pollAndClaim() {
        metrics.timePoll {
            val shards = shardAssignment.ownedShards()
            val claimed = taskRepository.claimDueTasks(shards, nodeIdentity.nodeId)

            if (claimed.isNotEmpty()) {
                log.info("claimed {} tasks", claimed.size)
            }

            claimed.forEach { c ->
                // §9: trigger delay recorded here, from the DB clock (c.dbFiredAt),
                // at the moment of the claim — the "actual trigger" instant.
                metrics.recordTriggerDelay(c.triggerDelay)
                taskExecutor.submit { handle(c.task) }
            }
        }
    }

    private fun handle(task: Task) {
        val triggerId = triggerIdFor(task.id)
        val result = taskSink.fire(task, triggerId, task.attemptCount)

        val applied = if (result.success) {
            taskRepository.markSucceeded(task.id, task.version)
        } else {
            taskRepository.markFailed(task.id, task.attemptCount, task.version)
        }

        when {
            applied && result.success ->
                log.info("task {} succeeded, triggerId={}, httpStatus={}", task.id, triggerId, result.httpStatus)

            applied && RetryPolicy.shouldRetry(task.attemptCount) ->
                log.warn(
                    "task {} sink call failed (attempt {}), retrying after backoff, httpStatus={}",
                    task.id, task.attemptCount, result.httpStatus
                )

            applied -> {
                metrics.taskDeadLettered()
                log.error(
                    "task {} sink call failed (attempt {}), exhausted retries, moved to dead, httpStatus={}",
                    task.id, task.attemptCount, result.httpStatus
                )
            }

            else -> {
                // 0-row conditional update. If the task is already 'succeeded',
                // another node completed it while this one was in flight — this
                // node's sink call was a duplicate delivery the optimistic lock
                // absorbed (ADR-004 decision 2). Any other state is a plain
                // version race (reaper reclaimed it, etc.), not a duplicate.
                val current = taskRepository.findById(task.id)
                if (current?.state == TaskState.SUCCEEDED) {
                    metrics.duplicateTriggerAbsorbed()
                    log.warn(
                        "task {} already succeeded on another node — this fire was a duplicate, absorbed (0-row update)",
                        task.id
                    )
                } else {
                    log.warn(
                        "mark{} affected 0 rows for task {} — version race, state now {}",
                        if (result.success) "Succeeded" else "Failed", task.id, current?.state
                    )
                }
            }
        }
    }
}
