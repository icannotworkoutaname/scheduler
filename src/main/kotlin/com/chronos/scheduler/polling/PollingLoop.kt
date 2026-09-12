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
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.ThreadPoolExecutor

@Component
class PollingLoop(
    private val taskRepository: TaskRepository,
    private val shardAssignment: ShardAssignment,
    private val nodeIdentity: NodeIdentity,
    private val taskSink: TaskSink,
    private val taskExecutor: ThreadPoolExecutor,
    private val metrics: SchedulerMetrics,
    // Ceiling, not a batch size: 500 × 5 polls/s × 2 nodes capped claiming at
    // ~4,400/s, short of the 5,000/s SLO. 2,000 clears it (5,118/s measured),
    // and the backpressure below keeps the effective claim within what the fire
    // pool can drain. See docs/performance.md §2.
    @Value("\${chronos.poll.claim-limit:2000}")
    private val claimLimit: Int,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * How many more tasks the firing pool can take: idle threads plus free queue
     * slots. Claiming beyond this grows the `firing` backlog past what the pool
     * can drain inside the 30s lease, which turns a burst into a re-fire storm
     * (ADR-004 decision 7).
     */
    private fun fireCapacity(): Int =
        (taskExecutor.maximumPoolSize - taskExecutor.activeCount) +
            taskExecutor.queue.remainingCapacity()

    @Scheduled(fixedDelay = 200)
    fun pollAndClaim() {
        metrics.timePoll {
            val limit = minOf(claimLimit, fireCapacity()).coerceAtLeast(0)
            if (limit == 0) return@timePoll

            val shards = shardAssignment.ownedShards()
            val claimed = taskRepository.claimDueTasks(shards, nodeIdentity.nodeId, limit)

            if (claimed.isNotEmpty()) {
                log.info("claimed {} tasks", claimed.size)
            }

            claimed.forEach { c ->
                // Recorded at claim time from the DB clock (c.dbFiredAt), which
                // is the actual trigger instant — ADR-004 decision 1.
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
                // another node completed it while this one was in flight, so
                // this node's sink call was an absorbed duplicate delivery
                // (ADR-004 decision 2). Any other state — the reaper reclaimed
                // it, for instance — is a plain version race, not a duplicate.
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
