package com.chronos.scheduler.polling

import com.chronos.scheduler.node.NodeIdentity
import com.chronos.scheduler.shard.ShardAssignment
import com.chronos.scheduler.sink.TaskSink
import com.chronos.scheduler.sink.triggerIdFor
import com.chronos.scheduler.task.Task
import com.chronos.scheduler.task.TaskRepository
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
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelay = 200)
    fun pollAndClaim() {
        val shards = shardAssignment.ownedShards()
        val claimed = taskRepository.claimDueTasks(shards, nodeIdentity.nodeId)

        if (claimed.isNotEmpty()) {
            log.info("claimed {} tasks", claimed.size)
        }

        claimed.forEach { task ->
            taskExecutor.submit { handle(task) }
        }
    }

    private fun handle(task: Task) {
        val triggerId = triggerIdFor(task.id)
        val result = taskSink.fire(task, triggerId, task.attemptCount)

        if (result.success) {
            val updated = taskRepository.markSucceeded(task.id, task.version)
            if (updated) {
                log.info("task {} succeeded, triggerId={}, httpStatus={}", task.id, triggerId, result.httpStatus)
            } else {
                log.warn("markSucceeded affected 0 rows for task {} — version mismatch", task.id)
            }
        } else {
            // 8/9 补:指数退避重试 + 死信 + reaper。今天失败的任务会卡在firing 状态出不来，后续添加其他
            // 处理的缺口
            log.warn("task {} sink call failed, httpStatus={} — retry/backoff lands 8/9", task.id, result.httpStatus)
        }
    }
}
