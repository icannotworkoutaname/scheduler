package com.chronos.scheduler.polling

import com.chronos.scheduler.node.NodeIdentity
import com.chronos.scheduler.shard.ShardAssignment
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
        // 占位。真正调用 HttpSink、状态转 succeeded/retrying 是 8/8 的任务。
        // 今天要验证的只是:claim 到的任务被正确交给线程池,不在轮询线程里同步处理。
        log.info("would fire task {} to {}", task.id, task.callbackUrl)
    }
}
