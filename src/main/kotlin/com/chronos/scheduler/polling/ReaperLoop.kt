package com.chronos.scheduler.polling

import com.chronos.scheduler.task.TaskRepository
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class ReaperLoop(private val taskRepository: TaskRepository) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelay = 5000)
    fun reclaimExpiredLeases() {
        val reclaimed = taskRepository.reclaimExpiredLeases()
        reclaimed.forEach { r ->
            log.warn("reclaimed expired lease: task={} previousOwner={}", r.taskId, r.previousOwner)
        }
    }
}
