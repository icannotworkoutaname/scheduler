package com.chronos.scheduler.sink

import com.chronos.scheduler.task.Task
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import org.springframework.web.client.RestClientResponseException

@Component
class HttpSink(private val restClient: RestClient) : TaskSink {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun fire(task: Task, triggerId: String, attempt: Int): SinkResult {
        return try {
            val response = restClient.post()
                .uri(task.callbackUrl)
                .header("X-Trigger-Id", triggerId)
                .header("X-Attempt", attempt.toString())
                .header("Content-Type", "application/json")
                .body(task.payload.toByteArray(Charsets.UTF_8))
                .retrieve()
                .toBodilessEntity()

            SinkResult(success = response.statusCode.is2xxSuccessful, httpStatus = response.statusCode.value())
        } catch (ex: RestClientResponseException) {
            log.warn("sink call failed: task={} triggerId={} status={}", task.id, triggerId, ex.statusCode.value())
            SinkResult(success = false, httpStatus = ex.statusCode.value())
        } catch (ex: Exception) {
            log.warn("sink call errored: task={} triggerId={} error={}", task.id, triggerId, ex.message)
            SinkResult(success = false, httpStatus = null)
        }
    }
}
