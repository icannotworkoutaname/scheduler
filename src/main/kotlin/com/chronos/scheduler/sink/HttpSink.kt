package com.chronos.scheduler.sink

import com.chronos.scheduler.config.SchedulerMetrics
import com.chronos.scheduler.task.Task
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import org.springframework.web.client.RestClientResponseException
import java.time.Duration

@Component
class HttpSink(
    private val restClient: RestClient,
    private val metrics: SchedulerMetrics,
) : TaskSink {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun fire(task: Task, triggerId: String, attempt: Int): SinkResult {
        // System.nanoTime(), not the injected Clock — this is elapsed real time
        // for a duration metric, which must be immune to clock skew/drift.
        val startNanos = System.nanoTime()
        val result = try {
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
        metrics.recordSinkCall(Duration.ofNanos(System.nanoTime() - startNanos), result.success)
        return result
    }
}
