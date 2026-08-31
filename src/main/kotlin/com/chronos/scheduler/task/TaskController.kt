package com.chronos.scheduler.task

import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.time.Clock
import java.util.UUID

@RestController
@RequestMapping("/tasks")
class TaskController(
    private val taskRepository: TaskRepository,
    private val clock: Clock,
) {

    @PostMapping
    fun submit(@Valid @RequestBody request: CreateTaskRequest): ResponseEntity<TaskResponse> {
        val now = clock.instant()
        TaskValidation.validateFireAt(request.fireAt, now)

        val task = Task.newPending(
            idempotencyKey = request.idempotencyKey,
            payload = request.payload,
            callbackUrl = request.callbackUrl,
            fireAt = request.fireAt,
            now = now,
        )

        val saved = taskRepository.insertOrGetExisting(task)
        return ResponseEntity.status(HttpStatus.CREATED).body(saved.toResponse())
    }

    @GetMapping("/{id}")
    fun get(@PathVariable id: UUID): ResponseEntity<TaskResponse> {
        val task = taskRepository.findById(id) ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(task.toResponse())
    }

    @PatchMapping("/{id}")
    fun reschedule(@PathVariable id: UUID, @Valid @RequestBody request: RescheduleRequest): ResponseEntity<Any> {
        val now = clock.instant()
        TaskValidation.validateFireAt(request.fireAt, now)

        if (taskRepository.rescheduleIfPending(id, request.fireAt, request.version)) {
            return ResponseEntity.ok(taskRepository.findById(id)!!.toResponse())
        }

        val current = taskRepository.findById(id) ?: return ResponseEntity.notFound().build()
        return ResponseEntity.status(409).body(mapOf(
            "error" to "cannot reschedule: state=${current.state.name.lowercase()}, " +
                "currentVersion=${current.version}, youSent=${request.version}"
        ))
    }

    @DeleteMapping("/{id}")
    fun cancel(@PathVariable id: UUID): ResponseEntity<Any> {
        if (taskRepository.cancelIfPending(id)) {
            return ResponseEntity.noContent().build()
        }

        val current = taskRepository.findById(id) ?: return ResponseEntity.notFound().build()
        return ResponseEntity.status(409).body(mapOf(
            "error" to "cannot cancel task in state ${current.state.name.lowercase()}"
        ))
    }

    @ExceptionHandler(InvalidFireAtException::class)
    fun handleInvalidFireAt(ex: InvalidFireAtException): ResponseEntity<Map<String, String>> =
        ResponseEntity.badRequest().body(mapOf("error" to (ex.message ?: "invalid fireAt")))
}
