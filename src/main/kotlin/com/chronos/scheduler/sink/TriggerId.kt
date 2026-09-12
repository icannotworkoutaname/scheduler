package com.chronos.scheduler.sink

import java.util.UUID

/**
 * requirements.md §3: triggerId is deterministic and stable across every firing
 * attempt of a task, including retries after downstream failure and re-fires
 * after lease takeover. Two nodes racing to fire the same task after a freeze
 * must produce the identical id independently, with no coordination.
 *
 * Deliberately just the taskId's string form: no hash, no transformation.
 * Anything added here is somewhere a second implementation could diverge.
 */
fun triggerIdFor(taskId: UUID): String = taskId.toString()
