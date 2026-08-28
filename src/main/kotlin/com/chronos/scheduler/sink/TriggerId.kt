package com.chronos.scheduler.sink

import java.util.UUID

/**
 * requirements.md §3: "triggerId is deterministic and STABLE across every
 * firing attempt of a task, including retries after downstream failure and
 * re-fires after lease takeover. It is derived from taskId, not generated
 * per attempt." This is the entire reason chaos scenario 6 works — node A
 * and node B, racing to fire the same task after a freeze, must produce the
 * identical triggerId independently, with no coordination between them.
 *
 * Deliberately just the taskId's own string form — no hash, no extra
 * transformation. Anything added here is a place a second implementation
 * could compute it differently and silently break the one guarantee this
 * system depends on.
 */
fun triggerIdFor(taskId: UUID): String = taskId.toString()
