# ADR-005: Clock injection for chaos scenarios 7/8

Status: accepted (8/23)

## Context

Scenarios 7 (clock skew) and 8 (clock drift) need a node whose notion of "now"
diverges from reality, without root (no `libfaketime`/`LD_PRELOAD` in WSL2) and
without faking the wall clock for the whole machine — only this one node's
time-based *decisions* should move.

## Decision

`java.time.Clock` is the only injection seam: `ClockConfig` produces
`Clock.offset(...)` for a constant skew (scenario 7), or wraps that in
`DriftingClock` — `instant() = anchor + realElapsed × rate` — for a rate drift
(scenario 8). `@Scheduled` itself stays on the system clock (it's a cheap
ticker); the injected clock only gates whether a tick actually renews —
`ShardHeartbeat` checks `clock.instant() >= nextRenewalDue` inside the method
body, so a slow clock genuinely renews less often, not just "thinks" it does.

This seam only reaches one code path: the heartbeat's renewal cadence. Two
things it explicitly does **not** reach:

- **Postgres' `now()`** — every TTL/expiry comparison (`lease_expires_at < now()`)
  runs in SQL against the database's own clock, by design (ADR-004 decision 1
  is the same principle applied to the delay metric).
- **JDK `HttpClient`'s read timeout** — `HttpSinkConfig` sets a 10s read
  timeout; that timer is the JVM's real monotonic clock, which `DriftingClock`
  cannot drive without `libfaketime` (ruled out above).

## The crossover: measured on one path, derived on the other

Both a slow heartbeat and scenario 6's double-fire hinge on the same ratio,
`rate = period / ttl` — production is 10s/30s = 1/3. Above it the node renews
inside the TTL and keeps its shards; at or below it, renewal loses the race.

- **Measured**: `ClockDriftTest` (period=2s, ttl=6s, same 1/3 crossover)
  instruments the renewal path directly — `r=0.5` holds all 32 shards for the
  full sampling window; `r=0.2` drops to ≤4 and sits below fair share for
  ≥75% of samples. This is a real Testcontainers assertion, not arithmetic.
- **Derived, not measured**: the equivalent collapse of the sink-timeout path
  (a drifting node's 10s read-timeout effectively lengthening, contributing to
  scenario 6's double-fire) follows the same `period/ttl` arithmetic but was
  never independently instrumented — `DriftingClock` has no seam into
  `HttpClient`'s timer, so there's nothing to inject drift into on that path.

## Consequences

- A skewed/drifting node stays *correct*, never early: scenario 7's node at
  `+5min` fired `9/20` tasks (the ones legitimately assigned to it) and
  `fired_early=0` — correctness held while doing real work, not by sitting out.
- Any writeup citing the 1/3 coefficient must say which side it's on:
  heartbeat-renewal crossover is measured; sink-timeout crossover is the same
  formula applied by inference. Don't present both as equally verified.
