#!/usr/bin/env bash
#
# `make demo` — one command, zero manual steps. Brings up the whole stack,
# runs the node-freeze / at-least-once scenario, narrates what happens in
# plain language, and cleans up on ANY exit path (including Ctrl-C — a
# kill -STOP'd JVM left behind would be the worst possible first impression).
#
#   make demo              accelerated: 8s lease TTL, scenario runs in ~30s
#   DEMO_FAST=0 make demo   production timings: 30s lease TTL, ~90s
#
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

FAST=${DEMO_FAST:-1}
if [ "$FAST" = 1 ]; then
  TASK_TTL=8; SHARD_TTL=8; HEARTBEAT=3; SETTLE=3
  TTL_NOTE="accelerated — 8s lease TTL (production default is 30s)"
else
  TASK_TTL=30; SHARD_TTL=30; HEARTBEAT=10; SETTLE=3
  TTL_NOTE="production timings — 30s lease TTL"
fi
RECEIVER_DELAY=8   # widen the "node is mid-flight" window well past the 300ms poll

JAR=build/libs/scheduler-0.0.1-SNAPSHOT.jar
PGURL="-h localhost -U scheduler -d scheduler"
export PGPASSWORD=scheduler
PID_A=""; PID_B=""; RECEIVER_PID=""

cleanup() {
  echo ""
  echo ">> cleanup"
  for p in "$PID_A" "$PID_B"; do
    # CONT before KILL — a STOPPED process ignores SIGTERM and can't be reaped
    # cleanly until resumed. This trap is the whole point: a Ctrl-C during the
    # frozen window must not leave a stuck JVM behind.
    if [ -n "$p" ] && kill -0 "$p" 2>/dev/null; then
      kill -CONT "$p" 2>/dev/null || true
      kill -9 "$p" 2>/dev/null || true
    fi
  done
  [ -n "$RECEIVER_PID" ] && kill -9 "$RECEIVER_PID" 2>/dev/null || true
  echo "   (docker compose left running — 'make demo-down' to stop it)"
}
trap cleanup EXIT INT TERM

step() { printf '\n\033[1m── %s\033[0m\n' "$1"; }
note() { printf '   %s\n' "$1"; }

echo "Chronos demo · node freeze → at-least-once fire ($TTL_NOTE)"

# ── prerequisites ──────────────────────────────────────────────────────────
step "[0/6] Postgres + Prometheus + Grafana"
docker compose up -d postgres prometheus grafana >/dev/null 2>&1
for _ in $(seq 1 60); do
  [ "$(docker compose ps postgres --format '{{.Health}}' 2>/dev/null)" = healthy ] && break
  sleep 1
done
note "Postgres healthy, Flyway migrations applied by the first node on boot"

step "[0/6] building the jar"
./gradlew bootJar -q --console=plain
[ -f "$JAR" ] || { echo "build failed"; exit 1; }

step "[0/6] starting the downstream receiver + two scheduler nodes"
psql $PGURL -q -c "TRUNCATE tasks;" -c "UPDATE shards SET lease_owner=NULL, lease_expires_at=NULL;"
python3 scripts/receiver.py --fail-rate 0 > /tmp/demo-receiver.log 2>&1 &
RECEIVER_PID=$!
sleep 1
curl -s -X POST localhost:9000/configure -d "{\"response_delay_seconds\":$RECEIVER_DELAY}" >/dev/null

ARGS="--chronos.task.lease-ttl-seconds=$TASK_TTL --chronos.shard.lease-ttl-seconds=$SHARD_TTL --chronos.heartbeat.period-seconds=$HEARTBEAT --chronos.shard.settle-delay-seconds=$SETTLE"
java -jar "$JAR" --server.port=8080 $ARGS > /tmp/demo-a.log 2>&1 &
PID_A=$!
java -jar "$JAR" --server.port=8081 $ARGS > /tmp/demo-b.log 2>&1 &
PID_B=$!
for i in $(seq 1 90); do
  curl -sf localhost:8080/actuator/health >/dev/null 2>&1 && curl -sf localhost:8081/actuator/health >/dev/null 2>&1 && break
  sleep 1
done
for _ in $(seq 1 20); do
  split=$(psql $PGURL -tAc "SELECT count(DISTINCT lease_owner) FROM shards WHERE lease_expires_at >= now()")
  [ "$split" = 2 ] && break
  sleep 1
done
note "two nodes up, 64 shards split $(psql $PGURL -tAc "SELECT string_agg(c::text,'/') FROM (SELECT count(*) c FROM shards WHERE lease_expires_at>=now() GROUP BY lease_owner) s")"

# ── the scenario ───────────────────────────────────────────────────────────
step "[1/6] submitting one task; a node claims it and starts firing"
FA=$(date -u -d '+3 seconds' +%Y-%m-%dT%H:%M:%SZ)
RESP=$(curl -s -X POST localhost:8080/tasks -H 'Content-Type: application/json' \
  -d "{\"payload\":\"{}\",\"fireAt\":\"$FA\",\"idempotencyKey\":\"demo-$(date +%s)\",\"callbackUrl\":\"http://localhost:9000/hook\"}")
TID=$(echo "$RESP" | python3 -c "import json,sys; print(json.load(sys.stdin).get('taskId',''))" 2>/dev/null || true)
[ -n "$TID" ] || { echo "submit failed: $RESP"; exit 1; }
OWNER=""
for _ in $(seq 1 60); do
  row=$(psql $PGURL -tA -F'|' -c "SELECT state, lease_owner FROM tasks WHERE id='$TID'")
  [ "${row%%|*}" = firing ] && [ -n "${row##*|}" ] && { OWNER="${row##*|}"; break; }
  sleep 0.3
done
[ -n "$OWNER" ] || { echo "task never entered firing — aborting"; exit 1; }
if grep -q "$OWNER" /tmp/demo-a.log; then FROZEN_PID=$PID_A; FROZEN_PORT=8080; else FROZEN_PID=$PID_B; FROZEN_PORT=8081; fi
note "task $TID → claimed by the node on :$FROZEN_PORT, now mid-flight (waiting on the downstream)"

step "[2/6] freezing that node — kill -STOP, simulating a long GC pause or a stalled host"
kill -STOP "$FROZEN_PID"
note "node :$FROZEN_PORT is frozen. Its HTTP call to the downstream is stuck; it can't renew its lease."

step "[3/6] waiting ~${TASK_TTL}s for the frozen node's task lease to expire"
DEADLINE=$(( $(date +%s) + 75 ))
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  st=$(psql $PGURL -tAc "SELECT state FROM tasks WHERE id='$TID'")
  [ "$st" = succeeded ] && break
  sleep 1
done
REFIRED_BY=$(psql $PGURL -tAc "SELECT lease_owner FROM tasks WHERE id='$TID'")
ATTEMPTS=$(psql $PGURL -tAc "SELECT attempt_count FROM tasks WHERE id='$TID'")

step "[4/6] the reaper returned the task to pending; the surviving node re-fired it"
note "re-fired by $REFIRED_BY — with the SAME triggerId (derived from the task id, not the attempt),"
note "so a downstream that dedups on triggerId runs the business action only once"

step "[5/6] waking the frozen node — kill -CONT"
kill -CONT "$FROZEN_PID"
sleep 8
note "node :$FROZEN_PORT resumes, finishes its stale HTTP call, and tries to mark the task done —"
note "but it still holds the version it saw before freezing. The conditional UPDATE matches 0 rows."
grep -h "already succeeded on another node" /tmp/demo-a.log /tmp/demo-b.log | tail -1 | sed 's/^/   log: /' || true

# ── result ────────────────────────────────────────────────────────────────
sleep 6
dup_of() { curl -s "localhost:$1/actuator/prometheus" | awk '/^chronos_duplicate_trigger_total /{v=$2} END{print int(v)+0}'; }
DUP=$(( $(dup_of 8080) + $(dup_of 8081) ))
STATS=$(curl -s localhost:9000/stats)
BIZ=$(echo "$STATS" | python3 -c "import json,sys; print(json.load(sys.stdin)['business_actions'])")
RECV_DUP=$(echo "$STATS" | python3 -c "import json,sys; print(json.load(sys.stdin)['duplicate_triggers'])")
LOST=$(psql $PGURL -tAc "SELECT count(*) FROM tasks WHERE state NOT IN ('succeeded','dead','cancelled')")
FINAL=$(psql $PGURL -tAc "SELECT state FROM tasks WHERE id='$TID'")

step "[6/6] result"
printf '   %-32s %s\n' "task"                       "$FINAL  (claimed $ATTEMPTS times)"
printf '   %-32s %s\n' "late fires absorbed"        "$DUP   (chronos_duplicate_trigger_total, app-side)"
printf '   %-32s %s\n' "business action executed"   "$BIZ time(s)"
printf '   %-32s %s\n' "tasks lost / stuck"         "$LOST"
printf '   %-32s %s\n' "downstream saw the retry"   "$([ "$RECV_DUP" -ge 1 ] && echo "yes — deduped on triggerId" || echo "no — frozen node's request never left")"
echo
if [ "$FINAL" = succeeded ] && [ "$DUP" -ge 1 ] && [ "$BIZ" = 1 ] && [ "$LOST" = 0 ]; then
  echo "   The node froze past its lease; another node re-claimed and completed the task."
  echo "   The frozen node's late write hit the optimistic-lock guard (0 rows) — no double"
  echo "   execution, nothing lost. Business action ran exactly once."
else
  echo "   !! did not land as expected — see /tmp/demo-a.log /tmp/demo-b.log"
  exit 1
fi
