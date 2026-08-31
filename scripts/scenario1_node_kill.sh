#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/chaos_inject.sh"

PREFIX="scenario1-$(date +%s)-"
VICTIM_PORT=8080

echo "=== resetting environment (tasks + receiver only — shard leases are left alone,"
echo "    see chaos_inject.sh's reset_environment for why) ==="
reset_environment
configure_receiver 0

echo "=== confirming the already-running nodes are still fairly split ==="
wait_for_shard_total 64 15
assert_distinct_owners 2

echo "=== submitting 10 tasks ==="
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:${VICTIM_PORT}/tasks -H "Content-Type: application/json" -d '{
    "payload": "{}", "fireAt": "'"$(date -u -d '+3 seconds' +%Y-%m-%dT%H:%M:%SZ)"'",
    "idempotencyKey": "'"${PREFIX}${i}"'", "callbackUrl": "http://localhost:9000/hook"
  }' > /dev/null
done

T0=$(db_now)
echo "=== T0 = $T0 ==="

sleep 1  # 让部分任务先进入 firing，贴近"kill mid-firing"这个场景的字面含义

VICTIM_PID=$(find_app_pid $VICTIM_PORT)
KILL_TIME=$(db_now)
echo "=== killing victim (pid=$VICTIM_PID, port=$VICTIM_PORT) at $KILL_TIME ==="
kill -9 "$VICTIM_PID"

echo "=== waiting for survivor to hold all 64 shards ==="
wait_for_single_owner "$KILL_TIME" 45
T1=$(db_now)
echo "=== T1 (shard takeover complete) = $T1 ==="

echo "=== waiting for stuck task-level leases to be reaped and re-fired ==="
sleep 10

echo "=== consistency report ==="
python3 "${SCRIPT_DIR}/consistency_check.py" --idempotency-prefix "$PREFIX" --expected-terminal-state succeeded
