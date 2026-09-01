#!/usr/bin/env bash
#
# 场景 3：并发重复提交去重（8/22）
#
# 比 8/6 那次"依次提交两次"严格：20 个并发请求同时打同一个 idempotencyKey，
# 真正压 tasks_idempotency_key_uq 这条唯一约束在并发下的表现。期望：不管
# 多少个请求同时进来，只产生 1 个 taskId、DB 里只有 1 行。
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/chaos_inject.sh"

KEY="scenario3-$(date +%s)-dup-key"
NODE_PORT=$(any_live_port)

echo "=== resetting environment ==="
reset_environment

RESULTS_DIR=$(mktemp -d)
trap 'rm -rf "$RESULTS_DIR"' EXIT

echo "=== firing 20 concurrent submissions with the same idempotencyKey (port $NODE_PORT) ==="
FIRE_AT=$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)
for i in $(seq 1 20); do
  curl -s -X POST "http://localhost:${NODE_PORT}/tasks" -H "Content-Type: application/json" -d '{
    "payload": "{}", "fireAt": "'"$FIRE_AT"'",
    "idempotencyKey": "'"$KEY"'", "callbackUrl": "http://localhost:9000/hook"
  }' > "${RESULTS_DIR}/resp-${i}.json" &
done
wait

DISTINCT_IDS=$(distinct_json_field_count taskId "${RESULTS_DIR}"/resp-*.json)
ROW_COUNT=$(PGPASSWORD=scheduler psql -h localhost -U scheduler -d scheduler -t -A -c \
  "SELECT count(*) FROM tasks WHERE idempotency_key = '$KEY';")

echo ""
echo "distinct taskIds returned to clients: $DISTINCT_IDS (expect 1)"
echo "rows in DB for that idempotencyKey:   $ROW_COUNT (expect 1)"
if [ "$DISTINCT_IDS" = "1" ] && [ "$ROW_COUNT" = "1" ]; then
  echo "OK"
else
  echo "FAIL"
  exit 1
fi
