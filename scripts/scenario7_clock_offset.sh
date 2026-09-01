#!/usr/bin/env bash
#
# 场景 7：时钟偏移（8/23）
#
# 把节点 A 的墙上时钟拨快 5 分钟（Clock.offset），证明触发行为完全不变——
# 因为 fire_at <= now()、lease_expires_at < now() 全部在 Postgres 侧用数据库
# 时钟算，节点自己的墙上时间不参与任何正确性判定。
#
# 注意：这里只做行为验证。plan.md 8/23 原本还要求"确认 trigger_delay_seconds
# 用数据库时钟算"，但那个指标现在还没埋（目前只有 lease.takeover.total 接了
# Micrometer）。那条顺延到 8/24 埋点那天，而且埋的时候就把时间源钉死为 DB 侧，
# 配一个"偏移 5 分钟的节点上报的 trigger_delay 依然正确"的单测。今天不验证一个
# 还不存在的指标。
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/chaos_inject.sh"

PREFIX="scenario7-$(date +%s)-"
LOG_A=/tmp/scenario7-node-a.log
LOG_B=/tmp/scenario7-node-b.log
JAR="${PROJECT_DIR}/build/libs/scheduler-0.0.1-SNAPSHOT.jar"
OFFSET=300

PID_A=""
PID_B=""
cleanup() {
  echo ""
  echo "=== cleanup ==="
  for pid in "$PID_A" "$PID_B"; do
    if [ -n "$pid" ]; then kill -9 "$pid" 2>/dev/null || true; fi
  done
}
trap cleanup EXIT

echo "=== building the jar ==="
(cd "$PROJECT_DIR" && ./gradlew bootJar -q --console=plain)
[ -f "$JAR" ] || { echo "[ERROR] jar not found" >&2; exit 1; }

echo "=== resetting environment ==="
reset_environment
reset_shard_leases
configure_receiver 0

echo "=== starting node A (+${OFFSET}s clock skew) and node B (normal) ==="
(cd "$PROJECT_DIR" && exec java -jar "$JAR" --server.port=8080 --chronos.clock.offset-seconds="$OFFSET") > "$LOG_A" 2>&1 &
PID_A=$!
(cd "$PROJECT_DIR" && exec java -jar "$JAR" --server.port=8081) > "$LOG_B" 2>&1 &
PID_B=$!

wait_for_health 8080 90
wait_for_health 8081 90
wait_for_shard_total 64 30
assert_distinct_owners 2
NODE_A_ID=$(grep -oP 'node \K[^ ]+(?= announced)' "$LOG_A" | head -1)
NODE_B_ID=$(grep -oP 'node \K[^ ]+(?= announced)' "$LOG_B" | head -1)
echo "=== 32/32 split. node A (skewed)=$NODE_A_ID  node B=$NODE_B_ID ==="

# 通过未偏移的节点 B 提交（偏移的节点会因为 validateFireAt 用它自己的墙上时钟
# 而误判 fire_at 已过期）。任务落在哪个 shard、由谁触发，由共享 DB 决定，跟提交
# 走哪个端口无关。
SUBMIT_AT=$(date -u -d '+4 seconds' +%Y-%m-%dT%H:%M:%SZ)
echo "=== submitting 20 tasks via node B, all firing at $SUBMIT_AT ==="
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:8081/tasks -H "Content-Type: application/json" -d '{
    "payload": "{}", "fireAt": "'"$SUBMIT_AT"'",
    "idempotencyKey": "'"${PREFIX}${i}"'", "callbackUrl": "http://localhost:9000/hook"
  }' > /dev/null
done

echo "=== waiting 15s for all to fire ==="
sleep 15

echo "=== results ==="
PGPASSWORD=scheduler psql -h localhost -U scheduler -d scheduler -t -A -F'|' -c "
  SELECT
    count(*)                                                        AS total,
    count(*) FILTER (WHERE state = 'succeeded')                      AS succeeded,
    count(*) FILTER (WHERE lease_owner = '$NODE_A_ID')              AS fired_by_skewed_node,
    count(*) FILTER (WHERE updated_at < fire_at)                     AS fired_early,
    round(max(extract(epoch FROM (updated_at - fire_at)))::numeric, 1) AS worst_delay_s
  FROM tasks WHERE idempotency_key LIKE '${PREFIX}%';
" | while IFS='|' read -r total ok skewed early worst; do
  echo "total=$total succeeded=$ok fired_by_skewed_node=$skewed fired_early=$early worst_delay_s=$worst"
  PASS=1
  [ "$ok" = "20" ] || { echo "FAIL: not all succeeded"; PASS=0; }
  [ "$early" = "0" ] || { echo "FAIL: $early task(s) fired before their fire_at — node clock leaked into triggering"; PASS=0; }
  [ "$skewed" -ge 1 ] || { echo "FAIL: the skewed node fired nothing — can't conclude it behaves correctly"; PASS=0; }
  # worst_delay 应该是几秒（poll + sink 往返），绝不该是 ~300s
  awk -v w="$worst" 'BEGIN { exit !(w < 30) }' || { echo "FAIL: worst delay ${worst}s — looks like the +${OFFSET}s skew affected timing"; PASS=0; }
  [ "$PASS" = 1 ] && echo "OK — skewed node fired its share on DB time, no early fires, delays are normal"
done

echo ""
echo "=== consistency report ==="
python3 "${SCRIPT_DIR}/consistency_check.py" --idempotency-prefix "$PREFIX" --expected-terminal-state succeeded
