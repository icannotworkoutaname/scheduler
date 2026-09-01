#!/usr/bin/env bash
#
# 场景 5：网络分区（8/22）
#
# 这套系统里节点之间没有直连通信，一个节点唯一依赖的外部协调渠道就是 Postgres
# ——shards 表本身既是资源也是"谁还活着"的信号板(8/10)。所以"网络分区"在这套
# 架构下精确地等价于"这个节点连不上 Postgres 了"。
#
# 实现：node A 只通过一个用户态 TCP 转发进程(db_proxy.py)访问 Postgres，node B
# 直连。冻结转发进程 = 分区(字节静默停流、socket 不关闭)，解冻 = 恢复。不需要
# root，精确隔离单个节点。
#
# 要验证的事——"分区节点恢复后发现自己没有 shard 了、不会继续触发"——完全靠
# 8/10(实时查询归属)和 8/11(心跳续约靠竞争、不靠通知)的已有设计结构性满足：
# A 恢复连接后，renewOwnedShards 自己名下 0 行续不出东西，claimAvailableShards
# 也抢不到(B 手里 64 个租约都没过期，available 的 SQL 条件一个都不满足)。A 就
# 这样自然停在"持有 0 个 shard"，不需要任何"我是不是刚从分区恢复"的判断代码。
# 这个脚本纯粹是验证它，不是实现它。
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/chaos_inject.sh"

PREFIX="scenario5-$(date +%s)-"
LOG_A=/tmp/scenario5-node-a.log
LOG_B=/tmp/scenario5-node-b.log
JAR="${PROJECT_DIR}/build/libs/scheduler-0.0.1-SNAPSHOT.jar"
DB_DIRECT="jdbc:postgresql://localhost:5432/scheduler?connectTimeout=5&socketTimeout=5"
DB_VIA_PROXY="jdbc:postgresql://localhost:${DB_PROXY_PORT}/scheduler?connectTimeout=5&socketTimeout=5"

PID_A=""
PID_B=""

cleanup() {
  echo ""
  echo "=== cleanup ==="
  heal_db_proxy
  stop_db_proxy
  for pid in "$PID_A" "$PID_B"; do
    if [ -n "$pid" ]; then kill -9 "$pid" 2>/dev/null || true; fi
  done
}
trap cleanup EXIT

echo "=== building the jar (sequential — two concurrent ./gradlew would contend on the build lock) ==="
(cd "$PROJECT_DIR" && ./gradlew bootJar -q --console=plain)
[ -f "$JAR" ] || { echo "[ERROR] jar not found at $JAR" >&2; exit 1; }

echo "=== resetting environment ==="
reset_environment
reset_shard_leases          # 自己起全新节点，必须清掉上一轮遗留租约(见 chaos_inject.sh)
configure_receiver 0

echo "=== starting db proxy (node A reaches postgres only through it) ==="
start_db_proxy
echo "=== db_proxy pid=$DB_PROXY_PID on :$DB_PROXY_PORT ==="

echo "=== starting node A (via proxy :$DB_PROXY_PORT) and node B (direct :5432) ==="
(cd "$PROJECT_DIR" && exec java -jar "$JAR" --server.port=8080 --spring.datasource.url="$DB_VIA_PROXY") > "$LOG_A" 2>&1 &
PID_A=$!
(cd "$PROJECT_DIR" && exec java -jar "$JAR" --server.port=8081 --spring.datasource.url="$DB_DIRECT") > "$LOG_B" 2>&1 &
PID_B=$!

wait_for_health 8080 90
wait_for_health 8081 90
wait_for_shard_total 64 30
assert_distinct_owners 2
echo "=== both nodes healthy, fair 32/32 split ==="

NODE_A_ID=$(grep -oP 'node \K[^ ]+(?= announced)' "$LOG_A" | head -1)
[ -n "$NODE_A_ID" ] || { echo "[ERROR] could not read node A id from $LOG_A" >&2; exit 1; }
echo "=== node A id=$NODE_A_ID pid=$PID_A ; node B pid=$PID_B ==="

echo "=== submitting 10 tasks firing ~5s out ==="
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8080/tasks -H "Content-Type: application/json" -d '{
    "payload": "{}", "fireAt": "'"$(date -u -d '+5 seconds' +%Y-%m-%dT%H:%M:%SZ)"'",
    "idempotencyKey": "'"${PREFIX}${i}"'", "callbackUrl": "http://localhost:9000/hook"
  }' > /dev/null
done

PARTITION_TIME=$(db_now)
echo "=== partitioning node A from postgres (freezing the proxy) at $PARTITION_TIME ==="
partition_db_proxy

echo "=== waiting for node B to take over all 64 shards (claimed after the partition) ==="
wait_for_single_owner "$PARTITION_TIME" 75

A_COUNT=$(shard_count_for_owner "$NODE_A_ID")
echo "=== node A holds while partitioned: $A_COUNT shard(s) (expect 0) ==="

echo "=== node A log during partition (expect socket-timeout / connection errors) ==="
grep -iE "timeout|SQLException|connection|Unable to acquire|dead" "$LOG_A" | tail -6 || true

echo "=== healing the partition (unfreezing the proxy) ==="
heal_db_proxy
echo "=== waiting 25s for node A's pool to recover and a heartbeat to run ==="
sleep 25

A_COUNT_AFTER=$(shard_count_for_owner "$NODE_A_ID")
echo "=== node A holds after recovery: $A_COUNT_AFTER shard(s) (expect 0 — B never hands any back) ==="

echo "=== node A health + its heartbeat line now ==="
curl -sf http://localhost:8080/actuator/health && echo || echo "[WARN] node A not healthy yet"
grep -E "heartbeat: renewed" "$LOG_A" | tail -3 || true

echo ""
echo "=== consistency report ==="
python3 "${SCRIPT_DIR}/consistency_check.py" --idempotency-prefix "$PREFIX" --expected-terminal-state succeeded
