#!/usr/bin/env bash
#
# 场景 8：时钟漂移（8/23）—— 交付物是一个数字：设计崩溃的漂移临界系数。
#
# 心跳续约的节奏现在由注入的 Clock 算（ShardHeartbeat：@Scheduled 只是频繁轮询，
# 真正的续约只在 clock.instant() 走过一整个周期后才发生），但 shard 租约 TTL
# 仍由数据库自己的时钟量（now() + interval）。所以一个以速率 r 运行的节点，每
# period/r 真实秒续约一次，对着一个固定真实秒数的 TTL。设计的安全边际——每 10s
# 续约、租约活 30s、容忍丢 2 次续约——恰好在
#
#     period / r = ttl   →   r = period / ttl = 10 / 30 = 1/3
#
# 处崩溃。这里用 period=2s / ttl=6s（同样的 1/3），把每个速率点的墙上耗时压到
# 几秒。跑两个点验证临界两侧：
#   r = 0.5  →  续约每 4s < 6s TTL  →  慢节点稳定保有它那 32 个 shard
#   r = 0.25 →  续约每 8s > 6s TTL  →  慢节点反复丢 shard，另一节点接管
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/chaos_inject.sh"

JAR="${PROJECT_DIR}/build/libs/scheduler-0.0.1-SNAPSHOT.jar"
PERIOD=2
TTL=6
COMMON_ARGS="--chronos.heartbeat.period-seconds=${PERIOD} --chronos.shard.lease-ttl-seconds=${TTL} --chronos.shard.settle-delay-seconds=5"

PID_A=""
PID_B=""
cleanup() {
  for pid in "$PID_A" "$PID_B"; do
    if [ -n "$pid" ]; then kill -9 "$pid" 2>/dev/null || true; fi
  done
}
trap cleanup EXIT

echo "=== building the jar ==="
(cd "$PROJECT_DIR" && ./gradlew bootJar -q --console=plain)
[ -f "$JAR" ] || { echo "[ERROR] jar not found" >&2; exit 1; }

# 跑一个速率点：起漂移节点 A + 正常节点 B，采样 A 的 shard 数 22 秒。
# 诊断输出走 stderr（好让用户实时看到），只有最小值走 stdout 供 $() 捕获。
run_rate() {
  local rate=$1
  local log_a=/tmp/scenario8-r${rate}-a.log
  local log_b=/tmp/scenario8-r${rate}-b.log
  {
    echo ""
    echo "############### drift rate r=$rate  (renewal every $(awk -v p=$PERIOD -v r=$rate 'BEGIN{printf "%.1f", p/r}')s real, vs ${TTL}s TTL) ###############"
  } >&2

  reset_environment >/dev/null
  reset_shard_leases
  configure_receiver 0 >/dev/null

  (cd "$PROJECT_DIR" && exec java -jar "$JAR" --server.port=8080 --chronos.clock.drift-rate="$rate" $COMMON_ARGS) > "$log_a" 2>&1 &
  PID_A=$!
  (cd "$PROJECT_DIR" && exec java -jar "$JAR" --server.port=8081 $COMMON_ARGS) > "$log_b" 2>&1 &
  PID_B=$!

  wait_for_health 8080 90
  wait_for_health 8081 90
  wait_for_shard_total 64 30
  assert_distinct_owners 2
  local node_a_id
  node_a_id=$(grep -oP 'node \K[^ ]+(?= announced)' "$log_a" | head -1)
  echo "=== split formed, node A (drifting)=$node_a_id — sampling its shard count for 22s ===" >&2

  local min=64 samples=""
  for _ in $(seq 1 22); do
    local c
    c=$(shard_count_for_owner "$node_a_id")
    samples="$samples $c"
    [ "$c" -lt "$min" ] && min=$c
    sleep 1
  done
  {
    echo "  samples:$samples"
    echo "  min A shard count over the window: $min"
  } >&2

  kill -9 "$PID_A" "$PID_B" 2>/dev/null || true
  PID_A=""; PID_B=""
  sleep 2

  echo "$min"
}

MIN_FAST=$(run_rate 0.5)
MIN_SLOW=$(run_rate 0.25)

echo ""
echo "==================== VERDICT ===================="
echo "r=0.5  (above 1/3): min A shard count = $MIN_FAST   expect 32 (holds steady)"
echo "r=0.25 (below 1/3): min A shard count = $MIN_SLOW   expect <32 (loses shards)"
PASS=1
[ "$MIN_FAST" = "32" ] || { echo "FAIL: at r=0.5 the node above the crossover should never drop a shard"; PASS=0; }
[ "$MIN_SLOW" -lt 32 ] || { echo "FAIL: at r=0.25 the node below the crossover should lose shards"; PASS=0; }
if [ "$PASS" = 1 ]; then
  echo "OK — crossover confirmed between r=0.25 and r=0.5; the design's exact collapse coefficient is"
  echo "     r = heartbeat_period / lease_ttl = ${PERIOD}/${TTL} = 1/3 (same ratio as the real 10s/30s)"
else
  exit 1
fi
