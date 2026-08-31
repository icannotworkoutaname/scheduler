#!/usr/bin/env bash
#
# 场景 6：节点假死导致的双发（8/21）
#
# 要证明的事实链：
#   1. A 认领任务、发出 HTTP，卡在等待响应时被 kill -STOP 冻住(不是被杀死)
#   2. A 的任务级租约到期 -> reaper 把行退回 pending
#   3. A 的 shard 租约到期 -> B 的心跳接管这些 shard
#   4. B 重新认领并重发同一个任务，triggerId 与 A 的完全相同(从 taskId 推导)
#   5. 下游 receiver 认出这是重复触发，业务动作只真正执行一次
#   6. A 被 CONT 唤醒后带着冻结前的旧 version 迟到写入，乐观锁让它影响 0 行
#
# 关键手法：让 receiver 响应前先睡 8 秒。A 从"claim 到任务"到"整个往返 +
# markSucceeded 落库"在本地网络下只要几毫秒到几十毫秒，而我们只能靠外部
# 脚本每 300ms 轮询数据库去发现"任务进了 firing"——不做干预的话，大概率
# 轮询还没反应过来 A 早就处理完了，kill -STOP 会落空，冻结一个已经无事可
# 做的进程，场景根本演不起来。8 秒延迟把"A 阻塞在等待响应"这个窗口人为
# 拉宽到远大于轮询粒度，让冻结确定性地命中"HTTP 还没返回"这个状态。
#
# 一个诚实的不确定性：A 被冻结 50 秒后恢复，这已经远超 HttpSink 自己设的
# 10 秒读超时。JDK HttpClient 此时是"读到早就缓冲好的响应"还是"先判超时抛
# 异常"，依赖挂钟时间的实现细节，我没把握预判。但这不影响要证明的东西——
# 两条分支下 A 手里的 version 都是冻结前的旧值，而 reaper 和 B 早已把 version
# 推进过，markSucceeded 和 markFailed 的 WHERE version = :expectedVersion
# 都会影响 0 行。殊途同归，被同一套乐观锁兜住。
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/chaos_inject.sh"

PREFIX="scenario6-$(date +%s)-"
LOG_A=/tmp/scenario6-node-a.log
LOG_B=/tmp/scenario6-node-b.log
JAR="${PROJECT_DIR}/build/libs/scheduler-0.0.1-SNAPSHOT.jar"
RESPONSE_DELAY=8
# 冻结要持续到 B 真正把任务做完为止。上限的推导见下面 wait_for_task_state
# 调用处的注释——最坏路径约 48 秒，留到 90 秒是余量，正常会提前退出。
FREEZE_TIMEOUT=90

PID_A=""
PID_B=""

cleanup() {
  echo ""
  echo "=== cleanup: making sure nothing is left frozen ==="
  # 顺序很重要：先 CONT 再 KILL。一个处于 STOPPED 状态的进程收到 SIGKILL
  # 会被杀掉没错，但如果只发 TERM 它是收不到的(TERM 会一直挂起到进程恢复)，
  # 留下一个永远冻着的 JVM 占着 8080/8081，下次跑什么都起不来。
  for pid in "$PID_A" "$PID_B"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill -CONT "$pid" 2>/dev/null || true
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT

# 等任务走到某个状态。用它代替"盲睡固定秒数"：整条恢复链路的最坏耗时是
# 几个独立周期叠加出来的(见调用处)，盲睡要么太短偶发失败，要么太长白等。
wait_for_task_state() {
  local task_id=$1 want=$2 timeout=$3
  local elapsed=0 state
  while true; do
    state=$(PGPASSWORD=scheduler psql -h localhost -U scheduler -d scheduler -t -A -c \
      "SELECT state FROM tasks WHERE id = '$task_id';")
    [ "$state" = "$want" ] && return 0
    sleep 1
    elapsed=$((elapsed + 1))
    if [ "$elapsed" -ge "$timeout" ]; then
      echo "[ERROR] task $task_id did not reach '$want' within ${timeout}s (still '$state')" >&2
      return 1
    fi
  done
}

echo "=== building the jar (one sequential build — two concurrent ./gradlew bootRun"
echo "    would contend on this project's known Gradle build lock) ==="
(cd "$PROJECT_DIR" && ./gradlew bootJar -q --console=plain)
[ -f "$JAR" ] || { echo "[ERROR] jar not found at $JAR" >&2; exit 1; }

echo "=== resetting environment ==="
reset_environment
# 场景 6 自己起全新节点，所以必须清掉上一轮遗留的 shard 租约，否则新节点
# 的"报到"阶段会撞上一堆还没过期、主人却早已不在的租约。这是 reset_shard_leases
# 从 8/20 保留至今的第一个真实使用者——它故意不在 reset_environment 里面。
reset_shard_leases
configure_receiver 0 "$RESPONSE_DELAY"

echo "=== starting node A (8080) and node B (8081) ==="
(cd "$PROJECT_DIR" && exec java -jar "$JAR" --server.port=8080) > "$LOG_A" 2>&1 &
PID_A=$!
(cd "$PROJECT_DIR" && exec java -jar "$JAR" --server.port=8081) > "$LOG_B" 2>&1 &
PID_B=$!

wait_for_health 8080 90
wait_for_health 8081 90
wait_for_shard_total 64 30
echo "=== both nodes healthy and shards distributed ==="

# 必须是公平的 32/32。如果启动竞态让某个节点独吞 64 个，冻结它之后另一个
# 节点手上一个 shard 都没有，reaper 把任务退回 pending 也没人来捡——场景会
# 以"任务卡在 pending"的形式失败，而失败原因跟今天要证明的东西毫无关系。
assert_distinct_owners 2
echo "=== PID_A(8080)=$PID_A  PID_B(8081)=$PID_B ==="

echo "=== submitting the canary task ==="
RESP=$(curl -s -X POST http://localhost:8080/tasks -H "Content-Type: application/json" -d '{
  "payload": "{}", "fireAt": "'"$(date -u -d '+3 seconds' +%Y-%m-%dT%H:%M:%SZ)"'",
  "idempotencyKey": "'"${PREFIX}canary"'", "callbackUrl": "http://localhost:9000/hook"
}')
TASK_ID=$(echo "$RESP" | json_field taskId)
[ -n "$TASK_ID" ] || { echo "[ERROR] could not parse task id from: $RESP" >&2; exit 1; }
echo "=== canary task id: $TASK_ID ==="

echo "=== polling for the task to enter firing, to find out which node claimed it ==="
FROZEN_NODE_ID=""
for _ in $(seq 1 40); do
  ROW=$(PGPASSWORD=scheduler psql -h localhost -U scheduler -d scheduler -t -A -F',' -c \
    "SELECT state, lease_owner FROM tasks WHERE id = '$TASK_ID';")
  STATE=$(echo "$ROW" | cut -d',' -f1)
  OWNER=$(echo "$ROW" | cut -d',' -f2)
  if [ "$STATE" = "firing" ] && [ -n "$OWNER" ]; then
    FROZEN_NODE_ID="$OWNER"
    break
  fi
  sleep 0.3
done
[ -z "$FROZEN_NODE_ID" ] && { echo "[ERROR] canary never entered firing" >&2; exit 1; }
echo "=== claimed by: $FROZEN_NODE_ID ==="

if grep -q "$FROZEN_NODE_ID" "$LOG_A"; then
  FROZEN_PID=$PID_A; FROZEN_PORT=8080; SURVIVOR_LOG=$LOG_B
elif grep -q "$FROZEN_NODE_ID" "$LOG_B"; then
  FROZEN_PID=$PID_B; FROZEN_PORT=8081; SURVIVOR_LOG=$LOG_A
else
  echo "[ERROR] could not map $FROZEN_NODE_ID to a running node's log" >&2; exit 1
fi
echo "=== freezing node at port $FROZEN_PORT (pid=$FROZEN_PID), thanks to the ${RESPONSE_DELAY}s"
echo "    response delay it should still be mid-flight, blocked reading the response ==="

T_STOP=$(db_now)
kill -STOP "$FROZEN_PID"
STOP_EPOCH=$(date +%s)
echo "=== T_STOP = $T_STOP ==="

# 恢复链路的最坏耗时，逐段相加(注意：真正的关键路径是 shard 租约，不是
# 任务租约——plan.md 里那版 50 秒的估算漏掉了这一段)：
#   A 的 shard 租约到期        <= 30s (冻结前最多 10s 才续过一次)
#   B 的心跳发现并接管         <= 10s (心跳周期)
#   ------------------------------- 到这里 B 才有资格碰这个 shard，最坏 40s
#   任务租约到期 + reaper 回收 <= 35s (30s TTL + 5s reaper 周期，与上面并行发生)
#   B 的轮询捡起              <= 0.2s
#   下游故意延迟              == 8s
#   ------------------------------- 合计最坏约 48s，50s 的盲睡余量几乎为零
# 所以这里不盲睡，直接等"任务变成 succeeded"这个事实——那正是"B 已经把活
# 干完了"的定义，也正是唤醒 A 让它迟到写入的最佳时刻。
echo "=== waiting for the survivor to reclaim, re-fire and complete the task (up to ${FREEZE_TIMEOUT}s) ==="
wait_for_task_state "$TASK_ID" succeeded "$FREEZE_TIMEOUT"
FREEZE_SECONDS=$(( $(date +%s) - STOP_EPOCH ))
echo "=== survivor finished the task; froze for ${FREEZE_SECONDS}s ==="

T_RESUME=$(db_now)
kill -CONT "$FROZEN_PID"
echo "=== T_RESUME (unfroze) = $T_RESUME ==="

echo "=== letting the resumed node finish whatever it does with its stale attempt ==="
sleep 8

echo ""
echo "=== consistency report ==="
python3 "${SCRIPT_DIR}/consistency_check.py" --idempotency-prefix "$PREFIX" --expected-terminal-state succeeded

echo ""
echo "=== scenario-6-specific check: exactly one duplicate expected, not zero ==="
STATS=$(curl -s http://localhost:9000/stats)
echo "$STATS"
DUP_COUNT=$(echo "$STATS" | json_field duplicate_triggers)
if [ "$DUP_COUNT" = "1" ]; then
  echo "=== CONFIRMED: 1 duplicate trigger, receiver deduped it, business action ran exactly once ==="
else
  echo "=== DID NOT REPRODUCE AS EXPECTED: duplicate_triggers=$DUP_COUNT (wanted exactly 1) ==="
fi

echo ""
echo "=== what the frozen node did upon resume (which branch it took, and whether"
echo "    its stale-version write was correctly rejected) ==="
grep -hE "markSucceeded|markFailed|affected 0 rows|succeeded, triggerId|sink call" "$LOG_A" "$LOG_B" | tail -10 || true
