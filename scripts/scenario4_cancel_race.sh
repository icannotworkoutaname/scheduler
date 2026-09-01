#!/usr/bin/env bash
#
# 场景 4：取消与触发竞态（8/22）
#
# 40 个任务，一半 fire_at 很近(提交后 sleep 期间就会被 claim 并触发)，一半很远
# (保持 pending)，然后并发对全部 40 个发 DELETE。第一次实测发现：如果全部 40 个
# fire_at 都设在 now+2s，40 个 DELETE 会在任何任务到期前就全部到达，cancel 100%
# 全赢，fire-win 分支一次都测不到。靠顺序提交的时间铺开不可靠(40 个 curl 太快、
# 而且 poller 一批就能 claim 完 40 个)。所以改成"按构造跨越"：不再追求毫秒级
# race，而是保证每次运行两种结局都有样本。要验证的那个不变量(三者一致)不变。
#
# 关键：判定不是只看 HTTP 状态码，是拿状态码去交叉核对 DB 状态 + receiver 的
# /seen 记录，三者必须一致。单纯拿到 204 不代表任务真的没被触发过——必须跟
# receiver 的独立记录对上，才能排除"客户端以为赢了、服务端其实两边都做了"
# 这种最危险的不一致。
#   cancel 赢：HTTP 204  <->  DB state=cancelled  <->  /seen 里没有它
#   fire   赢：HTTP 409  <->  DB state=succeeded  <->  /seen 里有它
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/chaos_inject.sh"

PREFIX="scenario4-$(date +%s)-"
COUNT=40
NODE_PORT=$(any_live_port)
RESULTS_DIR=$(mktemp -d)
trap 'rm -rf "$RESULTS_DIR"' EXIT

echo "=== resetting environment ==="
reset_environment
configure_receiver 0

# 奇数 i：fire_at 近(now+3s)，会在下面的 sleep 期间被 claim + 触发 -> fire 赢
# 偶数 i：fire_at 远(now+40s)，一直是 pending -> cancel 赢
FIRE_NEAR=$(date -u -d '+3 seconds' +%Y-%m-%dT%H:%M:%SZ)
FIRE_FAR=$(date -u -d '+40 seconds' +%Y-%m-%dT%H:%M:%SZ)
echo "=== submitting $COUNT tasks: odd i fire at $FIRE_NEAR, even i at $FIRE_FAR (port $NODE_PORT) ==="
for i in $(seq 1 "$COUNT"); do
  if [ $((i % 2)) -eq 1 ]; then FA=$FIRE_NEAR; else FA=$FIRE_FAR; fi
  curl -s -X POST "http://localhost:${NODE_PORT}/tasks" -H "Content-Type: application/json" -d '{
    "payload": "{}", "fireAt": "'"$FA"'",
    "idempotencyKey": "'"${PREFIX}${i}"'", "callbackUrl": "http://localhost:9000/hook"
  }' > "${RESULTS_DIR}/submit-${i}.json"
done

echo "=== waiting ~5s for the near-fire half to be claimed and fired ==="
sleep 5

echo "=== firing $COUNT concurrent DELETEs (near-fire half already gone, far half still pending) ==="
for i in $(seq 1 "$COUNT"); do
  TASK_ID=$(json_field taskId < "${RESULTS_DIR}/submit-${i}.json")
  {
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "http://localhost:${NODE_PORT}/tasks/${TASK_ID}")
    echo "$TASK_ID $STATUS" >> "${RESULTS_DIR}/results.txt"
  } &
done
wait

echo "=== letting fire-winners reach a terminal state ==="
sleep 6
SEEN=$(curl -s http://localhost:9000/seen)

CANCEL_WON=0
FIRE_WON=0
BAD=0
while read -r TASK_ID STATUS; do
  DB_STATE=$(PGPASSWORD=scheduler psql -h localhost -U scheduler -d scheduler -t -A -c \
    "SELECT state FROM tasks WHERE id = '$TASK_ID';")
  WAS_SEEN=$(printf '%s' "$SEEN" | json_array_contains "$TASK_ID")

  if [ "$STATUS" = "204" ]; then
    CANCEL_WON=$((CANCEL_WON + 1))
    if [ "$DB_STATE" != "cancelled" ] || [ "$WAS_SEEN" != "false" ]; then
      echo "BAD: $TASK_ID got 204 but state=$DB_STATE seen=$WAS_SEEN"
      BAD=$((BAD + 1))
    fi
  elif [ "$STATUS" = "409" ]; then
    FIRE_WON=$((FIRE_WON + 1))
    if [ "$DB_STATE" != "succeeded" ] || [ "$WAS_SEEN" != "true" ]; then
      echo "BAD: $TASK_ID got 409 but state=$DB_STATE seen=$WAS_SEEN"
      BAD=$((BAD + 1))
    fi
  else
    echo "BAD: $TASK_ID unexpected status $STATUS"
    BAD=$((BAD + 1))
  fi
done < "${RESULTS_DIR}/results.txt"

echo ""
echo "cancel_won=$CANCEL_WON  fire_won=$FIRE_WON  bad=$BAD  (of $COUNT)"
if [ "$CANCEL_WON" -eq 0 ] || [ "$FIRE_WON" -eq 0 ]; then
  echo "NOTE: 只覆盖到一种结局——按构造本应各 20，检查 poller 是否在跑 / fire_at 计算"
fi
if [ "$BAD" -eq 0 ]; then
  echo "OK"
else
  echo "FAIL"
  exit 1
fi
