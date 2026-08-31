#!/usr/bin/env bash
set -euo pipefail

# 混沌注入的公共动作库，被具体场景脚本 source 引用，不是独立运行的。

find_app_pid() {
  local port=$1
  # 用监听端口反查 PID，比 jps 更精确——jps 找不到"哪个进程占了哪个端口"，
  # 只能列出所有 JVM 进程，端口号得靠猜或者靠日志时间戳对，容易对错。
  lsof -ti tcp:"$port" -sTCP:LISTEN || true
}

wait_for_health() {
  local port=$1
  local timeout=${2:-30}
  local elapsed=0
  until curl -sf "http://localhost:${port}/actuator/health" > /dev/null 2>&1; do
    sleep 1
    elapsed=$((elapsed + 1))
    if [ "$elapsed" -ge "$timeout" ]; then
      echo "[ERROR] node on port $port did not become healthy within ${timeout}s" >&2
      return 1
    fi
  done
}

db_now() {
  PGPASSWORD=scheduler psql -h localhost -U scheduler -d scheduler -t -A -c "SELECT now();"
}

# 只清任务表和 receiver 计数器，不碰 shard 租约。大多数场景（包括场景 1）
# 需要的只是"任务和计数器归零"这个干净起点，节点当前公平持有的 shard
# 分配不该被顺手清空——8/20 复盘发现：对着已经在跑、已经公平 32/32 的两个
# 节点清空 shards 表，等于把唯一保证公平的机制（节点启动时那次"报到-等待-
# 认领"协议）绕过去，之后只能指望心跳去抢救，而心跳从设计上就不保证公平，
# 短时间内重试也救不回来（两个节点的心跳相位在几秒内几乎不变，谁先手
# 抢到全部 64 个是确定性的，不是随机的）。
# receiver 的响应延迟(8/21 场景 6 引入)也必须在这里归零。POST /reset 只清
# 计数器和 triggerId 集合，不碰 configure 设的开关——延迟是进程级的全局状态，
# 会跨场景残留：跑完场景 6 再复用同一个 receiver 进程跑场景 1，残留的 8 秒
# 延迟会悄悄打乱 wait_for_single_owner 45 之类等待窗口的假设，而且不会报错，
# 只会表现为莫名其妙的超时。这是 8/19 那次"数据库和 receiver 两边必须一起
# 归零"教训的延续。
reset_environment() {
  PGPASSWORD=scheduler psql -h localhost -U scheduler -d scheduler -c "TRUNCATE tasks;" > /dev/null
  curl -sf -X POST "http://localhost:9000/reset" > /dev/null \
    || echo "[WARN] receiver reset failed — is scripts/receiver.py running on :9000?" >&2
  curl -sf -X POST "http://localhost:9000/configure" -H "Content-Type: application/json" \
    -d '{"fail_rate": 0, "dedup_enabled": true, "response_delay_seconds": 0}' > /dev/null || true
}

# 真正需要清空 shard 租约、让节点从零开始重新分配的场景才调用这个。
# 8/21 起有了第一个真正的使用者：场景 6 自己启动两个全新节点，必须在启动
# 之前清掉上一轮遗留的租约，否则新节点会看到一堆还没过期、但主人早已不在
# 的租约，Phase 1 的"报到"抢不到 shard，公平分配从一开始就是歪的。
#
# 注意它为什么不能被塞进 reset_environment()：场景 1/2 是对着**已经在跑**的
# 节点调用 reset_environment 的，对它们清空租约等于把唯一保证公平的机制
# （启动时那次"报到-等待-认领"协议）绕过去，之后只能指望心跳去抢救，而心跳
# 从设计上就不保证公平（8/20 复盘）。"清任务"和"清 shard"必须保持是两件事。
reset_shard_leases() {
  PGPASSWORD=scheduler psql -h localhost -U scheduler -d scheduler -c \
    "UPDATE shards SET lease_owner = NULL, lease_expires_at = NULL;" > /dev/null
}

# 等待"活跃租约总数达到 expected"——用于 reset 之后确认初始分配收敛完成。
# 注意：这个函数不能用来判断"接管是否真的发生"，见下面 wait_for_single_owner
# 的注释——原因是死节点的旧租约在过期前依然会被计入总数，造成假阳性。
wait_for_shard_total() {
  local expected=$1
  local timeout=${2:-30}
  local elapsed=0
  while true; do
    local total
    total=$(PGPASSWORD=scheduler psql -h localhost -U scheduler -d scheduler -t -A -c \
      "SELECT count(*) FROM shards WHERE lease_owner IS NOT NULL AND lease_expires_at >= now();")
    [ "$total" -ge "$expected" ] && return 0
    sleep 1
    elapsed=$((elapsed + 1))
    [ "$elapsed" -ge "$timeout" ] && { echo "[ERROR] shard total did not reach $expected within ${timeout}s (got $total)" >&2; return 1; }
  done
}

# 断言当前恰好有 expected 个不同的存活 owner——用于在故障注入之前确认
# 初始分配确实公平，而不是被 wait_for_shard_total 只看总数掩盖过去的一次
# "赢家通吃"竞态（8/20 复盘：reset_environment() 作用于已经在跑的节点时，
# 心跳周期没对齐，仍然可能复现 8/10 那次不公平抢占，总数照样能凑够 64）。
assert_distinct_owners() {
  local expected=$1
  local distinct_owners
  distinct_owners=$(PGPASSWORD=scheduler psql -h localhost -U scheduler -d scheduler -t -A -c \
    "SELECT count(DISTINCT lease_owner) FROM shards WHERE lease_owner IS NOT NULL AND lease_expires_at >= now();")
  if [ "$distinct_owners" != "$expected" ]; then
    echo "[ERROR] expected $expected distinct shard owner(s), found $distinct_owners — initial convergence was not fair, aborting" >&2
    return 1
  fi
}

# 等待"恰好一个存活 owner 持有全部 64 个"——用于判断故障接管是否真的完成。
#
# 8/20 复盘发现：光检查"distinct owner = 1 且 total = 64"不够——刚杀掉的
# 那个节点，如果它死前恰好持有全部 64 个（比如启动阶段撞上过一次不公平的
# 抢占竞态），它名下的租约在自然过期之前依然满足这个条件，会被误判成
# "已经收敛"，实际上存活节点一个都没抢到，真正的接管还要等租约自然到期。
#
# 修法：借用这个项目从第一天验证 reaper 时就用的技巧——用
# lease_expires_at - interval '30 seconds' 反推"这一行最近一次被
# claim/续约是什么时候"，要求这个反推出来的时刻晚于 kill 发生的时刻
# （kill_time 参数）。死节点的陈旧行反推出来必然早于 kill，天然被排除；
# 只有存活节点在 kill 之后真正重新抢到的行才会通过这个判断。
wait_for_single_owner() {
  local kill_time=$1
  local timeout=${2:-45}
  local elapsed=0
  while true; do
    local distinct_owners claimed_after
    distinct_owners=$(PGPASSWORD=scheduler psql -h localhost -U scheduler -d scheduler -t -A -c \
      "SELECT count(DISTINCT lease_owner) FROM shards WHERE lease_owner IS NOT NULL AND lease_expires_at >= now();")
    claimed_after=$(PGPASSWORD=scheduler psql -h localhost -U scheduler -d scheduler -t -A -c \
      "SELECT count(*) FROM shards WHERE lease_owner IS NOT NULL AND lease_expires_at >= now() \
         AND (lease_expires_at - interval '30 seconds') > '$kill_time'::timestamptz;")
    [ "$distinct_owners" = "1" ] && [ "$claimed_after" = "64" ] && return 0
    sleep 2
    elapsed=$((elapsed + 2))
    [ "$elapsed" -ge "$timeout" ] && { echo "[ERROR] did not converge to single owner (claimed after kill) within ${timeout}s (distinct=$distinct_owners claimed_after=$claimed_after)" >&2; return 1; }
  done
}

# 返回当前存活的节点端口中的任意一个（8080 优先，其次 8081）。场景脚本
# 之间不该假设"上一个场景结束后所有节点都还活着"——场景 1 就是靠杀掉
# 一个节点才成立的，8/20 复盘发现场景 2 硬编码提交到 8080 在场景 1 之后
# 背靠背跑会直接连接失败。提交任务这件事只需要"任意一个活着的节点"，
# 不需要绑定具体端口——不管请求打到哪个节点，任务落哪个 shard、由谁处理
# 是共享数据库和 shard 所有权决定的，跟提交时connect的端口无关。
any_live_port() {
  for port in 8080 8081; do
    if curl -sf "http://localhost:${port}/actuator/health" > /dev/null 2>&1; then
      echo "$port"
      return 0
    fi
  done
  echo "[ERROR] no live node found on 8080 or 8081" >&2
  return 1
}

# 第二个参数(响应延迟秒数)可选，默认 0——昨天以前的调用方式不用改。
configure_receiver() {
  local fr=$1
  local delay=${2:-0}
  curl -sf -X POST "http://localhost:9000/configure" -H "Content-Type: application/json" \
    -d "{\"fail_rate\": $fr, \"dedup_enabled\": true, \"response_delay_seconds\": $delay}" > /dev/null \
    || echo "[WARN] receiver configure failed — is scripts/receiver.py running on :9000?" >&2
}

# 从 JSON 里取一个顶层字段。项目里没装 jq(装它要 sudo + 网络)，而 python3
# 本来就是 receiver 和 consistency_check 的依赖，不额外引入任何东西。
json_field() {
  python3 -c "import json,sys; print(json.load(sys.stdin).get('$1',''))"
}
