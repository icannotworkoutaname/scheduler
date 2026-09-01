#!/usr/bin/env bash
#
# Load test harness (Block 5, 8/26). Three independent loads — capacity,
# peak, submission — plus the query plans. Re-run any phase on 8/27 to get
# before/after numbers; write them into docs/performance.md.
#
#   scripts/loadtest.sh plans      # EXPLAIN ANALYZE the hot queries at 1M rows
#   scripts/loadtest.sh capacity   # 1M tasks over 1h, ~278/s — trigger P50/P99, idle poll
#   scripts/loadtest.sh peak       # 100k tasks in one instant — peak claim rate, backlog
#   scripts/loadtest.sh submit     # 50k via POST /tasks — submission TPS
#
# Needs: the monitoring stack up (scripts/monitoring-up.sh) for the promql
# samples, Postgres on :5432, a built jar.
#
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export PGPASSWORD=scheduler
PGURL="-h localhost -U scheduler -d scheduler"
JAR=build/libs/scheduler-0.0.1-SNAPSHOT.jar
RECV=http://localhost:9000
PROM=http://localhost:9090

promq() {
  python3 -c "
import urllib.parse,urllib.request,json,sys
u='${PROM}/api/v1/query?query='+urllib.parse.quote(sys.argv[1])
try: r=json.load(urllib.request.urlopen(u,timeout=5))['data']['result']
except Exception as e: print(sys.argv[2],'(promql err:',e,')'); sys.exit()
print(sys.argv[2], [round(float(x['value'][1]),4) if x['value'][1] not in ('NaN','+Inf') else x['value'][1] for x in r] or '(none)')
" "$1" "${2:-}"
}
kill_all() { pkill -9 -f 'scheduler-0.0.1-SNAPSHOT.jar' 2>/dev/null || true; pkill -9 -f 'receiver.py' 2>/dev/null || true; sleep 2; }
reset_db() { psql $PGURL -q -c "TRUNCATE tasks;" -c "UPDATE shards SET lease_owner=NULL, lease_expires_at=NULL;"; }
start_receiver() { nohup python3 scripts/receiver.py --fail-rate 0 > /tmp/loadtest-receiver.log 2>&1 & sleep 1; }
start_node() { nohup java -jar "$JAR" --server.port="$1" > "/tmp/loadtest-$1.log" 2>&1 & }
wait_health() { for _ in $(seq 1 90); do curl -sf "localhost:$1/actuator/health" >/dev/null 2>&1 && return 0; sleep 1; done; echo "[ERR] :$1 never healthy" >&2; return 1; }
copy_in() { psql $PGURL -c "\copy tasks (id,idempotency_key,payload,callback_url,fire_at,state,shard) FROM '$1' WITH (FORMAT csv)"; psql $PGURL -q -c "VACUUM ANALYZE tasks;"; }

phase_plans() {
  echo "### query plans @ $(psql $PGURL -tAc 'SELECT count(*) FROM tasks') rows"
  local S; S=$(python3 -c "print(','.join(map(str,range(0,32))))")
  echo "-- reaper --"
  psql $PGURL -c "EXPLAIN (ANALYZE,BUFFERS) SELECT id, lease_owner FROM tasks WHERE state='firing' AND lease_expires_at < now() FOR UPDATE SKIP LOCKED LIMIT 500;"
  echo "-- poll claim (nothing due path) --"
  psql $PGURL -c "EXPLAIN (ANALYZE,BUFFERS) SELECT id FROM tasks WHERE shard IN ($S) AND state IN ('pending','retrying') AND fire_at <= now() ORDER BY fire_at FOR UPDATE SKIP LOCKED LIMIT 500;"
  echo "-- tasks_pending gauge --"
  psql $PGURL -c "EXPLAIN (ANALYZE,BUFFERS) SELECT shard, count(*) FROM tasks WHERE state IN ('pending','retrying') GROUP BY shard;"
}

phase_capacity() {
  kill_all; reset_db
  python3 scripts/loadtest_gen.py --count 1000000 --prefix cap --spread-seconds 3600 --offset-seconds 300 --out /tmp/cap.csv
  local t0; t0=$(date +%s); copy_in /tmp/cap.csv; echo "COPY 1M + VACUUM in $(( $(date +%s) - t0 ))s"
  phase_plans
  start_receiver; start_node 8080; start_node 8081; wait_health 8080; wait_health 8081; sleep 8
  echo "=== idle window (nothing due), sampling 120s ==="
  sleep 120
  promq 'histogram_quantile(0.50, sum(rate(chronos_poll_duration_seconds_bucket[2m])) by (le))' 'idle poll P50 ='
  promq 'histogram_quantile(0.99, sum(rate(chronos_poll_duration_seconds_bucket[2m])) by (le))' 'idle poll P99 ='
  echo "=== drain window, 60s x 12 ==="
  for m in $(seq 1 12); do
    sleep 60
    echo "-- min $m --"
    promq 'sum(rate(chronos_trigger_delay_seconds_count[1m]))' '  triggers/s        ='
    promq 'histogram_quantile(0.50, sum(rate(chronos_trigger_delay_seconds_bucket[3m])) by (le))' '  trigger P50       ='
    promq 'histogram_quantile(0.99, sum(rate(chronos_trigger_delay_seconds_bucket[3m])) by (le))' '  trigger P99       ='
    psql $PGURL -tAc "SELECT '  pending: '||count(*) FROM tasks WHERE state='pending';"
  done
  psql $PGURL -c "SELECT state,count(*) FROM tasks GROUP BY state;"; curl -s "$RECV/stats"; echo
}

phase_peak() {
  kill_all; reset_db
  python3 scripts/loadtest_gen.py --count 100000 --prefix peak --at-seconds 50 --out /tmp/peak.csv
  copy_in /tmp/peak.csv
  start_receiver; start_node 8080; start_node 8081; wait_health 8080; wait_health 8081; sleep 8
  local fire now; fire=$(psql $PGURL -tAc "SELECT extract(epoch FROM min(fire_at))::int FROM tasks"); now=$(date +%s)
  echo "due in $((fire-now))s — sampling every 3s"
  for i in $(seq 1 40); do
    sleep 3
    printf '%3ds  ' "$((i*3))"
    promq 'sum(rate(chronos_trigger_delay_seconds_count[10s]))' 'claim/s ='
    local s; s=$(psql $PGURL -tAc "SELECT count(*) FROM tasks WHERE state='succeeded'")
    psql $PGURL -tAc "SELECT '     firing='||count(*) FILTER (WHERE state='firing')||' succeeded='||count(*) FILTER (WHERE state='succeeded')||' pending='||count(*) FILTER (WHERE state IN ('pending','retrying')) FROM tasks;"
    [ "$s" -ge 99000 ] && { echo drained; break; }
  done
  promq 'max_over_time(sum(rate(chronos_trigger_delay_seconds_count[10s]))[3m:5s])' 'PEAK claim/s ='
  promq 'histogram_quantile(0.99, sum(rate(chronos_trigger_delay_seconds_bucket[3m])) by (le))' 'trigger P99 (burst) ='
  promq 'sum(chronos_duplicate_trigger_total)' 'app duplicate_trigger_total ='
  psql $PGURL -c "SELECT state,count(*), max(attempt_count) FROM tasks GROUP BY state;"; curl -s "$RECV/stats"; echo
}

phase_submit() {
  kill_all; reset_db
  start_receiver; start_node 8080; wait_health 8080; sleep 5
  python3 scripts/loadtest_submit.py --count 50000 --workers 96 --url http://localhost:8080
  curl -s http://localhost:8080/actuator/prometheus | grep -E '^hikaricp_connections(_acquire_seconds|_usage_seconds|_timeout|_pending| )' || true
}

case "${1:-}" in
  plans)    phase_plans ;;
  capacity) phase_capacity ;;
  peak)     phase_peak ;;
  submit)   phase_submit ;;
  *) echo "usage: $0 {plans|capacity|peak|submit}" >&2; exit 2 ;;
esac
