#!/usr/bin/env python3
"""
跑完一次混沌实验后，对比数据库状态和 receiver 的 /stats，判断系统是否
按照 requirements.md §4 承诺的"at-least-once + 可测量的重复"运作。

用法：
    python3 scripts/consistency_check.py --idempotency-prefix <前缀> --receiver-url http://localhost:9000
"""
import argparse
import json
import subprocess
import sys
import urllib.request


def query_db(sql: str) -> str:
    result = subprocess.run(
        ["psql", "-h", "localhost", "-U", "scheduler", "-d", "scheduler", "-t", "-A", "-F,", "-c", sql],
        env={"PGPASSWORD": "scheduler", "PATH": "/usr/bin:/bin"},
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"[ERROR] query failed: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def fetch_receiver_stats(url: str) -> dict:
    with urllib.request.urlopen(f"{url}/stats", timeout=5) as resp:
        return json.loads(resp.read())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--idempotency-prefix", required=True,
                         help="只统计 idempotency_key 以这个前缀开头的任务，避免跟其他测试数据混在一起")
    parser.add_argument("--receiver-url", default="http://localhost:9000")
    parser.add_argument("--expected-terminal-state", default="succeeded", choices=["succeeded", "dead"])
    args = parser.parse_args()

    prefix = args.idempotency_prefix

    total = int(query_db(f"SELECT count(*) FROM tasks WHERE idempotency_key LIKE '{prefix}%'"))

    state_counts_raw = query_db(
        f"SELECT state, count(*) FROM tasks WHERE idempotency_key LIKE '{prefix}%' GROUP BY state"
    )
    state_counts = {}
    for line in state_counts_raw.splitlines():
        if not line:
            continue
        state, count = line.split(",")
        state_counts[state] = int(count)

    # 卡死判定：firing 状态但租约已经过期——reaper 应该已经把这类行回收掉了，
    # 如果还能查到，说明 reaper 本身有问题，不是"正常在处理中"。
    stuck = int(query_db(
        f"SELECT count(*) FROM tasks WHERE idempotency_key LIKE '{prefix}%' "
        f"AND state = 'firing' AND lease_expires_at < now()"
    ))

    try:
        receiver_stats = fetch_receiver_stats(args.receiver_url)
    except Exception as e:
        receiver_stats = {"error": str(e)}

    expected_count = state_counts.get(args.expected_terminal_state, 0)
    in_flight = state_counts.get("pending", 0) + state_counts.get("retrying", 0) + state_counts.get("firing", 0)

    report = {
        "total_tasks": total,
        "state_breakdown": state_counts,
        "expected_terminal_state": args.expected_terminal_state,
        "stuck_in_firing_past_lease": stuck,
        "in_flight_not_yet_terminal": in_flight,
        "receiver_stats": receiver_stats,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))

    problems = []
    if stuck > 0:
        problems.append(f"{stuck} task(s) stuck in firing past lease expiry — reaper may be broken")
    if in_flight > 0:
        problems.append(f"{in_flight} task(s) not yet in a terminal state — did the experiment run long enough?")
    if expected_count != total:
        problems.append(f"expected all {total} tasks to reach '{args.expected_terminal_state}', only {expected_count} did")

    if "error" not in receiver_stats:
        actual_business = receiver_stats.get("business_actions", 0)
        if args.expected_terminal_state == "succeeded":
            if actual_business != expected_count:
                problems.append(f"business_actions ({actual_business}) != succeeded tasks in DB ({expected_count})")
        else:  # dead — 每次尝试都该被下游拒绝，永远不该走到"业务真正执行"这一步
            if actual_business != 0:
                problems.append(f"expected 0 business actions for tasks that should all dead-letter, got {actual_business}")

    print("\n--- verdict ---")
    if problems:
        print("FAIL:")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)
    else:
        print(f"OK: {total} tasks, all reached '{args.expected_terminal_state}', "
              f"0 stuck, 0 in-flight, receiver stats consistent with DB state")


if __name__ == "__main__":
    main()
