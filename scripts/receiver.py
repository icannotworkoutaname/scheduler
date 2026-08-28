#!/usr/bin/env python3
"""测试用 receiver，模拟 8/8 + 8/21(场景6)需要的下游行为。

两个独立开关，命令行参数控制：
  --fail-rate   [0,1] 之间的概率，命中就返回 500(模拟下游故障)
  --no-dedup    默认开启按 triggerId 去重，加这个参数关掉它做对比实验

GET /stats 返回计数器 JSON
"""
import argparse
import json
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

lock = threading.Lock()
seen_trigger_ids: set[str] = set()
stats = {
    "total_requests": 0,
    "business_actions": 0,
    "duplicate_triggers": 0,
    "failures_injected": 0,
}

fail_rate = 0.0
dedup_enabled = True


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # 用下面自定义的打印代替默认访问日志

    def do_POST(self):
        if self.path == "/hook":
            self._handle_hook()
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if self.path == "/stats":
            self._handle_stats()
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_hook(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)  # 读完 body，不关心内容
        trigger_id = self.headers.get("X-Trigger-Id", "")
        attempt = self.headers.get("X-Attempt", "?")

        with lock:
            stats["total_requests"] += 1

            if random.random() < fail_rate:
                stats["failures_injected"] += 1
                print(f"[FAIL]  triggerId={trigger_id} attempt={attempt}  (injected 500)")
                self.send_response(500)
                self.end_headers()
                return

            if dedup_enabled and trigger_id in seen_trigger_ids:
                stats["duplicate_triggers"] += 1
                print(f"[DUP]   triggerId={trigger_id} attempt={attempt}  (already executed, ack without re-running)")
            else:
                seen_trigger_ids.add(trigger_id)
                stats["business_actions"] += 1
                print(f"[EXEC]  triggerId={trigger_id} attempt={attempt}  (business action runs)")

        self.send_response(200)
        self.end_headers()

    def _handle_stats(self):
        with lock:
            payload = json.dumps(stats, indent=2).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(payload)


def main():
    global fail_rate, dedup_enabled

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--fail-rate", type=float, default=0.0)
    parser.add_argument("--no-dedup", action="store_true")
    args = parser.parse_args()

    fail_rate = args.fail_rate
    dedup_enabled = not args.no_dedup

    print(f"receiver listening on :{args.port}  fail_rate={fail_rate}  dedup={dedup_enabled}")
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
