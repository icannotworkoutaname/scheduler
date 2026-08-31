#!/usr/bin/env python3
"""测试用 receiver，模拟 8/8 + 8/21(场景6)需要的下游行为。

两个独立开关，命令行参数控制：
  --fail-rate   [0,1] 之间的概率，命中就返回 500(模拟下游故障)
  --no-dedup    默认开启按 triggerId 去重，加这个参数关掉它做对比实验

第三个开关只能通过 POST /configure 设置(没有命令行参数,因为它的用途是
在一次实验中途改,不是启动时定死)：response_delay_seconds —— 响应前故意
先睡这么久。场景 6(8/21)靠它把"节点 A 阻塞在等待下游响应"这个窗口，从
本地网络下的几毫秒人为拉宽到远大于外部轮询脚本 300ms 的粒度，否则
kill -STOP 大概率落在 A 已经处理完之后，场景根本演不起来。

GET /stats 返回计数器 JSON
"""
import argparse
import json
import random
import threading
import time
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
response_delay_seconds = 0.0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # 用下面自定义的打印代替默认访问日志

    def do_POST(self):
        if self.path == "/hook":
            self._handle_hook()
        elif self.path == "/reset":
            self._handle_reset()
        elif self.path == "/configure":
            self._handle_configure()
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

        # 延迟发生在拿锁之前：ThreadingHTTPServer 每个请求一个线程，如果
        # 抱着锁睡，一个请求的 sleep 会把其他并发请求全部堵在锁上，计数器
        # 就不再反映真实的到达顺序——而场景 6 要证明的恰恰是"两次触发都
        # 到达了、第二次被识别为重复"，计数必须准。
        with lock:
            delay = response_delay_seconds
        if delay > 0:
            print(f"[DELAY] triggerId={trigger_id} attempt={attempt}  sleeping {delay}s before responding", flush=True)
            time.sleep(delay)

        with lock:
            stats["total_requests"] += 1

            if random.random() < fail_rate:
                stats["failures_injected"] += 1
                print(f"[FAIL]  triggerId={trigger_id} attempt={attempt}  (injected 500)", flush=True)
                self.send_response(500)
                self.end_headers()
                return

            if dedup_enabled and trigger_id in seen_trigger_ids:
                stats["duplicate_triggers"] += 1
                print(f"[DUP]   triggerId={trigger_id} attempt={attempt}  (already executed, ack without re-running)", flush=True)
            else:
                seen_trigger_ids.add(trigger_id)
                stats["business_actions"] += 1
                print(f"[EXEC]  triggerId={trigger_id} attempt={attempt}  (business action runs)", flush=True)

        self.send_response(200)
        self.end_headers()

    def _handle_reset(self):
        with lock:
            seen_trigger_ids.clear()
            stats.update({
                "total_requests": 0,
                "business_actions": 0,
                "duplicate_triggers": 0,
                "failures_injected": 0,
            })
        self.send_response(200)
        self.end_headers()

    def _handle_configure(self):
        global fail_rate, dedup_enabled, response_delay_seconds
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        with lock:
            if "fail_rate" in body:
                fail_rate = float(body["fail_rate"])
            if "dedup_enabled" in body:
                dedup_enabled = bool(body["dedup_enabled"])
            if "response_delay_seconds" in body:
                response_delay_seconds = float(body["response_delay_seconds"])
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({
            "fail_rate": fail_rate,
            "dedup_enabled": dedup_enabled,
            "response_delay_seconds": response_delay_seconds,
        }).encode())

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

    print(f"receiver listening on :{args.port}  fail_rate={fail_rate}  dedup={dedup_enabled}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
