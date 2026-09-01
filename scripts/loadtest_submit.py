#!/usr/bin/env python3
"""Hammer POST /tasks concurrently and report sustained submission TPS
(requirements.md §5). Tests the API path — insertOrGetExisting / the unique
index — not the bulk COPY path.

  loadtest_submit.py --count 50000 --workers 96 --url http://localhost:8080
"""
import argparse
import concurrent.futures as cf
import datetime as dt
import json
import time
import urllib.request
import uuid


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=50000)
    ap.add_argument("--workers", type=int, default=96)
    ap.add_argument("--url", default="http://localhost:8080")
    args = ap.parse_args()

    fire_at = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    endpoint = f"{args.url}/tasks"
    prefix = f"sub-{int(time.time())}"

    def submit(i: int) -> int:
        body = json.dumps({
            "payload": "{}",
            "fireAt": fire_at,
            "idempotencyKey": f"{prefix}-{i}",
            "callbackUrl": "http://localhost:9000/hook",
        }).encode()
        req = urllib.request.Request(endpoint, data=body,
                                     headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status
        except Exception:
            return 0

    codes = {}
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for code in ex.map(submit, range(args.count)):
            codes[code] = codes.get(code, 0) + 1
    elapsed = time.time() - t0

    ok = codes.get(201, 0) + codes.get(200, 0)
    print(f"submitted {ok}/{args.count} in {elapsed:.1f}s  =>  {ok/elapsed:,.0f} TPS")
    print(f"status codes: {codes}")


if __name__ == "__main__":
    main()
