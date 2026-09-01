#!/usr/bin/env python3
"""Generate a CSV of tasks for direct COPY into Postgres (8/26 load test).

Bypassing POST /tasks means we own the invariant the API normally enforces:
  shard = ShardCalculator.shardFor(id)
        = (id.leastSignificantBits & 63)          [Kotlin]
        = uuid.bytes[15] & 0x3F                    [same 6 bits, the low bits
                                                    of the UUID's last byte]
Getting this wrong (8/5's rule) silently misroutes every task.

Usage:
  loadtest_gen.py --count 1000000 --spread-seconds 3600 --offset-seconds 120 --prefix cap
  loadtest_gen.py --count 100000  --at-seconds 40 --prefix peak
"""
import argparse
import csv
import datetime as dt
import random
import sys
import uuid

CALLBACK = "http://localhost:9000/hook"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, required=True)
    ap.add_argument("--prefix", required=True, help="idempotency_key prefix")
    ap.add_argument("--spread-seconds", type=int, default=0,
                    help="fire_at uniformly in [offset, offset+spread] from now")
    ap.add_argument("--offset-seconds", type=int, default=0)
    ap.add_argument("--at-seconds", type=int, default=None,
                    help="all rows fire_at exactly now+at-seconds (peak test)")
    ap.add_argument("--out", default="-")
    args = ap.parse_args()

    now = dt.datetime.now(dt.timezone.utc)
    fh = sys.stdout if args.out == "-" else open(args.out, "w", newline="")
    w = csv.writer(fh)

    for i in range(args.count):
        u = uuid.uuid4()
        shard = u.bytes[15] & 0x3F
        if args.at_seconds is not None:
            offset = args.at_seconds
        else:
            offset = args.offset_seconds + random.uniform(0, args.spread_seconds)
        fire_at = (now + dt.timedelta(seconds=offset)).isoformat()
        # columns: id, idempotency_key, payload, callback_url, fire_at, state, shard
        w.writerow([str(u), f"{args.prefix}-{i}", "{}", CALLBACK, fire_at, "pending", shard])

    if fh is not sys.stdout:
        fh.close()


if __name__ == "__main__":
    main()
