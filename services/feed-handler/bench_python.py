"""Python baseline for the feed handler, so the C++ number means something.

GoalState: "Write the Python version first and compare. The honest claim is
'C++ sustained X msg/sec at p99 latency Y vs Python's Z', not 'used C++ for
performance'."

Same work as the C++ pipeline in --bench mode: read recorded frames, parse the
JSON, filter to ticker messages, build a record, keep feed order. No disk write,
matching the C++ bench path.

    python bench_python.py FRAMES.jsonl
"""

import json
import sys
import time
from datetime import datetime, timezone


def run(path):
    frames = 0
    ticks = 0
    skipped = 0

    started = time.perf_counter()

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            frames += 1

            # The C++ side stamps the clock before touching the frame, so do
            # the same here rather than quietly skipping the cost.
            recv = datetime.now(timezone.utc)

            try:
                j = json.loads(line)
            except ValueError:
                skipped += 1
                continue

            if j.get("type") != "ticker":
                skipped += 1
                continue

            try:
                tick = {
                    "sequence": int(j["sequence"]),
                    "product_id": j["product_id"],
                    "price": float(j["price"]),
                    "last_size": float(j["last_size"]),
                    "side": j["side"],
                    "best_bid": float(j["best_bid"]),
                    "best_bid_size": float(j["best_bid_size"]),
                    "best_ask": float(j["best_ask"]),
                    "best_ask_size": float(j["best_ask_size"]),
                    "time": j["time"],
                    "trade_id": int(j["trade_id"]),
                    "recv_time": recv.isoformat(),
                }
            except (KeyError, TypeError, ValueError):
                skipped += 1
                continue

            # Serialise too -- the C++ writer pays this cost, so the comparison
            # is unfair without it.
            json.dumps(tick)
            ticks += 1

    elapsed = time.perf_counter() - started

    print(f"frames read   : {frames}")
    print(f"ticks parsed  : {ticks}")
    print(f"skipped       : {skipped}")
    print(f"elapsed       : {elapsed:.3f} s")
    print(f"throughput    : {frames / elapsed:,.0f} frames/sec")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    run(sys.argv[1])
