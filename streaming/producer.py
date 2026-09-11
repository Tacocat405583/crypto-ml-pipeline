"""Replay ticks from the Parquet lake onto a Kafka topic, at N x market speed.

    docker compose -f docker/compose.yml up -d
    uv run python streaming/producer.py --pace 100

One JSON message per tick, keyed by symbol, so every tick of a symbol lands on
one partition and keeps its order. Paced like the feed handler's --pace: each
message is due at its exchange-time offset from the first, divided by pace. A
silence longer than --max-gap seconds of market time (the days between two
capture sessions) is skipped rather than waited out.

The producer is idempotent (enable.idempotence, acks=all), so a send the client
retries after a timeout cannot land on the topic twice.
"""

import argparse
import json
import sys
import time
from pathlib import Path

import duckdb
from confluent_kafka import Producer

ROOT = Path(__file__).resolve().parents[1]
LAKE = ROOT / "data" / "lake" / "ticks"


def load(symbol: str, lake: Path = LAKE) -> list[dict]:
    cur = duckdb.connect().execute(
        f"""SELECT symbol, sequence, trade_id, time, recv_time, price, last_size, side
            FROM read_parquet('{lake.as_posix()}/**/*.parquet', hive_partitioning = true)
            WHERE symbol = ? ORDER BY sequence""", [symbol])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def encode(t: dict) -> bytes:
    return json.dumps({k: (v.isoformat() + "Z" if hasattr(v, "isoformat") else v) for k, v in t.items()}).encode()


def publish(ticks: list[dict], bootstrap: str, topic: str, pace: float = 0.0, max_gap: float = 60.0) -> dict:
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                         "acks": "all", "linger.ms": 5})
    failed = []
    skipped = 0.0
    start = time.perf_counter()
    prev = ticks[0]["time"] if ticks else None
    for t in ticks:
        if pace > 0:
            gap = (t["time"] - prev).total_seconds()
            if gap > max_gap:
                skipped += gap
            prev = t["time"]
            due = start + ((t["time"] - ticks[0]["time"]).total_seconds() - skipped) / pace
            wait = due - time.perf_counter()
            if wait > 0:
                time.sleep(wait)
        producer.produce(topic, key=t["symbol"], value=encode(t),
                         on_delivery=lambda err, msg: err and failed.append(err))
        producer.poll(0)
    wall = time.perf_counter() - start
    producer.flush(30)
    market = (ticks[-1]["time"] - ticks[0]["time"]).total_seconds() - skipped if ticks else 0.0
    return {"sent": len(ticks), "failed": len(failed), "market_s": market, "wall_s": wall,
            "pace": market / wall if pace > 0 and wall > 0 else None, "gaps_skipped_s": skipped}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--topic", default="ticks")
    ap.add_argument("--symbol", default="BTC-USD")
    ap.add_argument("--pace", type=float, default=100.0, help="N x market speed; 0 = as fast as possible")
    ap.add_argument("--max-gap", type=float, default=60.0, help="skip silences longer than this (market seconds)")
    args = ap.parse_args()

    ticks = load(args.symbol)
    if not ticks:
        sys.exit(f"no {args.symbol} ticks in the lake; run pipeline/compact_ticks.py first")
    r = publish(ticks, args.bootstrap, args.topic, args.pace, args.max_gap)
    print(f"sent {r['sent']} ticks to {args.topic}, {r['failed']} failed")
    if r["pace"]:
        print(f"pace {r['pace']:.1f}x (asked {args.pace:g}x): {r['market_s']:.0f}s of market in {r['wall_s']:.1f}s, "
              f"{r['gaps_skipped_s']:.0f}s of silence between sessions skipped")


if __name__ == "__main__":
    main()
