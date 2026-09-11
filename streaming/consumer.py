"""Consume ticks from Kafka and write one-minute bars into DuckDB.

    uv run python streaming/consumer.py --exit-when-idle 5

At-least-once delivery, exactly-once results. Two rules make that hold:

  - The committed offset never passes the first tick of a bar that is still
    open. A consumer that dies mid-minute restarts at the start of that
    minute's ticks and rebuilds the bar, rather than resuming past the ticks
    the unwritten bar needed.
  - Every bar is an upsert keyed on (symbol, minute). Whatever a restart
    re-reads and re-emits overwrites the same row with the same values.

So a crash, a restart, or a redelivered batch all end on the same table.
"""

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

import duckdb
from confluent_kafka import Consumer, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bars import Bar, BarBuilder  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "stream.duckdb"

DDL = """CREATE TABLE IF NOT EXISTS bars_1m (
    symbol VARCHAR, minute TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    volume DOUBLE, trades INTEGER, PRIMARY KEY (symbol, minute))"""


def write(con, bars: list[Bar]) -> None:
    if bars:
        con.executemany("INSERT OR REPLACE INTO bars_1m VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [(b.symbol, b.minute, b.open, b.high, b.low, b.close, b.volume, b.trades) for b in bars])


def run(bootstrap: str, topic: str, group: str, db: Path = DB, exit_when_idle: float | None = None,
        max_messages: int | None = None, flush_on_exit: bool = True) -> dict:
    """Consume until idle for exit_when_idle seconds, or max_messages have been read.

    flush_on_exit=False leaves open bars unwritten and uncommitted -- which is
    exactly the state a crash leaves behind, and what the restart test uses.
    """
    con = duckdb.connect(str(db))
    con.execute(DDL)
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group,
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe([topic])

    builder = BarBuilder()
    open_from: dict[int, dict[str, int]] = {}    # partition -> symbol -> first offset of its open bar
    next_offset: dict[int, int] = {}             # partition -> offset after the last one read
    seen = written = 0
    idle_since = time.monotonic()

    def commit() -> None:
        tps = [TopicPartition(topic, p, min(open_from.get(p, {}).values(), default=nxt))
               for p, nxt in next_offset.items()]
        if tps:
            consumer.commit(offsets=tps, asynchronous=False)

    try:
        while max_messages is None or seen < max_messages:
            msgs = consumer.consume(num_messages=500, timeout=0.5)
            msgs = [m for m in msgs if not m.error()]
            if not msgs:
                if exit_when_idle is not None and time.monotonic() - idle_since > exit_when_idle:
                    break
                continue
            idle_since = time.monotonic()

            done: list[Bar] = []
            for m in msgs[: None if max_messages is None else max_messages - seen]:
                t = json.loads(m.value())
                recv = dt.datetime.fromisoformat(t["recv_time"].rstrip("Z"))
                emitted = builder.add(t["symbol"], recv, t["price"], t["last_size"])
                symbols = open_from.setdefault(m.partition(), {})
                for b in emitted:
                    symbols.pop(b.symbol, None)
                if t["symbol"] in builder.open_bars and t["symbol"] not in symbols:
                    symbols[t["symbol"]] = m.offset()
                next_offset[m.partition()] = m.offset() + 1
                done += emitted
                seen += 1

            write(con, done)           # write first, then commit: never the other way round
            written += len(done)
            commit()

        if flush_on_exit:
            done = builder.flush()
            write(con, done)
            written += len(done)
            open_from.clear()
            commit()
    finally:
        consumer.close()
        con.close()
    return {"messages": seen, "bars_written": written, "late": builder.late}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--topic", default="ticks")
    ap.add_argument("--group", default="bars-1m")
    ap.add_argument("--exit-when-idle", type=float, default=None, help="stop after N idle seconds")
    args = ap.parse_args()
    r = run(args.bootstrap, args.topic, args.group, exit_when_idle=args.exit_when_idle)
    print(f"read {r['messages']} ticks, wrote {r['bars_written']} one-minute bars, {r['late']} late ticks dropped")


if __name__ == "__main__":
    main()
