"""Streamed one-minute bars must equal the batch answer, including across a crash.

The batch answer is DuckDB aggregating the same ticks in one query. The broker
tests need Redpanda (docker compose -f docker/compose.yml up -d) and are skipped
without it; the bar builder is checked against DuckDB with no broker at all.
"""

import sys
import uuid
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "streaming"))
from bars import BarBuilder  # noqa: E402
from test_quality import clean_ticks  # noqa: E402

BOOTSTRAP = "localhost:19092"
COLS = ["symbol", "minute", "open", "high", "low", "close", "volume", "trades"]


def batch_bars(ticks) -> list[tuple]:
    con = duckdb.connect()
    con.register("t", ticks)
    return con.execute("""
        SELECT product_id, date_trunc('minute', recv_time), first(price ORDER BY sequence), max(price),
               min(price), last(price ORDER BY sequence), sum(last_size), count(*)::INTEGER
        FROM t GROUP BY 1, 2 ORDER BY 1, 2""").fetchall()


def rounded(rows) -> list[tuple]:
    return [tuple(round(v, 9) if isinstance(v, float) else v for v in r) for r in rows]


def test_builder_matches_batch_aggregation():
    ticks = clean_ticks(200)                         # 2 s apart: seven minutes of bars
    b = BarBuilder()
    out = []
    for t in ticks.itertuples():
        out += b.add(t.product_id, t.recv_time.to_pydatetime(), t.price, t.last_size)
    out += b.flush()
    streamed = [(x.symbol, x.minute, x.open, x.high, x.low, x.close, x.volume, x.trades) for x in out]
    assert rounded(streamed) == rounded(batch_bars(ticks))
    assert len(streamed) == 7 and b.late == 0


def test_tick_for_an_emitted_minute_is_dropped_not_merged():
    ticks = clean_ticks(200)
    b = BarBuilder()
    for t in ticks.itertuples():
        b.add(t.product_id, t.recv_time.to_pydatetime(), t.price, t.last_size)
    first = ticks.iloc[0]
    assert b.add("BTC-USD", first.recv_time.to_pydatetime(), 1.0, 1.0) == []
    assert b.late == 1


# ---- with a broker -------------------------------------------------------

def broker_up() -> bool:
    try:
        from confluent_kafka.admin import AdminClient
        AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=3)
        return True
    except Exception:
        return False


needs_broker = pytest.mark.skipif(not broker_up(), reason="Redpanda not running on localhost:19092")


def as_messages(ticks) -> list[dict]:
    return [{"symbol": t.product_id, "sequence": int(t.sequence), "trade_id": int(t.trade_id),
             "time": t.time.to_pydatetime(), "recv_time": t.recv_time.to_pydatetime(),
             "price": float(t.price), "last_size": float(t.last_size), "side": t.side}
            for t in ticks.itertuples()]


def table(db: Path) -> list[tuple]:
    return duckdb.connect(str(db)).execute(f"SELECT {', '.join(COLS)} FROM bars_1m ORDER BY 1, 2").fetchall()


@needs_broker
def test_stream_through_the_broker_equals_batch(tmp_path):
    from consumer import run
    from producer import publish
    topic = f"ticks-{uuid.uuid4().hex[:8]}"
    ticks = clean_ticks(200)
    assert publish(as_messages(ticks), BOOTSTRAP, topic)["failed"] == 0
    r = run(BOOTSTRAP, topic, group=f"g-{topic}", db=tmp_path / "s.duckdb", exit_when_idle=3)
    assert r["messages"] == 200 and r["late"] == 0
    assert rounded(table(tmp_path / "s.duckdb")) == rounded(batch_bars(ticks))


@needs_broker
def test_consumer_crash_mid_minute_still_ends_on_the_batch_answer(tmp_path):
    from consumer import run
    from producer import publish
    topic = f"ticks-{uuid.uuid4().hex[:8]}"
    ticks = clean_ticks(200)
    publish(as_messages(ticks), BOOTSTRAP, topic)
    db, group = tmp_path / "s.duckdb", f"g-{topic}"

    # 95 ticks in, the consumer dies without flushing: minute 3 is half-built,
    # unwritten, and must not be skipped past by the committed offset.
    first = run(BOOTSTRAP, topic, group, db, max_messages=95, flush_on_exit=False)
    assert first["messages"] == 95
    assert len(table(db)) < 7

    second = run(BOOTSTRAP, topic, group, db, exit_when_idle=3)
    assert second["messages"] < 200                 # resumed, not replayed from zero
    assert rounded(table(db)) == rounded(batch_bars(ticks))
