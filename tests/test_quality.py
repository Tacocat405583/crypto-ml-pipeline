"""Each quality check against one corrupted row it must catch.

The frames are synthetic and small so every case is readable: a clean frame
must produce no findings at all, and each corruption must be flagged by the
check written for it, on exactly the row that was corrupted.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))
import quality as q  # noqa: E402

T0 = datetime(2026, 9, 7, 7, 0)


def clean_ticks(n: int = 200) -> pd.DataFrame:
    rng = np.random.default_rng(1)
    price = 79_000 + np.cumsum(rng.normal(0, 2, n))
    recv = [T0 + timedelta(seconds=2 * i) for i in range(n)]
    return pd.DataFrame({
        "sequence": 1_000 + 3 * np.arange(n), "product_id": "BTC-USD", "price": price,
        "last_size": rng.uniform(1e-4, 0.5, n), "side": rng.choice(["buy", "sell"], n),
        "best_bid": price - 0.01, "best_bid_size": 0.1, "best_ask": price + 0.01, "best_ask_size": 0.1,
        "time": [r - timedelta(milliseconds=40) for r in recv], "trade_id": 50_000 + np.arange(n),
        "recv_time": recv,
    })


def clean_bars(n: int = 100) -> pd.DataFrame:
    rng = np.random.default_rng(2)
    close = 79_000 * np.exp(np.cumsum(rng.normal(0, 0.004, n)))
    open_ = np.r_[close[0], close[:-1]]
    return pd.DataFrame({
        "time": [T0 + timedelta(hours=i) for i in range(n)], "symbol": "BTC-USD",
        "open": open_, "high": np.maximum(open_, close) * 1.002, "low": np.minimum(open_, close) * 0.998,
        "close": close, "volume": rng.uniform(20, 200, n),
    })


def test_clean_frames_produce_no_findings():
    assert q.check_ticks(clean_ticks()) == []
    assert q.check_bars(clean_bars()) == []


def _set(df, row, **values):
    df = df.copy()
    for col, v in values.items():
        df[col] = df[col].astype(object) if v is None else df[col]
        df.loc[row, col] = v
    return df


TICK_CASES = [
    ("not_null",            lambda d: _set(d, 50, price=np.nan)),
    ("not_null",            lambda d: _set(d, 50, side=None)),
    ("unique_trade_id",     lambda d: _set(d, 50, trade_id=int(d.loc[10, "trade_id"]))),
    ("sequence_increasing", lambda d: _set(d, 50, sequence=int(d.loc[40, "sequence"]))),
    ("positive",            lambda d: _set(d, 50, price=-1.0)),
    ("positive",            lambda d: _set(d, 50, last_size=0.0)),
    ("book_not_crossed",    lambda d: _set(d, 50, best_bid=d.loc[50, "best_ask"] + 1)),
    ("side_valid",          lambda d: _set(d, 50, side="BUY")),
    ("clock_consistent",    lambda d: _set(d, 50, recv_time=d.loc[50, "recv_time"] + timedelta(hours=2))),
    ("price_near_book",     lambda d: _set(d, 50, price=d.loc[50, "price"] * 1.01)),
    ("price_jump",          lambda d: _set(d, 50, price=d.loc[50, "price"] * 1.01)),
]


@pytest.mark.parametrize("check,corrupt", TICK_CASES, ids=[c for c, _ in TICK_CASES])
def test_tick_check_flags_the_corrupted_row(check, corrupt):
    findings = q.check_ticks(corrupt(clean_ticks()))
    hit = [f for f in findings if f.check == f"ticks.{check}"]
    assert hit and 50 in hit[0].rows, q.summarize(findings)


def test_feed_stall_flags_the_first_tick_after_the_silence():
    d = clean_ticks()
    d = d.drop(index=range(60, 100)).reset_index(drop=True)   # 80 s of silence
    hit = [f for f in q.check_ticks(d) if f.check == "ticks.feed_stall"]
    assert hit and hit[0].rows == (60,)


BAR_CASES = [
    ("not_null",                lambda d: _set(d, 50, close=np.nan)),
    ("unique_hour",             lambda d: _set(d, 50, time=d.loc[49, "time"])),
    ("ohlc_consistent",         lambda d: _set(d, 50, high=d.loc[50, "low"] * 0.99)),
    ("positive",                lambda d: _set(d, 50, volume=-5.0)),
    ("hour_aligned",            lambda d: _set(d, 50, time=d.loc[50, "time"] + timedelta(minutes=17))),
    ("open_matches_prev_close", lambda d: _set(d, 50, open=d.loc[50, "open"] * 1.03,
                                               high=d.loc[50, "high"] * 1.03)),
    ("hourly_move",             lambda d: _set(d, 50, close=d.loc[50, "close"] * 1.1,
                                               high=d.loc[50, "high"] * 1.1)),
    ("range_width",             lambda d: _set(d, 50, high=d.loc[50, "high"] * 1.2)),
    ("volume_spike",            lambda d: _set(d, 50, volume=d.loc[50, "volume"] * 500)),
]


@pytest.mark.parametrize("check,corrupt", BAR_CASES, ids=[c for c, _ in BAR_CASES])
def test_bar_check_flags_the_corrupted_row(check, corrupt):
    findings = q.check_bars(corrupt(clean_bars()))
    hit = [f for f in findings if f.check == f"bars.{check}"]
    assert hit and 50 in hit[0].rows, q.summarize(findings)


def test_missing_hour_is_flagged_on_the_bar_after_it():
    d = clean_bars().drop(index=50).reset_index(drop=True)
    hit = [f for f in q.check_bars(d) if f.check == "bars.continuous"]
    assert hit and hit[0].rows == (50,)


def test_type_drift_is_a_schema_error_and_skips_checks_that_read_the_column():
    d = clean_ticks()
    d["price"] = d["price"].map(lambda p: f"{p:.2f}")          # a price that arrived as text
    findings = q.check_ticks(d)
    assert any(f.check == "ticks.schema" and "price" in f.detail for f in findings)
    assert not any(f.check in ("ticks.positive", "ticks.price_jump") for f in findings)


def test_missing_column_is_a_schema_error():
    findings = q.check_bars(clean_bars().drop(columns=["volume"]))
    assert any(f.check == "bars.schema" and "volume" in f.detail and f.is_error for f in findings)


def test_anomalies_are_warnings_and_everything_else_is_an_error():
    kinds = {f.kind: f.is_error for f in q.check_bars(_set(clean_bars(), 50, volume=-1.0, high=1e9))}
    assert kinds == {"range": True, "anomaly": False}


def test_benchmark_harness_credits_nothing_for_a_no_op():
    # The benchmark's recall is only meaningful if an injection that changes
    # nothing is never scored as caught.
    import quality_benchmark as b
    rng = np.random.default_rng(0)
    noop = {"noop": lambda df, k, rng: (df.copy(), k)}
    for clean, checker in [(clean_ticks(), q.check_ticks), (clean_bars(), q.check_bars)]:
        _, res = b.run_table(clean, checker, noop, 50, rng)
        assert res["noop"][1] == 0
