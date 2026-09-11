"""The tick lake publishes clean closed hours and holds back everything else."""

import datetime as dt
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))
import compact_ticks as c  # noqa: E402
from test_quality import clean_ticks  # noqa: E402

NOW = dt.datetime(2026, 9, 7, 12, 0, tzinfo=dt.timezone.utc)


@pytest.fixture
def zones(tmp_path, monkeypatch):
    monkeypatch.setattr(c, "RAW", tmp_path / "raw")
    monkeypatch.setattr(c, "LAKE", tmp_path / "lake")
    return tmp_path


def land(zones, hour: int, df, stamp: str = "%Y-%m-%dT%H:%M:%S.%fZ") -> Path:
    """Write df as a raw partition, timestamps formatted the way TickWriter does."""
    df = df.copy()
    for col in ("time", "recv_time"):
        df[col] = df[col].dt.strftime(stamp)
    path = zones / "raw" / "symbol=BTC-USD" / "dt=2026-09-07" / f"hour={hour:02d}" / "ticks.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text(df.to_json(orient="records", lines=True))
    return path


def lake_rows(zones) -> int:
    return duckdb.sql(f"SELECT count(*) FROM read_parquet('{(zones / 'lake').as_posix()}/**/*.parquet')").fetchone()[0]


def test_clean_closed_hour_is_published_with_every_row(zones):
    msg = c.compact(land(zones, 7, clean_ticks()), NOW)
    assert msg.startswith("wrote")
    assert lake_rows(zones) == 200


def test_second_run_skips_an_hour_already_published(zones):
    jsonl = land(zones, 7, clean_ticks())
    c.compact(jsonl, NOW)
    assert c.compact(jsonl, NOW).startswith("current")


def test_hour_still_being_written_is_left_alone(zones):
    jsonl = land(zones, 11, clean_ticks())            # 11:00-12:00, now is 12:00
    assert c.compact(jsonl, NOW).startswith("open")
    assert not (zones / "lake").exists()


def test_hour_with_an_error_is_held_back_not_published(zones):
    df = clean_ticks()
    df.loc[50, "trade_id"] = df.loc[10, "trade_id"]  # the same trade written twice
    msg = c.compact(land(zones, 7, df), NOW)
    assert msg.startswith("HELD") and "unique_trade_id" in msg
    assert not list((zones / "lake").rglob("*.parquet"))


def test_hour_whose_timestamps_changed_format_is_held_back(zones):
    # Found by accident: pandas' default ISO format has no Z and reads as text.
    # If the writer's format ever drifted like that, nothing should publish.
    msg = c.compact(land(zones, 7, clean_ticks(), stamp="%d/%m/%Y %H:%M:%S"), NOW)
    assert msg.startswith("HELD") and "recv_time is VARCHAR" in msg


def test_hour_with_only_an_anomaly_is_published_and_reported(zones):
    df = clean_ticks()
    df.loc[50, "price"] *= 1.01                        # odd, but could be real
    msg = c.compact(land(zones, 7, df), NOW)
    assert msg.startswith("wrote") and "price_near_book" in msg
    assert lake_rows(zones) == 200
