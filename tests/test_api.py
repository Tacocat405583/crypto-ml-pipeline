"""The query API against small Parquet fixtures laid out like the real zones."""

import sys
import warnings
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services" / "api"))
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from fastapi.testclient import TestClient
import app as api  # noqa: E402
from test_quality import clean_bars, clean_ticks  # noqa: E402


@pytest.fixture
def zones(tmp_path):
    bars, ticks = tmp_path / "bars", tmp_path / "ticks"
    con = duckdb.connect()
    con.register("b", clean_bars(100))
    con.sql(f"COPY (FROM b) TO '{bars.as_posix()}' (FORMAT PARQUET, PARTITION_BY (symbol))")
    hour = ticks / "symbol=BTC-USD" / "dt=2026-09-07" / "hour=07"
    hour.mkdir(parents=True)
    con.register("t", clean_ticks(200))
    con.sql(f"COPY (SELECT * EXCLUDE (product_id), product_id FROM t) TO '{(hour / 'ticks.parquet').as_posix()}' "
            "(FORMAT PARQUET)")
    return bars, ticks


@pytest.fixture
def client(zones):
    return TestClient(api.create_app(*zones, rate=1000, burst=1000))


def walk(client, path, **params):
    """Follow next_cursor to the end; return every row and the number of pages."""
    out, pages, cursor = [], 0, None
    while True:
        r = client.get(path, params={**params, **({"cursor": cursor} if cursor else {})})
        assert r.status_code == 200, r.text
        body = r.json()
        out += body["data"]
        pages += 1
        cursor = body["next_cursor"]
        if cursor is None:
            return out, pages


def test_health_and_symbols(client):
    assert client.get("/health").json()["status"] == "ok"
    (sym,) = client.get("/symbols").json()
    assert sym["symbol"] == "BTC-USD" and sym["bars"] == 100


def test_bar_pages_cover_every_row_once_in_order(client):
    rows, pages = walk(client, "/bars", symbol="BTC-USD", limit=7)
    times = [r["time"] for r in rows]
    assert len(rows) == 100 and pages == 15
    assert times == sorted(times) and len(set(times)) == 100


def test_tick_pages_cover_every_row_once_in_order(client):
    rows, pages = walk(client, "/ticks", symbol="BTC-USD", limit=64)
    seqs = [r["sequence"] for r in rows]
    assert len(rows) == 200 and pages == 4
    assert seqs == sorted(seqs) and len(set(seqs)) == 200


def test_times_with_an_offset_are_read_as_utc(client):
    # 09:00+02:00 is 07:00 UTC, the first bar. Handed to DuckDB tz-aware, it
    # would be shifted by the session time zone instead.
    first = client.get("/bars", params={"symbol": "BTC-USD", "start": "2026-09-07T09:00:00+02:00",
                                        "limit": 1}).json()["data"][0]
    assert first["time"] == "2026-09-07T07:00:00Z"


def test_time_window_is_inclusive_start_exclusive_end(client):
    body = client.get("/bars", params={"symbol": "BTC-USD", "start": "2026-09-07T10:00:00Z",
                                       "end": "2026-09-07T13:00:00Z"}).json()
    assert [r["time"][11:13] for r in body["data"]] == ["10", "11", "12"]


@pytest.mark.parametrize("params", [
    {"symbol": "btc"},                                                 # not SYMBOL-SHAPED
    {"symbol": "BTC-USD", "limit": 0},
    {"symbol": "BTC-USD", "limit": 5000},
    {"symbol": "BTC-USD", "start": "2026-09-08T00:00:00Z", "end": "2026-09-07T00:00:00Z"},
])
def test_bad_requests_are_rejected(client, params):
    assert client.get("/bars", params=params).status_code == 422


def test_quality_reports_a_missing_hour(zones, tmp_path):
    bars = tmp_path / "gappy"
    con = duckdb.connect()
    con.register("b", clean_bars(100).drop(index=50))
    con.sql(f"COPY (FROM b) TO '{bars.as_posix()}' (FORMAT PARQUET, PARTITION_BY (symbol))")
    report = TestClient(api.create_app(bars, zones[1])).get("/quality").json()
    assert report["errors"] == 0 and report["warnings"] == 1
    assert report["findings"][0]["check"] == "bars.continuous"


def test_nothing_published_yet_is_a_503_not_a_crash(tmp_path):
    c = TestClient(api.create_app(tmp_path / "none", tmp_path / "none"))
    assert c.get("/health").json()["status"] == "degraded"
    assert c.get("/bars", params={"symbol": "BTC-USD"}).status_code == 503


def test_rate_limit_answers_429_with_retry_after(zones):
    c = TestClient(api.create_app(*zones, rate=0.5, burst=3))
    codes = [c.get("/health").status_code for _ in range(4)]
    assert codes == [200, 200, 200, 429]
    assert int(c.get("/health").headers["Retry-After"]) >= 1


@pytest.fixture
def forecasts(tmp_path):
    import pandas as pd
    fc = tmp_path / "forecasts"
    fc.mkdir()
    idx = pd.date_range("2026-08-01", periods=4, freq="h", tz="UTC", name="time")   # tz-aware, as pandas writes it
    pd.DataFrame({"target": [1.0, 2.0, 3.0, 4.0], "persistence": [2.0, 2.0, 2.0, 2.0],
                  "mean_24h": [2.5] * 4, "gbm": [1.5, 2.0, 2.5, 3.5], "symbol": "BTC-USD"},
                 index=idx).to_parquet(fc / "vol_walkforward.parquet")
    pd.DataFrame({"as_of": idx[-1:], "for_hour": idx[-1:] + pd.Timedelta(hours=1),
                  "vol_1h_forecast": [0.0017], "vol_1h_last": [0.0007], "symbol": "BTC-USD"}
                 ).to_parquet(fc / "vol_next.parquet")
    return fc


def test_forecast_serves_the_latest_and_its_walk_forward_record(zones, forecasts):
    body = TestClient(api.create_app(*zones, forecasts_dir=forecasts)).get("/forecast", params={"symbol": "BTC-USD"}).json()
    assert body["for_hour"] == "2026-08-01T04:00:00Z"            # UTC, not the session zone
    wf = body["walk_forward"]
    # |gbm - target| = .5, 0, .5, .5 ; |persistence - target| = 1, 0, 1, 2
    assert wf["mae_model"] == pytest.approx(0.375) and wf["mae_persistence"] == pytest.approx(1.0)
    assert wf["vs_persistence"] == pytest.approx(0.625)


def test_forecast_before_any_model_run_is_a_503(zones, tmp_path):
    c = TestClient(api.create_app(*zones, forecasts_dir=tmp_path / "none"))
    assert c.get("/forecast", params={"symbol": "BTC-USD"}).status_code == 503


def test_forecast_history_pages_in_utc(zones, forecasts):
    c = TestClient(api.create_app(*zones, forecasts_dir=forecasts))
    rows, pages = walk(c, "/forecast/history", symbol="BTC-USD", limit=3)
    assert pages == 2 and [r["time"] for r in rows] == [f"2026-08-01T0{h}:00:00Z" for h in range(4)]
    assert rows[1] == {"time": "2026-08-01T01:00:00Z", "actual": 2.0, "forecast": 2.0,
                       "persistence": 2.0, "mean_24h": 2.5}
    # The column is tz-aware and the parameter is not: this must still mean 02:00 UTC.
    later = c.get("/forecast/history", params={"symbol": "BTC-USD", "start": "2026-08-01T02:00:00Z"}).json()
    assert [r["time"][11:13] for r in later["data"]] == ["02", "03"]


def test_health_reports_the_latest_tick(client):
    # clean_ticks: 200 ticks two seconds apart from 07:00:00, so the last is 07:06:38
    assert client.get("/health").json()["latest_tick"] == "2026-09-07T07:06:38Z"
