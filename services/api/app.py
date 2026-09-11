"""Read-only query API over the warehouse and the tick lake.

    uv run uvicorn app:app --app-dir services/api       # then open /docs

It reads Parquet only -- data/warehouse/bars (the hourly bars the model trains
on) and data/lake/ticks (closed hours of ticks that passed quality checks) -- so
it never contends with the feed handler for the raw zone and never takes the
single-writer lock on data/warehouse.duckdb.

Pagination is keyset, not offset: each page returns a cursor, the last key it
contained, and the next page starts strictly after it. Offsets skip or repeat
rows when data lands between requests; a key cannot.

Times are UTC. Inputs with an offset are converted before they reach DuckDB,
because comparing a tz-aware parameter with the plain TIMESTAMP columns makes
DuckDB apply the session time zone -- on this machine, a silent 7-hour shift.
Outputs carry a Z.
"""

import datetime as dt
import sys
import threading
import time
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "pipeline"))
import quality  # noqa: E402

SYMBOL = r"^[A-Z0-9]{2,10}-[A-Z0-9]{2,10}$"


class RateLimiter:
    """Token bucket per client: `rate` requests a second, bursts up to `burst`.

    In-process and in-memory, which matches the scale: one process, one machine.
    Behind several workers each would keep its own buckets, and the limit would
    need shared state."""

    def __init__(self, rate: float, burst: int):
        self.rate, self.burst = rate, burst
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = threading.Lock()

    def wait(self, key: str) -> float:
        """0 if the request may proceed, else seconds until it could."""
        now = time.monotonic()
        with self._lock:
            tokens, last = self._buckets.get(key, (float(self.burst), now))
            tokens = min(self.burst, tokens + (now - last) * self.rate)
            if tokens >= 1:
                self._buckets[key] = (tokens - 1, now)
                return 0.0
            self._buckets[key] = (tokens, now)
            return (1 - tokens) / self.rate


def utc(t: dt.datetime | None) -> dt.datetime | None:
    if t is None or t.tzinfo is None:
        return t
    return t.astimezone(dt.timezone.utc).replace(tzinfo=None)


def create_app(bars_dir: Path = ROOT / "data" / "warehouse" / "bars",
               ticks_dir: Path = ROOT / "data" / "lake" / "ticks",
               rate: float = 10.0, burst: int = 20) -> FastAPI:
    app = FastAPI(title="crypto-ml-pipeline", version="1.0",
                  description="Hourly OHLCV bars and trade ticks from the Coinbase feed.")
    limiter = RateLimiter(rate, burst)

    @app.middleware("http")
    async def rate_limit(request: Request, call_next):
        wait = limiter.wait(request.client.host if request.client else "unknown")
        if wait:
            return JSONResponse({"detail": "rate limit exceeded"}, status_code=429,
                                headers={"Retry-After": str(max(1, round(wait)))})
        return await call_next(request)

    def source(root: Path, what: str) -> str:
        if not any(root.rglob("*.parquet")):
            raise HTTPException(503, f"no {what} published yet under {root.relative_to(ROOT).as_posix()}"
                                if root.is_relative_to(ROOT) else f"no {what} published yet")
        return f"read_parquet('{root.as_posix()}/**/*.parquet', hive_partitioning = true)"

    def rows(sql: str, params: list) -> list[dict]:
        # One in-memory connection per request: a DuckDB connection is not safe
        # to share across the threads FastAPI runs sync endpoints on.
        con = duckdb.connect()
        try:
            cur = con.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [{c: (v.isoformat() + "Z" if isinstance(v, dt.datetime) else v) for c, v in zip(cols, r)}
                    for r in cur.fetchall()]
        finally:
            con.close()

    def page(data: list[dict], limit: int, key: str) -> dict:
        more = len(data) > limit
        data = data[:limit]
        return {"count": len(data), "data": data, "next_cursor": data[-1][key] if more else None}

    @app.get("/health")
    def health():
        bar_files = list(bars_dir.rglob("*.parquet"))
        latest = rows(f"SELECT max(time) AS t FROM {source(bars_dir, 'bars')}", [])[0]["t"] if bar_files else None
        return {"status": "ok" if bar_files else "degraded", "latest_bar": latest,
                "tick_hours_published": len(list(ticks_dir.rglob("*.parquet")))}

    @app.get("/symbols")
    def symbols():
        return rows(f"""SELECT symbol, count(*) AS bars, min(time) AS first_bar, max(time) AS last_bar
                        FROM {source(bars_dir, 'bars')} GROUP BY symbol ORDER BY symbol""", [])

    @app.get("/bars")
    def bars(symbol: str = Query(..., pattern=SYMBOL),
             start: dt.datetime | None = Query(None, description="inclusive, UTC"),
             end: dt.datetime | None = Query(None, description="exclusive, UTC"),
             limit: int = Query(500, ge=1, le=1000),
             cursor: dt.datetime | None = Query(None, description="next_cursor from the previous page")):
        start, end, cursor = utc(start), utc(end), utc(cursor)
        if start and end and start >= end:
            raise HTTPException(422, "start must be before end")
        data = rows(f"""SELECT time, symbol, open, high, low, close, volume FROM {source(bars_dir, 'bars')}
                        WHERE symbol = ? AND (?::TIMESTAMP IS NULL OR time >= ?)
                          AND (?::TIMESTAMP IS NULL OR time < ?) AND (?::TIMESTAMP IS NULL OR time > ?)
                        ORDER BY time LIMIT ?""",
                    [symbol, start, start, end, end, cursor, cursor, limit + 1])
        return {"symbol": symbol, **page(data, limit, "time")}

    @app.get("/ticks")
    def ticks(symbol: str = Query(..., pattern=SYMBOL),
              start: dt.datetime | None = Query(None, description="inclusive, UTC, on recv_time"),
              end: dt.datetime | None = Query(None, description="exclusive, UTC, on recv_time"),
              limit: int = Query(500, ge=1, le=1000),
              cursor: int | None = Query(None, description="next_cursor (a sequence number)")):
        start, end = utc(start), utc(end)
        if start and end and start >= end:
            raise HTTPException(422, "start must be before end")
        data = rows(f"""SELECT sequence, trade_id, time, recv_time, price, last_size, side,
                               best_bid, best_bid_size, best_ask, best_ask_size
                        FROM {source(ticks_dir, 'ticks')}
                        WHERE symbol = ? AND (?::TIMESTAMP IS NULL OR recv_time >= ?)
                          AND (?::TIMESTAMP IS NULL OR recv_time < ?) AND (?::BIGINT IS NULL OR sequence > ?)
                        ORDER BY sequence LIMIT ?""",
                    [symbol, start, start, end, end, cursor, cursor, limit + 1])
        return {"symbol": symbol, **page(data, limit, "sequence")}

    @app.get("/quality")
    def quality_report():
        con = duckdb.connect()
        try:
            df = con.sql(f"SELECT time, symbol, open, high, low, close, volume FROM {source(bars_dir, 'bars')} "
                         "ORDER BY symbol, time").df()
        finally:
            con.close()
        findings = quality.check_bars(df)
        return {
            "rows_checked": len(df),
            "errors": sum(f.is_error for f in findings),
            "warnings": sum(not f.is_error for f in findings),
            "findings": [{"check": f.check, "kind": f.kind, "rows": len(f.rows), "detail": f.detail,
                          "first_times": [df.loc[r, "time"].isoformat() + "Z" for r in f.rows[:10]]}
                         for f in findings],
        }

    return app


app = create_app()
