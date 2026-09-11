"""Data-quality checks for ticks and hourly bars.

Every check is one SQL query over the data and returns the rows it flags, so a
failure points at records rather than at a count -- and so a benchmark can ask
whether a specific injected error was caught. Four kinds, matching what goes
wrong in this pipeline:

  schema      the columns, types and non-null fields the contracts promise
  uniqueness  a trade or an hour appearing twice, or a sequence going backwards:
              what a replay into the raw zone, or a UNION over an overlap, produces
  range       values that cannot be real -- non-positive prices, a crossed book,
              OHLC that contradicts itself, clocks a minute apart
  anomaly     values that could be real but almost never are -- a print far from
              the book, a hole in the series, a bar that disagrees with its
              neighbour

schema, uniqueness and range findings are errors and block publishing; anomaly
findings are warnings. Thresholds for the anomaly and clock checks were set from
clean data -- the real captures and the year of candles, with margin -- and never
from the injected errors used to measure them:

  price vs quoted book     clean max 0.019%    flag at 0.25%
  price vs recent median   clean max 0.034%    flag at 0.5%   (within an hour)
  feed silence             clean max 7.7 s     flag at 60 s
  exchange vs our clock    clean max 1.4 s     flag at 60 s
  bar open vs prev close   clean max 0.17%     flag at 1%
  hourly close-to-close    clean max 4.9%      flag at 8%
  hourly high/low range    clean max 7.3%      flag at 12%
  volume vs 24h median     clean max 38.7x     flag at 50x
"""

from dataclasses import dataclass

import duckdb
import numpy as np
import pandas as pd

TICK_SCHEMA = {
    "sequence": "BIGINT", "product_id": "VARCHAR", "price": "DOUBLE", "last_size": "DOUBLE",
    "side": "VARCHAR", "best_bid": "DOUBLE", "best_bid_size": "DOUBLE", "best_ask": "DOUBLE",
    "best_ask_size": "DOUBLE", "time": "TIMESTAMP", "trade_id": "BIGINT", "recv_time": "TIMESTAMP",
}
BAR_SCHEMA = {
    "time": "TIMESTAMP", "symbol": "VARCHAR", "open": "DOUBLE", "high": "DOUBLE",
    "low": "DOUBLE", "close": "DOUBLE", "volume": "DOUBLE",
}

ERROR_KINDS = ("schema", "uniqueness", "range")


@dataclass(frozen=True)
class Finding:
    check: str
    kind: str                 # schema | uniqueness | range | anomaly
    rows: tuple[int, ...]     # positions in the input frame; empty for table-level
    detail: str

    @property
    def is_error(self) -> bool:
        return self.kind in ERROR_KINDS


def _family(duck_type: str) -> str:
    # pandas datetime64[ns] arrives as TIMESTAMP_NS and a tz-aware one as
    # TIMESTAMP WITH TIME ZONE; all of them satisfy a TIMESTAMP contract.
    t = duck_type.upper()
    if t.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    return {"INTEGER": "BIGINT", "FLOAT": "DOUBLE"}.get(t, t)


def _schema(con, name: str, contract: dict[str, str]) -> tuple[list[Finding], set[str]]:
    """Column-level findings, plus the set of columns safe to query."""
    actual = {c: _family(t) for c, t in con.execute(f"SELECT column_name, column_type FROM (DESCRIBE {name})").fetchall()}
    findings, ok = [], set()
    for col, want in contract.items():
        if col not in actual:
            findings.append(Finding(f"{name}.schema", "schema", (), f"missing column {col}"))
        elif actual[col] != want:
            findings.append(Finding(f"{name}.schema", "schema", (), f"{col} is {actual[col]}, contract says {want}"))
        else:
            ok.add(col)
    return findings, ok


def _not_null(contract: dict[str, str], ok: set[str]) -> str:
    parts = []
    for col in ok:
        parts.append(f"{col} IS NULL" + (f" OR isnan({col})" if contract[col] == "DOUBLE" else ""))
    return " OR ".join(parts) or "false"


def _run(con, name: str, contract: dict[str, str], checks) -> list[Finding]:
    findings, ok = _schema(con, name, contract)
    row_checks = [
        ("not_null", "schema", set(), f"SELECT _row FROM {name} WHERE {_not_null(contract, ok)}",
         "null or NaN in a required field"),
        *checks,
    ]
    for check, kind, needs, sql, detail in row_checks:
        # A check only runs when every column it reads has the contracted type:
        # comparing a price that arrived as text would error, or worse, compare
        # strings. The schema finding already reports that column.
        if not needs <= ok:
            continue
        rows = tuple(sorted({r for (r,) in con.execute(sql).fetchall()}))
        if rows:
            findings.append(Finding(f"{name}.{check}", kind, rows, detail))
    return findings


def _frame(con, name: str, df: pd.DataFrame) -> None:
    df = df.reset_index(drop=True).copy()
    df["_row"] = np.arange(len(df))
    con.register(name, df)


def check_ticks(df: pd.DataFrame) -> list[Finding]:
    con = duckdb.connect()
    _frame(con, "ticks", df)
    hour = "PARTITION BY product_id, date_trunc('hour', recv_time) ORDER BY _row"
    checks = [
        ("unique_trade_id", "uniqueness", {"product_id", "trade_id"},
         "SELECT _row FROM ticks QUALIFY count(*) OVER (PARTITION BY product_id, trade_id) > 1",
         "trade_id appears more than once"),
        ("sequence_increasing", "uniqueness", {"product_id", "sequence"},
         """SELECT _row FROM (SELECT _row, sequence,
                   lag(sequence) OVER (PARTITION BY product_id ORDER BY _row) AS prev FROM ticks)
            WHERE sequence <= prev""",
         "sequence did not increase: a duplicate or a replayed frame"),
        ("positive", "range", {"price", "last_size", "best_bid", "best_ask", "best_bid_size", "best_ask_size"},
         """SELECT _row FROM ticks WHERE price <= 0 OR last_size <= 0 OR best_bid <= 0 OR best_ask <= 0
                                    OR best_bid_size < 0 OR best_ask_size < 0""",
         "a price or size that cannot be real"),
        ("book_not_crossed", "range", {"best_bid", "best_ask"},
         "SELECT _row FROM ticks WHERE best_bid > best_ask",
         "best bid above best ask"),
        ("side_valid", "range", {"side"},
         "SELECT _row FROM ticks WHERE side NOT IN ('buy', 'sell')",
         "side is not buy or sell"),
        ("clock_consistent", "range", {"recv_time", "time"},
         "SELECT _row FROM ticks WHERE abs(epoch(recv_time - time)) > 60",
         "exchange time and receive time more than 60 s apart"),
        ("price_near_book", "anomaly", {"price", "best_bid", "best_ask"},
         """SELECT _row FROM ticks WHERE price > 0 AND best_bid > 0 AND best_ask > 0
                                    AND abs(ln(price / ((best_bid + best_ask) / 2))) > 0.0025""",
         "trade price more than 0.25% from the quoted mid"),
        ("price_jump", "anomaly", {"product_id", "recv_time", "price"},
         f"""SELECT _row FROM (SELECT _row, price,
                    median(price) OVER ({hour} ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS med
                    FROM ticks WHERE price > 0)
             WHERE abs(ln(price / med)) > 0.005""",
         "price more than 0.5% from the median of the previous 20 ticks"),
        ("feed_stall", "anomaly", {"product_id", "recv_time"},
         f"""SELECT _row FROM (SELECT _row, recv_time, lag(recv_time) OVER ({hour}) AS prev FROM ticks)
             WHERE epoch(recv_time - prev) > 60""",
         "more than 60 s without a tick"),
    ]
    return _run(con, "ticks", TICK_SCHEMA, checks)


def check_bars(df: pd.DataFrame) -> list[Finding]:
    con = duckdb.connect()
    _frame(con, "bars", df)
    w = "PARTITION BY symbol ORDER BY time, _row"
    checks = [
        ("unique_hour", "uniqueness", {"symbol", "time"},
         "SELECT _row FROM bars QUALIFY count(*) OVER (PARTITION BY symbol, time) > 1",
         "the same symbol and hour appear more than once"),
        ("ohlc_consistent", "range", {"open", "high", "low", "close"},
         "SELECT _row FROM bars WHERE low > least(open, close) OR high < greatest(open, close) OR low > high",
         "high or low does not contain open and close"),
        ("positive", "range", {"open", "high", "low", "close", "volume"},
         "SELECT _row FROM bars WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 OR volume <= 0",
         "a price or volume that cannot be real"),
        ("hour_aligned", "range", {"time"},
         "SELECT _row FROM bars WHERE time <> date_trunc('hour', time)",
         "bar time is not on the hour"),
        ("continuous", "anomaly", {"symbol", "time"},
         f"""SELECT _row FROM (SELECT _row, time, lag(time) OVER ({w}) AS prev FROM bars)
             WHERE time - prev > INTERVAL 1 HOUR""",
         "one or more missing hours before this bar"),
        ("open_matches_prev_close", "anomaly", {"symbol", "time", "open", "close"},
         # Both bars of a disagreeing pair are suspects: the error could be in
         # either the previous close or this open.
         f"""SELECT unnest([_row, prev_row]) FROM (
                 SELECT _row, open, lag(close) OVER ({w}) AS pc, lag(_row) OVER ({w}) AS prev_row
                 FROM bars WHERE open > 0 AND close > 0)
             WHERE abs(ln(open / pc)) > 0.01""",
         "open more than 1% from the previous bar's close"),
        ("hourly_move", "anomaly", {"symbol", "time", "close"},
         f"""SELECT _row FROM (SELECT _row, close, lag(close) OVER ({w}) AS pc FROM bars WHERE close > 0)
             WHERE abs(ln(close / pc)) > 0.08""",
         "close moved more than 8% in an hour"),
        ("range_width", "anomaly", {"high", "low"},
         "SELECT _row FROM bars WHERE high > 0 AND low > 0 AND ln(high / low) > 0.12",
         "high/low range wider than 12%"),
        ("volume_spike", "anomaly", {"symbol", "time", "volume"},
         f"""SELECT _row FROM (SELECT _row, volume,
                    median(volume) OVER ({w} ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS med FROM bars)
             WHERE med > 0 AND volume / med > 50""",
         "volume more than 50x the trailing 24-hour median"),
    ]
    return _run(con, "bars", BAR_SCHEMA, checks)


def flagged_rows(findings: list[Finding]) -> set[int]:
    return {r for f in findings for r in f.rows}


def summarize(findings: list[Finding]) -> str:
    if not findings:
        return "  clean"
    return "\n".join(
        f"  {'ERROR' if f.is_error else 'warn '} {f.check:<32} {len(f.rows) or '':>6}  {f.detail}"
        for f in findings)
