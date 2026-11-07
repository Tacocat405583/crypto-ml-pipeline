# Where DuckDB attaches

## What it is

A library, not a server. `import duckdb` and you are done — no container, no port, no
daemon. Think SQLite, but built for analytics: columnar, reads files directly, fast on
aggregations. It is already a dependency in `pyproject.toml`, so nothing gets installed.

It reads JSONL and Parquet **as tables**, with no schema definition and no load step. That
is the whole reason it fits here: your two producers write files, and DuckDB queries files.

## The one gap it fills

```
 raw ticks ---+
              +--> [ DuckDB ] --> bars --> notebooks/data.py
 candles -----+
```

It sits between the files on disk and `data.py`. Nothing else in the system changes.

## Job 1 — change the grain

This is the one that matters, and the reason a plain `read_json_auto` is not enough.

```
   ticks.jsonl                  DuckDB                      bars
   -----------                  ------                      ----
   07:32:03  $79,483  -+
   07:32:04  $79,481   |   date_trunc('hour')
   07:32:04  $79,490   +--> first/max/min/last  -->  07:00  O H L C V
   07:32:11  $79,477   |    sum(last_size)
   ...        ...     -+
   (hundreds of rows)                                  (one row)
```

```sql
SELECT date_trunc('hour', recv_time)   AS time,
       product_id                      AS symbol,
       first(price ORDER BY sequence)  AS open,
       max(price)                      AS high,
       min(price)                      AS low,
       last(price ORDER BY sequence)   AS close,
       sum(last_size)                  AS volume
FROM read_json_auto('data/raw/ticks/**/*.jsonl')
GROUP BY 1, 2
ORDER BY 1, 2
```

**Built** — this is `pipeline/sql/tick_bars.sql`. No `CAST` needed: DuckDB already reads
`recv_time` as a plain `TIMESTAMP`, in UTC.

Note the output columns are deliberately the **same seven** as `candles_1h.jsonl`. That is
what makes Job 2 a one-liner.

`ORDER BY sequence` inside `first`/`last` matters: rows in the file are in arrival order,
which is feed order, but being explicit costs nothing and makes the query correct even if
you later read partitions out of order.

## Job 2 — merge the two sources

Once ticks are hourly bars, both sources have the same shape:

```sql
CREATE OR REPLACE VIEW bars AS
  SELECT * FROM tick_bars
  UNION ALL
  SELECT time, symbol, open, high, low, close, volume
  FROM read_json_auto('src/tracker/candles_1h.jsonl')
```

**Not a continuous history — not yet.** The backfill ends 2026-08-11 08:00 and the feed
starts 2026-09-07 07:00, so this union has a 27-day hole in it. `features.py` counts rows, not
time, so the row after the hole is scored as one hour's return: +23.85%. Run through
`main.py`, that single row moves RMSE from 0.0045 to 0.0072 for every model. Close the hole
(re-run `candles.py`) before this union goes anywhere near the model — which then raises the
overlap question in `contracts.md`, decision 2.

## Job 3 — hand it to the model

```sql
COPY (SELECT * FROM bars ORDER BY time)
  TO 'data/warehouse/bars' (FORMAT PARQUET, PARTITION_BY (symbol), OVERWRITE_OR_IGNORE);
```

Then the connection is made by changing **one line** in `notebooks/data.py`:

```python
# from
df = pd.read_json(config.CANDLES_PATH, lines=True)
# to
df = duckdb.sql("SELECT * FROM 'data/warehouse/bars/**/*.parquet'").df()
```

That single line is the whole integration. Everything downstream — `features.py`,
`model.py`, `main.py` — is untouched, because the frame it receives has the same columns it
always had.

## Suggested order to actually do this

Each step is independently checkable, which is the point — you find out what you got wrong
one step at a time rather than at the end.

**Status 2026-09-11: steps 1 and 2 done.** The original order put the union (Job 2) before
the rewire test, and that order cannot pass — see Job 2 above. The Parquet export is now
split out and done first, on the candles alone.

1. ✓ **Read the raw ticks.** `recv_time` and `time` come back as `TIMESTAMP`, prices as
   `DOUBLE`, and DuckDB adds `dt` / `hour` / `symbol` from the Hive folder names. 879 ticks,
   not 878: the file has no trailing newline, which hides one record from `wc -l`.
2. ✓ **Aggregate to bars.** `pipeline/sql/tick_bars.sql`. One bar at 07:00, matching an
   independent pandas computation exactly.
3. **Export the candles alone to Parquet.** Job 3 with `FROM raw_candles` — no union. **← next**
4. **Repoint `data.py`** at the Parquet. `main.py` must then match
   `data/baseline_metrics_pre_duckdb.txt` exactly: the data has not changed, only its path.
   Do this before re-running `candles.py` — the baseline is tied to today's candle file.
5. **Close the hole and get a real hour.** Re-run `candles.py`; run the feed handler for one
   full, uninterrupted hour.
6. **Compare against the REST candles** on that hour — decision 3 in `contracts.md`. Blocked
   until 5: today no hour is in both sources, and the one tick hour covers 86 seconds.
7. **Union** (Job 2) with a rule for overlapping hours and one for partial-hour bars, then
   re-export.

Step 4 producing *identical* numbers is the test that you wired it up without breaking
anything. New numbers at that point mean something changed that should not have.

## Why not just do this in pandas

You could, and for the current data size it would work. The reasons to prefer DuckDB:

- **The glob.** `read_json_auto('**/*.jsonl')` across hundreds of hour partitions is one
  line; the pandas version is a directory walk and a `concat`.
- **It does not load everything into memory.** Once the live feed has been running for
  months, the tick history will not fit comfortably in a DataFrame. DuckDB streams it.
- **The aggregation is the natural shape for SQL.** `GROUP BY` with `first`/`last` is
  clearer than the pandas `resample().agg()` equivalent, and harder to get subtly wrong.
- **It is the Phase 2 direction.** GoalState names DuckDB as the warehouse, with dbt on top.
  Doing the transform here means the dbt step later is a move, not a rewrite.
