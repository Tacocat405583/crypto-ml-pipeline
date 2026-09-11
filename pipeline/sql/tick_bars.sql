-- Job 1: change the grain. One row per TRADE in, one row per HOUR out.
--
-- The output is the same seven columns, in the same order and with the same
-- types, as src/tracker/candles_1h.jsonl. That is what makes Job 2 (merging
-- with the REST candles) a plain UNION.
--
-- GROUP BY 1, 2 rather than names, on purpose. The input already has a column
-- called `time` (Coinbase's exchange clock) and, from the folder names, one
-- called `symbol`. GROUP BY time would bind to the input column and return one
-- row per trade. An ordinal always means "output column N".
--
-- recv_time is our clock, written in UTC by the feed handler, and it is the
-- same clock the writer partitions on, so every tick in hour=07/ lands in the
-- 07:00 bar. DuckDB reads it as a plain TIMESTAMP, so the session time zone
-- does not shift the hour boundaries.
--
-- Paths are relative to the repo root: run from there.
SELECT
    date_trunc('hour', recv_time)   AS time,
    product_id                      AS symbol,
    first(price ORDER BY sequence)  AS open,
    max(price)                      AS high,
    min(price)                      AS low,
    last(price ORDER BY sequence)   AS close,
    sum(last_size)                  AS volume
FROM read_json_auto('data/raw/ticks/**/*.jsonl')
GROUP BY 1, 2
ORDER BY 1, 2;
