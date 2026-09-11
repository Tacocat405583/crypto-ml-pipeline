-- Every tick the feed handler has written, one row per trade.
-- DuckDB adds dt, hour and symbol columns from the Hive-style folder names.
-- Paths are relative to the repo root: run from there.
SELECT * FROM read_json_auto('data/raw/ticks/**/*.jsonl');
