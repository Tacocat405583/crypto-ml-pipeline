-- The REST backfill written by src/tracker/candles.py, one row per hour.
-- Paths are relative to the repo root: run from there.
SELECT * FROM read_json_auto('src/tracker/candles_1h.jsonl');
