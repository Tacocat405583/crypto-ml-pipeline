# Data flow — what connects to what

## The rule

Every connection in this system is a **file on disk**. No process imports, calls, or
sockets to another. `feed_handler` does not know Python exists; `data.py` does not know C++
exists.

That is deliberate, and it has consequences worth knowing:

- Any piece can be replaced without touching the others. Swap the C++ for Rust, or the
  notebooks for a script, and nothing downstream notices.
- Nothing is real-time end to end. The model sees whatever was on disk when it ran.
- **The file format IS the API.** Changing a column name in `ticks.jsonl` is a breaking
  change even though no function signature moved.

## The map

```
   WHO WRITES                WHAT LANDS ON DISK              WHO READS IT
   ----------                ------------------              ------------

 feed_handler.exe  --write--> data/raw/ticks/**/ticks.jsonl --+
 C++ - live wss               grain: one row per TRADE        |
 runs forever                 (irregular, ~10/sec)            |
                                                              +--> DuckDB
 candles.py       --write--> src/tracker/candles_1h.jsonl ----+      |
 Python - REST                grain: one row per HOUR                |
 run by hand                  (regular, 8750 rows)                   | reshapes
                                                                     v
                                                         data/warehouse/bars/*.parquet
                                                         grain: one row per HOUR
                                                                     |
                                                                     v
                                                              notebooks/data.py
                                                                     |
                                                          features.py -> model.py -> main.py
```

## The four processes

| Process | Language | Lifetime | Writes | Reads |
|---|---|---|---|---|
| `services/feed-handler/build/feed_handler.exe` | C++20 | runs forever | `data/raw/ticks/` | Coinbase wss |
| `src/tracker/candles.py` | Python | one-shot, by hand | `src/tracker/candles_1h.jsonl` | Coinbase REST |
| *(DuckDB step)* | SQL | on demand | `data/warehouse/` | both files above |
| `notebooks/main.py` | Python | on demand | nothing (prints) | the warehouse |

`src/tracker/coinbase.py` (spot price poller) is **retired** — see PROJECT.md, 2026-08-22.
`infra/` is parked and wired to nothing.

## Directory layout of the raw zone

```
data/raw/ticks/
  symbol=BTC-USD/
    dt=2026-09-07/
      hour=07/
        ticks.jsonl      <- append-only, rolls at the hour boundary
      hour=08/
        ticks.jsonl
```

Hive-style partitioning: the directory names carry `key=value` pairs. This is not
decoration — it is what lets DuckDB read a year of files with one glob
(`data/raw/ticks/**/*.jsonl`) and, if you want, filter whole directories without opening
them.

The partition is chosen from `recv_time` (our clock), not the exchange's `time`. That
guarantees partitions only ever move forward, so a tick whose exchange timestamp straddles
an hour boundary cannot send the writer back to reopen a file it already closed.

## What is NOT connected yet

`data/raw/ticks/` currently feeds **nothing**. The live C++ service has been running and
writing real trades, and no downstream code reads them. `notebooks/data.py` reaches past it
to `candles_1h.jsonl`.

Closing that is the DuckDB work — see [duckdb.md](duckdb.md).
