# architecture/

How the pieces of crypto-ml-pipeline fit together, and where DuckDB plugs into them.

**Interactive diagram:** https://claude.ai/code/artifact/eaccaa07-d15f-4675-a6c1-71f1c4ada9ad

## Read in this order

| File | Answers |
|---|---|
| [data-flow.md](data-flow.md) | What connects to what, and which direction |
| [contracts.md](contracts.md) | The exact row shape at each connection point |
| [duckdb.md](duckdb.md) | Where DuckDB attaches, and what it has to produce |

For the C++ service specifically — how to build it, run it, and what the benchmarks say —
see [`services/feed-handler/README.md`](../../services/feed-handler/README.md).

For *why* things are shaped this way, see [`../PROJECT.md`](../PROJECT.md) (decision log).
For where the project is heading, [`../GoalState.md`](../GoalState.md).

## The whole thing on one screen

```
 feed_handler.exe  --write--> data/raw/ticks/**/ticks.jsonl --+
 C++ - live wss               one row per TRADE               |
                                                              +--> DuckDB --> bars
 candles.py       --write--> src/tracker/candles_1h.jsonl ----+              |
 Python - REST                one row per HOUR                              v
                                                                    notebooks/data.py
                                                                            |
                                                        features.py -> model.py -> main.py
```

Four processes. **None of them call each other** — every connection is a file on disk.
That is the whole architecture, and it is why any single piece can be replaced without
touching the others.

## The three connection points

Numbered because they are the seams DuckDB has to sit between, and because two of the
three are already fixed by code that exists.

1. **`feed_handler` -> raw ticks.** Fixed. Written by the C++ writer, 12 columns.
2. **`candles.py` -> hourly candles.** Fixed. Written by the Python backfill, 7 columns.
3. **`pipeline/sql/` -> `notebooks/data.py`.** *In progress.* Job 1 (`tick_bars.sql`) is
   built and `data/warehouse.duckdb` exists, but `data.py` still reads seam 2 directly with
   pandas, so seam 1 feeds nothing yet. `duckdb.md` has where it stands.

Seams 1 and 2 are contracts you should not change casually — code on both sides depends on
them. Seam 3 has exactly one reader and one line of code, so it is cheap to move.
