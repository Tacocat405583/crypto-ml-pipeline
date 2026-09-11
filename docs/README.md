# crypto-ml-pipeline

A market-data pipeline for Coinbase BTC-USD: a multithreaded C++20 feed handler, a
partitioned Parquet lake with quality gates, a DuckDB warehouse, a FastAPI query service,
a Kafka (Redpanda) streaming path, and a walk-forward volatility forecast.

Every number below was measured in this repo, and each one sits next to the control that
makes it mean something. A zero-loss result is only evidence if the same load loses data
under a policy that allows it. A quiet race detector is only evidence if it catches a
planted race.

```
 Coinbase WSS ──▶ feed_handler (C++20) ──▶ data/raw/ticks/…/ticks.jsonl      landing zone
                  source → queue/ring → parser pool → reorder → writer       (crash-safe JSONL)
                                                        │
                        pipeline/compact_ticks.py ◀─────┘  quality-gated, closed hours only
                                     │
                                     ▼
                  data/lake/ticks/symbol=/dt=/hour=/ticks.parquet            tick lake
                                     │                           │
 Coinbase REST ──▶ candles_1h.jsonl  │                           └──▶ streaming/producer.py (100×)
                        │            │                                      │ Redpanda topic
                        ▼            ▼                                      ▼
            pipeline/sql/*.sql ──▶ data/warehouse.duckdb (views)   streaming/consumer.py
                        │                                            → one-minute bars in DuckDB
            build_warehouse.py ── quality gate ──▶ data/warehouse/bars/ (Parquet)
                                                        │
                            ┌───────────────────────────┼──────────────────────────┐
                            ▼                           ▼                          ▼
                 notebooks/volatility.py        services/api (FastAPI)     notebooks/main.py
                 walk-forward forecast          /bars /ticks /quality      the returns model
                                                /forecast /health
```

## Results

| Claim | Measured | How, and the control |
|---|---|---|
| Lock-free SPSC ring, parser pool, cache-line padding, benchmarked against Python | best C++ 149,854 frames/s vs Python 135,526 | 129,601 recorded frames replayed unpaced. The honest finding: `nlohmann::json` is the bottleneck, not the queue ([feed-handler README](../services/feed-handler/README.md#benchmarks)) |
| Race-free under ThreadSanitizer | 6 of 6 load tests clean | Linux container, `-fsanitize=thread`, halt on first report. A planted race (`tsan/canary.cpp`) runs first and must be caught, or the run is rejected |
| Zero data loss under backpressure | 200,000 of 200,000 written, 0 dropped | 16-slot queue, 4 parsers, unpaced. **Control:** the same load under `--overflow drop` sheds 186,385 (93%) |
| Zero data loss on graceful shutdown | 29,792 read → 29,792 written | Paced replay interrupted at 1.5 s. Every accepted frame drained, exit 0, every file ends on a complete record |
| 100× real-time replay | 99.89× (asked 100×) | 300 s of market data in 3.003 s, scheduled from exchange timestamps, worst frame 28 ms late (Windows timer granularity) |
| Quality checks catch injected errors | **873 of 880 (99.2%)** | 22 error classes × 40 trials in real data; one error per trial, credited only if the exact row is flagged. **Control:** a no-op injection is caught 0 of 200 times |
| Volatility forecast beats persistence | **21.6% lower MAE** (20.0% vs the 24-hour mean) | Walk-forward, 11 weekly folds, gradient boosting chosen in advance. It wins all 11 folds. **Control:** trained on shuffled labels, it does 14% *worse* than the baseline |
| Streaming results survive a consumer crash | streamed bars == batch bars, crash included | The consumer is killed mid-minute and restarted. **Control:** committing the last offset read instead loses 5 of 30 trades in that bar |

`uv run pytest` — 61 tests, about 30 s. The broker tests need `docker compose -f docker/compose.yml up -d`.

## What is where

| Path | What |
|---|---|
| `services/feed-handler/` | C++20 ingestion: WebSocket, bounded queue and SPSC ring, parser pool, backpressure, graceful shutdown, `--replay`/`--pace`, TSan image |
| `pipeline/sql/` | DuckDB models: `raw_ticks`, `raw_candles`, `tick_bars` (ticks → hourly OHLCV), `bars` |
| `pipeline/build_warehouse.py` | Builds the views in dependency order, quality-gates `bars`, exports it to Parquet |
| `pipeline/compact_ticks.py` | Publishes closed tick hours from JSONL to the Parquet lake, holding back any hour with an error |
| `pipeline/quality.py` | Schema, uniqueness, range and anomaly checks; thresholds set from clean data only |
| `pipeline/quality_benchmark.py` | The injected-error benchmark behind the 99.2% |
| `services/api/` | FastAPI: keyset pagination, per-client rate limit, UTC everywhere, OpenAPI docs at `/docs` |
| `streaming/` | Redpanda producer (idempotent, paced) and consumer (open-bar-aware commits, upserts) |
| `notebooks/` | Modelling: `volatility.py` (the forecast), `main.py` (the returns model, kept as a negative result) |
| `tests/` | Everything above, as black-box and unit tests |

## Run it

```sh
uv sync                                            # build the C++: services/feed-handler/README.md
./services/feed-handler/build/feed_handler.exe --record data/recordings/frames.jsonl   # live capture
uv run python pipeline/compact_ticks.py            # closed hours → Parquet lake
uv run python pipeline/build_warehouse.py          # views, quality gate, bars → Parquet
uv run python notebooks/volatility.py              # walk-forward forecast + next hour
uv run uvicorn app:app --app-dir services/api      # http://localhost:8000/docs
docker compose -f docker/compose.yml up -d         # Redpanda
uv run python streaming/producer.py --pace 100
uv run python streaming/consumer.py --exit-when-idle 5
```

## Limits, stated plainly

- **Returns are not forecastable here.** The first model targeted next-hour returns, and none
  of its variants beat "no change" (`notebooks/main.py`). Volatility is the target that
  works; the returns model stays in the repo as the negative result it is.
- **Price errors under about 1% on hourly bars can't be told from real moves.** That is
  the one class the checks miss: 82.5% of price errors on bars are caught, 100% on ticks,
  where the quoted book gives a reference point.
- **ThreadSanitizer covers the pipeline's threads, driven by replay.** The live socket read is
  single-threaded synchronous I/O and isn't exercised under TSan.
- **Kafka isn't justified by the volume.** One symbol at a few ticks a second doesn't need a
  broker. It's here to show the streaming semantics — per-key ordering, offsets, restart
  behaviour — not because throughput demanded it.
- **Not yet:** a 72-hour soak (the longest unattended run is about 96 minutes), and an NTP
  check. Ingest latency has a negative minimum, so the local clock trails Coinbase's.
