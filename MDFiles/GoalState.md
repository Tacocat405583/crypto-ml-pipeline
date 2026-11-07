# crypto-ml-pipeline — target state

Written against the 2026-08-16 audit of `main @ a8ed22f`.

This document describes where the project is going and in what order. It is the reference for
any agent or session working in this repo. Read it before proposing changes.

---

## Purpose of the project

A single, fully worked-out crypto data project that demonstrates:

1. **C++ concurrency** — a real multithreaded service, not C++ bolted on for decoration
2. **Data engineering** — raw zone, warehouse, orchestration, quality checks, backfills
3. **Operational maturity** — health endpoints, metrics, monitoring, graceful shutdown
4. **Applications on top** — a query API and an ML model as consumers, not as the point

The ML model is the *least* important part. It is last on purpose.

---

## Ground rules

These hold for every phase. Violating one is a bug even if the code works.

- **Raw data is immutable.** Once written, a raw record is never edited, pruned, or hand-deleted.
  Corrections happen in downstream layers, never at the source.
- **Every transformation is reproducible from raw.** If a mart is deleted, a backfill rebuilds it
  byte-identically. No step depends on manual intervention.
- **Idempotency everywhere.** Re-running any job over any window produces the same result. Upsert,
  don't append blindly.
- **No secrets in git.** Ever. `.env` files are gitignored without exception; config is loaded from
  environment.
- **Data does not live in `src/`.** Source and data are separate trees.
- **Every dependency in `pyproject.toml` is imported by first-party code.** If nothing imports it,
  it comes out.
- **Failures are loud.** Any error path that exits non-zero must also write a diagnostic. Silent
  failure is the one unacceptable behaviour.

---

## Phase 0 — triage

Nothing else starts until this is done. The current repo has accumulated enough drift that
building on it directly would carry the problems forward.

### 0.1 Secrets

- [ ] Determine whether the values in `infra/.env` were ever real credentials. If yes, rotate them.
- [ ] `git rm --cached infra/.env`
- [ ] Fix the broken gitignore negation — `.gitignore` does not support trailing comments, so
      `!infra/.env        # ...` matches nothing. Remove the negation entirely; the file should be
      ignored, not whitelisted.
- [ ] Add `infra/.env.example` with variable names and empty values.
- [ ] Decide separately whether history rewriting (`git-filter-repo`) is warranted. Only if the
      values were real.

### 0.2 Collection host — decision required

The current host is a laptop with `WakeToRun` disabled. Realised capture rate is ~33%, with holes
up to 23 hours. Every later phase assumes a long-lived process.

Pick one and record the choice in the decision log:

- **(a) Move collection to an always-on host** — a $5/mo VPS or a Raspberry Pi on the LAN. Unblocks
  the WebSocket feed, the streaming layer, and any claim about uptime. Recommended.
- **(b) Stay on the laptop and accept gaps** — viable only if the project is reframed as batch-only.
  Phases 1 and 4 lose most of their meaning; the C++ handler becomes a toy.

Do not proceed past Phase 0 without answering this. It is the single highest-leverage decision in
the project.

### 0.3 Scheduler

**Resolved 2026-08-22 — task disabled.** Not replaced. The hourly poller was redundant
with the candles backfill, which is historical and does not care about uptime. Collection
is manual until Phase 2 brings in Dagster. See PROJECT.md decision log.


- [ ] Clear the phantom cadences: disable the task, re-enable it, verify with the log that runs
      occur once per hour at a single offset.
- [ ] Confirm `StartWhenAvailable` remains `False` (catch-up off).
- [ ] If 0.2 resolves to (a), this becomes moot — the scheduler is replaced by a long-running
      process under a supervisor.

### 0.4 Data reconciliation

Three versions of the ticker file disagree. Establish the record of truth once, then never
hand-edit again.

- [ ] Treat `ticker_price.jsonl.preprune-20260816` (106 records) as canonical raw — it is the
      superset and matches the log.
- [ ] Move all raw files out of `src/` into `data/raw/`. This includes `candles_1h.jsonl` (1.3 MB, 8,750 rows) — it is raw data living in a source directory, and it is what the model in `notebooks/` actually trains on.
- [ ] Delete `ticker_price.json` (a bare float, dead since 2026-08-09).
- [ ] Keep `.bak` (old 2-field schema) as `data/raw/archive/` with a note, or delete it — it has 10
      records and no forward value.
- [ ] The duplicate-cadence rows are *not* deleted. They are real observations that happened to be
      collected redundantly. Deduplicate downstream on `(symbol, timestamp)`, in the staging layer,
      where the logic is visible and reproducible.

### 0.5 Repo hygiene

- [ ] Move `README.md` (the Mini Exchange design doc) to its own repo. Write a real README for
      *this* project.
- [ ] Fix `pyproject.toml`: point `readme` at the new README, add a `[build-system]` table, and
      prune dependencies to what is actually imported. `mlflow`, `xgboost`, `scikit-learn`,
      `boto3`, `confluent-kafka`, `websockets`, `cryptography`, `pyyaml`, `python-dotenv` all come
      out for now — they return when the phase that needs them arrives.
- [ ] Decide on `infra/`. It is 600+ lines that have never run, targets Airflow (this project is
      going to Dagster), contains an unparseable `producer/main.py`, empty Dockerfiles, and a
      committed runtime log. **Recommendation: delete it.** It is preserved in git history if ever
      needed. Do not carry it forward as scaffolding for the new stack.
- [ ] Commit `SCHEDULING.md` — it is the most substantial doc in the repo and is untracked.
- [ ] Add `data/` to `.gitignore` and stop tracking `*.jsonl`. Add a `.gitattributes` for line
      endings.
- [ ] Resolve the staged deletions of `rate_limiter.py` and `coinbase_test.py` — commit them.

### 0.6 Fix the known bugs

- [ ] `coinbase.py:71` — non-200 responses retry instantly with no log line. Move the `sleep` and
      add a diagnostic, matching `candles.py`'s correct handling.
- [ ] `coinbase.py:32` — `data == False` should be `if not data`.
- [ ] Make the append crash-safe: write to a temp file and rename, or at minimum `flush()` +
      `os.fsync()` before close.
- [ ] Write the tests already specified in `PROJECT.md` §4 — but against the *current* API shape.
      `NOTES.md`'s test cases describe the old `/v2/prices` payload and would test the wrong thing.

**Exit criteria for Phase 0:** a fresh clone installs and runs; no secrets tracked; one canonical
raw dataset outside `src/`; the collector runs on a schedule that fires once per interval; tests
pass.

---

## Phase 1 — C++ feed handler

Replaces REST polling with a long-lived WebSocket service. This is the concurrency work.

**Target:** `services/feed-handler/`, C++20, CMake.

### Architecture

```
network thread  →  ring buffer  →  parser pool (2–4)  →  queue  →  writer thread
                                                                →  aggregator thread
                                        metrics (atomics) ←  all threads
                                                             ↓
                                                       HTTP thread (health, metrics)
```

**Scaffold landed 2026-08-22.** `services/feed-handler/` has CMakeLists, a `mingw`
CMakePreset, and a stub `main.cpp` that compiles and links Beast + OpenSSL + nlohmann.
Build with `cmake --preset mingw && cmake --build --preset mingw`.

*Build gotcha, do not remove:* `CMAKE_NO_SYSTEM_FROM_IMPORTED ON`. All three deps live in
`/mingw64/include`, already a default system include dir; without that flag CMake re-adds
it as `-isystem`, reordering the search path and breaking libstdc++'s
`#include_next <stdlib.h>`. It fails as `stdlib.h: No such file or directory`, which
points nowhere near the cause.

### Build order — each step is benchmarked before moving on

1. **Single-threaded, end to end.** WebSocket → parse → Parquet. Working and boring.
2. **Bounded queue with `std::mutex` + `std::condition_variable`.** Producer-consumer, correct
   handling of spurious wakeups. Benchmark.
3. **Replace the hot queue with a lock-free SPSC ring buffer** using `std::atomic` and
   acquire/release ordering. Benchmark. Record the delta.
4. **Cache-line padding** (`alignas(64)`) to eliminate false sharing. Benchmark a third time.
5. **Parser thread pool** — fan out parsing, preserve ordering on the write side.
6. **Backpressure policy** — decide and document: block the producer, or drop oldest. Both are
   defensible; the choice must be explicit and the dropped count must be a metric.
7. **Graceful shutdown** — `std::atomic<bool>` stop flag, `notify_all`, drain in-flight work,
   flush the writer. SIGTERM must never lose a buffered record.

### Non-negotiable practices

- **Build with `-fsanitize=thread` in CI and in dev.** Data races do not surface in normal test
  runs.
- **Replay harness** — record raw frames to a file, replay at 100× for reproducible load testing.
  No waiting on live market conditions to test burst behaviour.
- **Benchmark numbers go in the README.** Write the Python version first and compare. The honest
  claim is "C++ sustained X msg/sec at p99 latency Y vs Python's Z," not "used C++ for
  performance."

### Output contract

Parquet files under `data/raw/ticks/symbol=<S>/dt=<YYYY-MM-DD>/hour=<HH>/`. Append-only, never
rewritten. The existing JSONL history backfills into this layout via DuckDB:
`COPY (SELECT * FROM read_json_auto('data/raw/legacy/*.jsonl')) TO ... (FORMAT PARQUET)`.

**Exit criteria:** handler runs for 72 hours unattended without loss, TSan clean, benchmarks
recorded, legacy JSONL migrated into the raw zone.

---

## Phase 2 — batch layer

**Stack:** Dagster (orchestration) + dbt-duckdb (transforms) + DuckDB (warehouse).

Airflow is explicitly rejected: Dagster's asset model fits this shape better, local dev is far
easier, and its asset checks and freshness policies do double duty in Phase 3.

### Layers

- `raw/` — Parquet, immutable, as written by the feed handler
- `staging` — typed, deduplicated on `(symbol, timestamp)`, one row per observation
- `marts` — `ohlcv_1m`, `ohlcv_1h`, `features_1h`

### Assets

- `ingest_ticks` (continuous, or 5-min if batch)
- `build_bars` (hourly)
- `build_features` (hourly)
- `quality_checks` (after each build)

### Requirements

- **Idempotent upserts** on `(symbol, ts)`. Re-running a window is a no-op.
- **Backfill entrypoint** taking a date range and replaying from `raw/`. Prove it: delete a day,
  rebuild it, diff the result against a snapshot.
- **The existing 8,750 candle records** load into `marts.ohlcv_1h` as historical backfill.

**Exit criteria:** `dagster dev` brings up the full asset graph; deleting and rebuilding any day
produces identical output.

---

## Phase 3 — observability

Ships before the streaming layer, deliberately — instrumentation on a two-service system is easy to
reason about, and it must already exist when a broker is added.

### Data quality (dbt tests + Dagster asset checks)

- `not_null` on ts, price; `unique` on `(symbol, ts)`; accepted range on price and volume
- **Gap detection** — expected vs actual bar count per hour. This is the failure mode that actually
  occurs here (see the audit's 33% capture rate) and no generic test catches it.
- Volume/row-count anomaly vs trailing median

### Service health

- `/healthz` — process liveness
- `/readyz` — feed connected, warehouse reachable, last tick within N seconds
- `/metrics` — Prometheus: ticks/sec, buffer depth, dropped messages, write latency p50/p99,
  seconds since last message

### Monitoring

- Prometheus + Grafana in Docker Compose (a *new*, minimal compose file — not the deleted `infra/`
  one)
- Structured JSON logging, single timezone (UTC everywhere — the current split between local-time
  batch stamps and UTC records is a defect)
- Log rotation

### Dashboard — four panels, hard cap

Freshness per table · last run status and duration · quality check pass/fail history · gap rate
over 24h. Monitoring is where projects like this die; the cap is the point.

**Exit criteria:** killing the feed handler turns `/readyz` red within one interval and fires a
visible alert.

---

## Phase 4 — streaming layer

**Stack:** Redpanda (Kafka-compatible, single binary, no ZooKeeper) + Quix Streams (Python).

Kafka Streams is rejected: JVM-only, and a third language is not worth it. Faust is rejected: the
maintained line is a community fork of an abandoned project. Bytewax is rejected: went
community-maintained in 2025.

- C++ feed handler becomes a Kafka producer
- Quix Streams consumer computes rolling moving averages over hopping windows
- Results sink back to a topic and into DuckDB

**Also implement the same moving average in the C++ aggregator thread** and compare. "Same
computation in-process vs distributed, here's what each buys you" is the most interesting thing
this phase produces.

**Honesty requirement:** at one symbol's volume, Kafka is not technically justified. The README
says so plainly — it is here to learn streaming semantics, not because throughput demanded it.
Overclaiming this is the fastest way to lose credibility in an interview.

**Exit criteria:** end-to-end latency from tick to computed MA measured and recorded; broker
restart loses nothing.

---

## Phase 5 — applications

- **Query API** — FastAPI over the marts. Rate limiting, pagination, OpenAPI docs.
- **ML model** — feature pipeline off `features_1h`, a baseline (persistence / naive forecast)
  before anything else, then a real model. MLflow for experiment tracking.

The baseline is mandatory. A price prediction model that isn't compared against "tomorrow equals
today" is not a result.

---

## Target repo layout

```
crypto-ml-pipeline/
├── README.md                    (this project — not the exchange doc)
├── docs/
│   ├── goal-state.md            (this file)
│   ├── PROJECT.md               (decision log)
│   └── SCHEDULING.md
├── data/                        (gitignored)
│   ├── raw/
│   └── warehouse.duckdb
├── services/
│   ├── feed-handler/            (C++)
│   └── api/                     (FastAPI)
├── pipeline/
│   ├── dagster/
│   └── dbt/
├── streaming/                   (Quix Streams consumers)
├── ml/
├── tests/
└── docker/
    └── compose.yml              (Redpanda, Prometheus, Grafana)
```

---

## Non-goals

Explicitly out of scope. Adding any of these is scope creep:

- Multiple exchanges (one venue until everything else works)
- More than two symbols before Phase 4
- Trading, backtesting, or order execution of any kind
- Kubernetes
- A frontend beyond the four-panel health dashboard
- Airflow, Spark, MinIO, or anything else from the deleted `infra/` stack

---

## Working agreement for agents in this repo

- Do not skip Phase 0. Building on the current state carries its problems forward.
- Do not add a dependency without adding the import that uses it in the same change.
- Do not hand-edit files under `data/raw/`. If raw data looks wrong, fix the reader.
- Benchmark before and after any change justified by performance. Unmeasured optimisation claims
  do not go in the README.
- When a phase's exit criteria are not met, say so rather than moving on.