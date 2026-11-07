# PROJECT — crypto-ml-pipeline

Scope, decisions, and open ideas for **this repo only**. NOTES.md is the dev journal
(what broke and why); this file is the plan (what we're building and what we've ruled out).

Last reviewed: 2026-09-06

---

## 1. Decision log

**2026-09-11 (later) — v1 built to the resume, every number with its control.**
The target was the resume bullet list, and the rule was that each claim gets measured, not
asserted, beside a control that shows the measurement could have failed.

- **Load tests** (`tests/test_feed_handler.py`): 200,000 of 200,000 written through a 16-slot
  queue; the same load under `--overflow drop` sheds 93%, which is what makes the zero mean
  something. Ctrl+Break at 1.5 s: 29,792 read, 29,792 written. Needed a `SIGBREAK` handler --
  a Windows harness can only send Ctrl+Break to a process group, and unhandled it kills the
  process without the drain.
- **ThreadSanitizer** in Docker (`gcc:14`, because this network refuses port 80 and Ubuntu's
  apt mirrors default to it). A planted race runs first and must be reported, or the run is
  rejected. 6 of 6 clean. The `-Wtsan` fence warning is Boost.Asio's, on the live path only.
- **`--pace N`**: replay scheduled from exchange timestamps. 99.89x at 100x over 300 s of market.
- **Quality**: thresholds from clean data only (a trade never sat more than 0.019% from the
  quoted mid). Benchmark: 873 of 880 injected errors caught, 99.2%; a no-op injection scores 0.
  The floor is honest and stays in the report: bar price errors under ~1% look like real moves.
- **ML target moved from returns to volatility.** Returns never beat "no change"; next-hour
  Parkinson volatility, walk-forward with weekly refits, gets 21.6% lower MAE than persistence
  and 20.0% below the 24-hour mean. Gradient boosting was named the headline before any result.
  Shuffled labels make it 14% worse than baseline -- the gain is signal, not leakage.
- **Kafka as Redpanda + Python**, not a C++ producer. The consumer commits no further than the
  first tick of the open bar and upserts; a crash mid-minute ends on the batch answer, where
  committing the last offset read loses 5 of 30 trades.
- **README in `docs/`** so the Mini Exchange doc at the root stays untouched.

The resume should say ~20%, not ~12%, and its dates read 2025 where the history is 2026.

**2026-09-11 — DuckDB seam started: Job 1 built, and the documented order was wrong.**
`pipeline/sql/` holds three queries: `raw_ticks` and `raw_candles`, views over the two source
files, and `tick_bars`, which is Job 1 — ticks to hourly OHLCV. `pipeline/build_warehouse.py`
turns each file into a view in `data/warehouse.duckdb`. Views rather than tables, so the
warehouse stores queries and the JSONL stays the only copy of the data: the "every connection
is a file" rule survives. `data/test.duckdb` is a table copy to experiment on.

Readiness was checked on both sides first. The current `feed_handler.exe` replayed 879 frames
rebuilt from the raw file into a scratch directory and wrote 879 ticks that match the original
on all eleven non-clock fields. On the Python side nothing needs a cast, and both sources'
timestamps come back as plain `TIMESTAMP` in UTC.

**The union-then-test order in `duckdb.md` could not have passed.** Unioning the tick bar onto
the candles puts it 26 days 23 hours after the last candle. `features.py` counts rows, so that
becomes a +23.85% "hourly" return in the test set, and RMSE goes from 0.0045 to 0.0072 for
every model. The Parquet export and the `data.py` rewire now come first, on the candles alone,
and must reproduce `data/baseline_metrics_pre_duckdb.txt` exactly. The union waits until
`candles.py` has closed the hole.

Also found: `--replay` stamps `recv_time` with the current clock, so the README's replay
example, which has no `--out`, would write a fake hour of duplicate trades into the raw zone.

**2026-09-09 — `infra/.env` found in public history; key rotated, history left alone.**
The file was committed in four commits, oldest `439bcd5` (2026-06-07), all reachable from
`origin/main` on a public repo. `c5ea14d` deleted it going forward, but a delete does not
un-commit — `git show ef798ab:infra/.env` still returns it on any clone.

Contents were less alarming than the filename: `AWS_ACCESS_KEY_ID=minio` /
`AWS_SECRET_ACCESS_KEY=minio123` are MinIO's S3-compatible auth using AWS-shaped variable
names, not Amazon credentials — they are the defaults printed in every MinIO quickstart.
`MINIO_USERNAME`/`MINIO_PASSWORD` are the same pair. `AIRFLOW_UID` is not a secret. The one
real generated value was `FERNET_KEY`, which decrypts connection passwords stored in
Airflow's metadata database — a database that never existed, because the stack never ran.

**Rotated the Fernet key anyway**, since it costs nothing and makes the published value
dead. Verified no tracked file contains a secret-shaped assignment, and that every `.env`
on disk resolves to ignored. Added `*.duckdb` to `.gitignore` — `*.db` was there but did
not cover the extension DuckDB actually uses.

**Did not rewrite history**, deliberately. A `filter-repo` purge plus force-push rewrites
all 67 commit hashes, and on a public repo it still does not guarantee erasure: GitHub
keeps unreferenced objects reachable by SHA, and any fork or existing clone keeps them
permanently. It cleans the visible history, not the internet. Rotation is the remediation
that actually works, and it is done.

The remaining argument for purging is optics, not security — a reviewer running `git log`
on a portfolio repo and finding a committed `.env` draws a conclusion about habits
regardless of the values. Open, and worth deciding before this repo gets shown to anyone.

**2026-09-07 -- Phase 1 concurrency landed; both queue implementations kept.**
The feed handler is now `source -> queue -> parser pool -> queue -> writer`, with the
whole of GoalState's step list built: bounded mutex/condvar queue (2), lock-free SPSC ring
(3), cache-line padding on the ring indices (4), a parser pool with write-side reordering
(5), an explicit `--overflow block|drop` policy with a drop counter (6), and an ordered
three-stage drain on shutdown (7).

Both queues ship rather than one replacing the other, selectable with `--queue`. GoalState
asks for the delta between steps to be *recorded*, and a benchmark you can re-run against
either implementation is worth more than a number in a commit message. Numbers and the
replay instructions live in `services/feed-handler/README.md`.

**The ring is single-consumer by construction, so `--queue spsc` forces `--parsers 1`.**
That is not a limitation to fix later: one-producer/one-consumer is the entire reason it
needs no mutex. A parser pool makes it single-producer/multi-consumer, which it is not.

Replay came first, because every step above needs a benchmark and a benchmark against the
live feed is not reproducible -- volume changes hour to hour. `--record FILE` captures raw
frames, `--replay FILE` feeds them back through the identical downstream path, `--bench`
reports throughput and pipeline latency without touching disk.

**2026-09-06 — Modelling moved out of the notebook into modules; v2 lost to v1.**
Ported a walkthrough pipeline (`config.py` / `data.py` / `features.py` / `model.py` /
`main.py`, all sitting in `notebooks/`) and re-pointed it from its original weekly-NVDA
data at our hourly BTC candles. It regresses the *next bar's return* over 15 features and
grades four models against two do-nothing baselines.

Result: **nothing beat `baseline_zero`.** The random forest's directional accuracy is
50.34% — a coin flip, and worse than the notebook's 3-feature logistic regression at
52.67%. Full numbers and the v1-vs-v2 table in `MODEL_RESULTS.md`.

Kept anyway, because the module split is the part worth having: features, split, and
scoring are importable and testable instead of living in one notebook cell. **Next
experiment is the isolation run** — logistic regression on v2's 15 features. Same task and
same model as v1, only the feature set changes, which is the only way to tell whether the
features or the forest cost us the edge. Three variables moved at once in v2; that was the
mistake worth not repeating.

Worth naming: this is v1-finish-line drift. The last box in §4 (tests) is still open, and
modelling lives in §5's "only if v1 lands" bucket.

**2026-08-22 — Hourly polling retired; candles backfill is the data path.**
`CoinbasePriceTracker` is disabled. The hourly poller (`coinbase.py`) captured a single
spot price per run and realised only ~33% of them, because the laptop sleeps. It was also
redundant: an hourly *candle* contains that hour's close plus open/high/low/volume, and
`candles.py` fetches candles *historically* — one run rebuilds the full `DAYS_BACK=365`
window regardless of whether this machine was awake. So the gaps were never a real loss.

Data collection is now manual: run `python src/tracker/candles.py` when fresh data is
wanted. No scheduler, no orchestrator. Airflow was considered and rejected again — it does
not run natively on Windows (see SCHEDULING.md), and this repo is going to Dagster in
Phase 2 anyway. `ticker_price.jsonl` is kept as raw history, not deleted.

Uptime still matters, but only for Phase 1: a WebSocket is a live firehose and Coinbase
does not store it for you. There is no backfill for missed frames. That is what the
Phase 0.2 host question is really about.

**2026-08-09 — Mini Exchange moves to its own repo.**
The 277-line README written on 08-08 describes a different project: a C++ matching engine,
sequencer, WAL, and React dashboard, on a 6–8 week plan. It shares no code, no language,
and no dependencies with this repo. It references the crypto work only as a stretch goal.

Keeping both here would repeat what happened to `infra/` — a large plan parked next to a
small one, where the small one stalls because the large one is always more interesting.
So: Mini Exchange gets a clean repo, and this repo gets a finish line.

**2026-07-09 — `src/` → `infra/` rename.** Freed `src/` for real app code. The MLOps stack
(Airflow/Kafka/MLflow/Postgres) stayed behind in `infra/` and has never run.

**2026-06-25 — `rate_limiter.py`** is an interview exercise, not part of this project.

---

## 2. What this repo is

A **Coinbase price tracker**: poll the public spot endpoint on a cadence, append each
reading to a durable price-history file, run unattended without corrupting itself.

That's it. Not an ML pipeline yet — the name is aspirational and that's fine.

**Non-goals for v1:** Kafka, Airflow, MLflow, Spark, model training, trading logic,
authenticated Coinbase endpoints, a web UI.

---

## 3. Where it actually stands

**Architecture diagram (2026-09-07):** https://claude.ai/code/artifact/eaccaa07-d15f-4675-a6c1-71f1c4ada9ad
System-design snapshot -- both ingest paths, the raw zone, the unbuilt DuckDB seam, the Phase 1
step ladder, and the first ingest-latency numbers. Private to my Claude account; shareable from
the page's own share menu. Regenerate or update it rather than starting a new one, so the link
stays stable.

| Piece | State |
|---|---|
| `src/tracker/coinbase.py` | Fetches BTC-USD spot, retries, validates, appends JSONL. ~70 lines. |
| `src/tracker/ticker_price.jsonl` | Real records, one JSON object per line. |
| `src/tracker/ticker_price.json` | Dead — the old `65343.745`. Delete whenever. |
| `src/tracker/coinbase_test.py` | Empty shell; its `import coinbase` only resolves from inside the folder. |
| `src/tracker/candles.py` | One-shot backfill of hourly OHLCV. Not scheduled; safe to re-run. |
| `src/tracker/candles_1h.jsonl` | 8,750 hourly BTC-USD candles, 2025-08-11 → 2026-08-11. 1.3 MB, 10 missing hours, no dupes. Stale since 08-11. |
| `notebooks/model.ipynb` | Working logistic regression on 6 steps: target → features → dropna → allowlist X → chronological split → fit. Committed 2026-08-22. |
| `notebooks/{config,data,features,model,main}.py` | **v2 pipeline.** 15 features → Ridge + RandomForest vs `baseline_zero` / `baseline_last_week`. Runs in ~10s: `uv run notebooks\main.py`. Committed in `5a5dad9` (2026-09-05). |
| `MDFiles/MODEL_RESULTS.md` | Results log, one section per model version, newest last. Added 2026-09-06, uncommitted like the rest of MDFiles. |
| `notebooks/testing.ipynb` | Scratch. Contains the RandomForest run that leaked `target_next_close` — kept deliberately as the worked example of the mistake. |
| Scheduler | Set up 2026-08-11, **disabled 2026-08-22**. Redundant with the candles backfill. |
| `services/feed-handler/` | **Phase 1 complete, single machine.** Threaded pipeline, both queues, replay + bench modes. Steps 1-7 built; 72h soak still unproven. |
| `services/feed-handler/include/` | `tick.hpp`, `tick_writer.hpp`, `bounded_queue.hpp`, `spsc_ring.hpp`. |
| `data/raw/ticks/` | Live raw zone, hour-partitioned JSONL. Gitignored. |
| `infra/` | Parked. `config.yaml` empty, `producer/main.py` is a syntax error, Kafka broker never defined. |
| `rate_limiter.py` | All `TODO`. Unrelated. |
| `pyproject.toml` | Declares mlflow, kafka, xgboost, boto3. Code uses `requests`. |

Two-week gap from 07-27 to 08-08, which ended in a README for a different project. Worth
naming: the tracker stalled one step short of being finished, and finishing it is cheap.

---

## 4. v1 — the finish line

Done means: **it runs unattended for a week and produces a price history you'd trust
enough to load into pandas.** Five things stand between here and there.

- [x] **Real records.** *(2026-08-09)* Writes one JSON object per line to
      `ticker_price.jsonl` — `{"timestamp": <UTC ISO8601>, "price": <float>}`. Price cast
      to float at parse time, so garbage fails at fetch instead of landing in the file.
- [x] **Anchor the path.** *(2026-08-09)* `Path(__file__).parent / "ticker_price.jsonl"`,
      so it writes to the same place regardless of cwd. Passed as a default arg to
      `append_json`, so tests can hand it a tmp path without patching a global.
- [x] **Survive the network.** *(2026-08-11)* All four pieces landed:
      - `timeout=(5,10)` — connect and read bounded separately. Without it `requests`
        waits *forever*, which is the failure that hangs an unattended run.
      - `status_code == 200` check, and a `try/except requests.RequestException` around
        both the GET and the `.json()`. One exception class covers timeout, connection,
        SSL, and non-JSON body — `JSONDecodeError` subclasses `RequestException`.
      - Retry loop, `ATTEMPTS = 3`, `sleep(10)` between attempts but not after the last.
        Success returns from inside the loop; give-up returns `False` below it.
      - Payload validation in `get_spot_price`: one `except (KeyError, TypeError,
        ValueError)` around the cast, plus a `price <= 0` check. The zero check matters
        most — it's the only bad value that doesn't raise, so it would otherwise append
        silently and look like real data.

      Every failure converges on one signal: falsy record → stderr message → skip the
      append → `sys.exit(1)`, so a failed run shows red in the scheduler.
- [x] **Schedule it.** *(2026-08-11)* Windows Task Scheduler task `CoinbasePriceTracker`,
      hourly, indefinitely, expiring 2027-08-11. Runs `run_tracker.bat`, which anchors the
      working directory, redirects stdout+stderr to `logs/tracker.log`, and propagates the
      exit code so a failed run reports as failed. Verified: `LastTaskResult 0`.
      Gotchas hit along the way, for next time: Task Scheduler rejects a quoted path in
      the *Start in* field; a quoted *Program/script* makes it think the path is arguments;
      and **"start only if on AC power" is checked by default**, which silently skips every
      run on battery. Modifying a task in the library root needs elevation — the GUI
      prompts, `Set-ScheduledTask` just returns Access Denied.
      Known behaviour: "wake the computer" is off, so sleep produces gaps, and the
      missed-start catch-up fires once on wake rather than backfilling each missed hour.
- [ ] **Tests.** `coinbase_test.py` is still the empty shell. Needs a real import path (run
      pytest from the repo root) and the cases already proven by hand during development:
      good response appends exactly one line; malformed payload (missing `data`, missing
      `amount`, `amount` null, non-numeric, zero, negative) appends nothing and returns
      falsy; all attempts failing returns falsy and exits non-zero; file is still valid
      JSONL after N calls. Fake the response rather than hitting the network, and pass a
      tmp path to `append_json` so tests never touch the real history file.

Then stop. That's a small, complete, honest project.

**Cleanups, whenever:** trim `pyproject.toml` to what's imported; decide whether `infra/`
gets archived to a branch or deleted; move `rate_limiter.py` to a practice repo.

---

## 5. Ideas — only if v1 lands

Parked deliberately. Each is a separate decision, not a queue to work through.

- **WebSocket feed instead of polling.** `wss://ws-feed.exchange.coinbase.com`, the
  `ticker` channel. Real-time instead of snapshots. This is the natural next step and the
  first genuinely interesting one — reconnects, heartbeats, and sequence gaps are real
  problems.
- **Features.** ~~Rolling returns, volatility, moving averages over the history file.~~
  *Partly landed 2026-08-22* — `ret_1`, `ret_6`, `ma_ratio` on the candle file, feeding a
  logistic regression that scores **52.67% vs a 50.89% baseline** (ROC-AUC 0.537) on a
  chronological 80/20 split. Honest and small. Measured but unused: `range_pct`,
  `close_loc`, wick ratios, `vol_z`, hour-of-day — only the return/MA family cleared the
  noise floor. Signal is mean-reverting and peaks around a 6-hour horizon, not 1 hour.
  *v2 attempt 2026-09-06* — widened to 15 features (lags, momentum, SMA ratios, realised
  vol, RSI, relative volume) under a regression target. It went **backwards**: 50.34% vs
  v1's 52.67%. The mean-reversion finding held up independently — `baseline_last_week`
  (assume this hour repeats) scored R² −1.09, actively worse than predicting nothing.
- **Paper broker.** Simulated fills against live prices, with a clean seam so real orders
  could slot in later. Keep the seam even if real orders never happen.
- **RL / MCTS agent.** The original ambition. Needs features and a paper broker first, and
  needs enough price history to be worth training on — which is the actual reason v1
  matters.
- **Re-add the real infra.** MLflow → Postgres → Kafka → Airflow, one at a time, only once
  there's something running that genuinely needs each one.

### Added 2026-08-21 — feature/project ideas

- **Information diffusion / event lag.** Measure how fast new information gets priced in:
  join an FOMC calendar to the candles, then look at how many bars pass before a move is
  fully absorbed. The only one of this batch that is a *feature of this repo* — everything
  else below is its own project. Needs an external event calendar, which the tracker
  doesn't collect today.
- **Monte Carlo for American option pricing.** Option vol and expected PnL. Separate repo:
  different asset class, different math, shares no data or code with the spot pipeline.
- **Kalshi mispricing / arbitrage engine.** Probability model over Kalshi contracts.
  Separate repo: different venue, different API, different data.
- **C++ pivot.** Already the stated point of the project in `GoalState.md`, where the ML
  model is explicitly last. Not a new idea so much as a reminder of the actual ordering.

The 2026-08-09 Mini Exchange decision applies to the middle two: a large plan parked next
to a small one means the small one stalls. If either gets built, it gets its own repo.

---

## 6. Open questions

- **Poll cadence?** 24h (per NOTES.md) vs hourly vs the WS feed. Affects how long until
  the history is useful for anything.
- **How many symbols?** BTC-USD only, or a handful? Changes the file layout — one file per
  symbol vs one file with a `symbol` field.
- **Does `infra/` get deleted?** It's dead code that makes the repo look bigger than it is.
  A `parked-infra` branch preserves it at zero cost.
- **Does this repo continue past v1**, or does Mini Exchange become the main project and
  this one gets archived as finished? Fine to answer later — but answer it, rather than
  letting it drift.
