# PROJECT — crypto-ml-pipeline

Scope, decisions, and open ideas for **this repo only**. NOTES.md is the dev journal
(what broke and why); this file is the plan (what we're building and what we've ruled out).

Last reviewed: 2026-08-11

---

## 1. Decision log

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

| Piece | State |
|---|---|
| `src/tracker/coinbase.py` | Fetches BTC-USD spot, retries, validates, appends JSONL. ~70 lines. |
| `src/tracker/ticker_price.jsonl` | Real records, one JSON object per line. |
| `src/tracker/ticker_price.json` | Dead — the old `65343.745`. Delete whenever. |
| `src/tracker/coinbase_test.py` | Empty shell; its `import coinbase` only resolves from inside the folder. |
| Scheduler | Not started. Last thing before this runs unattended. |
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
- [ ] **Schedule it.** Windows Task Scheduler is the one that survives a reboot; a
      `while True: sleep()` loop doesn't. Cadence is yours to pick — 24h builds history
      slowly, hourly gives you something to look at sooner.
      Gotchas to expect: point it at the absolute path to `.venv\Scripts\python.exe`, not
      a bare `python`; set the working directory on the task. The `Path(__file__)`
      anchoring above is what keeps the output landing in the right place regardless.
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
- **Features.** Rolling returns, volatility, moving averages over the history file. The
  first point where the "ML" in the repo name means anything.
- **Paper broker.** Simulated fills against live prices, with a clean seam so real orders
  could slot in later. Keep the seam even if real orders never happen.
- **RL / MCTS agent.** The original ambition. Needs features and a paper broker first, and
  needs enough price history to be worth training on — which is the actual reason v1
  matters.
- **Re-add the real infra.** MLflow → Postgres → Kafka → Airflow, one at a time, only once
  there's something running that genuinely needs each one.

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
