# Data contracts — the shape at each connection point

Three shapes. Two are already fixed by code that exists; the third is the one you are
deciding. Everything DuckDB has to do is get from the first two to the third.

---

## 1. Tick row — `data/raw/ticks/**/ticks.jsonl`

**Written by:** `TickWriter::write` in `services/feed-handler/src/tick_writer.cpp`
**Grain:** one row per trade. Irregular, roughly 10/sec on BTC-USD.
**Status:** FIXED. Changing this breaks the C++ writer and anything reading the files.

| Column | Type | Notes |
|---|---|---|
| `sequence` | int64 | Coinbase's per-product message counter. **Gaps are normal** — see below. |
| `product_id` | string | `"BTC-USD"` |
| `price` | double | Trade price. Arrives from Coinbase as a *string*, cast on parse. |
| `last_size` | double | Size of this trade. Also a string on the wire. |
| `side` | string | `"buy"` / `"sell"` — which side initiated |
| `best_bid` | double | Top of book at the time |
| `best_bid_size` | double | |
| `best_ask` | double | |
| `best_ask_size` | double | |
| `time` | string | Exchange timestamp, ISO 8601 |
| `trade_id` | int64 | |
| `recv_time` | string | **Ours, not Coinbase's.** ISO 8601 with microseconds, UTC. Clock read at the moment `ws.read()` returned. |

**`sequence` gaps do not mean lost data.** It counts every message the product's feed
generates, and `ticker` is a filtered view of that stream. A sample had a jump on 878 of 878
consecutive pairs. Do not write a query that treats a gap as a problem. A *non-increasing*
sequence would be a real error (duplicate or replay) — the C++ counts those.

**`recv_time` vs `time`** is the ingestion latency. Useful, but the local clock is currently
unverified against NTP — a sample produced a minimum of −29 ms, which is impossible and
therefore proof of skew. Treat absolute latency figures as approximate.

---

## 2. Candle row — `src/tracker/candles_1h.jsonl`

**Written by:** `write_jsonl` in `src/tracker/candles.py`
**Grain:** one row per hour. Regular. 8,750 rows, 2025-08-11 → 2026-08-11.
**Status:** FIXED, and stale — the file has not been refreshed since 2026-08-11.

| Column | Type | Notes |
|---|---|---|
| `time` | string | UTC ISO 8601, start of the hour |
| `symbol` | string | `"BTC-USD"` |
| `open` | double | |
| `high` | double | |
| `low` | double | |
| `close` | double | |
| `volume` | double | Base-asset volume for the hour |

Known: about 10 hours are missing across the year, no duplicates.

---

## 3. Modelling table — what `notebooks/` needs

**Read by:** `data.py` → `features.py` → `model.py`
**Grain:** one row per hour, regular and evenly spaced.
**Status:** OPEN. This is the seam you are building.

`data.py` must return a frame indexed by UTC timestamp with these columns, because
`features.py` uses exactly these and nothing else:

| Column | Used by |
|---|---|
| `close` | returns, momentum, SMA ratios, RSI |
| `volume` | `rel_volume` |
| `open`, `high`, `low` | *nothing today* — carried but unused |

From those, `build_features` derives 15 features plus the target:

```
ret_1w                              1
ret_lag_1 .. ret_lag_5              5
mom_5w, mom_10w, mom_20w            3
close_over_sma_10, close_over_sma_20  2
vol_10w, vol_20w                    2
rsi_14                              1
rel_volume                          1
                                   --
                                   15 features
target = ret.shift(-HORIZON)        + 1
```

**Why the grain must be regular.** Every one of those uses `.shift()`, `.rolling()`, or
`.pct_change(n)` — all of which mean "n *rows* ago". On irregular ticks, "one row ago" might
be 40 ms or 6 minutes, and the feature silently becomes meaningless. This is the single
most important reason DuckDB exists in this pipeline.

The suffixes (`_1w`, `_20w`) are inherited from the source this pipeline was ported from and
say "week". **They mean bars.** On hourly data `mom_20w` is 20 hours.

---

## Decisions still yours

These are not blockers — pick either and note which. They only matter once DuckDB runs.

1. **Does the warehouse keep ticks at full grain, or only hourly bars?**
   Bars-only is all the model needs today and is simpler. Keeping ticks preserves the
   option of a sub-hourly model later, at the cost of storage and a second table.

2. **What happens where the two sources overlap?**
   The REST backfill covers through 2026-08-11 and the live feed starts 2026-09-07, so today
   there is a *gap*, not an overlap. Once you re-run `candles.py` they will overlap, and a
   naive `UNION` will produce duplicate hours. Prefer one source per hour, or dedupe.

3. **Does the tick-derived bar match the REST candle?**
   The `ticker` channel is a filtered stream — it does not carry every trade. So volume
   summed from ticks will likely be *lower* than the REST candle's volume for the same hour.
   Worth measuring once you can compute both; it decides whether the two sources are really
   interchangeable or only look it.
