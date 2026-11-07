# Model results

A running log of every model tried against BTC-USD, newest last. Same dataset
throughout: `src/tracker/candles_1h.jsonl` — 8,750 hourly candles,
2025-08-11 → 2026-08-11, backfilled from the Coinbase Exchange REST API.

Every run uses a **chronological 80/20 split** (no shuffling — the test set is
always the last ~10 weeks, which the model never sees during training).

---

## v1 — hourly direction classifier

**Where:** `notebooks/model.ipynb`, cell 6.

**Strategy.** Treat it as a *classification* problem: will the next hourly candle
close higher than this one? Three hand-picked features, all ratios or returns so
nothing is denominated in raw dollars (a model fed raw price learns the 2025→2026
downtrend, not a signal).

| | |
|---|---|
| Target | `close.shift(-1) > close` → 1/0 |
| Features (3) | `ret_1`, `ret_6`, `ma_ratio` (close ÷ 50-bar SMA) |
| Model | `StandardScaler` → `LogisticRegression(max_iter=1000)` |
| Rows | train 6,960 / test 1,741 |

**Results**

```
baseline      : 50.89%     <- always guess the majority class
accuracy      : 52.67%
roc auc       : 0.5371
```

**Read.** Beats the majority-class baseline by **1.78 points**, and an AUC of
0.537 means the ranking is slightly better than a coin flip. Small, but pointing
the right way.

---

## v2 — multi-feature return regressor

**Where:** `notebooks/main.py` + `config.py` / `data.py` / `features.py` / `model.py`.
Ported from a walkthrough that ran this on weekly NVDA bars; re-pointed at our
hourly BTC candles.

**Strategy.** Switch from classification to *regression* — predict the size of
the next bar's return, not just its sign. Widen from 3 features to 15
(lagged returns, momentum, SMA ratios, realised vol, RSI, relative volume), and
grade every model against two do-nothing baselines instead of a single accuracy
number.

| | |
|---|---|
| Target | `ret.shift(-1)` — next bar's % return |
| Features (15) | `ret_1w`, `ret_lag_1..5`, `mom_5w/10w/20w`, `close_over_sma_10/20`, `vol_10w/20w`, `rsi_14`, `rel_volume` |
| Models | `baseline_zero`, `baseline_last_week`, `Ridge(alpha=10)`, `RandomForestRegressor(300 trees, depth 4)` |
| Rows | train 6,983 / test 1,746 (test window 2026-05-30 → 2026-08-11) |

> Naming note: the `_1w` / `_10w` / `_20w` suffixes are inherited from the weekly
> original. On our data a bar is **one hour**, so they mean 1 / 10 / 20 hours.

**Results**

```
--- Out-of-sample metrics (test set the model never saw) ---
                     RMSE    MAE      R2  DirAcc      IC
model
baseline_zero      0.0045 0.0029 -0.0003     NaN     NaN
baseline_last_week 0.0064 0.0042 -1.0876  0.4668 -0.0438
ridge              0.0045 0.0029 -0.0061  0.4908 -0.0586
random_forest      0.0045 0.0029 -0.0009  0.5034  0.0087

--- Random forest: top feature importances ---
vol_20w     0.0963
ret_lag_1   0.0863
ret_1w      0.0813
ret_lag_5   0.0811
mom_5w      0.0786
mom_20w     0.0730
```

**Metric glossary**

- **RMSE / MAE** — average size of the prediction error, in return units. 0.0045 = 0.45%.
- **R²** — fraction of variance explained. Negative means *worse than predicting the test-set mean*.
- **DirAcc** — how often the predicted sign matched the real sign. 0.50 is a coin flip.
- **IC** (information coefficient) — correlation between prediction and truth. Standard quant scorecard metric; anything under ~0.03 is noise.

**Read.** Nothing beats doing nothing. `baseline_zero` — which literally always
predicts a 0% return — has the *best* R² of the four. Ridge and the random forest
land on the same RMSE to four decimals, and both have essentially zero IC. The
`baseline_last_week` row is the interesting failure: assuming the next hour
repeats the last hour is actively harmful (R² of −1.09, DirAcc 0.467), which says
hourly BTC returns are slightly **mean-reverting**, not trending.

---

## v1 vs v2

The metric sets don't overlap much, but two pairs are directly comparable —
both models predict the same underlying event (does the next hour close up?),
one binarised and one not:

| | v1 (logistic, 3 features) | v2 (random forest, 15 features) |
|---|---|---|
| Direction correct | **52.67%** | 50.34% |
| Ranking quality | AUC **0.5371** | IC 0.0087 |
| Test rows | 1,741 | 1,746 |

**v1 is ahead.** Tripling the feature count and swapping in a random forest made
directional accuracy *worse* — 2.3 points worse, from a small edge down to a coin
flip. Worth sitting with rather than explaining away: more features on a noisy
target mostly buys more ways to fit noise, and the forest's depth-4 / 300-tree
setup was tuned for ~1,375 weekly stock bars, not 8,700 hourly crypto bars.

Also worth noting the two aren't a perfectly clean A/B — v2 changed the *task*
(regression, where a tiny predicted return still counts as a directional call)
at the same time it changed the features and the model. Three variables at once.

## Ideas for v3

- Isolate the change: run logistic regression on v2's 15 features. If accuracy
  goes back to ~52.7%, the features were fine and the forest was the problem.
- Add v1's `ma_ratio` at window 50 — v2's longest lookback is 20 bars, and
  `vol_20w` topping the importance list hints longer windows carry more.
- Only trade the confident tail (|prediction| in the top decile) instead of
  grading all 1,746 test bars equally.
- Resample to daily bars so `_20w` means something closer to its name — though
  a year of data is only ~364 daily rows.
- Cost check: at 0.45% typical hourly moves, Coinbase taker fees would eat any
  edge this thin. Worth pricing before chasing accuracy further.
