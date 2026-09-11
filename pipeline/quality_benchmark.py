"""Measure the quality checks against errors injected into real data.

The error mix is fixed here, before any result was seen, and is not tuned to
the checks. Every class is a way data actually goes wrong in this pipeline,
and the magnitudes include errors too small to see: price errors are drawn
log-uniformly from 0.3% to 20%, below the thresholds several checks use, so
the benchmark measures the detection floor instead of hiding it.

Protocol: one error per trial, injected into a fresh copy of the clean data at
a random row. Rows a check already flags on the clean data, and their
neighbours, are never targets, so a pre-existing issue can't be credited as a
catch. Caught means the injected row itself is flagged by at least one check;
for table-level classes (type drift, missing column), a schema finding.

    uv run python pipeline/quality_benchmark.py
"""

import argparse
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import quality as q  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def log_factor(rng) -> float:
    """A multiplicative price error of 0.3% to 20%, log-uniform, either sign."""
    m = np.exp(rng.uniform(np.log(0.003), np.log(0.20)))
    return float(np.exp(m if rng.random() < 0.5 else -m))


def _null(df, k, col):
    df = df.copy()
    if pd.api.types.is_integer_dtype(df[col]):
        df[col] = df[col].astype("Int64")
        df.loc[k, col] = pd.NA
    elif pd.api.types.is_datetime64_any_dtype(df[col]):
        df.loc[k, col] = pd.NaT
    elif pd.api.types.is_float_dtype(df[col]):
        df.loc[k, col] = np.nan
    else:
        df[col] = df[col].astype(object)
        df.loc[k, col] = None
    return df


def _insert_after(df, k, row):
    return pd.concat([df.iloc[:k + 1], row, df.iloc[k + 1:]], ignore_index=True)


# ---- ticks ----------------------------------------------------------------

def t_null(df, k, rng):
    return _null(df, k, str(rng.choice(list(q.TICK_SCHEMA)))), k


def t_non_positive(df, k, rng):
    col = str(rng.choice(["price", "last_size", "best_bid", "best_ask"]))
    df = df.copy()
    df.loc[k, col] = 0.0 if rng.random() < 0.5 else -abs(df.loc[k, col])
    return df, k


def t_crossed_book(df, k, rng):
    df = df.copy()
    df.loc[k, "best_bid"] = df.loc[k, "best_ask"] * (1 + rng.uniform(1e-4, 1e-2))
    return df, k


def t_bad_side(df, k, rng):
    df = df.copy()
    df.loc[k, "side"] = str(rng.choice(["BUY", "Sell", "", "unknown"]))
    return df, k


def t_duplicate_trade(df, k, rng):
    return _insert_after(df, k, df.iloc[[k]]), k + 1       # a frame written twice


def t_sequence_regression(df, k, rng):
    if k == 0:
        return None
    df = df.copy()
    df.loc[k, "sequence"] = df.loc[k - int(rng.integers(1, min(50, k) + 1)), "sequence"]
    return df, k


def t_price_spike(df, k, rng):
    df = df.copy()
    df.loc[k, "price"] *= log_factor(rng)
    return df, k


def t_clock_skew(df, k, rng):
    col = str(rng.choice(["recv_time", "time"]))
    shift = pd.Timedelta(seconds=float(np.exp(rng.uniform(np.log(120), np.log(86_400)))))
    df = df.copy()
    df.loc[k, col] = df.loc[k, col] + (shift if rng.random() < 0.5 else -shift)
    return df, k


def t_feed_gap(df, k, rng, banned=frozenset()):
    end = df.loc[k, "recv_time"] + pd.Timedelta(minutes=float(rng.uniform(2, 10)))
    j = k + 1
    while j < len(df) and df.loc[j, "recv_time"] < end:
        j += 1
    if j >= len(df) or any(r in banned for r in range(k, j + 1)):
        return None
    return pd.concat([df.iloc[:k + 1], df.iloc[j:]], ignore_index=True), k + 1


def t_type_drift(df, k, rng):
    col = str(rng.choice(["sequence", "price", "last_size", "best_bid", "trade_id", "recv_time"]))
    df = df.copy()
    df[col] = df[col].astype(str)
    return df, None


def t_missing_column(df, k, rng):
    return df.drop(columns=[str(rng.choice(list(q.TICK_SCHEMA)))]), None


TICK_CLASSES = {
    "null_field": t_null, "non_positive": t_non_positive, "crossed_book": t_crossed_book,
    "bad_side": t_bad_side, "duplicate_trade": t_duplicate_trade,
    "sequence_regression": t_sequence_regression, "price_spike": t_price_spike,
    "clock_skew": t_clock_skew, "feed_gap": t_feed_gap, "type_drift": t_type_drift,
    "missing_column": t_missing_column,
}


# ---- bars -----------------------------------------------------------------

PRICES = ["open", "high", "low", "close"]


def b_null(df, k, rng):
    return _null(df, k, str(rng.choice(list(q.BAR_SCHEMA)))), k


def b_non_positive(df, k, rng):
    col = str(rng.choice(PRICES + ["volume"]))
    df = df.copy()
    df.loc[k, col] = 0.0 if rng.random() < 0.5 else -abs(df.loc[k, col])
    return df, k


def b_ohlc_inconsistent(df, k, rng):
    df = df.copy()
    body_lo, body_hi = sorted((df.loc[k, "open"], df.loc[k, "close"]))
    if rng.random() < 0.5:
        df.loc[k, "high"] = body_lo * rng.uniform(0.95, 0.999)
    else:
        df.loc[k, "low"] = body_hi * rng.uniform(1.001, 1.05)
    return df, k


def b_duplicate_hour(df, k, rng):
    # The same hour from the other source: the union-over-overlap failure.
    row = df.iloc[[k]].copy()
    row[PRICES] = row[PRICES] * rng.uniform(0.999, 1.001)
    return _insert_after(df, k, row), k + 1


def b_missing_hour(df, k, rng):
    if k == len(df) - 1:
        return None
    return df.drop(index=k).reset_index(drop=True), k     # flagged on the bar after it


def b_misaligned_time(df, k, rng):
    df = df.copy()
    df.loc[k, "time"] = df.loc[k, "time"] + pd.Timedelta(minutes=int(rng.integers(1, 60)))
    return df, k


def b_price_spike(df, k, rng):
    df = df.copy()
    df.loc[k, str(rng.choice(PRICES))] *= log_factor(rng)
    return df, k


def b_unit_error(df, k, rng):
    df = df.copy()
    df.loc[k, PRICES] = df.loc[k, PRICES] * (100.0 if rng.random() < 0.5 else 0.01)
    return df, k


def b_volume_spike(df, k, rng):
    df = df.copy()
    df.loc[k, "volume"] *= rng.uniform(100, 1000)
    return df, k


def b_type_drift(df, k, rng):
    col = str(rng.choice(PRICES + ["volume", "time"]))
    df = df.copy()
    df[col] = df[col].astype(str)
    return df, None


def b_missing_column(df, k, rng):
    return df.drop(columns=[str(rng.choice(list(q.BAR_SCHEMA)))]), None


BAR_CLASSES = {
    "null_field": b_null, "non_positive": b_non_positive, "ohlc_inconsistent": b_ohlc_inconsistent,
    "duplicate_hour": b_duplicate_hour, "missing_hour": b_missing_hour,
    "misaligned_time": b_misaligned_time, "price_spike": b_price_spike, "unit_error": b_unit_error,
    "volume_spike": b_volume_spike, "type_drift": b_type_drift, "missing_column": b_missing_column,
}


# ---- harness --------------------------------------------------------------

def run_table(clean, checker, classes, trials, rng):
    pre = q.flagged_rows(checker(clean))
    banned = {r + d for r in pre for d in (-2, -1, 0, 1, 2)}
    results = {}
    for name, inject in classes.items():
        caught = as_error = 0
        for _ in range(trials):
            while True:
                k = int(rng.integers(0, len(clean)))
                if k in banned:
                    continue
                res = inject(clean, k, rng, banned) if inject is t_feed_gap else inject(clean, k, rng)
                if res is not None:
                    break
            df, target = res
            findings = checker(df)
            if target is None:
                hit = [f for f in findings if f.check.endswith(".schema")]
            else:
                hit = [f for f in findings if target in f.rows]
            caught += bool(hit)
            as_error += any(f.is_error for f in hit)
        results[name] = (trials, caught, as_error)
    return pre, results


def report(title, n_rows, pre_findings, results) -> str:
    lines = [f"{title}  ({n_rows:,} real rows; clean run: {len(pre_findings)} finding(s))"]
    for f in pre_findings:
        lines.append(f"  pre-existing  {f.check}: {len(f.rows)} row(s) -- {f.detail}")
    lines.append(f"  {'class':<22}{'injected':>9}{'caught':>8}{'recall':>9}{'as error':>10}")
    for name, (n, c, e) in results.items():
        lines.append(f"  {name:<22}{n:>9}{c:>8}{c / n:>9.1%}{e / n:>10.1%}")
    n, c = sum(r[0] for r in results.values()), sum(r[1] for r in results.values())
    lines.append(f"  {'all':<22}{n:>9}{c:>8}{c / n:>9.1%}")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--trials", type=int, default=40, help="injections per class (default 40)")
    ap.add_argument("--seed", type=int, default=20260911)
    args = ap.parse_args()
    rng = np.random.default_rng(args.seed)

    ticks = duckdb.sql(f"SELECT * FROM read_json_auto('{ROOT.as_posix()}/data/raw/ticks/**/*.jsonl', "
                       "hive_partitioning = false)").df()
    bars = duckdb.sql(f"SELECT time, symbol, open, high, low, close, volume FROM "
                      f"read_json_auto('{ROOT.as_posix()}/src/tracker/candles_1h.jsonl') ORDER BY time").df()

    t_pre, t_res = run_table(ticks, q.check_ticks, TICK_CLASSES, args.trials, rng)
    b_pre, b_res = run_table(bars, q.check_bars, BAR_CLASSES, args.trials, rng)

    print(report("TICKS", len(ticks), q.check_ticks(ticks), t_res))
    print()
    print(report("BARS", len(bars), q.check_bars(bars), b_res))
    everything = list(t_res.values()) + list(b_res.values())
    n, c = sum(r[0] for r in everything), sum(r[1] for r in everything)
    print(f"\nOVERALL  {c} of {n} injected errors caught: {c / n:.1%}   (seed {args.seed})")


if __name__ == "__main__":
    main()
