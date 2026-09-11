"""Next-hour volatility forecasting, evaluated walk-forward.

Next-hour returns are the wrong target: main.py's models tie the zero-return
baseline (MODEL_RESULTS.md). Volatility is the forecastable quantity -- it
clusters, it mean-reverts, it has a daily cycle -- and it is what a risk system
actually consumes.

Target: the Parkinson volatility of the next hour, ln(high/low) / sqrt(4 ln 2).
It uses the whole bar rather than two closes, so it is a far less noisy
per-hour estimate than |return|.

Protocol, fixed before any result was seen:
  - features use only bars at or before hour t; the target is hour t+1, and a
    row whose next bar is not exactly one hour later is dropped (the gaps)
  - the last 20% of hours are the test period, cut into weekly folds; each
    fold trains on every hour before it and predicts the week -- an expanding
    window, retrained weekly, never shuffled and never refit on test data
  - models fit log volatility and predict exp() of it: the target is heavily
    right-skewed, and exp of a log-space fit targets the median, which is
    what MAE rewards
  - hyperparameters are fixed below and were not tuned on the test period
  - the headline model is the gradient-boosted one, chosen in advance; the
    others are reported so the comparison is visible rather than picked from

Two baselines: persistence (next hour = this hour), and the trailing 24-hour
mean, which is the harder one to beat.

    uv run python notebooks/volatility.py
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

import config
import data

PARKINSON = 1 / np.sqrt(4 * np.log(2))
FOLD_HOURS = 7 * 24
FLOOR = 1e-5          # a bar with high == low has zero range; log needs a floor

FEATURES = ["lvol_1h", "lvol_6h", "lvol_24h", "lvol_168h", "absret_1h", "ret_1h", "ret_24h",
            "lvolume_rel_24h", "hour_sin", "hour_cos", "weekend"]
HAR = ["lvol_1h", "lvol_24h", "lvol_168h"]   # the classic heterogeneous-horizon regression


def parkinson(df: pd.DataFrame) -> pd.Series:
    return (np.log(df["high"] / df["low"]) * PARKINSON).clip(lower=FLOOR)


def build(df: pd.DataFrame) -> pd.DataFrame:
    """One row per hour: features known at the close of hour t, target t+1.

    The last hour keeps a NaN target -- it is the row the live forecast is made
    from. Everything here looks backwards: rolling windows end at t, and the
    only forward shift is the target itself.
    """
    v = parkinson(df)
    logc = np.log(df["close"])
    lvolume = np.log(df["volume"])
    hour = df.index.hour

    out = pd.DataFrame(index=df.index)
    out["vol_1h"] = v                                   # raw, for the baselines
    out["vol_24h"] = v.rolling(24).mean()
    out["lvol_1h"] = np.log(v)
    for h in (6, 24, 168):
        out[f"lvol_{h}h"] = np.log(v.rolling(h).mean())
    out["ret_1h"] = logc.diff()
    out["absret_1h"] = out["ret_1h"].abs()
    out["ret_24h"] = logc.diff(24)
    out["lvolume_rel_24h"] = lvolume - lvolume.rolling(24).mean()
    out["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    out["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    out["weekend"] = (df.index.dayofweek >= 5).astype(float)

    step = df.index.to_series().shift(-1) - df.index.to_series()
    out["target"] = v.shift(-1).where(step == pd.Timedelta(hours=1))
    out.loc[out.index[-1], "target"] = np.nan
    return out.dropna(subset=FEATURES + ["vol_24h"])


def models() -> dict:
    return {
        "har_ols": (HAR, LinearRegression()),
        "ridge": (FEATURES, make_pipeline(StandardScaler(), Ridge(alpha=1.0))),
        "gbm": (FEATURES, HistGradientBoostingRegressor(
            max_iter=300, learning_rate=0.05, max_leaf_nodes=31, random_state=config.RANDOM_STATE)),
    }


def walk_forward(feat: pd.DataFrame) -> pd.DataFrame:
    """Out-of-sample predictions for every test hour, one weekly fold at a time."""
    feat = feat.dropna(subset=["target"])
    test_start = int(len(feat) * config.TRAIN_FRACTION)
    out = feat.iloc[test_start:][["target"]].copy()
    out["persistence"] = feat["vol_1h"].iloc[test_start:]
    out["mean_24h"] = feat["vol_24h"].iloc[test_start:]
    out["fold"] = (np.arange(len(out)) // FOLD_HOURS).astype(int)

    for start in range(test_start, len(feat), FOLD_HOURS):
        train, test = feat.iloc[:start], feat.iloc[start:start + FOLD_HOURS]
        for name, (cols, model) in models().items():
            model.fit(train[cols], np.log(train["target"]))
            out.loc[test.index, name] = np.exp(model.predict(test[cols]))
    return out


def score(pred: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for name in ("persistence", "mean_24h", "har_ols", "ridge", "gbm"):
        mae = (pred[name] - pred["target"]).abs().mean()
        rows.append({"model": name, "MAE": mae})
    table = pd.DataFrame(rows).set_index("model")
    for base in ("persistence", "mean_24h"):
        table[f"vs {base}"] = 1 - table["MAE"] / table.loc[base, "MAE"]
    return table


def forecast_next(feat: pd.DataFrame) -> pd.DataFrame:
    """Refit the headline model on every labelled hour and forecast the next one."""
    cols, model = models()["gbm"]
    labelled = feat.dropna(subset=["target"])
    model.fit(labelled[cols], np.log(labelled["target"]))
    last = feat.iloc[[-1]]
    return pd.DataFrame({"as_of": last.index,                      # last bar the forecast used
                         "for_hour": last.index + pd.Timedelta(hours=1),
                         "vol_1h_forecast": np.exp(model.predict(last[cols])),
                         "vol_1h_last": last["vol_1h"].values})


def main() -> None:
    pd.set_option("display.float_format", lambda x: f"{x:0.5f}")
    for ticker in config.TICKERS:
        feat = build(data.load_prices(ticker))
        pred = walk_forward(feat)
        table = score(pred)

        err = pd.DataFrame({"gbm": (pred["gbm"] - pred["target"]).abs(),
                            "persistence": (pred["persistence"] - pred["target"]).abs(),
                            "fold": pred["fold"]}).groupby("fold").mean()
        weekly = err["gbm"] < err["persistence"]
        print("=" * 62)
        print(f"{ticker}  next-hour Parkinson volatility, walk-forward")
        print("=" * 62)
        print(f"test {pred.index[0]:%Y-%m-%d} -> {pred.index[-1]:%Y-%m-%d} | {len(pred)} hours | "
              f"{pred['fold'].nunique()} weekly folds | train grows from {len(feat) - len(pred)} hours")
        print()
        print(table.to_string(formatters={"vs persistence": "{:+.1%}".format, "vs mean_24h": "{:+.1%}".format}))
        print(f"\ngbm beats persistence in {weekly.sum()} of {len(weekly)} weekly folds")

        out = config.ROOT / "data" / "warehouse" / "forecasts"
        out.mkdir(parents=True, exist_ok=True)
        pred.assign(symbol=ticker).to_parquet(out / "vol_walkforward.parquet")
        nxt = forecast_next(feat).assign(symbol=ticker)
        nxt.to_parquet(out / "vol_next.parquet")
        print(f"\nnext hour {nxt['for_hour'].iloc[0]:%Y-%m-%d %H:%M} UTC: "
              f"forecast {nxt['vol_1h_forecast'].iloc[0]:.5f} (last hour {nxt['vol_1h_last'].iloc[0]:.5f})")


if __name__ == "__main__":
    main()
