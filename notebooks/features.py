"""Feature engineering.

The `_1w` / `_10w` / `_20w` suffixes are the video's names, kept as-is. Our bars
are hourly, so read them as 1 / 10 / 20 *bars* — everything is measured in bars,
not calendar weeks.
"""

import pandas as pd

import config


def _rsi(close: pd.Series, window: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.ewm(alpha=1 / window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / window, adjust=False).mean()

    return 100 - (100 / (1 + avg_gain / avg_loss))


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    ret = df["close"].pct_change()
    out["ret_1w"] = ret

    for lag in range(1, 6):
        out[f"ret_lag_{lag}"] = ret.shift(lag)

    for window in (5, 10, 20):
        out[f"mom_{window}w"] = df["close"].pct_change(window)

    for window in (10, 20):
        sma = df["close"].rolling(window).mean()
        out[f"close_over_sma_{window}"] = df["close"] / sma

    for window in (10, 20):
        out[f"vol_{window}w"] = ret.rolling(window).std()

    out["rsi_14"] = _rsi(df["close"], 14)

    out["rel_volume"] = df["volume"] / df["volume"].rolling(20).mean()

    out["target"] = ret.shift(-config.HORIZON)

    out = out.dropna()
    return out


def feature_columns(feat: pd.DataFrame) -> list:
    return [c for c in feat.columns if c != "target"]


if __name__ == "__main__":
    import data

    f = build_features(data.load_prices(config.TICKERS[0]))
    print("features:", feature_columns(f))
    print("rows:", len(f))
    print(f.tail(3))
