"""Price loading.

The video pulled weekly bars from Alpha Vantage; we already have a year of
hourly BTC-USD candles on disk, so this reads those instead of hitting an API.
"""

import pandas as pd

import config


def load_prices(ticker: str) -> pd.DataFrame:
    """Return an OHLCV frame indexed by UTC timestamp, oldest first."""
    df = pd.read_json(config.CANDLES_PATH, lines=True)
    df = df[df["symbol"] == ticker]

    if df.empty:
        raise ValueError(f"no rows for {ticker} in {config.CANDLES_PATH}")

    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.sort_values("time").drop_duplicates("time").set_index("time")
    return df[["open", "high", "low", "close", "volume"]]


if __name__ == "__main__":
    d = load_prices(config.TICKERS[0])
    print("rows:", len(d))
    print(d.tail(3))
