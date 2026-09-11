"""Price loading.

The video pulled weekly bars from Alpha Vantage; we already have a year of
hourly BTC-USD bars on disk, so this reads those instead of hitting an API.
They come from the Parquet that pipeline/build_warehouse.py exports -- the
only place this module knows data comes from.
"""

import duckdb
import pandas as pd

import config


def load_prices(ticker: str) -> pd.DataFrame:
    """Return an OHLCV frame indexed by UTC timestamp, oldest first."""
    df = duckdb.sql(f"SELECT * FROM read_parquet('{config.BARS_DIR.as_posix()}/**/*.parquet')").df()
    df = df[df["symbol"] == ticker]

    if df.empty:
        raise ValueError(f"no rows for {ticker} in {config.BARS_DIR}")

    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.sort_values("time").drop_duplicates("time").set_index("time")
    return df[["open", "high", "low", "close", "volume"]]


if __name__ == "__main__":
    d = load_prices(config.TICKERS[0])
    print("rows:", len(d))
    print(d.tail(3))
