"""Backfill historical hourly candles into candles_1h.jsonl.

One-shot script, not scheduled. Run it again later and it will re-fetch and
rewrite the file — candles are immutable once closed, so that is safe.

Output is one JSON object per line, oldest first:
    {"time": "<UTC ISO8601>", "symbol": ..., "open":, "high":, "low":, "close":, "volume":}
"""

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

SYMBOL = "BTC-USD"
GRANULARITY = 3600          # seconds per candle — 3600 = hourly
DAYS_BACK = 365
WINDOW = 300                # candles per request; Coinbase caps around 300
ATTEMPTS = 3
PAUSE = 0.35                # between requests, to stay well under the rate limit

url = f"https://api.exchange.coinbase.com/products/{SYMBOL}/candles"
file_path = Path(__file__).parent / "candles_1h.jsonl"


def fetch_window(start, end):
    # returns a list of raw candle rows, or None if every attempt failed
    params = {
        "granularity": GRANULARITY,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }

    for i in range(ATTEMPTS):
        try:
            response = requests.get(url, params=params, timeout=(5, 10))

            if response.status_code == 200:
                return response.json()

            print(f"HTTP {response.status_code} for {start.date()}", file=sys.stderr)
        except requests.RequestException as e:
            print(f"{type(e).__name__}: {e}", file=sys.stderr)

        if i < ATTEMPTS - 1:
            time.sleep(5)

    return None


def backfill(days_back=DAYS_BACK):
    # keyed by epoch so overlapping windows collapse instead of duplicating
    candles = {}
    end = datetime.now(timezone.utc)
    floor = end - timedelta(days=days_back)

    while end > floor:
        start = max(end - timedelta(seconds=GRANULARITY * WINDOW), floor)

        rows = fetch_window(start, end)
        if rows is None:
            print("giving up on this window, stopping", file=sys.stderr)
            break

        if not rows:
            print(f"no candles before {end.date()}, stopping", file=sys.stderr)
            break

        for row in rows:
            # [time, low, high, open, close, volume]
            candles[row[0]] = row

        print(f"{start.date()} -> {end.date()}  {len(rows):4} rows  ({len(candles)} total)")

        end = start
        time.sleep(PAUSE)

    return candles


def write_jsonl(candles, path=file_path):
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for epoch in sorted(candles):
            _, low, high, open_, close, volume = candles[epoch]
            record = {
                "time": datetime.fromtimestamp(epoch, timezone.utc).isoformat(),
                "symbol": SYMBOL,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
            f.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    days = int(sys.argv[1]) if len(sys.argv) > 1 else DAYS_BACK

    candles = backfill(days)
    if not candles:
        print("nothing fetched", file=sys.stderr)
        sys.exit(1)

    write_jsonl(candles)
    print(f"wrote {len(candles)} candles to {file_path}")
