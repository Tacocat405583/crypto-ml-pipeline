"""Knobs for the modelling pipeline."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# Hourly OHLCV bars exported by pipeline/build_warehouse.py, one Hive partition
# per symbol. Run that first; this directory is generated, not committed.
BARS_DIR = ROOT / "data" / "warehouse" / "bars"

TICKERS = ["BTC-USD"]

HORIZON = 1          # bars ahead we try to predict
TRAIN_FRACTION = 0.8  # chronological split, no shuffling
RANDOM_STATE = 42
