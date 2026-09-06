"""Knobs for the modelling pipeline."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CANDLES_PATH = ROOT / "src" / "tracker" / "candles_1h.jsonl"

TICKERS = ["BTC-USD"]

HORIZON = 1          # bars ahead we try to predict
TRAIN_FRACTION = 0.8  # chronological split, no shuffling
RANDOM_STATE = 42
