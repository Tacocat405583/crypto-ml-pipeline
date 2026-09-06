"""Run the pipeline end to end and print out-of-sample metrics."""

import warnings

import pandas as pd

import config
import data
import features as feat_lib
import model as model_lib

warnings.simplefilter("ignore", category=FutureWarning)
pd.set_option("display.float_format", lambda v: f"{v:0.4f}")


def run(ticker: str) -> None:
    df = data.load_prices(ticker)
    feat = feat_lib.build_features(df)
    split = model_lib.make_split(feat)

    print("=" * 62)
    print(ticker)
    print("=" * 62)
    print(
        f"[{ticker}] {len(feat)} usable rows | "
        f"train={len(split.y_train)} test={len(split.y_test)} | "
        f"test window {split.test_index[0]:%Y-%m-%d} -> {split.test_index[-1]:%Y-%m-%d}"
    )

    rf_pred, rf_model = model_lib.train_random_forest(split)

    rows = [
        model_lib.evaluate("baseline_zero", split.y_test, model_lib.baseline_zero(split)),
        model_lib.evaluate("baseline_last_week", split.y_test, model_lib.baseline_last_week(split, feat)),
        model_lib.evaluate("ridge", split.y_test, model_lib.train_ridge(split)),
        model_lib.evaluate("random_forest", split.y_test, rf_pred),
    ]

    print("\n--- Out-of-sample metrics (test set the model never saw) ---")
    print(pd.DataFrame(rows).set_index("model"))

    print("\n--- Random forest: top feature importances ---")
    importances = pd.Series(rf_model.feature_importances_, index=split.feature_names)
    print(importances.sort_values(ascending=False).head(6))


if __name__ == "__main__":
    for ticker in config.TICKERS:
        run(ticker)
