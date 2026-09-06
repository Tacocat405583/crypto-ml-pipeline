from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

import config
import features as feat_lib


@dataclass
class Split:
    X_train: np.ndarray
    X_test: np.ndarray
    y_train: np.ndarray
    y_test: np.ndarray
    test_index: pd.DatetimeIndex
    feature_names: list


def make_split(feat: pd.DataFrame) -> Split:
    cols = feat_lib.feature_columns(feat)
    X = feat[cols].values
    y = feat["target"].values

    n_train = int(len(feat) * config.TRAIN_FRACTION)

    X_train_raw, X_test_raw = X[:n_train], X[n_train:]
    y_train, y_test = y[:n_train], y[n_train:]
    test_index = feat.index[n_train:]

    scaler = StandardScaler().fit(X_train_raw)
    X_train = scaler.transform(X_train_raw)
    X_test = scaler.transform(X_test_raw)

    return Split(X_train, X_test, y_train, y_test, test_index, cols)


def baseline_zero(split: Split) -> np.ndarray:
    return np.zeros_like(split.y_test)


def baseline_last_week(split: Split, feat: pd.DataFrame) -> np.ndarray:
    n_train = len(split.y_train)
    return feat["ret_1w"].values[n_train:]


def train_ridge(split: Split) -> np.ndarray:
    model = Ridge(alpha=10.0)
    model.fit(split.X_train, split.y_train)
    return model.predict(split.X_test)


def train_random_forest(split: Split) -> tuple:
    model = RandomForestRegressor(
        n_estimators=300,
        max_depth=4,
        min_samples_leaf=25,
        max_features="sqrt",
        random_state=config.RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(split.X_train, split.y_train)
    return model.predict(split.X_test), model


def evaluate(name: str, y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    r2 = float(r2_score(y_true, y_pred))

    has_view = y_pred != 0
    if has_view.sum() > 0:
        dir_acc = float(np.mean(np.sign(y_pred[has_view]) == np.sign(y_true[has_view])))
    else:
        dir_acc = float("nan")

    if np.std(y_pred) > 0:
        ic = float(np.corrcoef(y_pred, y_true)[0, 1])
    else:
        ic = float("nan")

    return {
        "model": name,
        "RMSE": rmse,
        "MAE": mae,
        "R2": r2,
        "DirAcc": dir_acc,
        "IC": ic,
    }
