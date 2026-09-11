"""Guards on the volatility forecast: no look-ahead, and no training on test hours.

The headline number is only worth quoting if both hold, so both are tests
rather than one-off checks. Runs on synthetic bars with volatility clustering,
so it needs no data on disk.
"""

import numpy as np
import pandas as pd
import pytest

import volatility as v


@pytest.fixture(scope="module")
def prices() -> pd.DataFrame:
    rng = np.random.default_rng(3)
    n = 24 * 120
    sigma = np.empty(n)
    sigma[0] = 0.004
    for i in range(1, n):                       # GARCH-ish: volatility clusters
        sigma[i] = np.sqrt(1e-6 + 0.9 * sigma[i - 1] ** 2 + 0.08 * (sigma[i - 1] * rng.normal()) ** 2)
    close = 60_000 * np.exp(np.cumsum(sigma * rng.normal(size=n)))
    open_ = np.r_[close[0], close[:-1]]
    spread = sigma * np.abs(rng.normal(1.0, 0.3, n))
    return pd.DataFrame({
        "open": open_, "close": close,
        "high": np.maximum(open_, close) * np.exp(spread / 2),
        "low": np.minimum(open_, close) * np.exp(-spread / 2),
        "volume": rng.uniform(20, 200, n),
    }, index=pd.date_range("2026-01-01", periods=n, freq="h", tz="UTC"))


def test_features_never_look_ahead(prices):
    k = len(prices) // 2
    future_scrambled = prices.copy()
    future_scrambled.iloc[k + 1:] = future_scrambled.iloc[k + 1:].sample(frac=1, random_state=0).values
    cut = prices.index[k]
    cols = v.FEATURES + ["vol_1h", "vol_24h"]
    a, b = v.build(prices).loc[:cut, cols], v.build(future_scrambled).loc[:cut, cols]
    pd.testing.assert_frame_equal(a, b)


def test_target_is_the_next_hour_and_gaps_are_dropped(prices):
    gappy = prices.drop(index=prices.index[500])
    feat = v.build(gappy)
    expected = v.parkinson(gappy).shift(-1)
    labelled = feat["target"].dropna()
    assert np.allclose(labelled, expected.loc[labelled.index])
    assert pd.isna(feat.loc[gappy.index[499], "target"])     # its next bar is two hours on
    assert pd.notna(feat.loc[gappy.index[498], "target"])    # one step earlier is untouched


class Recorder:
    """Stands in for a model and remembers the latest hour it was trained on."""
    seen: list = []

    def fit(self, X, y):
        Recorder.seen.append(X.index.max())
        return self

    def predict(self, X):
        Recorder.seen.append(("predict", X.index.min()))
        return np.zeros(len(X))


def test_walk_forward_never_trains_on_an_hour_it_predicts(prices, monkeypatch):
    Recorder.seen = []
    monkeypatch.setattr(v, "models", lambda: {n: (v.FEATURES, Recorder()) for n in ("har_ols", "ridge", "gbm")})
    v.walk_forward(v.build(prices))
    fits = [s for s in Recorder.seen if not isinstance(s, tuple)]
    predicts = [s[1] for s in Recorder.seen if isinstance(s, tuple)]
    assert len(fits) == len(predicts) > 3
    assert all(last_train < first_test for last_train, first_test in zip(fits, predicts))


def test_models_beat_persistence_on_clustered_volatility(prices):
    table = v.score(v.walk_forward(v.build(prices)))
    assert table.loc["gbm", "vs persistence"] > 0
    assert table.loc["har_ols", "vs persistence"] > 0
