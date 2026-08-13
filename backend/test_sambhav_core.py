"""Core Sambhav unit tests — aggregation, timezone, features, leakage, targets, calibration."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pytest
import pytz

from backend.services.sambhav.calibration import ProbabilityCalibrator, calibration_buckets
from backend.services.sambhav.candles import (
    aggregate_1m_to_10m,
    candle_start_10m,
    in_session,
    to_ist,
    validate_ohlc,
)
from backend.services.sambhav.config import IST, SESSION_START
from backend.services.sambhav.features import assert_no_lookahead_features, compute_features, bars_to_dataframe
from backend.services.sambhav.targets import attach_targets
from backend.services.sambhav.walk_forward import walk_forward_splits


def _ist(y, m, d, hh, mm):
    return IST.localize(datetime(y, m, d, hh, mm))


def test_to_ist_and_session():
    assert to_ist("2026-04-13T09:15:00+05:30").hour == 9
    assert in_session(_ist(2026, 4, 13, 9, 15))
    assert in_session(_ist(2026, 4, 13, 15, 29))
    assert not in_session(_ist(2026, 4, 13, 9, 14))
    assert not in_session(_ist(2026, 4, 13, 15, 30))


def test_candle_start_0915_aligned_not_wall_clock():
    # Wall-clock 10m floor would put 09:18 → 09:10; Sambhav must use 09:15.
    assert candle_start_10m(_ist(2026, 4, 13, 9, 18)) == _ist(2026, 4, 13, 9, 15)
    assert candle_start_10m(_ist(2026, 4, 13, 9, 24)) == _ist(2026, 4, 13, 9, 15)
    assert candle_start_10m(_ist(2026, 4, 13, 9, 25)) == _ist(2026, 4, 13, 9, 25)
    assert candle_start_10m(_ist(2026, 4, 13, 15, 25)) == _ist(2026, 4, 13, 15, 25)
    assert candle_start_10m(_ist(2026, 4, 13, 9, 10)) is None


def test_aggregate_1m_to_10m_ohlc():
    base = _ist(2026, 4, 13, 9, 15)
    candles = []
    for i in range(10):
        ts = base + timedelta(minutes=i)
        o = 100 + i
        candles.append(
            {
                "timestamp": ts.isoformat(),
                "open": float(o),
                "high": float(o + 1),
                "low": float(o - 1),
                "close": float(o + 0.5),
                "volume": 10.0,
            }
        )
    # Second incomplete bucket
    for i in range(3):
        ts = base + timedelta(minutes=10 + i)
        candles.append(
            {
                "timestamp": ts.isoformat(),
                "open": 200.0,
                "high": 201.0,
                "low": 199.0,
                "close": 200.5,
                "volume": 1.0,
            }
        )
    complete = aggregate_1m_to_10m(candles, require_complete=True)
    assert len(complete) == 1
    row = complete[0]
    assert row["candle_start"] == base
    assert row["open"] == 100.0
    assert row["close"] == 109.5
    assert row["high"] == max(100 + i + 1 for i in range(10))
    assert row["low"] == min(100 + i - 1 for i in range(10))
    assert row["volume"] == 100.0
    assert row["n_1m"] == 10
    assert row["is_complete"] is True

    all_buckets = aggregate_1m_to_10m(candles, require_complete=False)
    assert len(all_buckets) == 2
    assert all_buckets[1]["is_complete"] is False


def test_validate_ohlc():
    assert validate_ohlc(100, 101, 99, 100.5)
    assert not validate_ohlc(100, 99, 101, 100)  # high < low
    assert not validate_ohlc(0, 1, 0.5, 1)


def _synthetic_bars(n: int = 80):
    bars = []
    start = _ist(2026, 4, 13, 9, 15)
    price = 22000.0
    for i in range(n):
        # Stay within session by wrapping days simply: only use morning buckets
        day = i // 20
        slot = i % 20
        ts = start + timedelta(days=day, minutes=slot * 10)
        if ts.time() >= SESSION_START and ts.hour < 15:
            o = price
            c = price + (1 if i % 3 else -1)
            h = max(o, c) + 2
            l = min(o, c) - 2
            bars.append(
                {
                    "candle_start": ts,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": 1000 + i,
                }
            )
            price = c
    return bars


def test_features_no_lookahead():
    bars = _synthetic_bars(60)
    assert_no_lookahead_features(bars)


def test_targets_horizon_3_bars():
    bars = _synthetic_bars(40)
    df = attach_targets(bars)
    assert "future_close" in df.columns
    assert "target_direction" in df.columns
    # Last 3 should be NaN targets
    assert df["future_close"].iloc[-3:].isna().all()
    # Spot-check middle
    i = 10
    assert df.loc[i, "future_close"] == df.loc[i + 3, "close"]
    expected = "UP" if df.loc[i + 3, "close"] > df.loc[i, "close"] else "DOWN"
    assert df.loc[i, "target_direction"] == expected


def test_calibration_platt_and_buckets():
    pytest.importorskip("sklearn")
    rng = np.random.default_rng(0)
    y = rng.integers(0, 2, size=200).astype(float)
    # Biased raw probs
    p = np.clip(0.3 + 0.4 * y + rng.normal(0, 0.1, size=200), 0.01, 0.99)
    cal = ProbabilityCalibrator("platt").fit(p, y)
    assert cal.fitted
    p2 = cal.transform(p)
    assert p2.shape == p.shape
    buckets = calibration_buckets(p2, y, n_bins=10)
    assert buckets["n"] == 200
    assert "ece" in buckets
    assert len(buckets["buckets"]) == 10


def test_walk_forward_splits_chronological():
    splits = walk_forward_splits(1000, train_bars=400, test_bars=100, step_bars=100, min_train=200)
    assert len(splits) > 0
    for tr, te in splits:
        assert tr.max() < te.min()  # no leakage across split
        assert len(tr) >= 200


def test_prediction_resolution_logic_unit():
    """future bar = candle_start + 30m (matches attach_targets shift -3 on 10m grid)."""
    from datetime import timedelta

    cs = _ist(2026, 4, 13, 10, 0)
    target = cs + timedelta(minutes=30)
    assert target == _ist(2026, 4, 13, 10, 30)


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("xgboost") is None,
    reason="xgboost not installed",
)
def test_xgb_smoke_fit():
    from backend.services.sambhav.models import make_xgb_classifier

    rng = np.random.default_rng(1)
    X = rng.normal(size=(120, 8))
    y = (X[:, 0] > 0).astype(int)
    clf = make_xgb_classifier(n_estimators=20, max_depth=2)
    clf.fit(X, y)
    p = clf.predict_proba_up(X[:5])
    assert p.shape == (5,)
    assert np.all((p >= 0) & (p <= 1))
