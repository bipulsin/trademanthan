"""Phase 2A tests — same-session targets, features v1, leakage, no training."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest
import pytz

from backend.services.sambhav.config import FEATURES_VERSION_V1, IST, SESSION_START
from backend.services.sambhav.features_v1 import (
    FEATURE_NAMES_V1,
    assert_no_lookahead_features_v1,
    assess_volume_availability,
    compute_features_v1,
)
from backend.services.sambhav.phase2a import (
    assert_same_session_target_no_overnight,
    build_phase2a_frame,
)
from backend.services.sambhav.source_guard import assert_feature_modules_do_not_mutate_source
from backend.services.sambhav.targets import (
    TARGET_EXCLUDE_TIMES,
    attach_same_session_targets,
    ternary_label,
)


def _ist(y, m, d, hh, mm):
    return IST.localize(datetime(y, m, d, hh, mm))


def _synthetic_session(day_offset: int = 0, n: int = 38):
    bars = []
    start = _ist(2025, 1, 2, 9, 15) + timedelta(days=day_offset)
    # skip weekends simply by using weekday offsets only in tests
    price = 22000.0 + day_offset
    for i in range(n):
        ts = start + timedelta(minutes=i * 10)
        o = price
        c = price + (1 if i % 2 == 0 else -0.5)
        h = max(o, c) + 2
        l = min(o, c) - 2
        bars.append(
            {
                "candle_start": ts,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": 0.0,
            }
        )
        price = c
    return bars


def test_same_session_excludes_last_three():
    bars = _synthetic_session()
    df = attach_same_session_targets(bars)
    assert set(df.loc[df["target_resolvable"], "candle_hm"]).isdisjoint(TARGET_EXCLUDE_TIMES)
    assert int(df["target_resolvable"].sum()) == 35  # 38 - 3
    # last resolvable is 14:55 → future 15:25
    last = df[df["target_resolvable"]].iloc[-1]
    assert last["candle_hm"] == "14:55"
    assert abs(last["future_close"] - bars[-1]["close"]) < 1e-9


def test_no_overnight_targets_across_days():
    bars = _synthetic_session(0) + _synthetic_session(1)
    df = attach_same_session_targets(bars)
    assert_same_session_target_no_overnight(df)
    # day1 last bars excluded; no cross-day future_close
    day0 = df[df["session_date"] == bars[0]["candle_start"].date()]
    assert day0[day0["target_resolvable"]]["future_close"].notna().all()


def test_features_v1_leakage():
    bars = []
    for d in range(3):
        bars.extend(_synthetic_session(d))
    assert_no_lookahead_features_v1(bars)


def test_volume_unavailable_when_zero():
    bars = _synthetic_session()
    from backend.services.sambhav.features_v1 import bars_to_dataframe

    info = assess_volume_availability(bars_to_dataframe(bars))
    assert info["volume_available"] is False


def test_feature_names_v1_count_and_no_volume_vwap():
    joined = ",".join(FEATURE_NAMES_V1)
    assert "vwap" not in joined.lower()
    assert "vol_z" not in joined
    assert "dollar_vol" not in joined
    assert len(FEATURE_NAMES_V1) >= 40
    assert FEATURES_VERSION_V1 == "sambhav_features_v1"


def test_phase2a_frame_research_keys():
    bars = []
    for d in range(5):
        bars.extend(_synthetic_session(d))
    out = build_phase2a_frame(bars)
    r = out["research"]
    assert r["usable_target_observations"] == 5 * 35
    assert r["excluded_no_30m_horizon"] == 5 * 3
    assert r["target_status"] == "UNDER RESEARCH"
    assert r["model_status"] == "NOT TRAINED"
    assert r["lookahead_tests"]["features_v1"] == "PASS"
    assert r["volume"]["volume_available"] is False
    assert r["vwap"]["available"] is False


def test_ternary_labels():
    assert ternary_label(0.003, 0.002) == "UP"
    assert ternary_label(-0.003, 0.002) == "DOWN"
    assert ternary_label(0.0005, 0.002) == "NEUTRAL"


def test_source_guard_includes_phase2a():
    repo = Path(__file__).resolve().parents[1]
    # phase2a must not mutate source OHLC tables
    from backend.services.sambhav.source_guard import scan_module_for_source_writes

    bad = scan_module_for_source_writes(repo / "backend/services/sambhav/phase2a.py")
    assert not bad, bad
    assert_feature_modules_do_not_mutate_source(repo)


def test_no_train_imports_in_phase2a():
    text = (Path(__file__).resolve().parents[1] / "backend/services/sambhav/phase2a.py").read_text()
    assert "xgboost" not in text.lower()
    assert "train_and_save" not in text
