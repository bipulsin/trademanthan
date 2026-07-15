"""Tests for full-universe VWAP slope sweep (research-only)."""
from datetime import datetime

import pytz

from backend.services.kavach_universe_vwap_scan import (
    _direction_from_slope,
    _rth_5m_timestamps,
    _score_row,
    _truncate_candles,
)
from backend.services.rs_vwap_quality import vwap_extension_pct
from backend.test_rs_vwap_quality import _trend_candles

IST = pytz.timezone("Asia/Kolkata")


def test_direction_from_slope():
    assert _direction_from_slope(0.5) == "LONG"
    assert _direction_from_slope(-0.1) == "SHORT"
    assert _direction_from_slope(0.0) == "LONG"


def test_rth_5m_timestamps_count():
    from datetime import date

    stamps = _rth_5m_timestamps(date(2026, 7, 15))
    assert stamps[0].hour == 9 and stamps[0].minute == 20
    assert stamps[-1].hour == 15 and stamps[-1].minute == 25
    # 09:20..15:25 inclusive every 5m = 74 bars
    assert len(stamps) == 74


def test_truncate_candles():
    candles = _trend_candles(40, up=True)
    mid = _truncate_candles(
        candles, IST.localize(datetime(2026, 7, 14, 10, 0))
    )
    assert len(mid) < len(candles)
    assert len(mid) > 0


def test_score_row_shape():
    candles = _trend_candles(90, up=True)
    row = _score_row(
        candles,
        atr_pct=0.25,
        in_lock=False,
        session_date="2026-07-15",
        symbol="policybzr",
        source="live",
    )
    assert row is not None
    assert row["symbol"] == "POLICYBZR"
    assert row["in_lock_at_time"] is False
    assert row["source"] == "live"
    assert "vwap_slope_score" in row
    assert "vwap_extension_pct" in row
    assert row["direction"] in ("LONG", "SHORT")
    # Extension helper still works standalone
    assert vwap_extension_pct(candles) is not None


def test_score_row_marks_in_lock():
    candles = _trend_candles(90, up=True)
    row = _score_row(
        candles,
        atr_pct=0.25,
        in_lock=True,
        session_date="2026-07-15",
        symbol="ABB",
        source="backfill",
        logged_at=IST.localize(datetime(2026, 7, 15, 11, 0)),
    )
    assert row["in_lock_at_time"] is True
    assert row["source"] == "backfill"
    assert row["logged_at"] is not None
