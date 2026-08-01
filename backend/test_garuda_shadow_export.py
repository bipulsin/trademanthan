"""Unit tests for Garuda shadow export helpers (no live DB)."""
from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytz

from backend.services.garuda_screener.export import (
    SCHEMA_VERSION,
    _forward_bars_for,
    _grade_points_for,
    _nearest_rs,
    build_data_completeness,
)

IST = pytz.timezone("Asia/Kolkata")


def _dt(s: str) -> datetime:
    return IST.localize(datetime.fromisoformat(s))


def test_schema_version_stable():
    assert SCHEMA_VERSION == 1


def test_nearest_rs_within_window():
    bar_end = _dt("2026-08-01T10:15:00")
    rows = [
        SimpleNamespace(
            symbol="RELIANCE",
            scan_time=_dt("2026-08-01T10:10:00"),
            ranking_type="BULLISH",
            rank_position=3,
            relative_strength=1.5,
            stock_percent=2.0,
            nifty_percent=0.5,
            current_price=1400.0,
            previous_close=1380.0,
            trade_score=82.0,
            confidence_grade="B",
            kavach_state="READY",
            kavach_strength=2,
            volume_label="High",
            volume_ratio=1.8,
            volume_tod_ratio=1.2,
            vwap_purity_pct=70.0,
            market_regime="TREND",
            adx=28.0,
        ),
        SimpleNamespace(
            symbol="RELIANCE",
            scan_time=_dt("2026-08-01T11:00:00"),  # outside ±20m
            ranking_type="BULLISH",
            rank_position=1,
            relative_strength=2.0,
            stock_percent=None,
            nifty_percent=None,
            current_price=1410.0,
            previous_close=None,
            trade_score=90.0,
            confidence_grade="A",
            kavach_state="READY",
            kavach_strength=3,
            volume_label="High",
            volume_ratio=2.0,
            volume_tod_ratio=None,
            vwap_purity_pct=80.0,
            market_regime="TREND",
            adx=30.0,
        ),
    ]
    out = _nearest_rs(rows, "reliance", bar_end)
    assert out is not None
    assert out["matched"] is True
    assert out["in_top10"] is True
    assert out["rank_position"] == 3
    assert out["confidence_grade"] == "B"
    assert out["match_delta_seconds"] == 300


def test_nearest_rs_miss_when_outside_window():
    bar_end = _dt("2026-08-01T10:15:00")
    rows = [
        SimpleNamespace(
            symbol="RELIANCE",
            scan_time=_dt("2026-08-01T11:00:00"),
            ranking_type="BULLISH",
            rank_position=1,
            relative_strength=2.0,
            stock_percent=None,
            nifty_percent=None,
            current_price=1410.0,
            previous_close=None,
            trade_score=90.0,
            confidence_grade="A",
            kavach_state="READY",
            kavach_strength=3,
            volume_label="High",
            volume_ratio=2.0,
            volume_tod_ratio=None,
            vwap_purity_pct=80.0,
            market_regime="TREND",
            adx=30.0,
        )
    ]
    assert _nearest_rs(rows, "RELIANCE", bar_end) is None


def test_grade_history_remainder_of_day_only():
    bar_end = _dt("2026-08-01T11:15:00")
    audit = [
        SimpleNamespace(
            session_date="2026-08-01",
            symbol="TCS",
            at=_dt("2026-08-01T10:15:00"),  # before qual — drop
            trade_score=70,
            confidence_grade="C",
            kavach_state="WATCH",
            price=3200.0,
            volume_label="Low",
            vwap_purity_pct=40.0,
            market_regime="RANGE",
            adx=18.0,
        ),
        SimpleNamespace(
            session_date="2026-08-01",
            symbol="TCS",
            at=_dt("2026-08-01T12:15:00"),
            trade_score=88,
            confidence_grade="B",
            kavach_state="READY",
            price=3220.0,
            volume_label="High",
            vwap_purity_pct=75.0,
            market_regime="TREND",
            adx=27.0,
        ),
    ]
    rs = [
        SimpleNamespace(
            symbol="TCS",
            scan_time=_dt("2026-08-01T13:00:00"),
            trade_score=91.0,
            confidence_grade="A",
            kavach_state="READY",
            current_price=3230.0,
            ranking_type="BULLISH",
            rank_position=2,
            relative_strength=1.1,
        )
    ]
    out = _grade_points_for(
        symbol="TCS",
        session_date="2026-08-01",
        bar_end=bar_end,
        audit_rows=audit,
        component_rows=[],
        rs_rows=rs,
    )
    assert len(out) == 2
    assert out[0]["confidence_grade"] == "B"
    assert out[0]["source"] == "rs_live_kavach_audit"
    assert out[1]["confidence_grade"] == "A"
    assert out[1]["source"] == "relative_strength_snapshot"


def test_forward_bars_use_garuda_price_as_close():
    bar_end = _dt("2026-08-01T10:15:00")
    prices = [
        SimpleNamespace(
            session_date="2026-08-01",
            symbol="INFY",
            bar_end=_dt("2026-08-01T10:15:00"),
            price=1500.0,
        ),
        SimpleNamespace(
            session_date="2026-08-01",
            symbol="INFY",
            bar_end=_dt("2026-08-01T10:25:00"),
            price=1505.0,
        ),
        SimpleNamespace(
            session_date="2026-08-01",
            symbol="INFY",
            bar_end=_dt("2026-08-01T10:05:00"),  # before — drop
            price=1490.0,
        ),
    ]
    bars = _forward_bars_for(
        symbol="INFY",
        session_date="2026-08-01",
        bar_end=bar_end,
        price_rows=prices,
        cache_ohlc=None,
    )
    assert len(bars) == 2
    assert bars[0]["close"] == 1500.0
    assert bars[0]["open"] is None
    assert bars[0]["source"] == "garuda_screener_log_price"
    assert bars[1]["close"] == 1505.0


def test_data_completeness_flags_missing_joins():
    completeness = build_data_completeness(
        qualifier={
            "price": 100.0,
            "side": "LONG",
            "rank_score": 80.0,
            "top6_rank": 1,
            "direction_side": "LONG",
        },
        rs_top10=None,
        grade_history=[],
        forward_bars=[{"bar_end": "x", "open": None, "high": None, "low": None, "close": 101.0}],
    )
    assert completeness["complete"] is False
    assert "rs_top10" in completeness["missing_fields"]
    assert "grade_history" in completeness["missing_fields"]
    assert "forward_bars.ohlc" in completeness["missing_fields"]


def test_data_completeness_complete_when_all_present():
    completeness = build_data_completeness(
        qualifier={
            "price": 100.0,
            "side": "LONG",
            "rank_score": 80.0,
            "top6_rank": 1,
            "direction_side": "LONG",
        },
        rs_top10={
            "matched": True,
            "rank_position": 2,
            "relative_strength": 1.0,
            "trade_score": 85.0,
            "confidence_grade": "B",
        },
        grade_history=[{"at": "t", "trade_score": 85.0, "confidence_grade": "B"}],
        forward_bars=[
            {
                "bar_end": "t",
                "open": 99.0,
                "high": 101.0,
                "low": 98.0,
                "close": 100.5,
                "volume": 1000.0,
            }
        ],
    )
    assert completeness["complete"] is True
    assert completeness["missing_fields"] == []
