"""Tests for Vajra Executable Entry Score (EES)."""
from backend.services.vajra.ees import (
    EES_ENTER_MIN,
    ENTRY_EXECUTABLE,
    TPS_ENTER_MIN,
    classify_entry_state,
    compute_ees,
    enter_action_label,
)
from test_vajra_engine import _synthetic_candles


def test_classify_entry_state_bands():
    assert classify_entry_state(80) == ENTRY_EXECUTABLE
    assert classify_entry_state(70) == "PULLBACK PREFERRED"
    assert classify_entry_state(50) == "WATCHLIST ONLY"
    assert classify_entry_state(30) == "AVOID CHASING"


def test_compute_ees_returns_score():
    candles = _synthetic_candles(90, trend=0.25)
    out = compute_ees(candles, bull_dir=True, tps_score=85)
    assert out is not None
    assert 0 <= out.ees_score <= 100
    assert out.entry_state
    assert isinstance(out.ees_alerts, list)


def test_enter_action_requires_tps_and_ees():
    hi = enter_action_label(tps_score=TPS_ENTER_MIN + 5, ees_score=EES_ENTER_MIN + 5, entry_state=ENTRY_EXECUTABLE)
    assert hi["action"] == "ENTER"
    assert hi["enabled"] is True

    low = enter_action_label(tps_score=90, ees_score=40, entry_state="AVOID CHASING")
    assert low["enabled"] is False
    assert low["action"] in ("EXTENDED", "WATCH")


def test_sort_vajra_rows_for_display_entry_state_then_quality():
    from backend.services.vajra.ranking import sort_vajra_rows_for_display

    rows = [
        {"stock": "A", "entry_state": "WATCHLIST", "trade_quality_score": 90},
        {"stock": "B", "entry_state": "EXECUTABLE", "trade_quality_score": 50},
        {"stock": "C", "entry_state": "EXECUTABLE", "trade_quality_score": 80},
        {"stock": "D", "entry_state": "REJECT", "trade_quality_score": 99},
    ]
    out = sort_vajra_rows_for_display(rows)
    assert [r["stock"] for r in out] == ["C", "B", "A", "D"]
