"""Tests for Vajra trade quality engine."""
from backend.services.vajra.actions import resolve_enter_action
from backend.services.vajra.ranking import sort_vajra_rows_for_display
from backend.services.vajra.trade_quality import (
    EXECUTABLE_CONFIDENCE_MIN,
    STATE_EXECUTABLE,
    STATE_REJECT,
    STATE_WATCHLIST,
    compute_trade_quality,
)
from test_vajra_engine import _synthetic_candles


def _candles(n: int = 90, trend: float = 0.3):
    return _synthetic_candles(n, trend=trend)


def test_resolve_enter_only_executable_high_confidence():
    enter = resolve_enter_action(entry_state=STATE_EXECUTABLE, confidence=80)
    assert enter["enter_enabled"] is True
    assert enter["enter_action"] == "ENTER"

    watch = resolve_enter_action(entry_state=STATE_WATCHLIST, confidence=80)
    assert watch["enter_enabled"] is False
    assert watch["enter_action"] == "ARMED"

    reject = resolve_enter_action(entry_state=STATE_REJECT, confidence=90)
    assert reject["enter_enabled"] is False
    assert reject["enter_action"] == ""

    low = resolve_enter_action(entry_state=STATE_WATCHLIST, confidence=EXECUTABLE_CONFIDENCE_MIN - 1)
    assert low["enter_enabled"] is False


def test_sort_executable_before_watchlist():
    rows = [
        {"stock": "A", "entry_state": STATE_WATCHLIST, "trade_quality_score": 90},
        {"stock": "B", "entry_state": STATE_EXECUTABLE, "trade_quality_score": 70},
        {"stock": "C", "entry_state": STATE_REJECT, "trade_quality_score": 99},
    ]
    out = sort_vajra_rows_for_display(rows)
    assert [r["stock"] for r in out] == ["B", "A", "C"]


def test_compute_trade_quality_returns_scores():
    c30 = _candles(90, trend=0.25)
    c5 = _candles(90, trend=0.2)
    tq = compute_trade_quality(
        candles_30m=c30,
        candles_5m=c5,
        bull_dir=True,
        market_phase="EXPANSION",
        execution_validated=True,
        structure_pass=True,
        momentum_pass=True,
        trend_pass=True,
    )
    assert tq is not None
    assert 0 <= tq.trade_quality_score <= 100
    assert tq.state in (STATE_EXECUTABLE, STATE_WATCHLIST, STATE_REJECT)
