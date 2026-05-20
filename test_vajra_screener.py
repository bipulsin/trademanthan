"""Screener display — qualification, top picks, UI mapping."""
from backend.services.vajra.market_phase_scoring import select_top_picks
from backend.services.vajra.ranking import build_screener_display
from backend.services.vajra.qualification_config import (
    STATE_ARMED,
    STATE_EXECUTABLE,
    STATE_REJECT,
)
from backend.services.vajra.ui_mapping import finalize_screener_row, normalize_qualification


def test_normalize_qualification_v2_states():
    assert normalize_qualification("EXECUTABLE") == STATE_EXECUTABLE
    assert normalize_qualification("ARMED") == STATE_ARMED
    assert normalize_qualification("DISCOVERY") == "DISCOVERY"
    assert normalize_qualification("PULLBACK PREFERRED") == STATE_ARMED
    assert normalize_qualification("REJECT") == STATE_REJECT


def test_finalize_executable_enter_only():
    row = finalize_screener_row(
        {
            "stock": "BEL",
            "entry_state": STATE_EXECUTABLE,
            "confidence": 82,
            "enter_action": "WATCH",
            "enter_enabled": False,
            "ees_score": 70,
        }
    )
    assert row["qualification"] == STATE_EXECUTABLE
    assert row["enter_action"] == "ENTER"
    assert row["enter_enabled"] is True
    assert row["setup_potential_score"] == 70


def test_finalize_armed_action():
    row = finalize_screener_row(
        {
            "stock": "LTM",
            "entry_state": STATE_ARMED,
            "confidence": 70,
            "blocker_label": "Awaiting breakout close",
        }
    )
    assert row["qualification"] == STATE_ARMED
    assert row["enter_action"] == "ARMED"
    assert row["enter_enabled"] is False


def test_finalize_reject_no_button():
    row = finalize_screener_row(
        {"stock": "X", "entry_state": STATE_REJECT, "trade_type": "REJECT", "enter_action": "REJECT"}
    )
    assert row["qualification"] == STATE_REJECT
    assert row["enter_action"] == ""


def test_top_picks_executable_only_no_watch_padding():
    rows = [
        {
            "stock": "A",
            "qualification_state": STATE_ARMED,
            "market_phase": "Rotational",
            "execution_rank_score": 600,
            "top8_phase_bucket": 3,
            "execution_bias": "LONG",
            "directional_conviction": True,
            "execution_score": 62,
            "conviction_score": 66,
            "discovery_score": 55,
        },
        {
            "stock": "B",
            "qualification_state": STATE_EXECUTABLE,
            "market_phase": "Bull Expansion",
            "execution_rank_score": 1100,
            "top8_phase_bucket": 1,
            "execution_bias": "LONG",
            "directional_conviction": True,
            "execution_score": 78,
            "conviction_score": 80,
            "discovery_score": 60,
        },
        {"stock": "C", "qualification_state": STATE_REJECT, "execution_rank_score": 9999},
        {
            "stock": "D",
            "qualification_state": STATE_EXECUTABLE,
            "market_phase": "Bull Expansion",
            "execution_rank_score": 1200,
            "top8_phase_bucket": 1,
            "execution_bias": "LONG",
            "directional_conviction": True,
            "execution_score": 80,
            "conviction_score": 82,
            "discovery_score": 62,
        },
    ]
    for r in rows:
        finalize_screener_row(r)
    picks, sections = select_top_picks(rows, n=8)
    symbols = [p["stock"] for p in picks]
    assert "C" not in symbols
    assert "A" not in symbols
    assert symbols[0] == "D"
    assert symbols[1] == "B"
    assert len(sections[STATE_ARMED]) == 1


def test_direction_from_scores_when_trade_type_reject():
    row = finalize_screener_row(
        {
            "stock": "NHPC",
            "entry_state": STATE_REJECT,
            "trade_type": "REJECT",
            "bull_score": 55.0,
            "bear_score": 15.0,
            "structure": "PASS",
            "momentum": "PASS",
            "trend": "PASS",
        }
    )
    assert row.get("execution_bias") in ("LONG", "SHORT")


def test_build_screener_display_groups():
    rows = [
        {
            "stock": "E",
            "entry_state": STATE_EXECUTABLE,
            "confidence": 80,
            "execution_bias": "LONG",
            "directional_conviction": True,
            "execution_score": 75,
            "conviction_score": 80,
            "discovery_score": 55,
        },
        {
            "stock": "W",
            "entry_state": STATE_ARMED,
            "confidence": 60,
            "execution_bias": "LONG",
            "directional_conviction": True,
            "execution_score": 62,
            "conviction_score": 66,
            "discovery_score": 52,
        },
    ]
    out = build_screener_display(rows, top_n=8)
    assert len(out["top_picks"]) == 1
    assert out["top_picks"][0]["stock"] == "E"
    assert out["groups"][STATE_EXECUTABLE][0]["stock"] == "E"
    assert out["banner"] is None
