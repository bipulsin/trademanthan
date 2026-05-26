"""Sticky Top 3 executable-first ranking."""
from backend.services.vajra.sticky_ranking_engine import (
    apply_no_chase_filter,
    compute_executable_score,
    enrich_sticky_ranking_fields,
    eligible_for_executable_top3,
    sticky_health_ok,
    update_slot_breakout_tracking,
)


def _row(**kwargs):
    base = {
        "stock": "TEST",
        "execution_bias": "LONG",
        "momentum_score": 70,
        "breakout_score": 65,
        "structure_score": 60,
        "extension_risk_score": 35,
        "pullback_quality_score": 55,
        "ees_score": 72,
        "risk_efficiency_score": 60,
        "rsi_transition_status": "RSI 62",
        "ema_reclaim_status": "ABOVE EMA5",
        "vwap_reclaim_status": "ABOVE VWAP",
        "breakout_phase": "breakout_initiated",
        "qualification_state": "EXECUTABLE",
        "momentum": "✔ PASS",
        "obv": "RISING",
    }
    base.update(kwargs)
    return enrich_sticky_ranking_fields(base)


def test_executable_penalizes_overbought_extension():
    fresh = _row(rsi_transition_status="RSI 62", extension_risk_score=30)
    tired = _row(
        rsi_transition_status="RSI 84",
        extension_risk_score=72,
        expansion_count=4,
        breakout_phase="extended",
    )
    assert tired["executable_score"] < fresh["executable_score"]


def test_momentum_velocity_two_down_penalizes():
    slot = {"momentum_hist": [75, 68]}
    row = _row(momentum_score=60)
    row = enrich_sticky_ranking_fields(row, slot)
    assert row["momentum_decay_penalty"] >= 18
    assert not sticky_health_ok(slot, row)


def test_breakout_fail_kill_switch():
    slot = {"breakout_pass": True, "polls_since_breakout_pass": 1}
    row = _row(
        breakout_score=40,
        execution_validated=False,
        breakout_phase="compression",
    )
    slot = update_slot_breakout_tracking(slot, row)
    assert row.get("failed_followthrough")
    assert not eligible_for_executable_top3(row)


def test_no_chase_watch_only():
    row = _row(
        rsi_transition_status="RSI 80",
        extension_risk_score=65,
        expansion_count=4,
        pullback_quality_score=30,
        breakout_phase="extended",
    )
    assert apply_no_chase_filter(row)
    assert row.get("no_chase_watch_only")
    assert not eligible_for_executable_top3(row)


def test_fresh_breakout_beats_extended_on_rank():
    fresh = _row(breakout_phase="breakout_initiated", extension_risk_score=28, evs_score=58)
    stale = _row(
        breakout_phase="extended",
        extension_risk_score=68,
        momentum_score=78,
        armed_rank_score=90,
    )
    assert fresh["sticky_rank_score"] > stale["sticky_rank_score"]
