"""Vajra qualification v2 — DISCOVERY / ARMED / EXECUTABLE + hysteresis."""
from backend.services.vajra.qualification_config import (
    STATE_ARMED,
    STATE_DISCOVERY,
    STATE_EXECUTABLE,
)
from backend.services.vajra.qualification_engine import apply_hysteresis, classify_raw_stage
from backend.services.vajra.score_layers import ScoreLayers
from backend.services.vajra.screener_sections import build_screener_sections
from backend.services.vajra.actions import resolve_enter_action
from backend.services.vajra.state_persistence import PriorQualificationState, clear_session_cache


def _layers(**kw) -> ScoreLayers:
    defaults = dict(
        discovery_score=55.0,
        execution_score=50.0,
        conviction_score=60.0,
        risk_efficiency_score=70.0,
        structure_score=60.0,
        momentum_score=58.0,
        breakout_score=50.0,
        extension_quality_score=55.0,
        tps_score=60.0,
        volume_score=55.0,
        pullback_score=55.0,
        htf_alignment_score=58.0,
    )
    defaults.update(kw)
    return ScoreLayers(**defaults)


def test_discovery_raw_stage():
    row = {"structure": "PASS", "momentum": "FAIL", "tps_score": 50}
    reasons: list = []
    raw = classify_raw_stage(
        _layers(discovery_score=55, tps_score=50, structure_score=52, momentum_score=48),
        row,
        market_phase="Rotational",
        reject_reasons=reasons,
    )
    assert raw in (STATE_DISCOVERY, STATE_ARMED, "REJECT")


def test_hysteresis_holds_executable_on_dip():
    clear_session_cache()
    layers_mid = _layers(conviction_score=72, execution_score=70, breakout_score=55)
    row = {"execution_validated": True, "vwap_reclaim_status": "ABOVE VWAP", "instrument_key": "ITC"}
    prior = PriorQualificationState(qualification_state=STATE_EXECUTABLE, conviction_score=76, execution_score=74)
    final, applied = apply_hysteresis(
        STATE_ARMED, layers_mid, row, market_phase="Trend Continuation", prior=prior
    )
    assert final == STATE_EXECUTABLE
    assert applied is True


def test_top_picks_executable_only_no_padding():
    rows = [
        {
            "stock": "A",
            "qualification_state": STATE_ARMED,
            "market_phase": "Rotational",
            "execution_rank_score": 600,
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
            "execution_bias": "LONG",
            "directional_conviction": True,
            "execution_score": 78,
            "conviction_score": 80,
            "discovery_score": 60,
        },
        {
            "stock": "C",
            "qualification_state": STATE_DISCOVERY,
            "market_phase": "Rotational",
            "execution_rank_score": 400,
            "execution_bias": "LONG",
            "directional_conviction": True,
            "discovery_score": 58,
        },
    ]
    out = build_screener_sections(rows)
    assert len(out["top_picks"]) == 1
    assert out["top_picks"][0]["stock"] == "B"
    assert len(out["top_sections"][STATE_ARMED]) == 1
    assert len(out["top_sections"][STATE_DISCOVERY]) == 1
    assert out["banner"] is None


def test_banner_when_no_executable():
    rows = [
        {
            "stock": "X",
            "qualification_state": STATE_ARMED,
            "market_phase": "Rotational",
            "execution_bias": "LONG",
            "directional_conviction": True,
            "execution_score": 62,
            "conviction_score": 66,
            "discovery_score": 55,
        },
    ]
    out = build_screener_sections(rows)
    assert out["banner"] is not None
    assert "No executable" in out["banner"]["message"]


def test_actions_monitor_armed_enter():
    assert resolve_enter_action(entry_state=STATE_DISCOVERY, confidence=55)["enter_action"] == "MONITOR"
    assert resolve_enter_action(entry_state=STATE_ARMED, confidence=66, blocker_label="Waiting VWAP")["enter_action"] == "ARMED"
    assert resolve_enter_action(entry_state=STATE_EXECUTABLE, confidence=80)["enter_enabled"] is True
