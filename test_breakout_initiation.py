"""Breakout initiation, EVS, and ignition-stage ARMED qualification."""
from backend.services.vajra.breakout_phase import (
    PHASE_BREAKOUT_INITIATED,
    PHASE_COMPRESSION,
    classify_breakout_phase,
)
from backend.services.vajra.qualification_config import STATE_ARMED, STATE_DISCOVERY
from backend.services.vajra.qualification_engine import (
    classify_raw_stage,
    derive_trigger_tags,
    is_ignition_armed,
)
from backend.services.vajra.score_layers import ScoreLayers


def _layers(**kw) -> ScoreLayers:
    defaults = dict(
        discovery_score=58.0,
        execution_score=58.0,
        conviction_score=68.0,
        risk_efficiency_score=70.0,
        structure_score=58.0,
        momentum_score=56.0,
        breakout_score=48.0,
        extension_quality_score=42.0,
        tps_score=48.0,
        volume_score=55.0,
        pullback_score=55.0,
        htf_alignment_score=58.0,
    )
    defaults.update(kw)
    return ScoreLayers(**defaults)


def test_breakout_initiated_tag_not_waiting_breakout():
    row = {
        "evs_score": 62.0,
        "breakout_phase": PHASE_BREAKOUT_INITIATED,
        "compression_broken": True,
        "vwap_reclaim_status": "ABOVE VWAP",
        "structure": "PASS",
        "momentum": "PASS",
    }
    layers = _layers(breakout_score=48.0, tps_score=48.0)
    tags = derive_trigger_tags(row, layers, "Rotational")
    assert "breakout_initiated" in tags
    assert "waiting_breakout" not in tags
    assert "expansion_ignition" in tags


def test_ignition_armed_before_mature_breakout():
    row = {
        "evs_score": 62.0,
        "breakout_phase": PHASE_BREAKOUT_INITIATED,
        "compression_broken": True,
        "adx_accelerating": True,
        "vwap_reclaim_status": "ABOVE VWAP",
        "structure": "PASS",
        "momentum": "PASS",
        "execution_validated": False,
    }
    layers = _layers(
        structure_score=58.0,
        momentum_score=56.0,
        extension_quality_score=42.0,
        conviction_score=68.0,
        execution_score=58.0,
        tps_score=48.0,
        breakout_score=48.0,
    )
    assert is_ignition_armed(layers, row) is True
    reasons: list = []
    raw = classify_raw_stage(layers, row, market_phase="Rotational", reject_reasons=reasons)
    assert raw == STATE_ARMED


def test_low_evs_stays_discovery_not_ignition_armed():
    row = {
        "evs_score": 40.0,
        "breakout_phase": PHASE_COMPRESSION,
        "compression_broken": False,
        "vwap_reclaim_status": "ABOVE VWAP",
        "structure": "PASS",
    }
    layers = _layers(breakout_score=45.0, tps_score=46.0)
    assert is_ignition_armed(layers, row) is False
    reasons: list = []
    raw = classify_raw_stage(layers, row, market_phase="Compression", reject_reasons=reasons)
    assert raw in (STATE_DISCOVERY, STATE_ARMED, "REJECT")


def test_classify_breakout_phase_initiation():
    phase = classify_breakout_phase(
        evs_score=58.0,
        breakout_score=46.0,
        extension_risk=55.0,
        extension_quality=45.0,
        compression_broken=True,
        vwap_accepted=True,
        execution_validated=False,
        adx_accelerating=True,
        momentum_score=56.0,
    )
    assert phase == PHASE_BREAKOUT_INITIATED
