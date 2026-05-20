"""ARMED ranking v2 — setup quality vs confidence, POWERINDIA-style profiles."""
from backend.services.vajra.qualification_config import STATE_ARMED
from backend.services.vajra.screener_sections import rank_armed
from backend.services.vajra.setup_quality import (
    ARMED_RANK_W_CONFIDENCE,
    ARMED_RANK_W_PARTICIPATION,
    ARMED_RANK_W_SETUP,
    compute_armed_rank_score,
    enrich_setup_quality_fields,
    extension_decay_multiplier,
    is_ignition_context,
    simulate_powerindia_profiles,
)
from backend.services.vajra.score_layers import ScoreLayers
from backend.services.vajra.breakout_phase import PHASE_BREAKOUT_INITIATED, PHASE_MATURE_TREND


def test_armed_rank_weights():
    assert ARMED_RANK_W_SETUP == 0.65
    assert ARMED_RANK_W_CONFIDENCE == 0.25
    assert ARMED_RANK_W_PARTICIPATION == 0.10


def test_powerindia_ignition_beats_mature_setup_quality():
    profiles = simulate_powerindia_profiles()
    ign = profiles["ignition_0955"]
    mat = profiles["mature_1515"]
    assert ign["setup_quality_score"] > mat["setup_quality_score"]
    assert ign["armed_rank_score"] > mat["armed_rank_score"]
    assert ign["confidence_score"] < mat["confidence_score"]


def test_armed_rank_sorts_by_setup_not_confidence_alone():
    high_conf_low_setup = {
        "stock": "MATURE",
        "qualification_state": STATE_ARMED,
        "armed_rank_score": compute_armed_rank_score(25, 95, 50),
        "setup_quality_score": 25,
        "ignition_quality_score": 30,
        "institutional_participation_score": 50,
    }
    low_conf_high_setup = {
        "stock": "IGNITE",
        "qualification_state": STATE_ARMED,
        "armed_rank_score": compute_armed_rank_score(88, 70, 72),
        "setup_quality_score": 88,
        "ignition_quality_score": 80,
        "institutional_participation_score": 72,
    }
    armed = sorted(
        [high_conf_low_setup, low_conf_high_setup],
        key=rank_armed,
    )
    assert armed[0]["stock"] == "IGNITE"


def test_ignition_context_softens_extension_decay():
    row = {
        "breakout_phase": PHASE_BREAKOUT_INITIATED,
        "evs_score": 64,
        "compression_broken": True,
        "vwap_reclaim_status": "ABOVE VWAP",
    }
    layers = ScoreLayers(
        discovery_score=60,
        execution_score=65,
        conviction_score=70,
        risk_efficiency_score=45,
        structure_score=58,
        momentum_score=56,
        breakout_score=55,
        extension_quality_score=5,
        tps_score=58,
        volume_score=55,
        pullback_score=55,
        htf_alignment_score=58,
    )
    assert is_ignition_context(row, layers)
    decay_ign = extension_decay_multiplier(90, breakout_phase=PHASE_BREAKOUT_INITIATED, breakout_score=55)
    decay_mat = extension_decay_multiplier(90, breakout_phase=PHASE_MATURE_TREND, breakout_score=85)
    assert decay_ign > decay_mat


def test_enrich_setup_quality_fields_rounded():
    row = {
        "breakout_phase": PHASE_BREAKOUT_INITIATED,
        "evs_score": 64,
        "compression_broken": True,
        "vwap_reclaim_status": "ABOVE VWAP",
        "extension_risk_score": 55,
        "conviction_score": 70,
    }
    layers = ScoreLayers(
        discovery_score=65,
        execution_score=68,
        conviction_score=70,
        risk_efficiency_score=40,
        structure_score=58,
        momentum_score=56,
        breakout_score=55,
        extension_quality_score=45,
        tps_score=58,
        volume_score=55,
        pullback_score=55,
        htf_alignment_score=58,
    )
    out = enrich_setup_quality_fields(row, layers, market_phase="Bull Expansion")
    assert 0 < out["setup_quality_score"] <= 100
    assert out["armed_rank_score"] > 0
    assert out["expansion_velocity_score"] == 64
