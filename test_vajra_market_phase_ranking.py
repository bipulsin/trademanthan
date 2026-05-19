"""Market phase execution ranking and EXECUTABLE gating."""
from backend.services.vajra.market_phase_scoring import (
    apply_phase_executable_cap,
    compute_execution_rank_score,
    market_phase_score,
)
from backend.services.vajra.ranking import sort_vajra_rows_for_display
from backend.services.vajra.trade_quality import STATE_EXECUTABLE, STATE_WATCHLIST


def test_expansion_scores_above_compression():
    assert market_phase_score("BULL EXPANSION") == 100.0
    assert market_phase_score("COMPRESSION") == 0.0
    assert market_phase_score("WEAKENING") == 20.0


def test_compression_cannot_be_executable_without_exception():
    state = apply_phase_executable_cap(
        STATE_EXECUTABLE,
        market_phase="COMPRESSION",
        structure=60,
        momentum=60,
        breakout=70,
        volume_score=70,
        htf=60,
        confidence=78,
    )
    assert state == STATE_WATCHLIST


def test_bull_expansion_ranks_above_rotational_in_executable_band():
    expansion = {
        "stock": "MANAPPURAM",
        "qualification": STATE_EXECUTABLE,
        "market_phase": "BULL EXPANSION",
        "trade_type": "LONG",
        "structure_score": 65,
        "momentum_score": 62,
        "breakout_score": 58,
        "trend_score": 60,
        "pullback_score": 55,
        "volume_score": 55,
        "htf_alignment_score": 58,
        "extension_risk_score": 40,
        "confidence": 78,
    }
    rotational = {
        "stock": "NHPC",
        "qualification": STATE_EXECUTABLE,
        "market_phase": "ROTATIONAL",
        "trade_type": "LONG WATCH",
        "structure_score": 68,
        "momentum_score": 65,
        "breakout_score": 66,
        "trend_score": 55,
        "pullback_score": 72,
        "volume_score": 62,
        "htf_alignment_score": 55,
        "extension_risk_score": 38,
        "confidence": 80,
    }
    expansion["execution_rank_score"] = compute_execution_rank_score(expansion)
    rotational["execution_rank_score"] = compute_execution_rank_score(rotational)
    out = sort_vajra_rows_for_display([rotational, expansion])
    assert out[0]["stock"] == "MANAPPURAM"
