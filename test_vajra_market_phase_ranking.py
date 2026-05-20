"""Market phase execution ranking and Top 8 gating."""
from backend.services.vajra.market_phase_scoring import select_top_picks
from backend.services.vajra.ranking import sort_vajra_rows_for_display
from backend.services.vajra.trade_state import (
    PHASE_BULL_EXPANSION,
    PHASE_COMPRESSION,
    PHASE_ROTATIONAL,
    STATE_EXECUTABLE,
    STATE_WATCHLIST,
    apply_phase_qualification_cap,
    compute_execution_rank_score,
    derive_execution_bias,
    resolve_market_phase,
)
from backend.services.vajra.trade_state import PHASE_SCORES


def test_expansion_scores_above_compression():
    assert PHASE_SCORES[PHASE_BULL_EXPANSION] == 100.0
    assert PHASE_SCORES[PHASE_COMPRESSION] == 0.0


def test_single_canonical_phase_early_long():
    row = {"market_phase": "ROTATIONAL", "trade_type": "EARLY LONG TRANSITION"}
    assert resolve_market_phase(row) == "Early Bull Expansion"


def test_compression_cannot_be_executable_without_exception():
    state = apply_phase_qualification_cap(
        STATE_EXECUTABLE,
        market_phase=PHASE_COMPRESSION,
        structure=60,
        momentum=60,
        breakout=70,
        volume_score=70,
        htf=60,
        confidence=78,
        reject_reasons=[],
    )
    assert state == STATE_WATCHLIST


def test_bull_expansion_ranks_above_rotational():
    expansion = {
        "stock": "MANAPPURAM",
        "qualification_state": STATE_EXECUTABLE,
        "market_phase": PHASE_BULL_EXPANSION,
        "market_phase_score": 100,
        "top8_phase_bucket": 1,
        "structure_score": 65,
        "momentum_score": 62,
        "breakout_score": 58,
        "trend_score": 60,
        "pullback_score": 55,
        "volume_score": 55,
        "htf_alignment_score": 58,
        "extension_risk_score": 40,
        "execution_score": 78,
        "conviction_score": 80,
        "discovery_score": 62,
        "risk_efficiency_score": 70,
        "execution_rank_score": compute_execution_rank_score(
            qualification_state=STATE_EXECUTABLE,
            market_phase=PHASE_BULL_EXPANSION,
            structure_score=65,
            momentum_score=62,
            breakout_score=58,
            trend_strength_score=60,
            volume_score=55,
            pullback_score=55,
            htf_alignment_score=58,
            extension_quality_score=60,
            execution_score=78,
            conviction_score=80,
            discovery_score=62,
            risk_efficiency_score=70,
        ),
    }
    rotational = {
        "stock": "NHPC",
        "qualification_state": STATE_EXECUTABLE,
        "market_phase": PHASE_ROTATIONAL,
        "market_phase_score": 40,
        "top8_phase_bucket": 3,
        "structure_score": 68,
        "momentum_score": 65,
        "breakout_score": 66,
        "trend_score": 55,
        "pullback_score": 72,
        "volume_score": 62,
        "htf_alignment_score": 55,
        "extension_risk_score": 38,
        "execution_score": 74,
        "conviction_score": 76,
        "discovery_score": 58,
        "risk_efficiency_score": 65,
        "execution_rank_score": compute_execution_rank_score(
            qualification_state=STATE_EXECUTABLE,
            market_phase=PHASE_ROTATIONAL,
            structure_score=68,
            momentum_score=65,
            breakout_score=66,
            trend_strength_score=55,
            volume_score=62,
            pullback_score=72,
            htf_alignment_score=55,
            extension_quality_score=62,
            execution_score=74,
            conviction_score=76,
            discovery_score=58,
            risk_efficiency_score=65,
        ),
    }
    out = sort_vajra_rows_for_display([rotational, expansion])
    assert out[0]["stock"] == "MANAPPURAM"


def test_top8_excludes_compression():
    rows = [
        {
            "stock": "BAD",
            "qualification_state": STATE_EXECUTABLE,
            "market_phase": PHASE_COMPRESSION,
            "execution_rank_score": 999,
            "top8_phase_bucket": 99,
        },
        {
            "stock": "GOOD",
            "qualification_state": STATE_EXECUTABLE,
            "market_phase": PHASE_BULL_EXPANSION,
            "execution_rank_score": 100,
            "top8_phase_bucket": 1,
        },
    ]
    picks, _ = select_top_picks(rows, n=8)
    assert len(picks) == 1
    assert picks[0]["stock"] == "GOOD"


def test_below_vwap_can_still_resolve_long_when_bullish_dominant():
    from backend.services.vajra.trade_state import (
        compute_directional_scores,
        resolve_execution_direction,
    )

    row = {
        "vwap_reclaim_status": "BELOW VWAP",
        "structure": "PASS",
        "momentum": "PASS",
        "trend": "PASS",
        "breakout_score": 62,
        "bull_score": 72,
        "bear_score": 28,
        "trade_type": "LONG WATCH",
    }
    ls, ss = compute_directional_scores(row, PHASE_ROTATIONAL)
    assert ls > ss
    assert resolve_execution_direction(row, PHASE_ROTATIONAL) == "LONG"


def test_execution_direction_never_neutral_for_watchlist():
    from backend.services.vajra.trade_state import resolve_execution_direction

    row = {
        "vwap_reclaim_status": "NEAR VWAP",
        "structure": "FAIL",
        "momentum": "FAIL",
        "bull_score": 50,
        "bear_score": 49,
    }
    d = resolve_execution_direction(row, PHASE_ROTATIONAL, allow_neutral=False)
    assert d in ("LONG", "SHORT")
