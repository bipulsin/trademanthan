"""Market phase tiers for execution ranking and EXECUTABLE gating."""
from __future__ import annotations

from typing import Any, Dict, Optional

STATE_EXECUTABLE = "EXECUTABLE"
STATE_WATCHLIST = "WATCHLIST"
STATE_REJECT = "REJECT"

# Execution-priority scores (0–100)
PHASE_SCORES: Dict[str, float] = {
    "BULL EXPANSION": 100.0,
    "BEAR EXPANSION": 100.0,
    "MOMENTUM EXPANSION": 85.0,
    "EARLY EXPANSION": 85.0,
    "TREND CONTINUATION": 80.0,
    "EXHAUSTION": 25.0,
    "ROTATIONAL": 45.0,
    "WEAKENING": 20.0,
    "COMPRESSION": 0.0,
    "FAILURE": 0.0,
}

_QUAL_RANK = {
    STATE_EXECUTABLE: 1000.0,
    STATE_WATCHLIST: 500.0,
    STATE_REJECT: 0.0,
}


def _f(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def canonical_market_phase(
    market_phase: str,
    *,
    trade_type: str = "",
    structure_score: float = 0.0,
    momentum_score: float = 0.0,
    trend_score: float = 0.0,
) -> str:
    """Map ECS phase + transition hints to execution tier label."""
    mp = (market_phase or "").upper().strip()
    tt = (trade_type or "").upper().strip()

    if "EARLY LONG" in tt or "EARLY SHORT" in tt:
        return "EARLY EXPANSION"
    if mp in ("BULL EXPANSION", "BEAR EXPANSION"):
        return mp
    if "BULL" in mp and "EXPANSION" in mp:
        return "BULL EXPANSION"
    if "BEAR" in mp and "EXPANSION" in mp:
        return "BEAR EXPANSION"

    if mp in ("COMPRESSION", "WEAKENING", "ROTATIONAL", "FAILURE", "EXHAUSTION"):
        if (
            mp in ("ROTATIONAL", "WEAKENING", "EXHAUSTION")
            and structure_score >= 65
            and momentum_score >= 62
            and trend_score >= 58
        ):
            return "TREND CONTINUATION"
        return mp

    if structure_score >= 65 and momentum_score >= 62 and trend_score >= 58:
        return "TREND CONTINUATION"

    return mp or "ROTATIONAL"


def market_phase_score(
    market_phase: str,
    *,
    trade_type: str = "",
    structure_score: float = 0.0,
    momentum_score: float = 0.0,
    trend_score: float = 0.0,
) -> float:
    key = canonical_market_phase(
        market_phase,
        trade_type=trade_type,
        structure_score=structure_score,
        momentum_score=momentum_score,
        trend_score=trend_score,
    )
    return PHASE_SCORES.get(key, 40.0)


def apply_phase_executable_cap(
    state: str,
    *,
    market_phase: str,
    trade_type: str = "",
    structure: float,
    momentum: float,
    breakout: float,
    volume_score: float,
    htf: float,
    confidence: float,
    reject_reasons: Optional[list] = None,
) -> str:
    """Downgrade EXECUTABLE when market context is poor for execution."""
    if state != STATE_EXECUTABLE:
        return state

    reasons = reject_reasons if reject_reasons is not None else []
    phase = canonical_market_phase(
        market_phase,
        trade_type=trade_type,
        structure_score=structure,
        momentum_score=momentum,
    )

    if phase == "COMPRESSION":
        if structure >= 75 and momentum >= 75 and htf >= 70 and confidence >= 80:
            return STATE_EXECUTABLE
        reasons.append("compression_not_executable")
        return STATE_WATCHLIST

    if phase == "WEAKENING":
        if structure >= 70 and momentum >= 68 and htf >= 65:
            return STATE_EXECUTABLE
        reasons.append("weakening_phase")
        return STATE_WATCHLIST

    if phase == "ROTATIONAL":
        if breakout >= 65 and volume_score >= 60 and momentum >= 58:
            return STATE_EXECUTABLE
        reasons.append("rotational_needs_breakout")
        return STATE_WATCHLIST

    if phase == "EXHAUSTION":
        if structure >= 68 and momentum >= 65 and breakout >= 60:
            return STATE_WATCHLIST
        reasons.append("exhaustion_phase")
        return STATE_WATCHLIST

    return state


def compute_execution_rank_score(row: Dict[str, Any]) -> float:
    """
    Execution prioritization score — expansion phases dominate pullback-only setups.
    """
    qual = str(row.get("qualification") or row.get("entry_state") or STATE_WATCHLIST).upper()
    qual_w = _QUAL_RANK.get(qual, 0.0)

    phase_sc = _f(row.get("market_phase_score"))
    if phase_sc <= 0:
        phase_sc = market_phase_score(
            str(row.get("market_phase") or ""),
            trade_type=str(row.get("trade_type") or ""),
            structure_score=_f(row.get("structure_score")),
            momentum_score=_f(row.get("momentum_score")),
            trend_score=_f(row.get("trend_score")),
        )

    struct = _f(row.get("structure_score"))
    mom = _f(row.get("momentum_score"))
    brk = _f(row.get("breakout_score"))
    trend = _f(row.get("trend_score"))
    pb = _f(row.get("pullback_score"))
    vol = _f(row.get("volume_score"))
    htf = _f(row.get("htf_alignment_score"))
    ext_risk = _f(row.get("extension_risk_score"))

    extension_penalty = max(0.0, (ext_risk - 50.0) * 0.35)

    component = (
        phase_sc * 0.22
        + struct * 0.18
        + mom * 0.18
        + brk * 0.15
        + trend * 0.10
        + pb * 0.07
        + vol * 0.05
        + htf * 0.03
    )

    return qual_w + component - extension_penalty


def enrich_execution_scores(row: Dict[str, Any]) -> Dict[str, Any]:
    """Attach phase label score + composite rank score to a pipeline row."""
    struct = _f(row.get("structure_score"))
    mom = _f(row.get("momentum_score"))
    trend = _f(row.get("trend_score"))
    phase_key = canonical_market_phase(
        str(row.get("market_phase") or ""),
        trade_type=str(row.get("trade_type") or ""),
        structure_score=struct,
        momentum_score=mom,
        trend_score=trend,
    )
    row["market_phase_tier"] = phase_key
    row["market_phase_score"] = market_phase_score(
        str(row.get("market_phase") or ""),
        trade_type=str(row.get("trade_type") or ""),
        structure_score=struct,
        momentum_score=mom,
        trend_score=trend,
    )
    row["execution_rank_score"] = compute_execution_rank_score(row)
    return row
