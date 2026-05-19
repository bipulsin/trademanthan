"""
Unified Vajra trade state — single object for qualification, ranking, and UI.

Pipeline: signal row → trade quality → trade state → screener display
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from backend.services.vajra.trade_quality import (
    STATE_EXECUTABLE,
    STATE_REJECT,
    STATE_WATCHLIST,
    TradeQualityResult,
    compute_trade_quality,
)

# Canonical market phases (display labels — used everywhere)
PHASE_BULL_EXPANSION = "Bull Expansion"
PHASE_BEAR_EXPANSION = "Bear Expansion"
PHASE_EARLY_BULL = "Early Bull Expansion"
PHASE_EARLY_BEAR = "Early Bear Expansion"
PHASE_TREND_CONTINUATION = "Trend Continuation"
PHASE_ROTATIONAL = "Rotational"
PHASE_WEAKENING = "Weakening"
PHASE_COMPRESSION = "Compression"

PHASE_SCORES: Dict[str, float] = {
    PHASE_BULL_EXPANSION: 100.0,
    PHASE_BEAR_EXPANSION: 100.0,
    PHASE_EARLY_BULL: 85.0,
    PHASE_EARLY_BEAR: 85.0,
    PHASE_TREND_CONTINUATION: 75.0,
    PHASE_ROTATIONAL: 40.0,
    PHASE_WEAKENING: 15.0,
    PHASE_COMPRESSION: 0.0,
}

TOP8_EXCLUDED_PHASES = frozenset({PHASE_COMPRESSION, PHASE_WEAKENING})

EXPANSION_PHASES = frozenset(
    {PHASE_BULL_EXPANSION, PHASE_BEAR_EXPANSION, PHASE_EARLY_BULL, PHASE_EARLY_BEAR}
)
CONTINUATION_PHASES = frozenset({PHASE_TREND_CONTINUATION})


def _f(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _pass(val: Any) -> bool:
    return "PASS" in str(val or "").upper()


def resolve_market_phase(row: Dict[str, Any]) -> str:
    """Single canonical market phase — no conflicting tag vs column logic."""
    mp = str(row.get("market_phase") or "").upper().strip()
    tt = str(row.get("trade_type") or "").upper().strip()

    if "EARLY LONG" in tt:
        return PHASE_EARLY_BULL
    if "EARLY SHORT" in tt:
        return PHASE_EARLY_BEAR
    if mp in ("BULL EXPANSION",):
        return PHASE_BULL_EXPANSION
    if mp in ("BEAR EXPANSION",):
        return PHASE_BEAR_EXPANSION
    if "BULL" in mp and "EXPANSION" in mp:
        return PHASE_BULL_EXPANSION
    if "BEAR" in mp and "EXPANSION" in mp:
        return PHASE_BEAR_EXPANSION
    if mp in ("COMPRESSION",):
        return PHASE_COMPRESSION
    if mp in ("WEAKENING",):
        return PHASE_WEAKENING
    if mp in ("ROTATIONAL",):
        return PHASE_ROTATIONAL
    if mp in ("EXHAUSTION", "FAILURE"):
        return PHASE_WEAKENING
    if mp in ("TRENDING", "TREND", "EXPANSION"):
        bull = _f(row.get("bull_score")) >= _f(row.get("bear_score"))
        return PHASE_BULL_EXPANSION if bull else PHASE_BEAR_EXPANSION
    return PHASE_ROTATIONAL


def derive_structural_bias(row: Dict[str, Any]) -> str:
    """ECS structural tendency (can be stale vs current execution)."""
    tt = str(row.get("trade_type") or "").upper()
    if tt.startswith("SHORT") or "EARLY SHORT" in tt:
        return "SHORT"
    if tt.startswith("LONG") or "EARLY LONG" in tt:
        return "LONG"
    bull = _f(row.get("bull_score"))
    bear = _f(row.get("bear_score"))
    if bull > bear + 3:
        return "LONG"
    if bear > bull + 3:
        return "SHORT"
    return "NEUTRAL"


def _price_above_vwap(row: Dict[str, Any]) -> bool:
    v = str(row.get("vwap_reclaim_status") or "").upper()
    return v in ("RECLAIMED", "ABOVE VWAP") or ("ABOVE" in v and "BELOW" not in v)


def _price_below_vwap(row: Dict[str, Any]) -> bool:
    v = str(row.get("vwap_reclaim_status") or "").upper()
    return v == "BELOW VWAP" or ("BELOW" in v and "ABOVE" not in v)


def derive_execution_bias(row: Dict[str, Any], market_phase: str) -> str:
    """
    Executable trade direction now — not legacy structural bias alone.
    """
    struct_ok = _pass(row.get("structure"))
    mom_ok = _pass(row.get("momentum"))
    brk = _f(row.get("breakout_score"))
    bull = _f(row.get("bull_score"))
    bear = _f(row.get("bear_score"))
    htf = _f(row.get("htf_alignment_score"))
    above = _price_above_vwap(row)
    below = _price_below_vwap(row)

    phase = market_phase
    expansion_ok = phase in EXPANSION_PHASES or phase == PHASE_TREND_CONTINUATION
    not_compression = phase not in (PHASE_COMPRESSION, PHASE_WEAKENING)

    long_core = (
        above
        and struct_ok
        and mom_ok
        and brk >= 52
        and bull >= bear
        and not_compression
        and phase not in (PHASE_BEAR_EXPANSION, PHASE_EARLY_BEAR)
    )
    short_core = (
        below
        and struct_ok
        and mom_ok
        and brk >= 52
        and bear >= bull
        and not_compression
        and phase not in (PHASE_BULL_EXPANSION, PHASE_EARLY_BULL)
    )

    if long_core and expansion_ok:
        return "LONG"
    if short_core and expansion_ok:
        return "SHORT"

    if long_core and phase == PHASE_ROTATIONAL and brk >= 58:
        return "LONG"
    if short_core and phase == PHASE_ROTATIONAL and brk >= 58:
        return "SHORT"

    if above and bull > bear + 8 and struct_ok and not_compression:
        return "LONG"
    if below and bear > bull + 8 and struct_ok and not_compression:
        return "SHORT"

    if htf >= 62 and bull > bear + 5 and above:
        return "LONG"
    if htf >= 62 and bear > bull + 5 and below:
        return "SHORT"

    return "NEUTRAL"


def _extension_quality_score(row: Dict[str, Any]) -> float:
    ext_risk = _f(row.get("extension_risk_score"))
    if ext_risk <= 0:
        return 55.0
    return max(0.0, min(100.0, 100.0 - ext_risk))


def compute_execution_rank_score(
    *,
    qualification_state: str,
    market_phase: str,
    structure_score: float,
    momentum_score: float,
    breakout_score: float,
    trend_strength_score: float,
    volume_score: float,
    pullback_score: float,
    htf_alignment_score: float,
    extension_quality_score: float,
) -> float:
    phase_w = PHASE_SCORES.get(market_phase, 30.0)
    qual_bonus = 200.0 if qualification_state == STATE_EXECUTABLE else (
        80.0 if qualification_state == STATE_WATCHLIST else 0.0
    )
    return qual_bonus + (
        phase_w * 0.25
        + structure_score * 0.18
        + momentum_score * 0.18
        + breakout_score * 0.15
        + trend_strength_score * 0.10
        + volume_score * 0.05
        + pullback_score * 0.04
        + htf_alignment_score * 0.03
        + extension_quality_score * 0.02
    )


def apply_phase_qualification_cap(
    state: str,
    *,
    market_phase: str,
    structure: float,
    momentum: float,
    breakout: float,
    volume_score: float,
    htf: float,
    confidence: float,
    reject_reasons: List[str],
) -> str:
    if state != STATE_EXECUTABLE:
        return state

    if market_phase == PHASE_COMPRESSION:
        if structure >= 75 and momentum >= 75 and htf >= 70 and confidence >= 80:
            return STATE_EXECUTABLE
        reject_reasons.append("compression")
        return STATE_WATCHLIST

    if market_phase == PHASE_WEAKENING:
        if structure >= 72 and momentum >= 70 and htf >= 68 and breakout >= 62:
            return STATE_EXECUTABLE
        reject_reasons.append("weakening")
        return STATE_WATCHLIST

    if market_phase == PHASE_ROTATIONAL:
        if breakout >= 65 and volume_score >= 60 and momentum >= 58:
            return STATE_EXECUTABLE
        reject_reasons.append("rotational")
        return STATE_WATCHLIST

    return state


def build_reason_tags(
    *,
    qualification_state: str,
    market_phase: str,
    execution_bias: str,
    row: Dict[str, Any],
    reject_reasons: List[str],
) -> List[str]:
    tags: List[str] = []
    brk = _f(row.get("breakout_score"))
    mom = _f(row.get("momentum_score"))
    struct = _f(row.get("structure_score"))

    if qualification_state == STATE_EXECUTABLE:
        if brk >= 58:
            tags.append("Breakout confirmed")
        if mom >= 58:
            tags.append("Momentum expanding")
        if struct >= 62:
            tags.append("Structure aligned")
        if market_phase in EXPANSION_PHASES:
            tags.append(market_phase)
        if not tags:
            tags.append("Execution ready")
        return tags[:3]

    if qualification_state == STATE_WATCHLIST:
        if brk < 52:
            tags.append("Waiting breakout")
        if not _price_above_vwap(row) and not _price_below_vwap(row):
            tags.append("Waiting VWAP reclaim")
        elif _price_below_vwap(row) and execution_bias == "LONG":
            tags.append("Needs reclaim")
        if market_phase == PHASE_ROTATIONAL:
            tags.append("Rotational phase")
        elif market_phase in (PHASE_EARLY_BULL, PHASE_EARLY_BEAR):
            tags.append("Early expansion")
        elif market_phase == PHASE_TREND_CONTINUATION:
            tags.append("Trend forming")
        if mom < 52 and mom > 0:
            tags.append("Momentum building")
        if not tags:
            tags.append("Setup forming")
        return tags[:3]

    if market_phase == PHASE_COMPRESSION:
        tags.append("Compression")
    elif market_phase == PHASE_WEAKENING:
        tags.append("Weakening")
    for code in reject_reasons[:2]:
        if code == "weak_core_scores":
            tags.append("Weak momentum")
        elif code == "compression_chop":
            tags.append("Compression")
        elif code == "over_extended":
            tags.append("Over-extended")
        else:
            tags.append(code.replace("_", " ").title())
    if not tags:
        tags.append("No directional edge")
    return tags[:3]


def _phase_bucket_for_top8(market_phase: str) -> int:
    if market_phase in EXPANSION_PHASES:
        return 1
    if market_phase in CONTINUATION_PHASES:
        return 2
    if market_phase == PHASE_ROTATIONAL:
        return 3
    return 99


def build_trade_state_dict(
    row: Dict[str, Any],
    tq: TradeQualityResult,
    *,
    market_phase: str,
    execution_bias: str,
    structural_bias: str,
) -> Dict[str, Any]:
    reasons = list(tq.reject_reasons)
    qual = apply_phase_qualification_cap(
        tq.state,
        market_phase=market_phase,
        structure=tq.structure_score,
        momentum=tq.momentum_score,
        breakout=tq.breakout_score,
        volume_score=tq.volume_score,
        htf=tq.htf_alignment_score,
        confidence=tq.confidence,
        reject_reasons=reasons,
    )

    ext_q = _extension_quality_score(row)
    rank = compute_execution_rank_score(
        qualification_state=qual,
        market_phase=market_phase,
        structure_score=tq.structure_score,
        momentum_score=tq.momentum_score,
        breakout_score=tq.breakout_score,
        trend_strength_score=tq.trend_score,
        volume_score=tq.volume_score,
        pullback_score=tq.pullback_score,
        htf_alignment_score=tq.htf_alignment_score,
        extension_quality_score=ext_q,
    )

    from backend.services.vajra.actions import resolve_enter_action

    action = resolve_enter_action(
        entry_state=qual,
        confidence=tq.confidence,
        reject_reasons=reasons,
    )
    tags = build_reason_tags(
        qualification_state=qual,
        market_phase=market_phase,
        execution_bias=execution_bias,
        row=row,
        reject_reasons=reasons,
    )

    return {
        "symbol": row.get("stock") or row.get("security"),
        "structural_bias": structural_bias,
        "execution_bias": execution_bias,
        "qualification_state": qual,
        "qualification": qual,
        "entry_state": qual,
        "market_phase": market_phase,
        "market_context": market_phase,
        "market_phase_score": PHASE_SCORES.get(market_phase, 0.0),
        "confidence": round(tq.confidence, 1),
        "execution_rank_score": round(rank, 2),
        "structure_score": round(tq.structure_score, 1),
        "momentum_score": round(tq.momentum_score, 1),
        "breakout_score": round(tq.breakout_score, 1),
        "trend_strength_score": round(tq.trend_score, 1),
        "trend_score": round(tq.trend_score, 1),
        "volume_score": round(tq.volume_score, 1),
        "pullback_score": round(tq.pullback_score, 1),
        "extension_risk_score": round(tq.extension_risk_score, 1),
        "extension_quality_score": round(ext_q, 1),
        "htf_alignment_score": round(tq.htf_alignment_score, 1),
        "trade_quality_score": round(tq.trade_quality_score, 1),
        "setup_quality_score": round(tq.trade_quality_score, 1),
        "reason_tags": tags,
        "qualification_tags": tags,
        "reject_reasons": reasons,
        "action": action.get("enter_action") or "",
        "enter_action": action.get("enter_action") or "",
        "enter_enabled": action.get("enter_enabled", False),
        "enter_reason": action.get("enter_reason") or "",
        "top8_phase_bucket": _phase_bucket_for_top8(market_phase),
    }


def apply_trade_state(
    row: Dict[str, Any],
    *,
    candles_30m: Sequence[Dict[str, Any]],
    candles_5m: Optional[Sequence[Dict[str, Any]]] = None,
    candles_1hr: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Full qualification pipeline → unified trade state on row."""
    market_phase = resolve_market_phase(row)
    structural_bias = derive_structural_bias(row)
    execution_bias = derive_execution_bias(row, market_phase)

    bull_dir = execution_bias == "LONG" or (
        execution_bias == "NEUTRAL" and structural_bias == "LONG"
    )

    tq = compute_trade_quality(
        candles_30m=candles_30m,
        candles_5m=candles_5m,
        candles_1hr=candles_1hr,
        bull_dir=bull_dir,
        market_phase=str(row.get("market_phase") or ""),
        extension_risk=row.get("extension_risk_score"),
        pullback_quality=row.get("pullback_quality_score"),
        ees_score=row.get("ees_score"),
        execution_validated=bool(row.get("execution_validated")),
        structure_pass=_pass(row.get("structure")),
        momentum_pass=_pass(row.get("momentum")),
        trend_pass=_pass(row.get("trend")),
        volume_pass=_pass(row.get("volume")),
        tps_score=row.get("tps_score"),
        trade_type=str(row.get("trade_type") or ""),
    )
    if tq is None:
        row["market_phase"] = market_phase
        row["market_context"] = market_phase
        row["execution_bias"] = execution_bias
        row["structural_bias"] = structural_bias
        row["direction"] = execution_bias
        return row

    state = build_trade_state_dict(
        row, tq,
        market_phase=market_phase,
        execution_bias=execution_bias,
        structural_bias=structural_bias,
    )
    row.update(tq.to_dict())
    row.update(state)
    row["direction"] = execution_bias
  # API alias
    if state["qualification_state"] == STATE_EXECUTABLE:
        row["enter_action"] = "ENTER"
        row["enter_enabled"] = True
        row["action"] = "ENTER"
    elif state["qualification_state"] == STATE_WATCHLIST:
        row["enter_action"] = "WATCH"
        row["enter_enabled"] = False
        row["action"] = "WATCH"
    else:
        row["enter_action"] = ""
        row["enter_enabled"] = False
        row["action"] = ""
    return row
