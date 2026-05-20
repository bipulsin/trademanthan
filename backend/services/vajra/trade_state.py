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
from backend.services.vajra.qualification_config import (
    STATE_ARMED,
    STATE_DISCOVERY,
)
from backend.services.vajra.score_layers import compute_score_layers
from backend.services.vajra.qualification_engine import qualify_trade

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
    """ECS structural tendency — internal layer (not shown as NEUTRAL in Top 8)."""
    bull = _f(row.get("bull_score"))
    bear = _f(row.get("bear_score"))
    if bull > bear + 5:
        return "BULLISH"
    if bear > bull + 5:
        return "BEARISH"
    return "MIXED"


def _price_above_vwap(row: Dict[str, Any]) -> bool:
    v = str(row.get("vwap_reclaim_status") or "").upper()
    return v in ("RECLAIMED", "ABOVE VWAP") or ("ABOVE" in v and "BELOW" not in v)


def _price_below_vwap(row: Dict[str, Any]) -> bool:
    v = str(row.get("vwap_reclaim_status") or "").upper()
    return v == "BELOW VWAP" or ("BELOW" in v and "ABOVE" not in v)


def compute_directional_scores(row: Dict[str, Any], market_phase: str) -> tuple[float, float]:
    """
    Factor-sum model: higher-probability execution side right now.
    VWAP influences but does not veto pullback/reclaim longs or shorts.
    """
    struct = _f(row.get("structure_score"))
    mom = _f(row.get("momentum_score"))
    trend = _f(row.get("trend_score"))
    brk = _f(row.get("breakout_score"))
    htf = _f(row.get("htf_alignment_score"))
    bull = _f(row.get("bull_score"))
    bear = _f(row.get("bear_score"))

    long_s = 0.0
    short_s = 0.0

    if _pass(row.get("structure")):
        long_s += 1.0 + struct / 100.0
        short_s += 1.0 + struct / 100.0
    long_s += bull / 45.0
    short_s += bear / 45.0

    if _pass(row.get("momentum")):
        long_s += 0.9 + mom / 110.0
        short_s += 0.9 + mom / 110.0
    if mom >= 52:
        long_s += (mom - 50) / 40.0
    if mom <= 48:
        short_s += (50 - mom) / 40.0

    if _pass(row.get("trend")):
        long_s += 0.7 + trend / 130.0
        short_s += 0.7 + trend / 130.0

    if _price_above_vwap(row):
        long_s += 1.0
        short_s += 0.35
    elif _price_below_vwap(row):
        short_s += 1.0
        long_s += 0.55
        if bull > bear + 12:
            long_s += 0.85
    else:
        long_s += 0.45
        short_s += 0.45

    obv = str(row.get("obv") or "").upper()
    if "RISING" in obv or ("ABOVE" in obv and "FALLING" not in obv):
        long_s += 0.75
    if "FALLING" in obv or ("BELOW" in obv and "RISING" not in obv):
        short_s += 0.75

    long_s += brk / 85.0
    short_s += brk / 85.0
    long_s += htf / 120.0
    short_s += max(0.0, (62.0 - htf) / 90.0)

    if market_phase in (PHASE_BULL_EXPANSION, PHASE_EARLY_BULL):
        long_s += 1.6
    elif market_phase in (PHASE_BEAR_EXPANSION, PHASE_EARLY_BEAR):
        short_s += 1.6
    elif market_phase == PHASE_TREND_CONTINUATION:
        if bull >= bear:
            long_s += 0.8
        else:
            short_s += 0.8

    tt = str(row.get("trade_type") or "").upper()
    if "EARLY LONG" in tt or tt.startswith("LONG"):
        long_s += 0.6
    if "EARLY SHORT" in tt or tt.startswith("SHORT"):
        short_s += 0.6

    return long_s, short_s


def directional_conviction_margin(long_s: float, short_s: float) -> float:
    total = long_s + short_s
    if total <= 0:
        return 0.0
    return abs(long_s - short_s) / total


def resolve_execution_direction(
    row: Dict[str, Any],
    market_phase: str,
    *,
    allow_neutral: bool = False,
) -> str:
    """Always LONG or SHORT for trader-facing lists unless allow_neutral (REJECT only)."""
    long_s, short_s = compute_directional_scores(row, market_phase)
    if long_s > short_s:
        return "LONG"
    if short_s > long_s:
        return "SHORT"
    bull = _f(row.get("bull_score"))
    bear = _f(row.get("bear_score"))
    if bull >= bear:
        return "LONG"
    if allow_neutral and bull == bear and bull == 0:
        return "NEUTRAL"
    return "SHORT" if bear > bull else "LONG"


def directional_confidence_label(direction: str, long_s: float, short_s: float) -> str:
    margin = directional_conviction_margin(long_s, short_s)
    if margin >= 0.20:
        strength = "Strong"
    elif margin >= 0.09:
        strength = "Moderate"
    else:
        strength = "Weak"
    return f"{strength} {direction}"


def has_directional_conviction(row: Dict[str, Any], market_phase: str) -> bool:
    """Exclude unresolved chop from Top 8."""
    long_s, short_s = compute_directional_scores(row, market_phase)
    return directional_conviction_margin(long_s, short_s) >= 0.05


def derive_execution_bias(row: Dict[str, Any], market_phase: str) -> str:
    """Trader-facing execution side — never NEUTRAL for ranked setups."""
    return resolve_execution_direction(row, market_phase, allow_neutral=False)


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
    execution_score: float = 0.0,
    conviction_score: float = 0.0,
    discovery_score: float = 0.0,
    risk_efficiency_score: float = 0.0,
) -> float:
    phase_w = PHASE_SCORES.get(market_phase, 30.0)
    qual = qualification_state.upper()
    if qual == STATE_EXECUTABLE:
        return (
            execution_score * 0.40
            + conviction_score * 0.30
            + risk_efficiency_score * 0.15
            + volume_score * 0.08
            + phase_w * 0.07
        )
    if qual == STATE_ARMED:
        return (
            execution_score * 0.35
            + structure_score * 0.20
            + momentum_score * 0.20
            + breakout_score * 0.15
            + phase_w * 0.10
        )
    if qual == STATE_DISCOVERY:
        return discovery_score * 0.55 + volume_score * 0.25 + phase_w * 0.20
    return phase_w * 0.10 + structure_score * 0.05


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

    if qualification_state == STATE_ARMED:
        if brk < 52:
            tags.append("Waiting breakout")
        if not _price_above_vwap(row) and not _price_below_vwap(row):
            tags.append("Waiting VWAP reclaim")
        elif _price_below_vwap(row) and execution_bias == "LONG":
            tags.append("Needs reclaim")
        if market_phase in (PHASE_EARLY_BULL, PHASE_EARLY_BEAR):
            tags.append("Early expansion")
        elif market_phase == PHASE_COMPRESSION:
            tags.append("Compression ready")
        elif market_phase == PHASE_ROTATIONAL:
            tags.append("Rotational phase")
        if mom < 58 and mom >= 50:
            tags.append("Momentum confirmation pending")
        if not tags:
            tags.append("Setup forming")
        return tags[:3]

    if qualification_state == STATE_DISCOVERY:
        if market_phase in (PHASE_EARLY_BULL, PHASE_EARLY_BEAR):
            tags.append("Early expansion")
        elif market_phase == PHASE_ROTATIONAL:
            tags.append("Institutional rotation")
        if mom < 52 and mom > 0:
            tags.append("Participation building")
        if not tags:
            tags.append("Monitoring")
        return tags[:3]

    if qualification_state == STATE_WATCHLIST:
        if brk < 52:
            tags.append("Waiting breakout")
        if market_phase == PHASE_ROTATIONAL:
            tags.append("Rotational phase")
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
    ext_q = _extension_quality_score(row)
    merged = dict(row)
    merged.setdefault("structure_score", tq.structure_score)
    merged.setdefault("momentum_score", tq.momentum_score)
    merged.setdefault("breakout_score", tq.breakout_score)
    merged.setdefault("extension_quality_score", ext_q)

    layers = compute_score_layers(merged, tq, market_phase=market_phase, extension_quality=ext_q)
    qual_result = qualify_trade(
        merged,
        layers,
        market_phase=market_phase,
        reject_reasons=list(tq.reject_reasons),
        session_date=row.get("session_date"),
    )
    qual = qual_result.qualification_state
    reasons = qual_result.reject_reasons

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
        execution_score=layers.execution_score,
        conviction_score=layers.conviction_score,
        discovery_score=layers.discovery_score,
        risk_efficiency_score=layers.risk_efficiency_score,
    )

    from backend.services.vajra.actions import resolve_enter_action

    action = resolve_enter_action(
        entry_state=qual,
        confidence=layers.conviction_score,
        reject_reasons=reasons,
        blocker_label=qual_result.blocker_label,
    )
    long_s, short_s = compute_directional_scores(row, market_phase)
    dir_conf = directional_confidence_label(execution_bias, long_s, short_s)
    conviction = has_directional_conviction(row, market_phase)

    tags = qual_result.reason_tags or build_reason_tags(
        qualification_state=qual,
        market_phase=market_phase,
        execution_bias=execution_bias,
        row=row,
        reject_reasons=reasons,
    )

    out = {
        "symbol": row.get("stock") or row.get("security"),
        "structural_bias": structural_bias,
        "execution_bias": execution_bias,
        "direction": execution_bias,
        "directional_confidence": dir_conf,
        "directional_conviction": conviction,
        "directional_long_score": round(long_s, 2),
        "directional_short_score": round(short_s, 2),
        "qualification_state": qual,
        "qualification_stage": qual.lower(),
        "qualification": qual,
        "entry_state": qual,
        "market_phase": market_phase,
        "market_context": market_phase,
        "market_phase_score": PHASE_SCORES.get(market_phase, 0.0),
        "confidence": round(layers.conviction_score, 1),
        "conviction_score": round(layers.conviction_score, 1),
        "discovery_score": round(layers.discovery_score, 1),
        "execution_score": round(layers.execution_score, 1),
        "risk_efficiency_score": round(layers.risk_efficiency_score, 1),
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
        "trade_quality_score": round(layers.conviction_score, 1),
        "setup_quality_score": round(layers.conviction_score, 1),
        "reason_tags": tags,
        "qualification_tags": tags,
        "reject_reasons": reasons,
        "primary_blocker": qual_result.primary_blocker,
        "blocker_label": qual_result.blocker_label,
        "nearest_trigger": qual_result.nearest_trigger,
        "raw_stage": qual_result.raw_stage,
        "hysteresis_applied": qual_result.hysteresis_applied,
        "action": action.get("enter_action") or "",
        "enter_action": action.get("enter_action") or "",
        "enter_enabled": action.get("enter_enabled", False),
        "enter_reason": action.get("enter_reason") or "",
        "top8_phase_bucket": _phase_bucket_for_top8(market_phase),
    }
    return out


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
    long_s, short_s = compute_directional_scores(row, market_phase)
    execution_bias = resolve_execution_direction(row, market_phase, allow_neutral=False)

    bull_dir = execution_bias == "LONG"

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
    if state["qualification_state"] == STATE_REJECT:
        row["execution_bias"] = resolve_execution_direction(
            row, market_phase, allow_neutral=True
        )
        if row["execution_bias"] == "NEUTRAL":
            row["execution_bias"] = "LONG" if _f(row.get("bull_score")) >= _f(row.get("bear_score")) else "SHORT"
        row["direction"] = row["execution_bias"]
    return row
