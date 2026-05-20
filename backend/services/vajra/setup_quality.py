"""Setup quality, ignition, institutional participation, and ARMED rank scoring."""
from __future__ import annotations

from typing import Any, Dict, Optional

from backend.services.vajra.breakout_phase import (
    PHASE_BREAKOUT_INITIATED,
    PHASE_BREAKOUT_VALIDATED,
    PHASE_COMPRESSION,
    PHASE_EXHAUSTED,
    PHASE_EXPANSION,
    PHASE_EXTENDED,
    PHASE_MATURE_TREND,
)
from backend.services.vajra.score_layers import ScoreLayers
from backend.services.vajra.trade_quality import TradeQualityResult

ARMED_RANK_W_SETUP = 0.65
ARMED_RANK_W_CONFIDENCE = 0.25
ARMED_RANK_W_PARTICIPATION = 0.10


def _f(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _breakout_phase(row: Dict[str, Any]) -> str:
    return str(row.get("breakout_phase") or row.get("breakout_lifecycle") or "").lower()


def _vwap_accepted(row: Dict[str, Any]) -> bool:
    v = str(row.get("vwap_reclaim_status") or "").upper()
    return v in ("RECLAIMED", "ABOVE VWAP") or ("ABOVE" in v and "BELOW" not in v) or (
        "BELOW" in v and "ABOVE" not in v
    )


def is_ignition_context(row: Dict[str, Any], layers: Optional[ScoreLayers] = None) -> bool:
    phase = _breakout_phase(row)
    evs = _f(row.get("evs_score"))
    if phase in (PHASE_BREAKOUT_INITIATED, PHASE_EXPANSION, PHASE_BREAKOUT_VALIDATED):
        return True
    if evs >= 55 and row.get("compression_broken") and _vwap_accepted(row):
        return True
    if layers and layers.breakout_score >= 52 and evs >= 50:
        return True
    return False


def extension_decay_multiplier(
    extension_risk: float,
    *,
    breakout_phase: str,
    breakout_score: float = 0.0,
    execution_validated: bool = False,
) -> float:
    """Decay setup quality as extension rises; gentler during ignition."""
    ext_r = _f(extension_risk)
    phase = (breakout_phase or "").lower()
    brk = _f(breakout_score)

    if phase in (PHASE_EXTENDED, PHASE_EXHAUSTED):
        mult = max(0.12, 1.0 - ext_r / 95.0)
    elif phase == PHASE_MATURE_TREND:
        mult = max(0.18, 1.0 - ext_r / 110.0)
    elif phase in (PHASE_BREAKOUT_INITIATED, PHASE_BREAKOUT_VALIDATED, PHASE_EXPANSION):
        mult = max(0.42, 1.0 - ext_r / 220.0)
        if brk >= 55:
            mult = min(1.0, mult + 0.12)
    else:
        mult = max(0.28, 1.0 - ext_r / 160.0)

    if execution_validated and phase in (PHASE_EXPANSION, PHASE_BREAKOUT_VALIDATED):
        mult *= 0.88

    return _clamp(mult, 0.10, 1.0)


def expansion_velocity_score(row: Dict[str, Any]) -> float:
    evs = _f(row.get("evs_score"))
    if evs > 0:
        return evs
    brk = _f(row.get("breakout_score"))
    tps = _f(row.get("tps_score"))
    return _clamp(48.0 + brk * 0.25 + tps * 0.12)


def ignition_quality_score(row: Dict[str, Any], layers: ScoreLayers) -> float:
    phase = _breakout_phase(row)
    evs = expansion_velocity_score(row)
    parts = [
        (evs, 0.28),
        (_clamp(85.0 if row.get("compression_broken") else 40.0), 0.18),
        (_clamp(82.0 if phase == PHASE_BREAKOUT_INITIATED else 55.0), 0.15),
        (_clamp(layers.breakout_score, 0, 100), 0.14),
        (_clamp(80.0 if _vwap_accepted(row) else 35.0), 0.12),
        (_clamp(75.0 if row.get("adx_accelerating") else 48.0), 0.08),
        (_clamp(70.0 if row.get("range_expanding") else 45.0), 0.05),
    ]
    num = den = 0.0
    for v, w in parts:
        num += v * w
        den += w
    return _clamp(num / den if den else 50.0)


def institutional_participation_score(row: Dict[str, Any], layers: ScoreLayers) -> float:
    obv = str(row.get("obv") or "").upper()
    obv_sc = 72.0 if ("RISING" in obv or ("ABOVE" in obv and "FALLING" not in obv)) else (
        72.0 if ("FALLING" in obv or ("BELOW" in obv and "RISING" not in obv)) else 45.0
    )
    vol = layers.volume_score
    tps = layers.tps_score if layers.tps_score > 0 else _f(row.get("tps_score"))
    evs = expansion_velocity_score(row)
    tt = str(row.get("trade_type") or "").upper()
    early = 70.0 if "EARLY" in tt else 50.0
    return _clamp(
        obv_sc * 0.30 + vol * 0.25 + tps * 0.20 + evs * 0.15 + early * 0.10,
    )


def compute_setup_quality_score(
    row: Dict[str, Any],
    layers: ScoreLayers,
    *,
    market_phase: str,
    tq: Optional[TradeQualityResult] = None,
) -> float:
    """Fast-moving execution attractiveness — peaks at ignition, decays when mature/extended."""
    phase = _breakout_phase(row)
    evs = expansion_velocity_score(row)
    ign = ignition_quality_score(row, layers)
    inst = institutional_participation_score(row, layers)

    vwap_sc = 85.0 if _vwap_accepted(row) else 38.0
    mom_accel = _clamp(
        layers.momentum_score * 0.5
        + (78.0 if row.get("adx_accelerating") else 48.0) * 0.3
        + evs * 0.2,
    )
    compression_sc = 78.0 if row.get("compression_broken") or phase == PHASE_BREAKOUT_INITIATED else (
        62.0 if (market_phase or "").lower() == PHASE_COMPRESSION else 50.0
    )
    pullback = layers.pullback_score
    ext_risk = _f(row.get("extension_risk_score"))
    if tq:
        ext_risk = tq.extension_risk_score

    rr_sc = _clamp(pullback * 0.65 + max(0.0, 100.0 - ext_risk) * 0.35)

    raw = _clamp(
        ign * 0.22
        + evs * 0.20
        + vwap_sc * 0.14
        + mom_accel * 0.12
        + compression_sc * 0.10
        + inst * 0.08
        + layers.breakout_score * 0.06
        + rr_sc * 0.08,
    )

    if phase in (PHASE_BREAKOUT_INITIATED, PHASE_EXPANSION) and evs >= 55:
        raw = min(100.0, raw + 8.0)

    decay = extension_decay_multiplier(
        ext_risk,
        breakout_phase=phase,
        breakout_score=layers.breakout_score,
        execution_validated=bool(row.get("execution_validated")),
    )
    raw *= decay

    penalties = 0.0
    if phase in (PHASE_EXTENDED, PHASE_EXHAUSTED):
        penalties += 22.0
    elif phase == PHASE_MATURE_TREND:
        penalties += 14.0
    if ext_risk >= 88 and not is_ignition_context(row, layers):
        penalties += 18.0
    elif ext_risk >= 75 and phase not in (PHASE_BREAKOUT_INITIATED, PHASE_EXPANSION):
        penalties += 10.0

    return _clamp(raw - penalties)


def compute_armed_rank_score(
    setup_quality: float,
    confidence: float,
    institutional_participation: float,
) -> float:
    return _clamp(
        setup_quality * ARMED_RANK_W_SETUP
        + confidence * ARMED_RANK_W_CONFIDENCE
        + institutional_participation * ARMED_RANK_W_PARTICIPATION,
    )


def enrich_setup_quality_fields(
    row: Dict[str, Any],
    layers: ScoreLayers,
    *,
    market_phase: str,
    tq: Optional[TradeQualityResult] = None,
) -> Dict[str, float]:
    confidence = _f(row.get("conviction_score") or row.get("confidence"))
    inst = institutional_participation_score(row, layers)
    ign = ignition_quality_score(row, layers)
    evs = expansion_velocity_score(row)
    sq = compute_setup_quality_score(row, layers, market_phase=market_phase, tq=tq)
    armed_rank = compute_armed_rank_score(sq, confidence, inst)

    return {
        "setup_quality_score": round(sq, 1),
        "confidence_score": round(confidence, 1),
        "institutional_participation_score": round(inst, 1),
        "ignition_quality_score": round(ign, 1),
        "expansion_velocity_score": round(evs, 1),
        "armed_rank_score": round(armed_rank, 2),
    }


def simulate_powerindia_profiles() -> Dict[str, Dict[str, float]]:
    """Illustrative 09:55 ignition vs 15:15 mature (for tests/docs)."""
    from backend.services.vajra.score_layers import ScoreLayers

    base_layers = ScoreLayers(
        discovery_score=65,
        execution_score=68,
        conviction_score=83,
        risk_efficiency_score=40,
        structure_score=58,
        momentum_score=56,
        breakout_score=92,
        extension_quality_score=5,
        tps_score=58,
        volume_score=55,
        pullback_score=55,
        htf_alignment_score=58,
    )
    ignition_row = {
        "breakout_phase": PHASE_BREAKOUT_INITIATED,
        "evs_score": 64,
        "compression_broken": True,
        "adx_accelerating": True,
        "range_expanding": True,
        "vwap_reclaim_status": "ABOVE VWAP",
        "extension_risk_score": 55,
        "execution_validated": False,
        "conviction_score": 70,
    }
    mature_row = {
        "breakout_phase": PHASE_MATURE_TREND,
        "evs_score": 40,
        "compression_broken": True,
        "adx_accelerating": False,
        "vwap_reclaim_status": "ABOVE VWAP",
        "extension_risk_score": 95,
        "execution_validated": True,
        "conviction_score": 95,
    }
    ign_layers = ScoreLayers(
        **{**base_layers.__dict__, "conviction_score": 70, "breakout_score": 48}
    )
    mat_layers = ScoreLayers(
        **{**base_layers.__dict__, "conviction_score": 95, "breakout_score": 85}
    )
    ign_sq = enrich_setup_quality_fields(
        ignition_row, ign_layers, market_phase="Bull Expansion"
    )
    mat_sq = enrich_setup_quality_fields(
        mature_row, mat_layers, market_phase="Bull Expansion"
    )
    return {"ignition_0955": ign_sq, "mature_1515": mat_sq}
