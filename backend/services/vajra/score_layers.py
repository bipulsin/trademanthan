"""Split scoring layers — discovery vs execution vs conviction vs risk efficiency."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from backend.services.vajra.trade_quality import TradeQualityResult


def _f(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _weighted_mean(pairs: list[tuple[float, float]]) -> float:
    num = den = 0.0
    for val, w in pairs:
        if w <= 0:
            continue
        num += val * w
        den += w
    return num / den if den else 0.0


def _phase_discovery_boost(phase: str) -> float:
    p = (phase or "").lower()
    if "early" in p and "expansion" in p:
        return 78.0
    if "expansion" in p:
        return 72.0
    if "trend" in p or "continuation" in p:
        return 68.0
    if "rotational" in p:
        return 55.0
    if "compression" in p:
        return 52.0
    return 50.0


def _obv_participation_score(row: Dict[str, Any]) -> float:
    obv = str(row.get("obv") or "").upper()
    if "RISING" in obv or ("ABOVE" in obv and "FALLING" not in obv):
        return 72.0
    if "FALLING" in obv or ("BELOW" in obv and "RISING" not in obv):
        return 72.0
    if "FLAT" in obv:
        return 45.0
    return 50.0


def _early_transition_boost(row: Dict[str, Any]) -> float:
    tt = str(row.get("trade_type") or "").upper()
    stage = str(row.get("pipeline_stage") or "").upper()
    if "EARLY" in tt:
        return 75.0
    if stage in ("DISCOVERY", "SHORTLIST", "VALIDATION"):
        return 65.0
    if row.get("alertable"):
        return 70.0
    return 48.0


def _vwap_reclaim_score(row: Dict[str, Any]) -> float:
    v = str(row.get("vwap_reclaim_status") or "").upper()
    if v in ("RECLAIMED", "ABOVE VWAP"):
        return 85.0
    if "ABOVE" in v and "BELOW" not in v:
        return 80.0
    if v == "BELOW VWAP" or ("BELOW" in v and "ABOVE" not in v):
        return 35.0
    if "NEAR" in v:
        return 55.0
    return 50.0


def _pullback_rr_score(row: Dict[str, Any], pullback: float) -> float:
    ext_risk = _f(row.get("extension_risk_score"))
    base = pullback
    if ext_risk > 0:
        base = base * 0.6 + max(0.0, 100.0 - ext_risk) * 0.4
    return max(0.0, min(100.0, base))


def _late_impulse_penalty(row: Dict[str, Any], ext_risk: float) -> float:
    tt = str(row.get("trade_type") or "").upper()
    penalty = ext_risk * 0.5
    if "LATE" in tt or "EXTENDED" in tt:
        penalty += 25.0
    return min(100.0, penalty)


@dataclass
class ScoreLayers:
    discovery_score: float
    execution_score: float
    conviction_score: float
    risk_efficiency_score: float
    structure_score: float
    momentum_score: float
    breakout_score: float
    extension_quality_score: float
    tps_score: float
    volume_score: float
    pullback_score: float
    htf_alignment_score: float

    def to_dict(self) -> Dict[str, float]:
        return {
            "discovery_score": round(self.discovery_score, 1),
            "execution_score": round(self.execution_score, 1),
            "conviction_score": round(self.conviction_score, 1),
            "risk_efficiency_score": round(self.risk_efficiency_score, 1),
            "structure_score": round(self.structure_score, 1),
            "momentum_score": round(self.momentum_score, 1),
            "breakout_score": round(self.breakout_score, 1),
            "extension_quality_score": round(self.extension_quality_score, 1),
            "tps_score": round(self.tps_score, 1),
        }


def compute_score_layers(
    row: Dict[str, Any],
    tq: TradeQualityResult,
    *,
    market_phase: str,
    extension_quality: float,
) -> ScoreLayers:
    tps = _f(row.get("tps_score"))
    ees = row.get("ees_score")
    ees_f = _f(ees) if ees is not None else None

    evs = _f(row.get("evs_score"))
    discovery = _weighted_mean(
        [
            (tps if tps > 0 else tq.volume_score, 0.30),
            (evs if evs > 0 else tps, 0.15),
            (tq.volume_score, 0.15),
            (_phase_discovery_boost(market_phase), 0.12),
            (_obv_participation_score(row), 0.13),
            (_early_transition_boost(row), 0.15),
        ]
    )

    exec_component = tq.execution_score
    if ees_f is not None:
        exec_component = ees_f * 0.7 + exec_component * 0.3

    execution = _weighted_mean(
        [
            (tq.breakout_score, 0.24),
            (_vwap_reclaim_score(row), 0.22),
            (exec_component, 0.22),
            (_f(row.get("evs_score")) if row.get("evs_score") is not None else exec_component, 0.12),
            (100.0 if row.get("execution_validated") else 40.0, 0.10),
            (tq.pullback_score, 0.10),
        ]
    )

    conviction = _weighted_mean(
        [
            (tq.structure_score, 0.28),
            (tq.momentum_score, 0.28),
            (tq.htf_alignment_score, 0.22),
            (tq.trend_score, 0.22),
        ]
    )

    risk_eff = _weighted_mean(
        [
            (extension_quality, 0.50),
            (_pullback_rr_score(row, tq.pullback_score), 0.30),
            (100.0 - _late_impulse_penalty(row, tq.extension_risk_score), 0.20),
        ]
    )

    return ScoreLayers(
        discovery_score=max(0.0, min(100.0, discovery)),
        execution_score=max(0.0, min(100.0, execution)),
        conviction_score=max(0.0, min(100.0, conviction)),
        risk_efficiency_score=max(0.0, min(100.0, risk_eff)),
        structure_score=tq.structure_score,
        momentum_score=tq.momentum_score,
        breakout_score=tq.breakout_score,
        extension_quality_score=extension_quality,
        tps_score=tps,
        volume_score=tq.volume_score,
        pullback_score=tq.pullback_score,
        htf_alignment_score=tq.htf_alignment_score,
    )


def structure_improving(row: Dict[str, Any]) -> bool:
    return "PASS" in str(row.get("structure") or "").upper() or _f(row.get("structure_score")) >= 52


def momentum_improving(row: Dict[str, Any]) -> bool:
    return "PASS" in str(row.get("momentum") or "").upper() or _f(row.get("momentum_score")) >= 50
