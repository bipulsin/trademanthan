"""Execution Stability Score (ESS) — rewards persistence and clean structure over spike momentum."""
from __future__ import annotations

from typing import Any, Dict


def _f(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
    v = row.get(key)
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _pass_label(val: Any) -> bool:
    return "PASS" in str(val or "").upper()


def _vwap_stability(row: Dict[str, Any]) -> float:
    vwap = str(row.get("vwap_reclaim_status") or "").upper()
    if "ABOVE" in vwap or "BELOW" in vwap:
        if "NEAR" not in vwap:
            return 78.0
    if "NEAR" in vwap:
        return 52.0
    if "RECLAIM" in vwap:
        return 62.0
    return 38.0


def _structure_persistence(row: Dict[str, Any]) -> float:
    structure = _f(row, "structure_score")
    if structure <= 0:
        parts = [
            72.0 if _pass_label(row.get("structure")) else 28.0,
            68.0 if _pass_label(row.get("trend")) else 32.0,
            65.0 if _pass_label(row.get("momentum")) else 30.0,
        ]
        structure = sum(parts) / len(parts)
    exec_val = 88.0 if row.get("execution_validated") else 48.0
    ema = str(row.get("ema_reclaim_status") or "").upper()
    ema_pts = 70.0 if "RECLAIM" in ema or "ABOVE" in ema else 42.0
    breakout = _f(row, "breakout_score")
    if breakout <= 0:
        breakout = min(85.0, _f(row, "evs_score") + 12.0)
    hold = min(100.0, breakout * 0.55 + exec_val * 0.45)
    return structure * 0.45 + hold * 0.35 + ema_pts * 0.20


def _low_noise_movement(row: Dict[str, Any]) -> float:
    ext = _f(row, "extension_risk_score", 50.0)
    rev = str(row.get("reversal_risk") or "").upper()
    noise_penalty = 0.0
    if ext > 72:
        noise_penalty += (ext - 72) * 0.9
    if "HIGH" in rev or "ELEVATED" in rev:
        noise_penalty += 18.0
    base = 72.0 - noise_penalty
    pb = _f(row, "pullback_quality_score", 50.0)
    return max(0.0, min(100.0, base * 0.6 + pb * 0.4))


def _trend_continuation(row: Dict[str, Any]) -> float:
    tps = _f(row, "tps_score")
    exec_s = _f(row, "execution_score")
    conv = _f(row, "conviction_score") or _f(row, "confidence")
    phase = str(row.get("market_phase") or row.get("market_context") or "")
    phase_bonus = 8.0 if "Continuation" in phase or "Expansion" in phase else 0.0
    raw = tps * 0.35 + exec_s * 0.35 + conv * 0.30 + phase_bonus
    if row.get("execution_validated"):
        raw += 6.0
    return max(0.0, min(100.0, raw))


def _relative_strength_persistence(row: Dict[str, Any]) -> float:
    bull = _f(row, "bull_score")
    bear = _f(row, "bear_score")
    bias = str(row.get("execution_bias") or row.get("direction") or "").upper()
    if bias.startswith("L"):
        edge = bull - bear
    elif bias.startswith("S"):
        edge = bear - bull
    else:
        edge = abs(bull - bear)
    inst = _f(row, "institutional_participation_score", 50.0)
    vol = 62.0 if _pass_label(row.get("volume")) else 40.0
    spike_penalty = 12.0 if _f(row, "evs_score") > 78 and inst < 55 else 0.0
    rs = 50.0 + edge * 0.35 + inst * 0.25 + vol * 0.15 - spike_penalty
    return max(0.0, min(100.0, rs))


def compute_execution_stability_score(row: Dict[str, Any]) -> float:
    """
    ESS 0–100: stability-weighted composite (not short-term spike chasing).
    Weights: structure 30%, VWAP 20%, low noise 15%, trend continuation 20%, RS 15%.
    """
    s = _structure_persistence(row)
    v = _vwap_stability(row)
    n = _low_noise_movement(row)
    t = _trend_continuation(row)
    r = _relative_strength_persistence(row)
    ess = s * 0.30 + v * 0.20 + n * 0.15 + t * 0.20 + r * 0.15
    return round(max(0.0, min(100.0, ess)), 1)


def enrich_row_ess(row: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(row)
    row["ess_score"] = compute_execution_stability_score(row)
    row["execution_stability_score"] = row["ess_score"]
    return row
