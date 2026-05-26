"""
Sticky Top 3 ranking — prioritize actionable execution NOW over stale momentum persistence.

ExecutableScore > MomentumScore; velocity decay; breakout failure kill; no-chase filter.
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple

from backend.services.vajra.breakout_phase import (
    PHASE_BREAKOUT_INITIATED,
    PHASE_COMPRESSION,
    PHASE_EXHAUSTED,
    PHASE_EXTENDED,
    PHASE_EXPANSION,
)

# Configurable thresholds (env overrides for tuning without deploy)
RSI_CHASE = float(os.getenv("VAJRA_NO_CHASE_RSI", "75"))
RSI_EXHAUST = float(os.getenv("VAJRA_EXTENSION_RSI", "82"))
RSI_OVERBOUGHT = float(os.getenv("VAJRA_EXECUTABLE_RSI", "78"))
VWAP_EXT_PCT = float(os.getenv("VAJRA_EXTENSION_VWAP_PCT", "1.5"))
NO_CHASE_VWAP_PCT = float(os.getenv("VAJRA_NO_CHASE_VWAP_PCT", "1.8"))
EXPANSION_EXHAUST = int(os.getenv("VAJRA_EXTENSION_EXPANSION_BARS", "3"))
BREAKOUT_FAIL_POLLS = int(os.getenv("VAJRA_BREAKOUT_FAIL_POLLS", "3"))

_MOMENTUM_LEADERS_LIMIT = 8


def _f(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
    v = row.get(key)
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _bull_dir(row: Dict[str, Any]) -> bool:
    bias = str(row.get("execution_bias") or row.get("direction") or "LONG").upper()
    return not bias.startswith("S")


def _parse_rsi(row: Dict[str, Any]) -> float:
    s = str(row.get("rsi_transition_status") or row.get("rsi") or "")
    m = re.search(r"RSI\s*(\d+(?:\.\d+)?)", s, re.I)
    if m:
        return float(m.group(1))
    ext = _f(row, "extension_risk_score")
    mom = _f(row, "momentum_score")
    if _bull_dir(row):
        return min(88.0, 48.0 + mom * 0.32 + ext * 0.22)
    return max(12.0, 52.0 - mom * 0.32 - ext * 0.22)


def _vwap_dist_pct(row: Dict[str, Any]) -> float:
    v = row.get("distance_from_vwap_pct")
    if v is not None:
        return abs(float(v))
    for alert in row.get("ees_alerts") or []:
        a = str(alert).upper()
        if "EXTENDED" in a or "LATE IMPULSE" in a:
            return max(1.6, _f(row, "extension_risk_score") / 28.0)
    return min(3.5, _f(row, "extension_risk_score") / 22.0)


def _ema_dist_pct(row: Dict[str, Any]) -> float:
    v = row.get("distance_from_ema_pct")
    if v is not None:
        return abs(float(v))
    ema = str(row.get("ema_reclaim_status") or "").upper()
    if "NEAR" in ema:
        return 0.4
    if _bull_dir(row) and "ABOVE" in ema:
        return min(2.2, 0.5 + _f(row, "extension_risk_score") / 40.0)
    if not _bull_dir(row) and "BELOW" in ema:
        return min(2.2, 0.5 + _f(row, "extension_risk_score") / 40.0)
    return min(2.8, _f(row, "extension_risk_score") / 18.0)


def _expansion_count(row: Dict[str, Any]) -> int:
    ec = row.get("expansion_count")
    if ec is not None:
        try:
            return int(ec)
        except (TypeError, ValueError):
            pass
    for alert in row.get("ees_alerts") or []:
        m = re.search(r"(\d+)\s*expansion", str(alert), re.I)
        if m:
            return int(m.group(1))
    ext = _f(row, "extension_risk_score")
    if ext >= 62:
        return 4
    if ext >= 48:
        return 3
    if ext >= 34:
        return 2
    if ext >= 22:
        return 1
    return 0


def _has_consolidation(row: Dict[str, Any]) -> bool:
    phase = str(row.get("breakout_phase") or row.get("breakout_lifecycle") or "").lower()
    if phase == PHASE_COMPRESSION:
        return True
    if _f(row, "pullback_quality_score") >= 55:
        return True
    if _expansion_count(row) < 2:
        return True
    if "compression" in str(row.get("market_phase") or "").lower():
        return True
    return False


def _breakout_pass(row: Dict[str, Any]) -> bool:
    if row.get("execution_validated"):
        return True
    if _f(row, "breakout_score") >= 58:
        return True
    phase = str(row.get("breakout_phase") or "").lower()
    return phase in (
        PHASE_BREAKOUT_INITIATED,
        PHASE_EXPANSION,
        "breakout_validated",
    )


def _wick_heavy_near_high(row: Dict[str, Any]) -> bool:
    vq = str(row.get("vwap_reclaim_quality") or "").lower()
    if vq == "weak":
        return True
    alerts = " ".join(str(a) for a in (row.get("ees_alerts") or [])).upper()
    if "RESISTANCE" in alerts or "LATE IMPULSE" in alerts:
        return True
    ext = _f(row, "extension_risk_score")
    return ext >= 58 and _expansion_count(row) >= 2


def compute_executable_score(row: Dict[str, Any]) -> float:
    """0–100: can a trader enter NOW with acceptable RR and structure."""
    rsi = _parse_rsi(row)
    vwap_d = _vwap_dist_pct(row)
    ema_d = _ema_dist_pct(row)
    exp_n = _expansion_count(row)
    ext_risk = _f(row, "extension_risk_score")
    pb = _f(row, "pullback_quality_score")
    ees = _f(row, "ees_score")
    rr = _f(row, "risk_efficiency_score")
    structure = _f(row, "structure_score")
    brk = _f(row, "breakout_score")
    bull = _bull_dir(row)

    score = 52.0
    if ees > 0:
        score = ees * 0.45 + score * 0.55
    score += min(18, pb * 0.18)
    score += min(12, rr * 0.12)
    score += min(10, structure * 0.10)
    score += min(8, brk * 0.08)

    phase = str(row.get("breakout_phase") or "").lower()
    if phase in (PHASE_BREAKOUT_INITIATED, PHASE_COMPRESSION):
        score += 10
    elif phase in (PHASE_EXTENDED, PHASE_EXHAUSTED):
        score -= 22

    if bull:
        if rsi > RSI_OVERBOUGHT:
            score -= min(35, 12 + (rsi - RSI_OVERBOUGHT) * 1.4)
        if vwap_d > VWAP_EXT_PCT:
            score -= min(28, (vwap_d - VWAP_EXT_PCT) * 14)
        if ema_d > 1.2:
            score -= min(22, (ema_d - 1.2) * 12)
    else:
        if rsi < (100 - RSI_OVERBOUGHT):
            score -= min(35, 12 + ((100 - RSI_OVERBOUGHT) - rsi) * 1.4)
        if vwap_d > VWAP_EXT_PCT:
            score -= min(28, (vwap_d - VWAP_EXT_PCT) * 14)
        if ema_d > 1.2:
            score -= min(22, (ema_d - 1.2) * 12)

    if exp_n >= EXPANSION_EXHAUST:
        score -= 8 + (exp_n - EXPANSION_EXHAUST + 1) * 7
    if exp_n >= 4:
        score -= 12

    if ext_risk >= 65:
        score -= min(25, (ext_risk - 65) * 0.9)
    if not _has_consolidation(row) and exp_n >= 2 and brk >= 55:
        score -= 14

    if _wick_heavy_near_high(row):
        score -= 16

    if vwap_d > 2.0 and brk >= 60:
        score -= min(20, (vwap_d - 2.0) * 10)

    spread_accel = _f(row, "evs_score") > 75 and ext_risk > 55
    if spread_accel:
        score -= 10

    return max(0.0, min(100.0, round(score, 1)))


def compute_freshness_score(row: Dict[str, Any]) -> float:
    """Reward pre-breakout / fresh ignition; penalize late vertical moves."""
    phase = str(row.get("breakout_phase") or "").lower()
    exp_n = _expansion_count(row)
    evs = _f(row, "evs_score")
    brk = _f(row, "breakout_score")
    ext = _f(row, "extension_risk_score")

    score = 50.0
    if phase == PHASE_COMPRESSION:
        score += 22
    elif phase == PHASE_BREAKOUT_INITIATED:
        score += 28
    elif phase == PHASE_EXPANSION:
        score += 12
    elif phase in (PHASE_EXTENDED, PHASE_EXHAUSTED):
        score -= 25

    if exp_n <= 1:
        score += 15
    elif exp_n == 2:
        score += 5
    elif exp_n >= 4:
        score -= 20

    if evs >= 55 and exp_n <= 2:
        score += 10
    if ext >= 60:
        score -= min(30, (ext - 60) * 0.8)
    if brk >= 70 and exp_n >= 3:
        score -= 15

    return max(0.0, min(100.0, round(score, 1)))


def compute_extension_risk_display(row: Dict[str, Any]) -> float:
    """Unified extension risk 0–100 (higher = worse)."""
    base = _f(row, "extension_risk_score")
    rsi = _parse_rsi(row)
    vwap_d = _vwap_dist_pct(row)
    exp_n = _expansion_count(row)
    bull = _bull_dir(row)

    risk = base
    if bull and rsi > RSI_EXHAUST:
        risk += min(25, (rsi - RSI_EXHAUST) * 2.5)
    if not bull and rsi < (100 - RSI_EXHAUST):
        risk += min(25, ((100 - RSI_EXHAUST) - rsi) * 2.5)
    if vwap_d > VWAP_EXT_PCT:
        risk += min(20, (vwap_d - VWAP_EXT_PCT) * 12)
    if exp_n >= EXPANSION_EXHAUST:
        risk += 8 + (exp_n - EXPANSION_EXHAUST) * 5
    return max(0.0, min(100.0, round(risk, 1)))


def _momentum_velocity(hist: List[float], current: float) -> Tuple[float, float, str]:
    """Returns (velocity, acceleration, trend_arrow)."""
    h = list(hist or [])[-2:] + [current]
    h = h[-3:]
    velocity = 0.0
    acceleration = 0.0
    if len(h) >= 2:
        velocity = h[-1] - h[-2]
    if len(h) >= 3:
        acceleration = (h[-1] - h[-2]) - (h[-2] - h[-3])
    if len(h) >= 3 and h[-1] < h[-2] < h[-3]:
        arrow = "↓↓"
    elif velocity > 2:
        arrow = "↑"
    elif velocity < -2:
        arrow = "↓"
    else:
        arrow = "→"
    return round(velocity, 2), round(acceleration, 2), arrow


def compute_deterioration_penalty(row: Dict[str, Any], *, prior_market_phase: str = "") -> float:
    """Immediate ranking hit for live deterioration signals."""
    penalty = 0.0
    bull = _bull_dir(row)
    obv = str(row.get("obv") or "").upper()
    ema = str(row.get("ema_reclaim_status") or "").upper()
    mp = str(row.get("market_phase") or row.get("market_context") or "")
    prev_mp = prior_market_phase or str(row.get("_prior_market_phase") or "")

    if bull:
        if "FALLING" in obv and "RISING" not in obv:
            penalty += 12
        if "FLAT" in obv and _f(row, "momentum_score") >= 55:
            penalty += 8
        if "BELOW" in ema and _breakout_pass(row):
            penalty += 18
    else:
        if "RISING" in obv and "FALLING" not in obv:
            penalty += 12
        if "FLAT" in obv:
            penalty += 8
        if "ABOVE" in ema and _breakout_pass(row):
            penalty += 18

    if "PASS" not in str(row.get("momentum") or "").upper():
        if _f(row, "momentum_score") < 45:
            penalty += 10

    if prev_mp and "expansion" in prev_mp.lower() and (
        "rotational" in mp.lower() or "weakening" in mp.lower() or "compression" in mp.lower()
    ):
        penalty += 14

    if _wick_heavy_near_high(row):
        penalty += 10

    return min(45.0, penalty)


def compute_extension_decay_penalty(row: Dict[str, Any]) -> float:
    """Aggressive per-poll decay when exhausted extension stack."""
    rsi = _parse_rsi(row)
    vwap_d = _vwap_dist_pct(row)
    exp_n = _expansion_count(row)
    bull = _bull_dir(row)

    if not (
        (bull and rsi > RSI_EXHAUST)
        or (not bull and rsi < (100 - RSI_EXHAUST))
    ):
        return 0.0
    if vwap_d <= VWAP_EXT_PCT and exp_n < EXPANSION_EXHAUST:
        return 0.0

    decay = 8.0
    if bull:
        decay += max(0, (rsi - RSI_EXHAUST) * 1.2)
    else:
        decay += max(0, ((100 - RSI_EXHAUST) - rsi) * 1.2)
    decay += max(0, (vwap_d - VWAP_EXT_PCT) * 10)
    decay += max(0, (exp_n - EXPANSION_EXHAUST + 1) * 6)
    return min(40.0, round(decay, 1))


def apply_no_chase_filter(row: Dict[str, Any]) -> bool:
    """True if WATCH ONLY — cannot qualify for executable Top 3."""
    rsi = _parse_rsi(row)
    vwap_d = _vwap_dist_pct(row)
    bull = _bull_dir(row)

    if bull:
        extended = rsi > RSI_CHASE and vwap_d > NO_CHASE_VWAP_PCT
    else:
        extended = rsi < (100 - RSI_CHASE) and vwap_d > NO_CHASE_VWAP_PCT

    if extended and not _has_consolidation(row):
        row["no_chase_watch_only"] = True
        row["enter_action"] = "WATCH ONLY"
        row["enter_enabled"] = False
        row["action"] = "WATCH ONLY"
        row["chase_risk"] = "HIGH"
        return True
    row["chase_risk"] = "LOW" if vwap_d < 1.0 and rsi < 72 else "MED"
    return False


def update_slot_breakout_tracking(slot: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
    """Breakout PASS → FAIL within N polls → failed follow-through."""
    slot = dict(slot)
    brk_now = _breakout_pass(row)
    was_pass = bool(slot.get("breakout_pass"))
    polls = int(slot.get("polls_since_breakout_pass") or 0)

    if brk_now:
        if was_pass:
            polls += 1
        else:
            polls = 0
        slot["breakout_pass"] = True
        slot["polls_since_breakout_pass"] = polls
    else:
        if was_pass and polls <= BREAKOUT_FAIL_POLLS:
            row["failed_followthrough"] = True
            row["setup_failure"] = "Failed Follow-through"
            slot["breakout_fail_kill"] = True
        slot["breakout_pass"] = False
        slot["polls_since_breakout_pass"] = 0
    return slot


def sticky_health_ok(slot: Optional[Dict[str, Any]], row: Dict[str, Any]) -> bool:
    """Sticky only while improving or stable — not on stale persistence alone."""
    if row.get("failed_followthrough") or row.get("no_chase_watch_only"):
        return False
    if _f(row, "extension_decay_penalty") > 20:
        return False
    hist = list((slot or {}).get("momentum_hist") or [])
    mom = _f(row, "momentum_score")
    if len(hist) >= 2 and mom < hist[-1] < hist[-2]:
        return False

    if slot:
        lock_exec = _f(slot, "lock_executable_score")
        cur_exec = _f(row, "executable_score")
        if cur_exec < lock_exec - 8:
            return False
        lock_brk = _f(slot, "lock_breakout_score")
        if _f(row, "breakout_score") < lock_brk - 10:
            return False

    exec_s = _f(row, "executable_score")
    if exec_s < 38:
        return False
    return True


def composite_sticky_rank(row: Dict[str, Any]) -> float:
    """
    Final rank: ExecutableScore dominates; freshness helps; penalties apply.
    No ESS / armed_rank persistence bonus.
    """
    exec_s = _f(row, "executable_score")
    mom_s = _f(row, "momentum_score")
    fresh = _f(row, "freshness_score")
    struct = _f(row, "structure_score")

    base = exec_s * 0.62 + mom_s * 0.14 + fresh * 0.14 + struct * 0.10
    base -= _f(row, "deterioration_penalty")
    base -= _f(row, "extension_decay_penalty")
    base -= _f(row, "momentum_decay_penalty")

    if row.get("failed_followthrough"):
        base -= 40
    if row.get("no_chase_watch_only"):
        base -= 35

    try:
        from backend.services.vajra.sector_intelligence import sector_weighted_rank_adjustment

        base += sector_weighted_rank_adjustment(row) * 0.35
    except Exception:
        pass
    return max(0.0, min(100.0, round(base, 2)))


def enrich_sticky_ranking_fields(
    row: Dict[str, Any],
    slot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    row = dict(row)
    prior_mp = str((slot or {}).get("lock_market_phase") or "")
    row["executable_score"] = compute_executable_score(row)
    row["freshness_score"] = compute_freshness_score(row)
    row["extension_risk_display"] = compute_extension_risk_display(row)
    row["deterioration_penalty"] = compute_deterioration_penalty(row, prior_market_phase=prior_mp)
    row["extension_decay_penalty"] = compute_extension_decay_penalty(row)

    hist = list((slot or {}).get("momentum_hist") or [])
    mom = _f(row, "momentum_score")
    vel, acc, arrow = _momentum_velocity(hist, mom)
    row["momentum_velocity"] = vel
    row["momentum_acceleration"] = acc
    row["score_trend"] = arrow

    decay_pen = 0.0
    if len(hist) >= 2 and mom < hist[-1] < hist[-2]:
        decay_pen = 18.0 + max(0, hist[-2] - mom) * 0.4
    elif vel < -4:
        decay_pen = 10.0
    row["momentum_decay_penalty"] = round(decay_pen, 1)

    apply_no_chase_filter(row)
    row["sticky_rank_score"] = composite_sticky_rank(row)

    exec_ok = True
    if slot:
        exec_ok = _f(row, "executable_score") >= _f(slot, "lock_executable_score") - 3
    if vel > 1 and exec_ok:
        row["setup_trend"] = "improving"
    elif vel < -2 or row.get("extension_decay_penalty", 0) > 15:
        row["setup_trend"] = "deteriorating"
    elif _f(row, "extension_risk_display") >= 70:
        row["setup_trend"] = "exhausted"
    else:
        row["setup_trend"] = "stable"

    return row


def refresh_slot_metrics(slot: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
    """Update slot history after a poll."""
    slot = dict(slot)
    mom = _f(row, "momentum_score")
    hist = list(slot.get("momentum_hist") or [])
    hist.append(mom)
    slot["momentum_hist"] = hist[-5:]
    slot = update_slot_breakout_tracking(slot, row)
    slot["lock_market_phase"] = str(row.get("market_phase") or row.get("market_context") or "")
    return slot


def eligible_for_executable_top3(row: Dict[str, Any]) -> bool:
    if str(row.get("qualification_state") or row.get("qualification") or "").upper() == "REJECT":
        return False
    if not str(row.get("stock") or row.get("security") or "").strip():
        return False
    if row.get("no_chase_watch_only") or row.get("failed_followthrough"):
        return False
    if _f(row, "executable_score") < 35:
        return False
    return True


def select_momentum_leaders(
    rows: List[Dict[str, Any]],
    *,
    exclude: Optional[set] = None,
    limit: int = _MOMENTUM_LEADERS_LIMIT,
) -> List[Dict[str, Any]]:
    """Discovery/momentum leaders — separate from executable Top 3."""
    ex = exclude or set()
    pool = []
    for r in rows:
        sym = str(r.get("stock") or r.get("security") or "").strip().upper()
        if not sym or sym in ex:
            continue
        if str(r.get("qualification_state") or r.get("qualification") or "").upper() == "REJECT":
            continue
        pool.append(r)

    def _key(r: Dict[str, Any]) -> tuple:
        return (
            -_f(r, "discovery_score"),
            -_f(r, "tps_score"),
            -_f(r, "momentum_score"),
            -_f(r, "evs_score"),
            str(r.get("stock") or ""),
        )

    return sorted(pool, key=_key)[:limit]
