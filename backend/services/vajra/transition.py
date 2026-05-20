"""
Transition Potential Score (TPS), pullback quality, extension risk, and 5m execution validation.

TPS: early intraday transition discovery — does NOT require structure/breakout/OBV maturity.
ECS: computed separately in engine.py (existing Vajra bull/bear confirmation logic).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.services.vajra.engine import (
    ADX_LEN,
    RSI_LEN,
    SWING_LB,
    _dmi_adx,
    _rsi_wilder,
    _wilder_atr,
)
from backend.services.vajra.expansion_velocity import compute_expansion_velocity
from backend.services.vajra.indicators import cumulative_vwap, ema_series, sma_at, wma_series

EMA_EXEC_LEN = 5
RSI_EMA_LEN = 3
RSI_WMA_LEN = 21
IMPULSE_LB = 8
PULLBACK_LB = 6

EARLY_LONG = "EARLY LONG TRANSITION"
EARLY_SHORT = "EARLY SHORT TRANSITION"

TPS_SHORTLIST_MIN = 5
TPS_SHORTLIST_MAX = 15
TPS_EARLY_THRESHOLD = 52.0
EXTENSION_RISK_CAP = 65.0


@dataclass
class TransitionScores:
    tps_bull: float
    tps_bear: float
    pullback_quality: float
    extension_risk: float
    transition_state: str
    vwap_reclaim_status: str
    ema_reclaim_status: str
    rsi_transition_status: str
    bull_dir: bool
    trend_pass: bool
    momentum_improving: bool
    market_phase: str

    @property
    def tps_score(self) -> float:
        return self.tps_bull if self.bull_dir else self.tps_bear

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tps_score": round(self.tps_score, 1),
            "tps_bull": round(self.tps_bull, 1),
            "tps_bear": round(self.tps_bear, 1),
            "pullback_quality_score": round(self.pullback_quality, 1),
            "extension_risk_score": round(self.extension_risk, 1),
            "transition_state": self.transition_state,
            "vwap_reclaim_status": self.vwap_reclaim_status,
            "ema_reclaim_status": self.ema_reclaim_status,
            "rsi_transition_status": self.rsi_transition_status,
        }


@dataclass
class ExecutionValidation:
    validated: bool
    step_label: str
    steps_passed: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_validated": self.validated,
            "execution_step": self.step_label,
            "execution_steps_passed": self.steps_passed,
        }


def _phase_favorable(phase: str) -> bool:
    u = (phase or "").upper()
    return u in ("COMPRESSION", "ROTATIONAL", "WEAKENING")


def _crossed_above(series: Sequence[Optional[float]], threshold: float, lookback: int = 3) -> bool:
    n = len(series)
    if n < 2:
        return False
    cur = series[-1]
    if cur is None:
        return False
    if cur <= threshold:
        return False
    for j in range(max(0, n - lookback - 1), n - 1):
        prev = series[j]
        if prev is not None and prev <= threshold:
            return True
    return cur > threshold and (series[-2] is None or (series[-2] or 0) <= threshold)


def compute_pullback_quality(
    opens: Sequence[float],
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    ema_exec: Sequence[float],
    atr: float,
    *,
    bull_dir: bool,
) -> float:
    """0–100: higher = healthier shallow pullback after impulse."""
    i = len(closes) - 1
    if i < IMPULSE_LB + 2 or atr <= 0:
        return 50.0

    seg_start = max(0, i - IMPULSE_LB - PULLBACK_LB)
    seg_h = highs[seg_start : i + 1]
    seg_l = lows[seg_start : i + 1]
    seg_o = opens[seg_start : i + 1]
    seg_c = closes[seg_start : i + 1]

    if bull_dir:
        impulse_hi = max(seg_h)
        impulse_lo = min(seg_l[: max(1, len(seg_l) - PULLBACK_LB)])
        impulse_rng = max(impulse_hi - impulse_lo, atr * 0.5)
        pullback_lo = min(seg_l[-PULLBACK_LB:])
        retrace = (impulse_hi - pullback_lo) / impulse_rng if impulse_rng > 0 else 0.5
    else:
        impulse_lo = min(seg_l)
        impulse_hi = max(seg_h[: max(1, len(seg_h) - PULLBACK_LB)])
        impulse_rng = max(impulse_hi - impulse_lo, atr * 0.5)
        pullback_hi = max(seg_h[-PULLBACK_LB:])
        retrace = (pullback_hi - impulse_lo) / impulse_rng if impulse_rng > 0 else 0.5

    retrace = max(0.0, min(1.5, retrace))
    shallow_score = max(0.0, 100.0 - retrace * 70.0)

    body_scores: List[float] = []
    opp_penalty = 0.0
    for j in range(-PULLBACK_LB, 0):
        idx = len(seg_c) + j
        if idx < 0:
            continue
        rng = seg_h[idx] - seg_l[idx]
        body = abs(seg_c[idx] - seg_o[idx])
        body_ratio = body / rng if rng > 0 else 0.0
        body_scores.append(max(0.0, 100.0 - body_ratio * 80.0))
        if bull_dir and seg_c[idx] < seg_o[idx] and body_ratio > 0.55:
            opp_penalty += 12.0
        elif not bull_dir and seg_c[idx] > seg_o[idx] and body_ratio > 0.55:
            opp_penalty += 12.0

    body_avg = sum(body_scores) / len(body_scores) if body_scores else 50.0
    ema_respect = 0.0
    for j in range(-PULLBACK_LB, 0):
        ei = i + j
        if ei < 0:
            continue
        if bull_dir and lows[ei] >= ema_exec[ei] * 0.998:
            ema_respect += 20.0 / PULLBACK_LB
        elif not bull_dir and highs[ei] <= ema_exec[ei] * 1.002:
            ema_respect += 20.0 / PULLBACK_LB

    reclaim_bonus = 0.0
    if bull_dir and closes[i] > max(seg_h[-PULLBACK_LB:-1] or seg_h):
        reclaim_bonus = 15.0
    elif not bull_dir and closes[i] < min(seg_l[-PULLBACK_LB:-1] or seg_l):
        reclaim_bonus = 15.0

    raw = shallow_score * 0.35 + body_avg * 0.25 + ema_respect + reclaim_bonus - opp_penalty
    return max(0.0, min(100.0, raw))


def compute_extension_risk(
    close: float,
    vwap: float,
    ema_exec: float,
    atr: float,
    rsi: float,
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    opens: Sequence[float],
    *,
    bull_dir: bool,
) -> float:
    """0–100: higher = more extended (late entry risk)."""
    if atr <= 0:
        return 50.0
    vwap_dist = abs(close - vwap) / atr
    ema_dist = abs(close - ema_exec) / atr
    vwap_risk = min(40.0, vwap_dist * 12.0)
    ema_risk = min(25.0, ema_dist * 10.0)

    expansion_count = 0
    for j in range(max(0, len(closes) - 5), len(closes)):
        rng = highs[j] - lows[j]
        body = abs(closes[j] - opens[j])
        if rng > atr * 1.1 and body / rng > 0.6 if rng > 0 else False:
            if bull_dir and closes[j] > opens[j]:
                expansion_count += 1
            elif not bull_dir and closes[j] < opens[j]:
                expansion_count += 1
    exp_risk = min(25.0, expansion_count * 8.0)

    if bull_dir and rsi > 72:
        rsi_risk = min(20.0, (rsi - 72) * 2.5)
    elif not bull_dir and rsi < 28:
        rsi_risk = min(20.0, (28 - rsi) * 2.5)
    else:
        rsi_risk = 0.0

    vertical = 0.0
    if len(closes) >= 4:
        move = (closes[-1] - closes[-4]) / atr if bull_dir else (closes[-4] - closes[-1]) / atr
        if move > 2.5:
            vertical = min(15.0, (move - 2.5) * 6.0)

    return max(0.0, min(100.0, vwap_risk + ema_risk + exp_risk + rsi_risk + vertical))


def compute_tps(
    candles: Sequence[Dict[str, Any]],
    *,
    market_phase: str = "",
) -> Optional[TransitionScores]:
    """TPS on discovery timeframe (30m)."""
    if not candles or len(candles) < 60:
        return None

    opens = [float(c.get("open") or 0) for c in candles]
    highs = [float(c.get("high") or 0) for c in candles]
    lows = [float(c.get("low") or 0) for c in candles]
    closes = [float(c.get("close") or 0) for c in candles]
    volumes = [float(c.get("volume") or 0) for c in candles]
    traded = [closes[i] * volumes[i] for i in range(len(closes))]
    n = len(closes)
    i = n - 1
    close = closes[i]

    vwap_series = cumulative_vwap(highs, lows, closes, volumes)
    vwap = vwap_series[i]
    ema_exec = ema_series(closes, EMA_EXEC_LEN)
    rsi_series = _rsi_wilder(closes, RSI_LEN)
    rsi = rsi_series[i]
    if rsi is None:
        return None
    rsi_ema = ema_series([r if r is not None else 50.0 for r in rsi_series], RSI_EMA_LEN)
    rsi_wma = wma_series([r if r is not None else 50.0 for r in rsi_series], RSI_WMA_LEN)

    di_plus, di_minus, adx_series = _dmi_adx(highs, lows, closes, ADX_LEN)
    adx_val = adx_series[i]
    if adx_val is None:
        return None
    adx_prev = adx_series[i - 5] if i >= 5 and adx_series[i - 5] is not None else adx_val

    atr_series = _wilder_atr(highs, lows, closes, 14)
    atr = atr_series[i]
    if atr is None or atr <= 0:
        return None

    vol_ma = sma_at(traded, 20, i)
    rel_vol_improving = False
    if vol_ma and vol_ma > 0:
        rel_vol_improving = traded[i] > vol_ma and (
            i < 2 or traded[i] >= traded[i - 1] or traded[i - 1] >= traded[i - 2]
        )

    dip = di_plus[i] or 0.0
    dim = di_minus[i] or 0.0
    trend_bull = adx_val > 18 and dip > dim
    trend_bear = adx_val > 18 and dim > dip
    adx_rising_base = adx_val > adx_prev and adx_prev < 28

    rsi_cross_50 = _crossed_above(rsi_series, 50.0)
    rsi_ema_cross_50 = _crossed_above(rsi_ema, 50.0)
    rsi_bull_setup = rsi > 50 and rsi_ema[i] > 50 and (rsi_wma[i] is None or rsi_wma[i] < rsi)
    rsi_bear_setup = rsi < 50 and rsi_ema[i] < 50 and (rsi_wma[i] is None or rsi_wma[i] > rsi)

    vwap_reclaim_bull = close > vwap and (closes[i - 1] <= vwap_series[i - 1] if i > 0 else True)
    vwap_reclaim_bear = close < vwap and (closes[i - 1] >= vwap_series[i - 1] if i > 0 else True)
    ema_reclaim_bull = close > ema_exec[i] and closes[max(0, i - 2)] <= ema_exec[max(0, i - 2)]
    ema_reclaim_bear = close < ema_exec[i] and closes[max(0, i - 2)] >= ema_exec[max(0, i - 2)]

    vwap_dist_atr = abs(close - vwap) / atr
    not_excessive_vwap = vwap_dist_atr < 2.2

    bull_mom = (rsi_cross_50 or rsi_ema_cross_50 or (rsi > 52 and rsi > (rsi_series[i - 1] or rsi))) and dip >= dim
    bear_mom = (rsi < 48 and rsi < (rsi_series[i - 1] or rsi) or rsi < 50) and dim >= dip
    if rsi_cross_50 or rsi_ema_cross_50:
        bear_mom = rsi < 48 and dim > dip

    bull_pts = 0.0
    if trend_bull:
        bull_pts += 15
    if bull_mom or rsi_bull_setup:
        bull_pts += 20
    if _phase_favorable(market_phase):
        bull_pts += 15
    if vwap_reclaim_bull:
        bull_pts += 15
    if ema_reclaim_bull:
        bull_pts += 10
    if rel_vol_improving:
        bull_pts += 10
    if adx_rising_base:
        bull_pts += 10
    if rsi_bull_setup:
        bull_pts += 5
    if not_excessive_vwap:
        bull_pts += 5

    bear_pts = 0.0
    if trend_bear:
        bear_pts += 15
    if bear_mom or rsi_bear_setup:
        bear_pts += 20
    if _phase_favorable(market_phase):
        bear_pts += 15
    if vwap_reclaim_bear:
        bear_pts += 15
    if ema_reclaim_bear:
        bear_pts += 10
    if rel_vol_improving:
        bear_pts += 10
    if adx_rising_base:
        bear_pts += 10
    if rsi_bear_setup:
        bear_pts += 5
    if not_excessive_vwap:
        bear_pts += 5

    bull_dir = bull_pts >= bear_pts
    pb_q = compute_pullback_quality(opens, highs, lows, closes, ema_exec, atr, bull_dir=bull_dir)
    ext_r = compute_extension_risk(
        close, vwap, ema_exec[i], atr, rsi, highs, lows, closes, opens, bull_dir=bull_dir
    )

    if bull_dir:
        bull_pts += pb_q * 0.12
        bull_pts -= ext_r * 0.18
    else:
        bear_pts += pb_q * 0.12
        bear_pts -= ext_r * 0.18

    evs_result = compute_expansion_velocity(candles, bull_dir=bull_dir)
    if evs_result:
        accel = max(0.0, (evs_result.evs_score - 48.0) * 0.22)
        if evs_result.compression_broken:
            accel += 6.0
        if evs_result.adx_accelerating and evs_result.range_expanding:
            accel += 5.0
        if bull_dir:
            bull_pts += accel
        else:
            bear_pts += accel

    bull_pts = max(0.0, min(100.0, bull_pts))
    bear_pts = max(0.0, min(100.0, bear_pts))

    steps: List[str] = []
    if bull_dir:
        if vwap_reclaim_bull:
            steps.append("VWAP RECLAIM")
        if ema_reclaim_bull:
            steps.append("EMA RECLAIM")
        if bull_mom:
            steps.append("MOMENTUM SHIFT")
        if pb_q >= 55:
            steps.append("PULLBACK OK")
        if evs_result and evs_result.compression_broken and evs_result.evs_score >= 52:
            steps.append("IGNITION")
    else:
        if vwap_reclaim_bear:
            steps.append("VWAP RECLAIM")
        if ema_reclaim_bear:
            steps.append("EMA RECLAIM")
        if bear_mom:
            steps.append("MOMENTUM SHIFT")
        if pb_q >= 55:
            steps.append("PULLBACK OK")
        if evs_result and evs_result.compression_broken and evs_result.evs_score >= 52:
            steps.append("IGNITION")

    transition_state = " · ".join(steps) if steps else "SCANNING"

    vwap_status = "RECLAIMED" if (vwap_reclaim_bull if bull_dir else vwap_reclaim_bear) else (
        "NEAR VWAP" if vwap_dist_atr < 0.6 else ("ABOVE VWAP" if close > vwap else "BELOW VWAP")
    )
    ema_status = "RECLAIMED" if (ema_reclaim_bull if bull_dir else ema_reclaim_bear) else (
        "ABOVE EMA5" if close > ema_exec[i] else "BELOW EMA5"
    )
    if rsi_cross_50 or rsi_ema_cross_50:
        rsi_status = "CROSSING 50"
    elif rsi_bull_setup or rsi_bear_setup:
        rsi_status = "EARLY SETUP"
    else:
        rsi_status = f"RSI {rsi:.0f}"

    return TransitionScores(
        tps_bull=bull_pts,
        tps_bear=bear_pts,
        pullback_quality=pb_q,
        extension_risk=ext_r,
        transition_state=transition_state,
        vwap_reclaim_status=vwap_status,
        ema_reclaim_status=ema_status,
        rsi_transition_status=rsi_status,
        bull_dir=bull_dir,
        trend_pass=trend_bull if bull_dir else trend_bear,
        momentum_improving=bull_mom if bull_dir else bear_mom,
        market_phase=market_phase,
    )


def validate_execution_5m(
    candles: Sequence[Dict[str, Any]],
    *,
    bull_dir: bool,
) -> ExecutionValidation:
    """
    5m execution sequence:
    1. VWAP reclaim  2. EMA reclaim  3. Momentum  4. Shallow pullback  5. Pullback high reclaim
    """
    if not candles or len(candles) < 40:
        return ExecutionValidation(False, "INSUFFICIENT DATA", 0)

    opens = [float(c.get("open") or 0) for c in candles]
    highs = [float(c.get("high") or 0) for c in candles]
    lows = [float(c.get("low") or 0) for c in candles]
    closes = [float(c.get("close") or 0) for c in candles]
    volumes = [float(c.get("volume") or 0) for c in candles]
    i = len(closes) - 1
    close = closes[i]

    vwap_series = cumulative_vwap(highs, lows, closes, volumes)
    vwap = vwap_series[i]
    ema_exec = ema_series(closes, EMA_EXEC_LEN)
    rsi_series = _rsi_wilder(closes, RSI_LEN)
    rsi = rsi_series[i] or 50.0
    atr_series = _wilder_atr(highs, lows, closes, 14)
    atr = atr_series[i] or 1.0

    if bull_dir:
        s1 = close > vwap
        s2 = close > ema_exec[i]
        s3 = rsi > 50 or (rsi_series[i - 1] is not None and rsi_series[i - 1] <= 50 and rsi > 50)
        pb_q = compute_pullback_quality(opens, highs, lows, closes, ema_exec, atr, bull_dir=True)
        s4 = pb_q >= 50
        pb_hi = max(highs[max(0, i - PULLBACK_LB) : i]) if i > 0 else highs[i]
        s5 = close > pb_hi
    else:
        s1 = close < vwap
        s2 = close < ema_exec[i]
        s3 = rsi < 50 or (rsi_series[i - 1] is not None and rsi_series[i - 1] >= 50 and rsi < 50)
        pb_q = compute_pullback_quality(opens, highs, lows, closes, ema_exec, atr, bull_dir=False)
        s4 = pb_q >= 50
        pb_lo = min(lows[max(0, i - PULLBACK_LB) : i]) if i > 0 else lows[i]
        s5 = close < pb_lo

    steps = [s1, s2, s3, s4, s5]
    passed = sum(1 for x in steps if x)
    labels = ["VWAP", "EMA5", "MOMENTUM", "PULLBACK", "RECLAIM"]
    last_pass = 0
    for idx, ok in enumerate(steps):
        if ok:
            last_pass = idx + 1
    step_label = labels[last_pass - 1] if last_pass else "PENDING"
    validated = passed >= 3 and s1 and s2
    return ExecutionValidation(validated=validated, step_label=step_label, steps_passed=passed)


def classify_early_transition(
    tps: TransitionScores,
    *,
    ecs_trade_type: str,
    ecs_bull: float,
    ecs_bear: float,
    execution: Optional[ExecutionValidation] = None,
    require_execution: bool = False,
) -> Optional[str]:
    """Return EARLY LONG/SHORT TRANSITION or None."""
    score = tps.tps_score
    if score < TPS_EARLY_THRESHOLD:
        return None
    if tps.extension_risk > EXTENSION_RISK_CAP:
        return None
    if not tps.trend_pass and not tps.momentum_improving:
        return None

    tt = ecs_trade_type or ""
    if tps.bull_dir:
        if "SHORT" in tt and "WATCH" not in tt:
            return None
        if ecs_bull >= 85 and "A+" in tt:
            return None
        if require_execution and execution and not execution.validated:
            return None
        return EARLY_LONG
    else:
        if "LONG" in tt and "WATCH" not in tt:
            return None
        if ecs_bear >= 85 and "A+" in tt:
            return None
        if require_execution and execution and not execution.validated:
            return None
        return EARLY_SHORT


def merge_trade_type(
    ecs_trade_type: str,
    early: Optional[str],
) -> str:
    if early:
        return early
    return ecs_trade_type
