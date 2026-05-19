"""
Executable Entry Score (EES) — independent 5m execution timing model.

TPS = discovery / rotation strength (unchanged).
EES = "Can I safely enter RIGHT NOW?" with favorable RR.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from backend.services.vajra.engine import POC_BUCKETS, POC_LEN, _compute_poc, _dmi_adx, _rsi_wilder
from backend.services.vajra.indicators import cumulative_vwap, ema_series
from backend.services.vajra.transition import EMA_EXEC_LEN, IMPULSE_LB, PULLBACK_LB

# Entry / UI thresholds (independent of TPS discovery)
EES_ENTER_MIN = 65.0
TPS_ENTER_MIN = 52.0

ENTRY_EXECUTABLE = "EXECUTABLE"
ENTRY_PULLBACK = "PULLBACK PREFERRED"
ENTRY_WATCHLIST = "WATCHLIST ONLY"
ENTRY_AVOID = "AVOID CHASING"


@dataclass
class EESResult:
    ees_score: float
    entry_state: str
    ees_alerts: List[str]
    distance_from_ema_pct: float
    distance_from_vwap_pct: float
    distance_to_resistance_pct: Optional[float]
    expansion_count: int
    pullback_retrace_pct: Optional[float]
    vwap_reclaim_quality: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ees_score": round(self.ees_score, 1),
            "entry_state": self.entry_state,
            "ees_alerts": self.ees_alerts,
            "distance_from_ema_pct": round(self.distance_from_ema_pct, 3),
            "distance_from_vwap_pct": round(self.distance_from_vwap_pct, 3),
            "distance_to_resistance_pct": (
                round(self.distance_to_resistance_pct, 3)
                if self.distance_to_resistance_pct is not None
                else None
            ),
            "expansion_count": self.expansion_count,
            "pullback_retrace_pct": (
                round(self.pullback_retrace_pct, 3) if self.pullback_retrace_pct is not None else None
            ),
            "vwap_reclaim_quality": self.vwap_reclaim_quality,
        }


def classify_entry_state(ees_score: float) -> str:
    s = float(ees_score)
    if s >= 75:
        return ENTRY_EXECUTABLE
    if s >= 60:
        return ENTRY_PULLBACK
    if s >= 45:
        return ENTRY_WATCHLIST
    return ENTRY_AVOID


def _body_ratio(o: float, h: float, l: float, c: float) -> float:
    rng = h - l
    return abs(c - o) / rng if rng > 0 else 0.0


def _expansion_count(
    opens: Sequence[float],
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    *,
    bull_dir: bool,
    lookback: int = 5,
) -> int:
    i = len(closes) - 1
    start = max(0, i - lookback + 1)
    spreads = [highs[j] - lows[j] for j in range(max(0, i - 19), i + 1)]
    avg_spread = sum(spreads) / len(spreads) if spreads else 1.0
    count = 0
    for j in range(start, i + 1):
        rng = highs[j] - lows[j]
        if rng <= 0:
            continue
        if bull_dir:
            if closes[j] > opens[j] and rng > avg_spread * 1.3:
                count += 1
        elif closes[j] < opens[j] and rng > avg_spread * 1.3:
            count += 1
    return count


def _nearest_resistance_pct(
    close: float,
    highs: Sequence[float],
    lows: Sequence[float],
    volumes: Sequence[float],
    i: int,
    *,
    bull_dir: bool,
) -> Optional[float]:
    if close <= 0:
        return None
    look = max(0, i - 48)
    swing_hi = max(highs[look : i + 1])
    day_hi = max(highs[max(0, i - 78) : i + 1]) if i >= 10 else swing_hi
    prev_day_hi = max(highs[max(0, i - 156) : max(0, i - 78)]) if i >= 78 else day_hi
    poc = _compute_poc(highs[: i + 1], lows[: i + 1], volumes[: i + 1], POC_LEN, POC_BUCKETS)

    if bull_dir:
        levels = [x for x in (swing_hi, day_hi, prev_day_hi, poc) if x >= close * 0.999]
        if not levels:
            return 99.0
        nearest = min(levels)
        return (nearest - close) / close * 100
    levels = [x for x in (min(lows[look : i + 1]), poc) if x <= close * 1.001]
    if not levels:
        return 99.0
    nearest = max(levels)
    return (close - nearest) / close * 100


def _pullback_metrics(
    opens: Sequence[float],
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    volumes: Sequence[float],
    ema5: Sequence[float],
    i: int,
    *,
    bull_dir: bool,
) -> Dict[str, Any]:
    seg_start = max(0, i - IMPULSE_LB - PULLBACK_LB)
    seg_h = highs[seg_start : i + 1]
    seg_l = lows[seg_start : i + 1]
    seg_o = opens[seg_start : i + 1]
    seg_c = closes[seg_start : i + 1]
    seg_v = volumes[seg_start : i + 1]

    if bull_dir:
        impulse_hi = max(seg_h) if seg_h else highs[i]
        impulse_lo = min(seg_l[: max(1, len(seg_l) - PULLBACK_LB)]) if seg_l else lows[i]
        impulse_rng = max(impulse_hi - impulse_lo, 1e-6)
        pullback_lo = min(seg_l[-PULLBACK_LB:]) if seg_l else lows[i]
        retrace = (impulse_hi - pullback_lo) / impulse_rng
    else:
        impulse_lo = min(seg_l) if seg_l else lows[i]
        impulse_hi = max(seg_h[: max(1, len(seg_h) - PULLBACK_LB)]) if seg_h else highs[i]
        impulse_rng = max(impulse_hi - impulse_lo, 1e-6)
        pullback_hi = max(seg_h[-PULLBACK_LB:]) if seg_h else highs[i]
        retrace = (pullback_hi - impulse_lo) / impulse_rng

    pb_vol = sum(seg_v[-PULLBACK_LB:-1]) if len(seg_v) > 1 else 0.0
    bo_vol = seg_v[-1] if seg_v else volumes[i]
    pb_vol_lower = pb_vol < bo_vol * 1.05 if pb_vol > 0 else True

    ema_respect = True
    breakdown = False
    for j in range(-PULLBACK_LB, 0):
        ei = len(closes) + j
        if ei < 0:
            continue
        if bull_dir:
            if lows[ei] < ema5[ei] * 0.995:
                ema_respect = False
            body = abs(closes[ei] - opens[ei])
            rng = highs[ei] - lows[ei]
            if closes[ei] < opens[ei] and rng > 0 and body / rng > 0.55:
                breakdown = True
        else:
            if highs[ei] > ema5[ei] * 1.005:
                ema_respect = False
            body = abs(closes[ei] - opens[ei])
            rng = highs[ei] - lows[ei]
            if closes[ei] > opens[ei] and rng > 0 and body / rng > 0.55:
                breakdown = True

    rng_i = highs[i] - lows[i]
    if bull_dir:
        close_pos = (closes[i] - lows[i]) / rng_i if rng_i > 0 else 0.5
        reclaim_top = close_pos >= 0.7
    else:
        close_pos = (highs[i] - closes[i]) / rng_i if rng_i > 0 else 0.5
        reclaim_top = close_pos >= 0.7

    good_pullback = (
        0.25 <= retrace <= 0.45
        and pb_vol_lower
        and ema_respect
        and not breakdown
        and reclaim_top
    )
    shallow = retrace < 0.2 or retrace > 0.55
    strong_reclaim = reclaim_top and _body_ratio(opens[i], highs[i], lows[i], closes[i]) > 0.55

    return {
        "retrace": retrace,
        "good_pullback": good_pullback,
        "shallow": shallow,
        "strong_reclaim": strong_reclaim,
    }


def compute_ees(
    candles: Sequence[Dict[str, Any]],
    *,
    bull_dir: bool,
    tps_score: Optional[float] = None,
) -> Optional[EESResult]:
    """Compute EES from execution timeframe candles (5m preferred)."""
    if not candles or len(candles) < 30:
        return None

    opens = [float(c.get("open") or 0) for c in candles]
    highs = [float(c.get("high") or 0) for c in candles]
    lows = [float(c.get("low") or 0) for c in candles]
    closes = [float(c.get("close") or 0) for c in candles]
    volumes = [float(c.get("volume") or 0) for c in candles]
    i = len(closes) - 1
    close = closes[i]
    o, h, l, c = opens[i], highs[i], lows[i], closes[i]

    if close <= 0:
        return None

    vwap_s = cumulative_vwap(highs, lows, closes, volumes)
    vwap = vwap_s[i]
    ema5 = ema_series(closes, EMA_EXEC_LEN)
    ema = ema5[i]

    ees = 100.0

    dist_ema_pct = abs(close - ema) / ema * 100 if ema else 0.0
    dist_vwap_pct = abs(close - vwap) / vwap * 100 if vwap else 0.0

    if dist_ema_pct > 0.80:
        ees -= 10
    if dist_ema_pct > 1.20:
        ees -= 15
    if dist_ema_pct > 1.80:
        ees -= 25
    if dist_vwap_pct > 1.00:
        ees -= 10
    if dist_vwap_pct > 1.80:
        ees -= 20

    exp_count = _expansion_count(opens, highs, lows, closes, bull_dir=bull_dir)
    if exp_count >= 3:
        ees -= 10
    if exp_count >= 4:
        ees -= 20
    if exp_count >= 5:
        ees -= 30

    rsi_series = _rsi_wilder(closes, 14)
    rsi = float(rsi_series[i] or 50.0)
    if bull_dir:
        if rsi > 70:
            ees -= 10
        if rsi > 75:
            ees -= 15
        if rsi > 80:
            ees -= 25
    else:
        if rsi < 30:
            ees -= 10
        if rsi < 25:
            ees -= 15
        if rsi < 20:
            ees -= 25

    dist_res = _nearest_resistance_pct(close, highs, lows, volumes, i, bull_dir=bull_dir)
    if dist_res is not None:
        if dist_res < 0.50:
            ees -= 10
        if dist_res < 0.25:
            ees -= 20

    pb = _pullback_metrics(opens, highs, lows, closes, volumes, ema5, i, bull_dir=bull_dir)
    if pb["good_pullback"]:
        ees += 10
    if pb["strong_reclaim"]:
        ees += 5
    if pb["shallow"]:
        ees -= 15

    spreads = [highs[j] - lows[j] for j in range(max(0, i - 19), i + 1)]
    avg_spread = sum(spreads) / len(spreads) if spreads else 1.0
    cur_spread = highs[i] - lows[i]
    spread_ratio = cur_spread / avg_spread if avg_spread > 0 else 1.0
    if spread_ratio > 2.0:
        ees -= 10
    if spread_ratio > 2.8:
        ees -= 20

    rng_i = h - l
    upper_wick = (h - max(c, o)) / rng_i if rng_i > 0 else 0
    lower_wick = (min(c, o) - l) / rng_i if rng_i > 0 else 0
    body = abs(c - o)
    vol_ma = sum(volumes[max(0, i - 19) : i + 1]) / min(20, i + 1)
    vwap_quality = "neutral"

    if bull_dir:
        strong_reclaim = close > vwap and body > (h - max(c, o)) and volumes[i] > vol_ma
        weak_reclaim = upper_wick > 0.35 or (rng_i > 0 and (c - l) / rng_i < 0.55)
        if strong_reclaim:
            ees += 5
            vwap_quality = "strong"
        elif weak_reclaim and close > vwap:
            ees -= 5
            vwap_quality = "weak"
    else:
        strong_reclaim = close < vwap and body > (min(c, o) - l) and volumes[i] > vol_ma
        weak_reclaim = lower_wick > 0.35
        if strong_reclaim:
            ees += 5
            vwap_quality = "strong"
        elif weak_reclaim and close < vwap:
            ees -= 5
            vwap_quality = "weak"

    di_plus, di_minus, adx_series = _dmi_adx(highs, lows, closes, 14)
    adx = adx_series[i] or 0.0
    adx_prev = adx_series[i - 1] if i > 0 else adx
    dip = di_plus[i] or 0.0
    dim = di_minus[i] or 0.0
    if bull_dir:
        if adx > adx_prev and dip > dim:
            ees += 5
        elif adx < adx_prev and exp_count >= 3:
            ees -= 10
    else:
        if adx > adx_prev and dim > dip:
            ees += 5
        elif adx < adx_prev and exp_count >= 3:
            ees -= 10

    ees = max(0.0, min(100.0, ees))
    entry_state = classify_entry_state(ees)
    alerts = build_ees_alerts(
        ees_score=ees,
        entry_state=entry_state,
        tps_score=tps_score,
        expansion_count=exp_count,
        pullback_good=bool(pb["good_pullback"]),
        dist_res=dist_res,
    )

    return EESResult(
        ees_score=ees,
        entry_state=entry_state,
        ees_alerts=alerts,
        distance_from_ema_pct=dist_ema_pct,
        distance_from_vwap_pct=dist_vwap_pct,
        distance_to_resistance_pct=dist_res,
        expansion_count=exp_count,
        pullback_retrace_pct=float(pb["retrace"]),
        vwap_reclaim_quality=vwap_quality,
    )


def build_ees_alerts(
    *,
    ees_score: float,
    entry_state: str,
    tps_score: Optional[float],
    expansion_count: int,
    pullback_good: bool,
    dist_res: Optional[float],
) -> List[str]:
    alerts: List[str] = []
    tps = float(tps_score) if tps_score is not None else 0.0

    if tps >= TPS_ENTER_MIN and ees_score < 45:
        alerts.append("HIGH TPS BUT EXTENDED")
    if pullback_good and ees_score >= 60:
        alerts.append("PERFECT PULLBACK RECLAIM")
    if expansion_count >= 4:
        alerts.append("LATE IMPULSE WARNING")
    if entry_state == ENTRY_EXECUTABLE and tps >= TPS_ENTER_MIN:
        alerts.append("EXECUTABLE TRANSITION")
    if dist_res is not None and dist_res < 0.35 and ees_score < 55:
        alerts.append("RESISTANCE COLLISION RISK")

    seen: set = set()
    out: List[str] = []
    for a in alerts:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out


def enter_action_label(
    *,
    tps_score: Optional[float],
    ees_score: Optional[float],
    entry_state: Optional[str],
) -> Dict[str, Any]:
    """UI action: ENTER | WAIT PULLBACK | WATCH | EXTENDED (disabled)."""
    tps = float(tps_score) if tps_score is not None else 0.0
    ees = float(ees_score) if ees_score is not None else None
    state = entry_state or (classify_entry_state(ees) if ees is not None else ENTRY_WATCHLIST)

    if ees is None:
        return {"action": "WATCH", "enabled": False, "reason": "EES pending 5m data"}

    tps_ok = tps >= TPS_ENTER_MIN
    ees_ok = ees >= EES_ENTER_MIN

    if tps_ok and ees_ok:
        return {"action": "ENTER", "enabled": True, "reason": "TPS+EES executable"}

    if state == ENTRY_PULLBACK or (60 <= ees < EES_ENTER_MIN):
        return {"action": "WAIT PULLBACK", "enabled": False, "reason": "Prefer shallow pullback"}

    if state == ENTRY_AVOID or ees < 45:
        return {"action": "EXTENDED", "enabled": False, "reason": "Avoid chasing"}

    return {"action": "WATCH", "enabled": False, "reason": "Watch for better entry"}
