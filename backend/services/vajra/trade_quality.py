"""
Trade Quality Engine — standalone qualification on OHLCV (no TradingView).

Signal inputs (TPS/EES/ECS) inform direction; scores come from structure,
momentum, pullback, breakout, volume, HTF alignment, and extension control.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.services.vajra.engine import ADX_LEN, _dmi_adx, _rsi_wilder
from backend.services.vajra.indicators import cumulative_vwap, ema_series
from backend.services.vajra.transition import IMPULSE_LB, PULLBACK_LB

STATE_EXECUTABLE = "EXECUTABLE"
STATE_WATCHLIST = "WATCHLIST"
STATE_REJECT = "REJECT"

EXECUTABLE_CONFIDENCE_MIN = 75.0

_WEIGHTS = {
    "structure": 0.22,
    "momentum": 0.20,
    "breakout": 0.18,
    "trend": 0.15,
    "pullback": 0.10,
    "volume": 0.05,
    "htf_alignment": 0.05,
    "volatility_quality": 0.03,
    "extension_quality": 0.02,
}


@dataclass
class TradeQualityResult:
    trend_score: float
    momentum_score: float
    structure_score: float
    volume_score: float
    breakout_score: float
    pullback_score: float
    extension_risk_score: float
    htf_alignment_score: float
    volatility_quality_score: float
    execution_score: float
    confidence: float
    trade_quality_score: float
    state: str
    reject_reasons: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trend_score": round(self.trend_score, 1),
            "momentum_score": round(self.momentum_score, 1),
            "structure_score": round(self.structure_score, 1),
            "volume_score": round(self.volume_score, 1),
            "breakout_score": round(self.breakout_score, 1),
            "pullback_score": round(self.pullback_score, 1),
            "extension_risk_score": round(self.extension_risk_score, 1),
            "htf_alignment_score": round(self.htf_alignment_score, 1),
            "volatility_quality_score": round(self.volatility_quality_score, 1),
            "execution_score": round(self.execution_score, 1),
            "confidence": round(self.confidence, 1),
            "trade_quality_score": round(self.trade_quality_score, 1),
            "entry_state": self.state,
            "trade_quality_state": self.state,
            "reject_reasons": self.reject_reasons,
        }


def _ohlcv(candles: Sequence[Dict[str, Any]]) -> Tuple[List[float], ...]:
    o = [float(c.get("open") or 0) for c in candles]
    h = [float(c.get("high") or 0) for c in candles]
    l = [float(c.get("low") or 0) for c in candles]
    c = [float(c.get("close") or 0) for c in candles]
    v = [float(c.get("volume") or 0) for c in candles]
    return o, h, l, c, v


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _swing_structure_score(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    *,
    bull_dir: bool,
    lookback: int = 16,
) -> float:
    n = len(closes)
    if n < lookback + 2:
        return 40.0
    start = n - lookback
    hh = hl = lh = ll = 0
    for i in range(start + 1, n):
        if highs[i] > highs[i - 1]:
            hh += 1
        else:
            lh += 1
        if lows[i] > lows[i - 1]:
            hl += 1
        else:
            ll += 1
    total = max(1, hh + hl + ll + hl)
    if bull_dir:
        score = 50 + (hh + hl) / total * 35 - lh * 0.15 - ll * 0.25
    else:
        score = 50 + (lh + ll) / total * 35 - hh * 0.15 - hl * 0.25
    rng_hi = max(highs[start:n])
    rng_lo = min(lows[start:n])
    rng = rng_hi - rng_lo
    if rng > 0:
        pos = (closes[-1] - rng_lo) / rng
        if bull_dir:
            score += (pos - 0.5) * 20
        else:
            score += (0.5 - pos) * 20
    return _clamp(score)


def _ema_slope_score(closes: Sequence[float], period: int = 20) -> float:
    if len(closes) < period + 3:
        return 50.0
    ema = ema_series(closes, period)
    i = len(closes) - 1
    if ema[i] is None or ema[i - 3] is None or ema[i - 3] == 0:
        return 50.0
    slope_pct = (ema[i] - ema[i - 3]) / abs(ema[i - 3]) * 100
    return _clamp(50 + slope_pct * 25)


def _chop_penalty(
    closes: Sequence[float],
    ema_period: int = 20,
    lookback: int = 12,
) -> float:
    """Higher = more chop (bad). Returns penalty 0–40."""
    if len(closes) < lookback + ema_period:
        return 0.0
    ema = ema_series(closes, ema_period)
    n = len(closes)
    crosses = 0
    alts = 0
    for i in range(n - lookback, n - 1):
        if ema[i] is None or ema[i + 1] is None:
            continue
        side_a = closes[i] > ema[i]
        side_b = closes[i + 1] > ema[i + 1]
        if side_a != side_b:
            crosses += 1
        if i > n - lookback:
            up = closes[i] > closes[i - 1]
            up_prev = closes[i - 1] > closes[i - 2]
            if up != up_prev:
                alts += 1
    penalty = min(40, crosses * 8 + alts * 3)
    return float(penalty)


def _structure_score(
    o: List[float],
    h: List[float],
    l: List[float],
    c: List[float],
    v: List[float],
    *,
    bull_dir: bool,
) -> Tuple[float, List[str]]:
    reasons: List[str] = []
    i = len(c) - 1
    if i < 25:
        return 35.0, ["insufficient_bars"]

    vwap_s = cumulative_vwap(h, l, c, v)
    vwap = vwap_s[i]
    close = c[i]
    ema20 = ema_series(c, 20)
    ema = ema20[i] if ema20[i] is not None else close

    swing = _swing_structure_score(h, l, c, bull_dir=bull_dir)
    slope = _ema_slope_score(c, 20)
    chop = _chop_penalty(c, 20, 12)

    score = swing * 0.45 + slope * 0.25 + 50 * 0.30
    score -= chop

    if bull_dir:
        if close > vwap:
            score += 8
        else:
            score -= 12
            reasons.append("below_vwap")
        if close > ema:
            score += 6
        else:
            score -= 10
            reasons.append("below_ema")
    else:
        if close < vwap:
            score += 8
        else:
            score -= 12
            reasons.append("above_vwap")
        if close < ema:
            score += 6
        else:
            score -= 10
            reasons.append("above_ema")

    bodies = [abs(c[j] - o[j]) for j in range(max(0, i - 8), i + 1)]
    wicks = [h[j] - l[j] - bodies[j - max(0, i - 8)] for j in range(max(0, i - 8), i + 1)]
    if bodies and sum(bodies) / len(bodies) < 0.35 * (sum(h[j] - l[j] for j in range(max(0, i - 8), i + 1)) / len(bodies)):
        score -= 8
        reasons.append("weak_candle_spread")

    return _clamp(score), reasons


def _momentum_score(
    h: List[float],
    l: List[float],
    c: List[float],
    *,
    bull_dir: bool,
) -> Tuple[float, List[str]]:
    reasons: List[str] = []
    if len(c) < 30:
        return 40.0, reasons
    i = len(c) - 1
    rsi = _rsi_wilder(c, 14)
    r = float(rsi[i] or 50)
    r_prev = float(rsi[i - 3] or r) if i >= 3 else r
    di_p, di_m, adx = _dmi_adx(h, l, c, ADX_LEN)
    adx_v = float(adx[i] or 0)
    dip = float(di_p[i] or 0)
    dim = float(di_m[i] or 0)

    score = 45.0
    if bull_dir:
        if r >= 55:
            score += 15
        if r >= 60:
            score += 8
        if r > 78:
            score -= 20
            reasons.append("rsi_overbought")
        if dip > dim:
            score += 12
        else:
            score -= 15
            reasons.append("di_conflict")
        if r < r_prev - 2:
            score -= 10
            reasons.append("rsi_falling")
        elif r > r_prev + 1:
            score += 8
    else:
        if r <= 45:
            score += 15
        if r <= 40:
            score += 8
        if r < 22:
            score -= 20
            reasons.append("rsi_oversold")
        if dim > dip:
            score += 12
        else:
            score -= 15
            reasons.append("di_conflict")
        if r > r_prev + 2:
            score -= 10
            reasons.append("rsi_rising")
        elif r < r_prev - 1:
            score += 8

    if adx_v >= 22:
        score += min(15, (adx_v - 18) * 0.8)
    elif adx_v < 16:
        score -= 12
        reasons.append("weak_adx")

    return _clamp(score), reasons


def _pullback_score(
    o: List[float],
    h: List[float],
    l: List[float],
    c: List[float],
    v: List[float],
    *,
    bull_dir: bool,
    existing_pb: Optional[float] = None,
) -> float:
    i = len(c) - 1
    if i < PULLBACK_LB + IMPULSE_LB + 2:
        return float(existing_pb or 50.0)
    look_imp = max(0, i - IMPULSE_LB - PULLBACK_LB)
    imp_hi = max(h[look_imp:i + 1])
    imp_lo = min(l[look_imp:i + 1])
    imp_rng = imp_hi - imp_lo
    if imp_rng <= 0:
        return float(existing_pb or 50.0)

    pb_lo = min(l[i - PULLBACK_LB : i + 1])
    pb_hi = max(h[i - PULLBACK_LB : i + 1])
    if bull_dir:
        retrace = (imp_hi - pb_lo) / imp_rng
        reclaim = c[i] > o[i] and c[i] > (pb_hi + pb_lo) / 2
    else:
        retrace = (pb_hi - imp_lo) / imp_rng
        reclaim = c[i] < o[i] and c[i] < (pb_hi + pb_lo) / 2

    score = 50.0
    if 0.15 <= retrace <= 0.42:
        score += 25
    elif retrace > 0.55:
        score -= 25
    elif retrace < 0.08:
        score -= 10

    vol_imp = sum(v[max(0, i - IMPULSE_LB - 3) : i - PULLBACK_LB + 1]) or 1
    vol_pb = sum(v[i - PULLBACK_LB : i + 1]) or 1
    if vol_pb < vol_imp * 0.85:
        score += 12
    else:
        score -= 8

    if reclaim:
        score += 15
    rng = h[i] - l[i]
    if rng > 0:
        wick_reject = (min(c[i], o[i]) - l[i]) / rng if bull_dir else (h[i] - max(c[i], o[i])) / rng
        if wick_reject > 0.45:
            score += 10

    if existing_pb is not None:
        score = score * 0.6 + float(existing_pb) * 0.4
    return _clamp(score)


def _breakout_score(
    o: List[float],
    h: List[float],
    l: List[float],
    c: List[float],
    v: List[float],
    *,
    bull_dir: bool,
) -> float:
    i = len(c) - 1
    if i < 12:
        return 40.0
    look = max(0, i - 10)
    range_hi = max(h[look:i])
    range_lo = min(l[look:i])
    close = c[i]
    vol_ma = sum(v[look:i]) / max(1, i - look)

    score = 45.0
    if bull_dir and close > range_hi * 0.998:
        score += 20
    elif not bull_dir and close < range_lo * 1.002:
        score += 20

    rng = h[i] - l[i]
    if rng > 0:
        close_pos = (c[i] - l[i]) / rng
        if bull_dir and close_pos > 0.65:
            score += 15
        elif not bull_dir and close_pos < 0.35:
            score += 15
        upper = (h[i] - max(c[i], o[i])) / rng
        lower = (min(c[i], o[i]) - l[i]) / rng
        if bull_dir and upper > 0.45:
            score -= 15
        if not bull_dir and lower > 0.45:
            score -= 15

    if v[i] > vol_ma * 1.2:
        score += 12
    elif v[i] < vol_ma * 0.7:
        score -= 10

    if i >= 1:
        prev_rng = h[i - 1] - l[i - 1]
        if rng > prev_rng * 1.15:
            score += 8
        if bull_dir and c[i] > c[i - 1] and c[i - 1] > o[i - 1]:
            score += 10
        if not bull_dir and c[i] < c[i - 1] and c[i - 1] < o[i - 1]:
            score += 10
        if bull_dir and c[i] < c[i - 1] and close > range_hi * 0.995:
            score -= 18
        if not bull_dir and c[i] > c[i - 1] and close < range_lo * 1.005:
            score -= 18

    return _clamp(score)


def _extension_quality(
    o: List[float],
    h: List[float],
    l: List[float],
    c: List[float],
    v: List[float],
    *,
    bull_dir: bool,
    extension_risk: Optional[float] = None,
) -> Tuple[float, float]:
    """Returns (extension_quality 0-100, raw extension_risk 0-100)."""
    i = len(c) - 1
    if extension_risk is not None:
        raw_risk = float(extension_risk)
    else:
        raw_risk = 50.0
        exp = 0
        for j in range(max(0, i - 4), i + 1):
            rng = h[j] - l[j]
            if rng <= 0:
                continue
            body = abs(c[j] - o[j])
            if body / rng > 0.65 and ((bull_dir and c[j] > o[j]) or (not bull_dir and c[j] < o[j])):
                exp += 1
        raw_risk = _clamp(exp * 22, 0, 100)

    vwap_s = cumulative_vwap(h, l, c, v)
    dist_vwap = abs(c[i] - vwap_s[i]) / vwap_s[i] * 100 if vwap_s[i] else 0
    if dist_vwap > 1.5:
        raw_risk = min(100, raw_risk + 15)
    if dist_vwap > 2.5:
        raw_risk = min(100, raw_risk + 20)

    quality = _clamp(100 - raw_risk)
    return quality, raw_risk


def _htf_alignment(candles_1hr: Optional[Sequence[Dict[str, Any]]], *, bull_dir: bool) -> float:
    if not candles_1hr or len(candles_1hr) < 25:
        return 55.0
    _, _, _, c, _ = _ohlcv(candles_1hr)
    ema = ema_series(c, 20)
    i = len(c) - 1
    if ema[i] is None:
        return 55.0
    if bull_dir and c[i] > ema[i]:
        return 78.0
    if not bull_dir and c[i] < ema[i]:
        return 78.0
    return 38.0


def _volatility_quality(h: List[float], l: List[float], c: List[float]) -> float:
    i = len(c) - 1
    if i < 15:
        return 50.0
    ranges = [h[j] - l[j] for j in range(i - 14, i + 1)]
    avg = sum(ranges) / len(ranges)
    cur = ranges[-1]
    if avg <= 0:
        return 50.0
    ratio = cur / avg
    if 0.85 <= ratio <= 1.35:
        return 75.0
    if ratio < 0.55:
        return 30.0
    if ratio > 2.2:
        return 35.0
    return 55.0


def _trend_score(
    structure: float,
    htf: float,
    *,
    market_phase: str,
    bull_dir: bool,
) -> float:
    phase = (market_phase or "").upper()
    score = structure * 0.35 + htf * 0.35 + 30
    if bull_dir and phase in ("EXPANSION", "TRENDING", "BREAKOUT"):
        score += 12
    if not bull_dir and phase in ("EXPANSION", "TRENDING", "BREAKDOWN"):
        score += 12
    if phase in ("COMPRESSION", "ROTATIONAL"):
        score -= 18
    return _clamp(score)


def _execution_score(
    *,
    ees_score: Optional[float],
    execution_validated: bool,
    pullback: float,
    momentum: float,
) -> float:
    base = 50.0
    if ees_score is not None:
        base = float(ees_score) * 0.7 + base * 0.3
    if execution_validated:
        base += 12
    base += (pullback - 50) * 0.1 + (momentum - 50) * 0.08
    return _clamp(base)


def _classify_state(
    *,
    confidence: float,
    structure: float,
    momentum: float,
    extension_quality: float,
    market_phase: str,
    reject_reasons: List[str],
    hard_reject: bool,
) -> str:
    phase = (market_phase or "").upper()
    if hard_reject:
        return STATE_REJECT
    if phase == "COMPRESSION" and momentum < 50 and structure < 55:
        reject_reasons.append("compression_chop")
        return STATE_REJECT
    if structure < 38 or momentum < 32:
        reject_reasons.append("weak_core_scores")
        return STATE_REJECT
    if extension_quality < 28:
        reject_reasons.append("over_extended")
        return STATE_REJECT
    if (
        confidence >= EXECUTABLE_CONFIDENCE_MIN
        and structure >= 62
        and momentum >= 58
        and extension_quality >= 45
    ):
        return STATE_EXECUTABLE
    if confidence < 42 or (structure < 48 and momentum < 45):
        return STATE_REJECT
    return STATE_WATCHLIST


def compute_trade_quality(
    *,
    candles_30m: Sequence[Dict[str, Any]],
    candles_5m: Optional[Sequence[Dict[str, Any]]] = None,
    candles_1hr: Optional[Sequence[Dict[str, Any]]] = None,
    bull_dir: bool,
    market_phase: str = "",
    extension_risk: Optional[float] = None,
    pullback_quality: Optional[float] = None,
    ees_score: Optional[float] = None,
    execution_validated: bool = False,
    structure_pass: bool = False,
    momentum_pass: bool = False,
    trend_pass: bool = False,
    volume_pass: bool = False,
) -> Optional[TradeQualityResult]:
    """Qualification layer: scores + EXECUTABLE / WATCHLIST / REJECT."""
    primary = candles_5m if candles_5m and len(candles_5m) >= 30 else candles_30m
    if not primary or len(primary) < 30:
        return None

    o, h, l, c, v = _ohlcv(primary)
    o30, h30, l30, c30, v30 = _ohlcv(candles_30m) if candles_30m else (o, h, l, c, v)

    struct, struct_reasons = _structure_score(o30, h30, l30, c30, v30, bull_dir=bull_dir)
    mom, mom_reasons = _momentum_score(h30, l30, c30, bull_dir=bull_dir)
    pb = _pullback_score(o, h, l, c, v, bull_dir=bull_dir, existing_pb=pullback_quality)
    brk = _breakout_score(o, h, l, c, v, bull_dir=bull_dir)
    ext_q, ext_risk = _extension_quality(
        o, h, l, c, v, bull_dir=bull_dir, extension_risk=extension_risk
    )
    htf = _htf_alignment(candles_1hr, bull_dir=bull_dir)
    vol_qual = _volatility_quality(h, l, c)
    trend = _trend_score(struct, htf, market_phase=market_phase, bull_dir=bull_dir)

    i = len(c) - 1
    vol_ma = sum(v[max(0, i - 19) : i + 1]) / min(20, i + 1)
    vol_score = 55.0
    if v[i] > vol_ma * 1.2:
        vol_score = 78.0
    elif v[i] < vol_ma * 0.75:
        vol_score = 38.0
    if volume_pass:
        vol_score = min(100, vol_score + 10)

    exec_sc = _execution_score(
        ees_score=ees_score,
        execution_validated=execution_validated,
        pullback=pb,
        momentum=mom,
    )

    if not structure_pass and struct < 55:
        struct = min(struct, 48)
    if not momentum_pass and mom < 55:
        mom = min(mom, 48)
    if not trend_pass:
        trend = min(trend, 52)

    trade_quality_score = (
        struct * _WEIGHTS["structure"]
        + mom * _WEIGHTS["momentum"]
        + brk * _WEIGHTS["breakout"]
        + trend * _WEIGHTS["trend"]
        + pb * _WEIGHTS["pullback"]
        + vol_score * _WEIGHTS["volume"]
        + htf * _WEIGHTS["htf_alignment"]
        + vol_qual * _WEIGHTS["volatility_quality"]
        + ext_q * _WEIGHTS["extension_quality"]
    )

    reject_reasons = list(dict.fromkeys(struct_reasons + mom_reasons))
    chop = _chop_penalty(c30, 20, 12)
    hard_reject = chop >= 28 or (
        (market_phase or "").upper() in ("COMPRESSION", "ROTATIONAL")
        and mom < 48
        and brk < 45
    )

    confidence = trade_quality_score
    if hard_reject:
        confidence = min(confidence, 40)
    state = _classify_state(
        confidence=confidence,
        structure=struct,
        momentum=mom,
        extension_quality=ext_q,
        market_phase=market_phase,
        reject_reasons=reject_reasons,
        hard_reject=hard_reject,
    )

    return TradeQualityResult(
        trend_score=trend,
        momentum_score=mom,
        structure_score=struct,
        volume_score=vol_score,
        breakout_score=brk,
        pullback_score=pb,
        extension_risk_score=ext_risk,
        htf_alignment_score=htf,
        volatility_quality_score=vol_qual,
        execution_score=exec_sc,
        confidence=confidence,
        trade_quality_score=trade_quality_score,
        state=state,
        reject_reasons=reject_reasons,
    )
