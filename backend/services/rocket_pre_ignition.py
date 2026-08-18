"""Rocket Pre-Ignition score — 10m futures candle coil detector (0–4).

Candle-only proxies for seller failure, pressure build-up, shallower pullbacks,
and volume wake-up. Not true bid/ask delta. Not a Kavach readiness substitute.

Tune thresholds in the constants below.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)

# --- tunables (10-minute completed bars) -----------------------------------
ROCKET_MIN_BARS = 20
ROCKET_TINY = 1e-9

# Signal 1: lower-wick / failed-down close
ROCKET_SELLER_FAIL_WICK = 0.50  # (close-low) / range

# Signal 2: pseudo cumulative-delta lead vs price
ROCKET_CUMDELTA_PCT = 0.97  # cum-delta at/near 20-bar high
ROCKET_PRICE_LAG_PCT = 0.995  # close still below 20-bar high

# Signal 4: volume coil wake-up
ROCKET_VOL_WAKE_MULT = 1.50
ROCKET_VOL_CLOSE_POS = 0.60  # close in upper 40% of bar

# Anti-chase: suppress 3/4 when already stretched
ROCKET_EMA_SPAN = 5
ROCKET_ATR_BARS = 10
ROCKET_STRETCH_ATR = 1.50
ROCKET_STRETCH_HIGH_PCT = 0.998

_EMPTY = {
    "rocket_score": 0,
    "rocket_signals": [],
    "rocket_label": "",
}


def empty_rocket() -> Dict[str, Any]:
    return dict(_EMPTY)


def rocket_label_for(score: int) -> str:
    s = int(score or 0)
    if s <= 0:
        return ""
    return f"🚀 {min(s, 4)}/4"


def _f(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _ema_last(closes: Sequence[float], span: int) -> Optional[float]:
    if not closes:
        return None
    k = 2.0 / (float(span) + 1.0)
    e = float(closes[0])
    for x in closes[1:]:
        e = float(x) * k + e * (1.0 - k)
    return e


def compute_rocket_score(bars: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Score latest *completed* 10m OHLCV bars (open/high/low/close/volume).

    Returns rocket_score (0–4), rocket_signals (list of fired ids), rocket_label.
    """
    if bars is None or len(bars) < ROCKET_MIN_BARS:
        return empty_rocket()

    window = list(bars[-ROCKET_MIN_BARS:])
    last = window[-1]
    prev = window[-2]
    o = _f(last.get("open"))
    h = _f(last.get("high"))
    lo = _f(last.get("low"))
    c = _f(last.get("close"))
    prev_c = _f(prev.get("close"))
    bar_range = max(h - lo, ROCKET_TINY)
    close_pos = (c - lo) / bar_range

    score = 0
    signals: List[str] = []

    # Signal 1 — seller failure: red bar but price not yielding (close holds
    # prior close, or lower wick takes ≥50% of the range). Candle proxy, not delta.
    red = c < o
    seller_failure = red and (c >= prev_c or close_pos >= ROCKET_SELLER_FAIL_WICK)
    if seller_failure:
        score += 1
        signals.append("seller_failure")

    # Signal 2 — pressure build-up: signed-volume cum-delta near 20-bar high
    # while price has not yet taken the 20-bar high (lead vs lag).
    delta: List[float] = []
    for b in window:
        bo, bc, bv = _f(b.get("open")), _f(b.get("close")), _f(b.get("volume"))
        if bc > bo:
            delta.append(bv)
        elif bc < bo:
            delta.append(-bv)
        else:
            delta.append(0.0)
    cum = 0.0
    cum_series: List[float] = []
    for d in delta:
        cum += d
        cum_series.append(cum)
    cum_now = cum_series[-1]
    cum_high = max(cum_series)
    price_high_20 = max(_f(b.get("high")) for b in window)
    cumdelta_lead = (
        cum_high > 0
        and cum_now >= ROCKET_CUMDELTA_PCT * cum_high
        and c < ROCKET_PRICE_LAG_PCT * price_high_20
    )
    if cumdelta_lead:
        score += 1
        signals.append("cumdelta_lead")

    # Signal 3 — shallower pullbacks: last 3 lows rising, last high has not
    # yet taken out the prior 19-bar high (coil, not breakout).
    lows = [_f(b.get("low")) for b in window]
    highs = [_f(b.get("high")) for b in window]
    prior_high = max(highs[:-1]) if len(highs) > 1 else highs[-1]
    shallower_dips = lows[-1] > lows[-2] > lows[-3] and highs[-1] <= prior_high
    if shallower_dips:
        score += 1
        signals.append("shallower_dips")

    # Signal 4 — volume coil waking up: quiet prior 3 bars, then expansion
    # that closes strong in the upper half (wake-up, not climax).
    vols = [_f(b.get("volume")) for b in window]
    prior3 = vols[-4:-1]
    prior_avg = sum(prior3) / 3.0 if len(prior3) == 3 else 0.0
    volume_coil_wakeup = (
        prior_avg > 0
        and vols[-1] >= ROCKET_VOL_WAKE_MULT * prior_avg
        and c > o
        and close_pos >= ROCKET_VOL_CLOSE_POS
    )
    if volume_coil_wakeup:
        score += 1
        signals.append("volume_coil_wakeup")

    # Anti-chase: if already stretched, drop late-stage 3/4 so this stays
    # pre-ignition rather than a "already blasted off" badge.
    closes = [_f(b.get("close")) for b in window]
    ema5 = _ema_last(closes, ROCKET_EMA_SPAN)
    atr_n = min(ROCKET_ATR_BARS, len(window))
    atr_proxy = sum(_f(b.get("high")) - _f(b.get("low")) for b in window[-atr_n:]) / float(atr_n)
    overextended = False
    if ema5 is not None and atr_proxy > 0 and c > ema5 + ROCKET_STRETCH_ATR * atr_proxy:
        overextended = True
    if price_high_20 > 0 and c >= ROCKET_STRETCH_HIGH_PCT * price_high_20:
        overextended = True
    if overextended:
        for name in ("shallower_dips", "volume_coil_wakeup"):
            if name in signals:
                signals.remove(name)
                score -= 1

    score = max(0, min(4, score))
    return {
        "rocket_score": score,
        "rocket_signals": signals,
        "rocket_label": rocket_label_for(score),
    }


def log_rocket(symbol: str, result: Mapping[str, Any], *, level: int = logging.DEBUG) -> None:
    score = int(result.get("rocket_score") or 0)
    if score <= 0:
        return
    sigs = ",".join(result.get("rocket_signals") or [])
    logger.log(level, "[ROCKET] %s score=%s signals=%s", symbol, score, sigs or "-")
