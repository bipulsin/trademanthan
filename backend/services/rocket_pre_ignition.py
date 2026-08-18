"""Rocket Pre-Ignition / Crash scores — 10m futures coil detector (0–4).

Candle + optional live signed-volume delta. Not a Kavach readiness substitute.

Tune thresholds in the constants below.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# --- tunables (completed or forming bars) -----------------------------------
ROCKET_MIN_BARS = 20
ROCKET_TINY = 1e-9

# Signal 1: wick / failed-direction close
ROCKET_SELLER_FAIL_WICK = 0.50  # (close-low) / range
ROCKET_BUYER_FAIL_WICK = 0.50  # (high-close) / range

# Signal 2: cumulative-delta lead vs price
ROCKET_CUMDELTA_PCT = 0.97
ROCKET_PRICE_LAG_PCT = 0.995

# Signal 4: volume coil wake-up
ROCKET_VOL_WAKE_MULT = 1.50
ROCKET_VOL_CLOSE_POS = 0.60  # close in upper 40% of bar
CRASH_VOL_CLOSE_POS = 0.40  # close in lower 40% of bar

# Anti-chase: suppress 3/4 when already stretched
ROCKET_EMA_SPAN = 5
ROCKET_ATR_BARS = 10
ROCKET_STRETCH_ATR = 1.50
ROCKET_STRETCH_HIGH_PCT = 0.998

# Session phases (agreed live behavior)
PHASE1_MAX_BARS = 3  # S3 disabled, max score 3
LOOKBACK_CAP = 20

_EMPTY = {
    "rocket_score": 0,
    "rocket_signals": [],
    "rocket_label": "",
}

_EMPTY_CRASH = {
    "crash_score": 0,
    "crash_signals": [],
    "crash_label": "",
}


def empty_rocket() -> Dict[str, Any]:
    return dict(_EMPTY)


def empty_crash() -> Dict[str, Any]:
    return dict(_EMPTY_CRASH)


def empty_rocket_crash() -> Dict[str, Any]:
    out = empty_rocket()
    out.update(empty_crash())
    out["active_side"] = ""
    out["lookback_used"] = 0
    out["session_bar_number"] = 0
    return out


def rocket_label_for(score: int) -> str:
    s = int(score or 0)
    if s <= 0:
        return ""
    return f"🚀 {min(s, 4)}/4"


def crash_label_for(score: int) -> str:
    s = int(score or 0)
    if s <= 0:
        return ""
    return f"💥 {min(s, 4)}/4"


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


def _bar_delta(b: Mapping[str, Any]) -> float:
    """Prefer live signed volume; else close-vs-open volume proxy."""
    if b.get("delta") is not None:
        return _f(b.get("delta"))
    bo, bc, bv = _f(b.get("open")), _f(b.get("close")), _f(b.get("volume"))
    if bc > bo:
        return bv
    if bc < bo:
        return -bv
    return 0.0


def _lookback_for(session_bar_count: Optional[int], n_bars: int) -> int:
    if session_bar_count is None:
        return LOOKBACK_CAP if n_bars >= ROCKET_MIN_BARS else 0
    n = min(int(session_bar_count), LOOKBACK_CAP, n_bars)
    return max(0, n)


def _ohlc(last: Mapping[str, Any]) -> Tuple[float, float, float, float, float]:
    o = _f(last.get("open"))
    h = _f(last.get("high"))
    lo = _f(last.get("low"))
    c = _f(last.get("close"))
    bar_range = max(h - lo, ROCKET_TINY)
    close_pos = (c - lo) / bar_range
    return o, h, lo, c, close_pos


def _cum_series(window: Sequence[Mapping[str, Any]]) -> List[float]:
    cum = 0.0
    out: List[float] = []
    for b in window:
        cum += _bar_delta(b)
        out.append(cum)
    return out


def compute_rocket_score(
    bars: Sequence[Mapping[str, Any]],
    *,
    session_bar_count: Optional[int] = None,
) -> Dict[str, Any]:
    """Score latest OHLCV bars (open/high/low/close/volume[, delta]).

    Legacy callers omit ``session_bar_count`` and still require 20 bars.
    Live WS path passes session bar count for adaptive lookback / phases.
    """
    both = compute_rocket_crash(bars, session_bar_count=session_bar_count)
    return {
        "rocket_score": both.get("rocket_score") or 0,
        "rocket_signals": list(both.get("rocket_signals") or []),
        "rocket_label": both.get("rocket_label") or "",
    }


def compute_crash_score(
    bars: Sequence[Mapping[str, Any]],
    *,
    session_bar_count: Optional[int] = None,
) -> Dict[str, Any]:
    both = compute_rocket_crash(bars, session_bar_count=session_bar_count)
    return {
        "crash_score": both.get("crash_score") or 0,
        "crash_signals": list(both.get("crash_signals") or []),
        "crash_label": both.get("crash_label") or "",
    }


def compute_rocket_crash(
    bars: Sequence[Mapping[str, Any]],
    *,
    session_bar_count: Optional[int] = None,
) -> Dict[str, Any]:
    """Bullish Rocket + bearish Crash on the same window."""
    out = empty_rocket_crash()
    if bars is None:
        return out
    n_bars = len(bars)
    lookback = _lookback_for(session_bar_count, n_bars)
    if lookback < 2:
        return out

    window = list(bars[-lookback:])
    last = window[-1]
    prev = window[-2]
    o, h, lo, c, close_pos = _ohlc(last)
    prev_c = _f(prev.get("close"))
    phase1 = session_bar_count is not None and int(session_bar_count) <= PHASE1_MAX_BARS
    out["lookback_used"] = lookback
    out["session_bar_number"] = int(session_bar_count or lookback)

    # --- Rocket (bullish) ---
    r_score = 0
    r_sigs: List[str] = []
    red = c < o
    if red and (c >= prev_c or close_pos >= ROCKET_SELLER_FAIL_WICK):
        r_score += 1
        r_sigs.append("seller_failure")

    cum_series = _cum_series(window)
    cum_now = cum_series[-1]
    cum_high = max(cum_series)
    price_high = max(_f(b.get("high")) for b in window)
    if (
        cum_high > 0
        and cum_now >= ROCKET_CUMDELTA_PCT * cum_high
        and c < ROCKET_PRICE_LAG_PCT * price_high
    ):
        r_score += 1
        r_sigs.append("cumdelta_lead")

    lows = [_f(b.get("low")) for b in window]
    highs = [_f(b.get("high")) for b in window]
    prior_high = max(highs[:-1]) if len(highs) > 1 else highs[-1]
    if (
        not phase1
        and len(lows) >= 3
        and lows[-1] > lows[-2] > lows[-3]
        and highs[-1] <= prior_high
    ):
        r_score += 1
        r_sigs.append("shallower_dips")

    vols = [_f(b.get("volume")) for b in window]
    prior3 = vols[-4:-1]
    prior_avg = sum(prior3) / 3.0 if len(prior3) == 3 else 0.0
    if (
        prior_avg > 0
        and vols[-1] >= ROCKET_VOL_WAKE_MULT * prior_avg
        and c > o
        and close_pos >= ROCKET_VOL_CLOSE_POS
    ):
        r_score += 1
        r_sigs.append("volume_coil_wakeup")

    closes = [_f(b.get("close")) for b in window]
    ema5 = _ema_last(closes, ROCKET_EMA_SPAN)
    atr_n = min(ROCKET_ATR_BARS, len(window))
    atr_proxy = sum(_f(b.get("high")) - _f(b.get("low")) for b in window[-atr_n:]) / float(atr_n)
    rocket_stretch = False
    if ema5 is not None and atr_proxy > 0 and c > ema5 + ROCKET_STRETCH_ATR * atr_proxy:
        rocket_stretch = True
    if price_high > 0 and c >= ROCKET_STRETCH_HIGH_PCT * price_high:
        rocket_stretch = True
    if rocket_stretch:
        for name in ("shallower_dips", "volume_coil_wakeup"):
            if name in r_sigs:
                r_sigs.remove(name)
                r_score -= 1
    if phase1:
        r_score = min(r_score, 3)
    r_score = max(0, min(4, r_score))
    out["rocket_score"] = r_score
    out["rocket_signals"] = r_sigs
    out["rocket_label"] = rocket_label_for(r_score)

    # --- Crash (bearish mirror) ---
    c_score = 0
    c_sigs: List[str] = []
    green = c > o
    upper_wick = (h - c) / max(h - lo, ROCKET_TINY)
    if green and (c <= prev_c or upper_wick >= ROCKET_BUYER_FAIL_WICK):
        c_score += 1
        c_sigs.append("buyer_failure")

    cum_low = min(cum_series)
    price_low = min(lows)
    if (
        cum_low < 0
        and cum_now <= ROCKET_CUMDELTA_PCT * cum_low
        and c > price_low / ROCKET_PRICE_LAG_PCT
    ):
        c_score += 1
        c_sigs.append("cumdelta_lead_down")

    prior_low = min(lows[:-1]) if len(lows) > 1 else lows[-1]
    if (
        not phase1
        and len(highs) >= 3
        and highs[-1] < highs[-2] < highs[-3]
        and lows[-1] >= prior_low
    ):
        c_score += 1
        c_sigs.append("falling_highs")

    if (
        prior_avg > 0
        and vols[-1] >= ROCKET_VOL_WAKE_MULT * prior_avg
        and c < o
        and close_pos <= CRASH_VOL_CLOSE_POS
    ):
        c_score += 1
        c_sigs.append("volume_coil_wakeup")

    crash_stretch = False
    if ema5 is not None and atr_proxy > 0 and c < ema5 - ROCKET_STRETCH_ATR * atr_proxy:
        crash_stretch = True
    if price_low > 0 and c <= ROCKET_STRETCH_HIGH_PCT * price_low:
        crash_stretch = True
    if crash_stretch:
        for name in ("falling_highs", "volume_coil_wakeup"):
            if name in c_sigs:
                c_sigs.remove(name)
                c_score -= 1
    if phase1:
        c_score = min(c_score, 3)
    c_score = max(0, min(4, c_score))
    out["crash_score"] = c_score
    out["crash_signals"] = c_sigs
    out["crash_label"] = crash_label_for(c_score)

    if r_score > c_score and r_score >= 1:
        out["active_side"] = "bullish_rocket"
    elif c_score > r_score and c_score >= 1:
        out["active_side"] = "bearish_crash"
    elif r_score >= 1 and r_score == c_score:
        out["active_side"] = "both"
    else:
        out["active_side"] = ""

    out["ema5"] = ema5
    out["atr10"] = atr_proxy
    out["candle_delta"] = _bar_delta(last)
    out["cumulative_delta"] = cum_now
    return out


def log_rocket(symbol: str, result: Mapping[str, Any], *, level: int = logging.DEBUG) -> None:
    score = int(result.get("rocket_score") or 0)
    if score <= 0:
        return
    sigs = ",".join(result.get("rocket_signals") or [])
    logger.log(level, "[ROCKET] %s score=%s signals=%s", symbol, score, sigs or "-")


def log_crash(symbol: str, result: Mapping[str, Any], *, level: int = logging.DEBUG) -> None:
    score = int(result.get("crash_score") or 0)
    if score <= 0:
        return
    sigs = ",".join(result.get("crash_signals") or [])
    logger.log(level, "[CRASH] %s score=%s signals=%s", symbol, score, sigs or "-")
