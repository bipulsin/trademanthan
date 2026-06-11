"""Intraday trend assessment for Volume Mismatch direction flips."""
from __future__ import annotations

from typing import Any, Dict, Optional


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def assess_session_trend(
    price: float,
    vwap: float,
    ema5: float,
    cur_5m: Dict[str, Any],
    prev_5m: Optional[Dict[str, Any]],
) -> Optional[str]:
    """
    Session trend from fresh price vs VWAP/EMA(5) and 5m structure.

    BEARISH: price below VWAP & EMA5 and latest 5m bar makes a lower low.
    BULLISH: price above VWAP & EMA5 and latest 5m bar makes a higher high.
    """
    if price <= 0 or vwap <= 0 or ema5 <= 0 or not cur_5m:
        return None

    cur_low = _f(cur_5m.get("low"))
    cur_high = _f(cur_5m.get("high"))
    prev_low = _f((prev_5m or {}).get("low"))
    prev_high = _f((prev_5m or {}).get("high"))

    lower_low = prev_5m is not None and prev_low > 0 and cur_low > 0 and cur_low < prev_low
    higher_high = prev_5m is not None and prev_high > 0 and cur_high > 0 and cur_high > prev_high

    if price < vwap and price < ema5 and lower_low:
        return "BEARISH"
    if price > vwap and price > ema5 and higher_high:
        return "BULLISH"
    return None


def flipped_direction(current_direction: str, trend: Optional[str]) -> Optional[str]:
    """Return opposite side when intraday structure invalidates the scan direction."""
    cur = str(current_direction or "").strip().upper()
    if trend == "BEARISH" and cur == "LONG":
        return "SHORT"
    if trend == "BULLISH" and cur == "SHORT":
        return "LONG"
    return None
