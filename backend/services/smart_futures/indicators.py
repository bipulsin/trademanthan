"""ATR and simple indicators (pure math)."""
from __future__ import annotations

from typing import List, Optional, Sequence


def true_range(high: float, low: float, prev_close: float) -> float:
    return max(
        high - low,
        abs(high - prev_close),
        abs(low - prev_close),
    )


def atr_wilder(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 14,
) -> Optional[float]:
    """Last ATR(period) using Wilder smoothing. Needs len >= period + 1."""
    n = len(highs)
    if n != len(lows) or n != len(closes) or n < period + 1:
        return None
    trs: List[float] = []
    for i in range(1, n):
        trs.append(true_range(highs[i], lows[i], closes[i - 1]))
    if len(trs) < period:
        return None
    # seed: simple average of first period TRs
    atr_val = sum(trs[:period]) / float(period)
    for j in range(period, len(trs)):
        atr_val = (atr_val * (period - 1) + trs[j]) / float(period)
    return float(atr_val)


def atr_from_closes_only(closes: Sequence[float], period: int = 14) -> Optional[float]:
    """Fallback ATR when only closes available (uses high=low=close)."""
    if len(closes) < period + 1:
        return None
    highs = list(closes)
    lows = list(closes)
    return atr_wilder(highs, lows, closes, period=period)
