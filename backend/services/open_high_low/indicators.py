"""Indicator helpers for OpenHighLow (Wilder ATR)."""
from __future__ import annotations

from typing import List, Optional, Sequence


def _true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def wilder_atr(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 14,
) -> Optional[float]:
    """Wilder ATR of the last bar (SMA seed, then RMA). Needs period+1 bars."""
    p = int(period)
    n = len(closes)
    if p <= 0 or n < p + 1 or len(highs) != n or len(lows) != n:
        return None
    trs: List[float] = []
    for i in range(1, n):
        trs.append(_true_range(float(highs[i]), float(lows[i]), float(closes[i - 1])))
    if len(trs) < p:
        return None
    atr = sum(trs[:p]) / float(p)
    for i in range(p, len(trs)):
        atr = (atr * (p - 1) + trs[i]) / float(p)
    return float(atr)
