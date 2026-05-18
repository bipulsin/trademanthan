"""Shared technical indicators for Vajra TPS / ECS engines."""
from __future__ import annotations

from typing import List, Optional, Sequence


def ema_series(values: Sequence[float], period: int) -> List[float]:
    if not values:
        return []
    p = max(1, int(period))
    k = 2.0 / (p + 1.0)
    out: List[float] = []
    ema_v = float(values[0])
    for v in values:
        ema_v = float(v) * k + ema_v * (1.0 - k)
        out.append(ema_v)
    return out


def wma_series(values: Sequence[float], period: int) -> List[Optional[float]]:
    p = max(1, int(period))
    n = len(values)
    out: List[Optional[float]] = [None] * n
    weights = list(range(1, p + 1))
    denom = float(sum(weights))
    for i in range(p - 1, n):
        window = values[i - p + 1 : i + 1]
        out[i] = sum(float(window[j]) * weights[j] for j in range(p)) / denom
    return out


def sma_at(values: Sequence[float], period: int, idx: int) -> Optional[float]:
    p = max(1, int(period))
    if idx + 1 < p:
        return None
    window = values[idx - p + 1 : idx + 1]
    return sum(float(x) for x in window) / float(p)


def cumulative_vwap(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    volumes: Sequence[float],
) -> List[float]:
    """Rolling session-style VWAP from bar 0 → i."""
    out: List[float] = []
    cum_pv = 0.0
    cum_v = 0.0
    for i in range(len(closes)):
        tp = (highs[i] + lows[i] + closes[i]) / 3.0
        v = max(0.0, float(volumes[i]))
        cum_pv += tp * v
        cum_v += v
        out.append(cum_pv / cum_v if cum_v > 0 else closes[i])
    return out
