"""Technical helpers for Garuda screener."""
from __future__ import annotations

import math
from typing import List, Optional, Sequence


def sign(x: float, eps: float = 1e-9) -> int:
    if x > eps:
        return 1
    if x < -eps:
        return -1
    return 0


def avg_range(highs: Sequence[float], lows: Sequence[float], end: int, length: int) -> Optional[float]:
    if end < length - 1:
        return None
    start = end - length + 1
    ranges = [highs[i] - lows[i] for i in range(start, end + 1)]
    if not ranges:
        return None
    return sum(ranges) / len(ranges)


def avg_volume(volumes: Sequence[float], end: int, length: int) -> Optional[float]:
    if end < length - 1:
        return None
    start = end - length + 1
    window = [volumes[i] for i in range(start, end + 1)]
    return sum(window) / len(window) if window else None


def n_bar_high(highs: Sequence[float], end: int, length: int) -> Optional[float]:
    if end < length:
        return None
    return max(highs[end - length : end])


def n_bar_low(lows: Sequence[float], end: int, length: int) -> Optional[float]:
    if end < length:
        return None
    return min(lows[end - length : end])


def close_position(close: float, high: float, low: float) -> Optional[float]:
    rng = high - low
    if rng <= 0:
        return None
    return (close - low) / rng


def consecutive_directional_bars(
    opens: Sequence[float],
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    end: int,
    max_look: int = 6,
) -> tuple[int, int]:
    """Returns (bull_count, bear_count) of consecutive bars closing beyond prior midpoint."""
    bull = bear = 0
    for j in range(end, max(0, end - max_look), -1):
        if j == 0:
            break
        mid_prev = (opens[j - 1] + closes[j - 1]) / 2.0
        c = closes[j]
        if c > mid_prev and c >= opens[j]:
            if bear > 0:
                break
            bull += 1
        elif c < mid_prev and c <= opens[j]:
            if bull > 0:
                break
            bear += 1
        else:
            break
    return bull, bear


def efficiency_ratio(closes: Sequence[float], end: int, length: int) -> Optional[float]:
    if end < length:
        return None
    net = abs(closes[end] - closes[end - length])
    path = sum(abs(closes[i] - closes[i - 1]) for i in range(end - length + 1, end + 1))
    if path <= 0:
        return None
    return net / path


def roc(closes: Sequence[float], end: int, length: int) -> Optional[float]:
    if end < length:
        return None
    base = closes[end - length]
    if base == 0:
        return None
    return (closes[end] - base) / base


def percentile_rank(value: float, universe: List[float], *, higher_is_better: bool = True) -> Optional[float]:
    vals = [v for v in universe if v is not None and not math.isnan(v)]
    if not vals:
        return None
    if higher_is_better:
        below = sum(1 for v in vals if v < value)
    else:
        below = sum(1 for v in vals if v > value)
    return 100.0 * below / len(vals)


def rolling_beta(stock_rets: Sequence[float], nifty_rets: Sequence[float]) -> Optional[float]:
    n = min(len(stock_rets), len(nifty_rets))
    if n < 5:
        return None
    s = stock_rets[-n:]
    m = nifty_rets[-n:]
    mean_s = sum(s) / n
    mean_m = sum(m) / n
    var_m = sum((x - mean_m) ** 2 for x in m) / n
    if var_m <= 0:
        return None
    cov = sum((s[i] - mean_s) * (m[i] - mean_m) for i in range(n)) / n
    return cov / var_m


def vwap_slope_score_from_series(
    vwap_series: Sequence[float],
    end: int,
    close: float,
    atr_daily_pct: float,
    *,
    bars_back: int = 6,
) -> float:
    """Production-style normalized VWAP slope 0–100 for comparison."""
    if end < bars_back or end >= len(vwap_series):
        return 0.0
    atr = close * max(atr_daily_pct, 0.001) / 100.0
    if atr <= 0:
        return 0.0
    delta = abs(vwap_series[end] - vwap_series[end - bars_back])
    raw = delta / atr / 0.5
    return min(100.0, raw * 100.0)
