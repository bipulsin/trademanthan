"""Heikin Ashi, session VWAP (raw), EMA20, MACD hist on HA close."""
from __future__ import annotations

from typing import List, Sequence, Tuple


def heikin_ashi(opens: Sequence[float], highs: Sequence[float], lows: Sequence[float], closes: Sequence[float]) -> Tuple[List[float], List[float], List[float], List[float]]:
    """Standard HA recursion. First HA open = (O+C)/2."""
    n = len(closes)
    ha_o: List[float] = [0.0] * n
    ha_h: List[float] = [0.0] * n
    ha_l: List[float] = [0.0] * n
    ha_c: List[float] = [0.0] * n
    if n == 0:
        return ha_o, ha_h, ha_l, ha_c
    ha_o[0] = (float(opens[0]) + float(closes[0])) / 2.0
    ha_c[0] = (float(opens[0]) + float(highs[0]) + float(lows[0]) + float(closes[0])) / 4.0
    ha_h[0] = max(float(highs[0]), ha_o[0], ha_c[0])
    ha_l[0] = min(float(lows[0]), ha_o[0], ha_c[0])
    for i in range(1, n):
        ha_c[i] = (float(opens[i]) + float(highs[i]) + float(lows[i]) + float(closes[i])) / 4.0
        ha_o[i] = (ha_o[i - 1] + ha_c[i - 1]) / 2.0
        ha_h[i] = max(float(highs[i]), ha_o[i], ha_c[i])
        ha_l[i] = min(float(lows[i]), ha_o[i], ha_c[i])
    return ha_o, ha_h, ha_l, ha_c


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


def session_vwap(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    volumes: Sequence[float],
    session_ids: Sequence[object],
) -> List[float]:
    """Typical price × volume VWAP, reset when session_id changes."""
    out: List[float] = []
    cum_pv = 0.0
    cum_v = 0.0
    prev = object()
    for i in range(len(closes)):
        sid = session_ids[i]
        if sid != prev:
            cum_pv = 0.0
            cum_v = 0.0
            prev = sid
        tp = (float(highs[i]) + float(lows[i]) + float(closes[i])) / 3.0
        v = max(0.0, float(volumes[i]))
        cum_pv += tp * v
        cum_v += v
        out.append(cum_pv / cum_v if cum_v > 0 else float(closes[i]))
    return out


def macd_hist_series(closes: Sequence[float], fast: int, slow: int, signal: int) -> List[float]:
    """MACD histogram = (EMA_fast − EMA_slow) − EMA_signal of that line."""
    if not closes:
        return []
    fast_s = ema_series(closes, fast)
    slow_s = ema_series(closes, slow)
    line = [fast_s[i] - slow_s[i] for i in range(len(closes))]
    sig = ema_series(line, signal)
    return [line[i] - sig[i] for i in range(len(closes))]


def crossed_above(prev_close: float, prev_level: float, close: float, level: float) -> bool:
    """Prior close ≤ prior level and this close > this level."""
    return float(prev_close) <= float(prev_level) and float(close) > float(level)
