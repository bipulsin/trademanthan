"""Heikin Ashi, session VWAP (raw), EMA20, MACD hist on HA close."""
from __future__ import annotations

from typing import List, Optional, Sequence, Tuple


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


def wilder_atr_series(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 10,
) -> List[Optional[float]]:
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    p = max(1, int(period))
    if n < p + 1:
        return out
    trs = [0.0] * n
    for i in range(1, n):
        h, l_, pc = float(highs[i]), float(lows[i]), float(closes[i - 1])
        trs[i] = max(h - l_, abs(h - pc), abs(l_ - pc))
    atr = sum(trs[1 : p + 1]) / float(p)
    out[p] = atr
    for i in range(p + 1, n):
        atr = (atr * (p - 1) + trs[i]) / float(p)
        out[i] = atr
    return out


def supertrend_series(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 10,
    multiplier: float = 3.0,
) -> List[Optional[float]]:
    """SuperTrend on RAW OHLC (not HA). None until ATR is ready."""
    n = len(closes)
    atrs = wilder_atr_series(highs, lows, closes, period)
    st: List[Optional[float]] = [None] * n
    fub = 0.0
    flb = 0.0
    started = False
    for i in range(n):
        atr = atrs[i]
        if atr is None:
            continue
        hl2 = (float(highs[i]) + float(lows[i])) / 2.0
        bub = hl2 + float(multiplier) * float(atr)
        blb = hl2 - float(multiplier) * float(atr)
        if not started:
            fub, flb = bub, blb
            st[i] = blb
            started = True
            continue
        fub = bub if (bub < fub or float(closes[i - 1]) > fub) else fub
        flb = blb if (blb > flb or float(closes[i - 1]) < flb) else flb
        prev = st[i - 1]
        if prev is None:
            prev = flb
        if abs(float(prev) - fub) < 1e-12 or float(prev) == fub:
            cur = fub if float(closes[i]) <= fub else flb
        else:
            cur = flb if float(closes[i]) >= flb else fub
        st[i] = cur
    return st
