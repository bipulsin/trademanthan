"""Pure indicator calculations for BTST gates (underlying + option premium)."""
from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

from backend.services.smart_futures_picker.indicators import rsi_14, wilder_atr


def compute_cpr(prev_high: float, prev_low: float, prev_close: float) -> Tuple[float, float, float]:
    pivot = (prev_high + prev_low + prev_close) / 3.0
    bc = (prev_high + prev_low) / 2.0
    tc = (pivot - bc) + pivot
    return pivot, tc, bc


def wma(values: Sequence[float], period: int) -> Optional[float]:
    if period < 1 or len(values) < period:
        return None
    window = [float(v) for v in values[-period:]]
    weights = list(range(1, period + 1))
    return sum(w * v for w, v in zip(weights, window)) / float(sum(weights))


def hma_series(closes: Sequence[float], length: int) -> List[float]:
    n = len(closes)
    out: List[float] = []
    if n < length:
        return out
    half = max(1, length // 2)
    sqrt_n = max(1, int(round(length**0.5)))
    for i in range(length - 1, n):
        sub = [float(closes[j]) for j in range(i + 1)]
        w1 = wma(sub, half)
        w2 = wma(sub, length)
        if w1 is None or w2 is None:
            continue
        raw_hist = []
        for k in range(length - 1, i + 1):
            s = [float(closes[j]) for j in range(k + 1)]
            a = wma(s, half)
            b = wma(s, length)
            if a is not None and b is not None:
                raw_hist.append(2.0 * a - b)
        if len(raw_hist) < sqrt_n:
            continue
        h = wma(raw_hist, sqrt_n)
        if h is not None:
            out.append(h)
    return out


def hma_last_two(closes: Sequence[float], length: int = 32) -> Tuple[Optional[float], Optional[float]]:
    series = hma_series(closes, length)
    if len(series) < 2:
        return (series[-1] if series else None), None
    return series[-1], series[-2]


def supertrend_series(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 10,
    multiplier: float = 3.0,
) -> Tuple[List[float], List[int]]:
    n = len(closes)
    st: List[float] = []
    direction: List[int] = []
    fub = 0.0
    flb = 0.0
    for i in range(n):
        atr_i = wilder_atr(highs[: i + 1], lows[: i + 1], closes[: i + 1], period)
        if atr_i is None:
            continue
        hl2 = (float(highs[i]) + float(lows[i])) / 2.0
        bub = hl2 + float(multiplier) * float(atr_i)
        blb = hl2 - float(multiplier) * float(atr_i)
        if not st:
            fub, flb = bub, blb
            st.append(blb)
            direction.append(1)
            continue
        fub = bub if (bub < fub or float(closes[i - 1]) > fub) else fub
        flb = blb if (blb > flb or float(closes[i - 1]) < flb) else flb
        prev_st = st[-1]
        if prev_st == fub:
            cur = fub if float(closes[i]) <= fub else flb
        else:
            cur = flb if float(closes[i]) >= flb else fub
        st.append(cur)
        direction.append(1 if float(closes[i]) >= cur else -1)
    return st, direction


def rsi_at_session_close(candles_5min: List[dict], trade_date, hhmm: str) -> Optional[float]:
    from backend.services.btst_backtest.timing import bar_minutes, bars_on_session, parse_hhmm

    h, m = parse_hhmm(hhmm)
    target = h * 60 + m
    session = bars_on_session(candles_5min, trade_date)
    subset = []
    for c in session:
        tm = bar_minutes(c.get("timestamp"))
        if tm is not None and tm <= target:
            subset.append(c)
    if len(subset) < 16:
        return None
    closes = [float(c.get("close") or 0) for c in subset]
    series = rsi_14(closes)
    return float(series[-1]) if series else None
