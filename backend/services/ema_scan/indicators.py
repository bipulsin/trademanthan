"""Causal EMA, Wilder ATR, and Wilder ADX / +DI / −DI series.

Values at index i use bars 0..i only. Seeding matches
``backend.services.momentx.indicators`` (EMA seed = first close, ATR/ADX seed =
SMA of the first ``period`` samples, then Wilder RMA).
"""
from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

from backend.services.momentx.indicators import ema_series


def _true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def wilder_atr_series(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 14,
) -> List[Optional[float]]:
    """Wilder ATR aligned to each bar. None until the first seeded value."""
    p = int(period)
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    if p <= 0 or n < p + 1 or len(highs) != n or len(lows) != n:
        return out
    trs: List[float] = []
    for i in range(1, n):
        trs.append(_true_range(float(highs[i]), float(lows[i]), float(closes[i - 1])))
    if len(trs) < p:
        return out
    atr = sum(trs[:p]) / float(p)
    out[p] = float(atr)
    for i in range(p, len(trs)):
        atr = (atr * (p - 1) + trs[i]) / float(p)
        out[i + 1] = float(atr)
    return out


def adx_di_series(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 14,
) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    """Wilder ADX, +DI and −DI aligned to each bar.

    +DI / −DI start at the SMA seed (bar index ``period``). ADX starts one
    smoothing period later (bar index ``2*period - 1``). The last ADX matches
    ``momentx.indicators.wilder_adx`` on the same input.
    """
    p = int(period)
    n = len(closes)
    adx_out: List[Optional[float]] = [None] * n
    pdi_out: List[Optional[float]] = [None] * n
    mdi_out: List[Optional[float]] = [None] * n
    if p <= 0 or n < p * 2 or len(highs) != n or len(lows) != n:
        return adx_out, pdi_out, mdi_out

    trs: List[float] = []
    plus_dm: List[float] = []
    minus_dm: List[float] = []
    for i in range(1, n):
        high = float(highs[i])
        low = float(lows[i])
        prev_close = float(closes[i - 1])
        trs.append(_true_range(high, low, prev_close))
        up = high - float(highs[i - 1])
        down = float(lows[i - 1]) - low
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)

    if len(trs) < p:
        return adx_out, pdi_out, mdi_out

    atr = sum(trs[:p]) / float(p)
    p_dm = sum(plus_dm[:p]) / float(p)
    m_dm = sum(minus_dm[:p]) / float(p)
    dx_list: List[float] = []

    def _store(bar_index: int, atr_v: float, pd: float, md: float) -> float:
        if atr_v <= 0:
            pdi_out[bar_index] = 0.0
            mdi_out[bar_index] = 0.0
            return 0.0
        pdi = 100.0 * pd / atr_v
        mdi = 100.0 * md / atr_v
        pdi_out[bar_index] = float(pdi)
        mdi_out[bar_index] = float(mdi)
        denom = pdi + mdi
        if denom <= 0:
            return 0.0
        return 100.0 * abs(pdi - mdi) / denom

    dx_list.append(_store(p, atr, p_dm, m_dm))
    for i in range(p, len(trs)):
        atr = (atr * (p - 1) + trs[i]) / float(p)
        p_dm = (p_dm * (p - 1) + plus_dm[i]) / float(p)
        m_dm = (m_dm * (p - 1) + minus_dm[i]) / float(p)
        dx_list.append(_store(i + 1, atr, p_dm, m_dm))

    if len(dx_list) < p:
        return adx_out, pdi_out, mdi_out
    adx = sum(dx_list[:p]) / float(p)
    adx_out[p + (p - 1)] = float(adx)
    for i in range(p, len(dx_list)):
        adx = (adx * (p - 1) + dx_list[i]) / float(p)
        adx_out[p + i] = float(adx)
    return adx_out, pdi_out, mdi_out


__all__ = ["adx_di_series", "ema_series", "wilder_atr_series"]
