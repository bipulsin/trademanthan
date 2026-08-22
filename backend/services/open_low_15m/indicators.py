"""Indicator helpers for Open-Low 15m backtest."""
from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

from backend.services.open_low_15m.config import EMA_FAST, EMA_SLOW, ST_MULT, ST_PERIOD
from backend.services.smart_futures_exit import _ema_last, _supertrend_dir_last_two
from backend.services.smart_futures_picker.indicators import session_vwap, wilder_atr


def ema_series(values: Sequence[float], span: int) -> List[float]:
    if not values or span < 1:
        return []
    k = 2.0 / (float(span) + 1.0)
    out = [float(values[0])]
    for v in values[1:]:
        out.append(float(v) * k + out[-1] * (1.0 - k))
    return out


def daily_ema10_as_of(closes_before_session: Sequence[float]) -> Optional[float]:
    if len(closes_before_session) < EMA_SLOW:
        return None
    return _ema_last(closes_before_session, EMA_SLOW)


def bar_indicators(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    volumes: Sequence[float],
) -> dict:
    """Snapshot at last bar in window."""
    vwap = session_vwap(highs, lows, closes, volumes)
    ema5 = _ema_last(closes, EMA_FAST)
    ema10 = _ema_last(closes, EMA_SLOW)
    st_cur, st_prev = _supertrend_dir_last_two(
        highs, lows, closes, period=ST_PERIOD, multiplier=ST_MULT
    )
    atr14 = wilder_atr(highs, lows, closes, 14)
    atr5 = wilder_atr(highs, lows, closes, 5)
    return {
        "vwap": vwap,
        "ema5": ema5,
        "ema10": ema10,
        "supertrend_dir": st_cur,
        "supertrend_prev_dir": st_prev,
        "atr14": atr14,
        "atr5": atr5,
    }


def signal_exit_long(
    *,
    prev_close: float,
    close: float,
    prev_vwap: float,
    vwap: float,
    prev_ema5: Optional[float],
    ema5: Optional[float],
    prev_ema10: Optional[float],
    ema10: Optional[float],
    st_prev: Optional[int],
    st_cur: Optional[int],
    atr5: Optional[float],
    atr14: Optional[float],
) -> Tuple[bool, str]:
    """Exit if VWAP cross down, ST flip bearish, or EMA5 cross below EMA10."""
    reasons: List[str] = []
    if prev_close >= prev_vwap and close < vwap:
        reasons.append("vwap_cross")
    if st_prev == 1 and st_cur == -1:
        reasons.append("supertrend_flip")
    if (
        prev_ema5 is not None
        and prev_ema10 is not None
        and ema5 is not None
        and ema10 is not None
        and prev_ema5 >= prev_ema10
        and ema5 < ema10
    ):
        reasons.append("ema_cross")
    if not reasons:
        return False, ""
    # ATR(5)<ATR(14) alone is NOT a valid exit — only when paired above
    return True, "+".join(reasons)
