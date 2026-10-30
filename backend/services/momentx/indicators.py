"""Pure indicator helpers for MomentX (unit-tested).

RSI / ADX use Wilder's smoothing. Williams %%R uses the classic formula.
EMA uses the standard multiplier 2/(period+1) seeded from the first close.
"""
from __future__ import annotations

from typing import List, Optional, Sequence, Tuple


def sma(values: Sequence[float], period: int) -> Optional[float]:
    """Simple moving average of the last ``period`` values, or None if short."""
    p = int(period)
    if p <= 0 or len(values) < p:
        return None
    window = values[-p:]
    return float(sum(window)) / float(p)


def ema_series(values: Sequence[float], period: int) -> List[float]:
    """EMA series for every bar; seed = first close (same as daily_futures_service)."""
    if not values:
        return []
    p = max(1, int(period))
    k = 2.0 / (p + 1.0)
    out: List[float] = []
    ema_v = float(values[0])
    for v in values:
        ema_v = (float(v) * k) + (ema_v * (1.0 - k))
        out.append(float(ema_v))
    return out


def ema_last(values: Sequence[float], period: int) -> Optional[float]:
    series = ema_series(values, period)
    if not series:
        return None
    return series[-1]


def wilder_rsi(closes: Sequence[float], period: int = 14) -> Optional[float]:
    """Wilder RSI of the last close. Needs period+1 closes."""
    p = int(period)
    if p <= 0 or len(closes) < p + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, p + 1):
        diff = float(closes[i]) - float(closes[i - 1])
        if diff >= 0:
            gains += diff
        else:
            losses -= diff
    avg_gain = gains / float(p)
    avg_loss = losses / float(p)
    for i in range(p + 1, len(closes)):
        diff = float(closes[i]) - float(closes[i - 1])
        gain = diff if diff > 0 else 0.0
        loss = -diff if diff < 0 else 0.0
        avg_gain = (avg_gain * (p - 1) + gain) / float(p)
        avg_loss = (avg_loss * (p - 1) + loss) / float(p)
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def williams_r(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 14,
) -> Optional[float]:
    """Williams %%R of the last bar over the trailing period window."""
    p = int(period)
    n = len(closes)
    if p <= 0 or n < p or len(highs) < p or len(lows) < p:
        return None
    hh = max(float(x) for x in highs[-p:])
    ll = min(float(x) for x in lows[-p:])
    close = float(closes[-1])
    span = hh - ll
    if span <= 0:
        return -50.0
    return ((hh - close) / span) * -100.0


def _true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def wilder_adx(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 14,
) -> Optional[float]:
    """Wilder ADX of the last bar (RMA / alpha = 1/period).

    Seed ATR / +DM / −DM as the SMA of the first ``period`` true-range / DM
    values, then smooth with Wilder's recursive mean
    ``x := (x*(period-1) + sample) / period``. ADX itself is the same RMA of DX.
    Matches TradingView ``ta.adx`` / pandas_ta when fed the same multi-session
    15m OHLC history (callers must pass prior sessions, not today-only bars).
    """
    p = int(period)
    n = len(closes)
    if p <= 0 or n < p * 2 or len(highs) != n or len(lows) != n:
        return None

    trs: List[float] = []
    plus_dm: List[float] = []
    minus_dm: List[float] = []
    for i in range(1, n):
        h = float(highs[i])
        l = float(lows[i])
        pc = float(closes[i - 1])
        trs.append(_true_range(h, l, pc))
        up = h - float(highs[i - 1])
        down = float(lows[i - 1]) - l
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)

    if len(trs) < p:
        return None

    # Seed = SMA of first period (Wilder / TV RMA convention).
    atr = sum(trs[:p]) / float(p)
    p_dm = sum(plus_dm[:p]) / float(p)
    m_dm = sum(minus_dm[:p]) / float(p)

    dx_list: List[float] = []

    def _dx(atr_v: float, pd: float, md: float) -> float:
        if atr_v <= 0:
            return 0.0
        pdi = 100.0 * pd / atr_v
        mdi = 100.0 * md / atr_v
        denom = pdi + mdi
        if denom <= 0:
            return 0.0
        return 100.0 * abs(pdi - mdi) / denom

    dx_list.append(_dx(atr, p_dm, m_dm))

    for i in range(p, len(trs)):
        # alpha = 1/p  ⇔  x := (x*(p-1) + sample) / p
        atr = (atr * (p - 1) + trs[i]) / float(p)
        p_dm = (p_dm * (p - 1) + plus_dm[i]) / float(p)
        m_dm = (m_dm * (p - 1) + minus_dm[i]) / float(p)
        dx_list.append(_dx(atr, p_dm, m_dm))

    if len(dx_list) < p:
        return None
    adx = sum(dx_list[:p]) / float(p)
    for i in range(p, len(dx_list)):
        adx = (adx * (p - 1) + dx_list[i]) / float(p)
    return float(adx)


def order_emas_by_value(
    ema5: Optional[float],
    ema15: Optional[float],
    ema50: Optional[float],
    *,
    fast: int = 5,
    mid: int = 15,
    slow: int = 50,
) -> List[Tuple[str, float]]:
    """Return ``[(label, value), ...]`` sorted highest → lowest, skipping None."""
    items: List[Tuple[str, float]] = []
    if ema5 is not None:
        items.append((f"EMA({fast})", float(ema5)))
    if ema15 is not None:
        items.append((f"EMA({mid})", float(ema15)))
    if ema50 is not None:
        items.append((f"EMA({slow})", float(ema50)))
    items.sort(key=lambda x: x[1], reverse=True)
    return items


def is_ema_trigger(
    ema5: Optional[float],
    ema15: Optional[float],
    ema50: Optional[float],
) -> bool:
    """True when EMA(fast) is strictly above both mid and slow."""
    if ema5 is None or ema15 is None or ema50 is None:
        return False
    return float(ema5) > float(ema15) and float(ema5) > float(ema50)


def ema_cross_above_from_below(
    fast_series: Sequence[float],
    slow_series: Sequence[float],
) -> bool:
    """True when fast crosses above slow on the last closed bar (from below or equal)."""
    if len(fast_series) < 2 or len(slow_series) < 2:
        return False
    prev_f, curr_f = float(fast_series[-2]), float(fast_series[-1])
    prev_s, curr_s = float(slow_series[-2]), float(slow_series[-1])
    return prev_f <= prev_s and curr_f > curr_s
