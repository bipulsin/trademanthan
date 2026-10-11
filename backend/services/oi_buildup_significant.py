"""Extra Significant Long/Short Buildup labels for the OI heatmap.

These sit beside the existing 4-way OI signal. They do not replace it and they
do not add a heatmap tab. Missing inputs stay missing — never coerced to 0.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

SIGNIFICANT_LONG_BUILDUP = "SIGNIFICANT_LONG_BUILDUP"
SIGNIFICANT_SHORT_BUILDUP = "SIGNIFICANT_SHORT_BUILDUP"

# 1-day OI Signal column: vs previous session.
ONE_DAY_PRICE_ABS_PCT = 1.0
ONE_DAY_OI_UP_PCT = 2.0
# 5-session HTF OI column. The existing HTF grade stays on its own window.
FIVE_DAY_PRICE_ABS_PCT = 2.0
FIVE_DAY_OI_UP_PCT = 5.0
FIVE_DAY_SESSIONS = 5


def _num(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return number


def pct_change(current: Any, prior: Any) -> Optional[float]:
    """Percent change. A missing or non-positive prior is missing, not zero."""
    now = _num(current)
    base = _num(prior)
    if now is None or base is None or base <= 0:
        return None
    return (now - base) / base * 100.0


def session_volume(rest_volume: Any, ws_volume: Any = None) -> Optional[float]:
    """Positive session volume from the quote, else the WS sum.

    The quote mapper stores a missing volume as 0. Zero from both sources is
    not a volume confirmation.
    """
    rest = _num(rest_volume)
    if rest is not None and rest > 0:
        return rest
    ws = _num(ws_volume)
    if ws is not None and ws > 0:
        return ws
    return None


def one_day_price_chg_pct(ltp: Any, chg_pct: Any, prev_close: Any) -> Optional[float]:
    price = _num(ltp)
    prev = _num(prev_close)
    change = _num(chg_pct)
    if price is None or price <= 0 or prev is None or prev <= 0 or change is None:
        return None
    return change


def one_day_oi_chg_pct(oi: Any, oi_chg: Any) -> Optional[float]:
    current = _num(oi)
    change = _num(oi_chg)
    if current is None or change is None or current <= 0:
        return None
    prior = current - change
    if prior <= 0:
        return None
    return (change / prior) * 100.0


def significant_buildup(
    price_chg_pct: Any,
    oi_chg_pct: Any,
    volume_today: Any,
    volume_baseline: Any,
    *,
    price_abs_pct: float,
    oi_up_pct: float,
) -> Optional[str]:
    """Significant Long/Short when price, OI, and volume all confirm.

    Returns None unless every input is present. Volume must be strictly above
    the baseline (previous day, or the previous 5-session average).
    """
    price = _num(price_chg_pct)
    oi_change = _num(oi_chg_pct)
    volume = _num(volume_today)
    baseline = _num(volume_baseline)
    if price is None or oi_change is None or volume is None or baseline is None:
        return None
    if not (volume > baseline):
        return None
    if oi_change < oi_up_pct:
        return None
    if price >= price_abs_pct:
        return SIGNIFICANT_LONG_BUILDUP
    if price <= -price_abs_pct:
        return SIGNIFICANT_SHORT_BUILDUP
    return None


def one_day_significant(
    price_chg_pct: Any,
    oi_chg_pct: Any,
    volume_today: Any,
    volume_prev_day: Any,
) -> Optional[str]:
    return significant_buildup(
        price_chg_pct,
        oi_chg_pct,
        volume_today,
        volume_prev_day,
        price_abs_pct=ONE_DAY_PRICE_ABS_PCT,
        oi_up_pct=ONE_DAY_OI_UP_PCT,
    )


def average_volumes(volumes: List[Any]) -> Optional[float]:
    if len(volumes) != FIVE_DAY_SESSIONS:
        return None
    numbers: List[float] = []
    for value in volumes:
        number = _num(value)
        if number is None:
            return None
        numbers.append(number)
    return sum(numbers) / float(FIVE_DAY_SESSIONS)


def completed_daily_bars(
    candles: Optional[List[Dict[str, Any]]],
    session_date: date,
) -> List[Dict[str, Any]]:
    """Ascending daily bars strictly before ``session_date``.

    Today's partial bar is excluded so live quote price, OI, and volume stay
    the "today" side of the comparison. A missing close, OI, or volume is kept
    as None on that session instead of being dropped or zero-filled.
    """
    by_day: Dict[date, Dict[str, Any]] = {}
    for candle in candles or []:
        if not isinstance(candle, dict):
            continue
        ts = str(candle.get("timestamp") or "").strip()
        if len(ts) < 10:
            continue
        try:
            bar_day = date.fromisoformat(ts[:10])
        except ValueError:
            continue
        if bar_day >= session_date:
            continue
        if "volume" not in candle or candle.get("volume") is None or candle.get("volume") == "":
            volume = None
        else:
            volume = _num(candle.get("volume"))
        by_day[bar_day] = {
            "date": bar_day.isoformat(),
            "timestamp": ts,
            "close": _num(candle.get("close")),
            "oi": None if candle.get("oi") is None or candle.get("oi") == "" else _num(candle.get("oi")),
            "volume": volume,
        }
    return [by_day[day] for day in sorted(by_day)]


def previous_session_volume(bars: Optional[List[Dict[str, Any]]]) -> Optional[float]:
    if not bars:
        return None
    return bars[-1].get("volume")


def five_day_significant(
    bars: Optional[List[Dict[str, Any]]],
    *,
    price_now: Any,
    oi_now: Any,
    volume_today: Any,
) -> Optional[str]:
    """5-session move into the live price/OI/volume, vs the prior 5 daily bars."""
    if not bars or len(bars) < FIVE_DAY_SESSIONS:
        return None
    price = _num(price_now)
    oi = _num(oi_now)
    if price is None or price <= 0 or oi is None or oi <= 0:
        return None
    window = bars[-FIVE_DAY_SESSIONS:]
    anchor = window[0]
    return significant_buildup(
        pct_change(price, anchor.get("close")),
        pct_change(oi, anchor.get("oi")),
        volume_today,
        average_volumes([bar.get("volume") for bar in window]),
        price_abs_pct=FIVE_DAY_PRICE_ABS_PCT,
        oi_up_pct=FIVE_DAY_OI_UP_PCT,
    )
