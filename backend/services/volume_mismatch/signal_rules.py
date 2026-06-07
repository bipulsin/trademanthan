"""Shared Volume Mismatch signal criteria — live scanner and backtest."""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import pandas_ta as ta

from backend.services.upstox_service import _parse_ts_to_aware_ist
from backend.services.volume_mismatch.constants import (
    MIN_GAP_PCT_LONG,
    MIN_GAP_PCT_SHORT,
    RELATIVE_VOLUME_LOOKBACK_SESSIONS,
)
from backend.services.volume_mismatch.scoring import total_score

# Standard daily BB: 20-period SMA ± 2σ on closes completed before signal session.
BB_LENGTH = 20
BB_STD_DEV = 2.0
BB_COMPARE_FIELD = "open"


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def compute_net_volume(first_bar: Dict[str, Any], previous_close: float) -> float:
    """
    Signed volume for mismatch detection.

    Primary rule (spec): close vs previous day close.
    When the first 15m bar has range, use close position in the bar
    (upper half → buying pressure, lower half → selling).
    """
    close = _f(first_bar.get("close"))
    low = _f(first_bar.get("low"))
    high = _f(first_bar.get("high"))
    vol = _f(first_bar.get("volume"))
    if vol <= 0:
        return 0.0
    if high > low:
        pos = (close - low) / (high - low)
        return vol * (2.0 * pos - 1.0)
    if close > previous_close:
        return vol
    if close < previous_close:
        return -vol
    return 0.0


def compute_gap_percent(today_open: float, previous_close: float) -> Optional[float]:
    if previous_close <= 0 or today_open <= 0:
        return None
    return ((today_open - previous_close) / previous_close) * 100.0


def _candle_date(ts: Any) -> Optional[date]:
    dt = _parse_ts_to_aware_ist(ts)
    return dt.date() if dt is not None else None


def daily_closes_before_session(
    daily_candles: Sequence[Dict[str, Any]],
    session_date: date,
) -> List[float]:
    """Ascending daily closes strictly before ``session_date``."""
    rows: List[tuple] = []
    for c in daily_candles:
        d = _candle_date(c.get("timestamp"))
        if d is None:
            continue
        if d >= session_date:
            continue
        cl = _f(c.get("close"))
        if cl <= 0:
            continue
        rows.append((d, cl))
    rows.sort(key=lambda x: x[0])
    return [cl for _, cl in rows]


def _bb_band_from_row(
    row: pd.Series,
    prefix: str,
    length: int,
    std_dev: float,
) -> Optional[float]:
    """Resolve one BB band column across pandas_ta naming variants."""
    candidates = [
        f"{prefix}_{length}_{std_dev}",
        f"{prefix}_{length}_{std_dev}_{std_dev}",
        f"{prefix}_{length}_{int(std_dev)}",
        f"{prefix}_{length}_{int(std_dev)}_{int(std_dev)}",
    ]
    for key in candidates:
        if key in row.index:
            try:
                val = float(row[key])
            except (TypeError, ValueError):
                continue
            if val == val:
                return val
    stem = f"{prefix}_{length}_"
    for col in row.index:
        if str(col).startswith(stem):
            try:
                val = float(row[col])
            except (TypeError, ValueError):
                continue
            if val == val:
                return val
    return None


def bollinger_bands_as_of_session(
    daily_candles: Sequence[Dict[str, Any]],
    session_date: date,
    *,
    length: int = BB_LENGTH,
    std_dev: float = BB_STD_DEV,
) -> Optional[Dict[str, float]]:
    """
    Bollinger Bands from daily closes ending the day before ``session_date``.

    Uses pandas_ta ``bbands`` (20 / 2 on daily closes before session).
    """
    closes = daily_closes_before_session(daily_candles, session_date)
    if len(closes) < length:
        return None
    df = pd.DataFrame({"close": closes})
    bb = ta.bbands(df["close"], length=length, std=std_dev)
    if bb is None or bb.empty:
        return None
    last = bb.iloc[-1]
    upper = _bb_band_from_row(last, "BBU", length, std_dev)
    middle = _bb_band_from_row(last, "BBM", length, std_dev)
    lower = _bb_band_from_row(last, "BBL", length, std_dev)
    if upper is None or middle is None or lower is None:
        return None
    return {
        "bb_upper": round(upper, 4),
        "bb_middle": round(middle, 4),
        "bb_lower": round(lower, 4),
    }


def volume_bought_sold(first_bar: Dict[str, Any]) -> Tuple[int, int, int]:
    """
    Split first 15m volume into bought/sold legs.

    Upstox candles are OHLCV only; use close position in bar range when high > low.
    """
    vol = _f(first_bar.get("volume"))
    if vol <= 0:
        return 0, 0, 0
    o = _f(first_bar.get("open"))
    h = _f(first_bar.get("high"))
    l = _f(first_bar.get("low"))
    c = _f(first_bar.get("close"))
    total = int(round(vol))
    if h > l:
        buy = vol * (c - l) / (h - l)
        sell = vol * (h - c) / (h - l)
        return total, int(round(buy)), int(round(sell))
    if c > o:
        return total, total, 0
    if c < o:
        return total, 0, total
    half = total // 2
    return total, half, total - half


def compute_relative_volume(
    first_bar: Dict[str, Any],
    m15_candles: Sequence[Dict[str, Any]],
    session_date: date,
    *,
    lookback: int = RELATIVE_VOLUME_LOOKBACK_SESSIONS,
) -> Optional[float]:
    """Today's first 15m volume vs average of prior ``lookback`` sessions."""
    from backend.services.volume_mismatch.candles import first_15m_volumes_by_session

    try:
        today_vol = float(first_bar.get("volume") or 0)
    except (TypeError, ValueError):
        today_vol = 0.0
    hist_vols = first_15m_volumes_by_session(
        m15_candles,
        before_date=session_date,
        max_sessions=lookback,
    )
    if not hist_vols:
        return None
    avg = sum(v for _, v in hist_vols) / len(hist_vols)
    if avg <= 0:
        return None
    return today_vol / avg


def evaluate_vm_signal(
    *,
    symbol: str,
    future_symbol: str,
    instrument_key: str,
    first_bar: Dict[str, Any],
    previous_close: float,
    bb: Dict[str, float],
    relative_volume: Optional[float] = None,
    include_volume_split: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Unified gap + BB + candle color + net volume criteria.

    LONG: gap down <= -1%, open below lower BB, green candle, net volume > 0.
    SHORT: gap up >= +1%, open above upper BB, red candle, net volume < 0.

    ``total_score`` is computed for display only — never used to filter.
    """
    o = _f(first_bar.get("open"))
    h = _f(first_bar.get("high"))
    l = _f(first_bar.get("low"))
    c = _f(first_bar.get("close"))
    bb_px = _f(first_bar.get(BB_COMPARE_FIELD))
    if o <= 0 or h <= 0 or l <= 0 or c <= 0 or bb_px <= 0 or previous_close <= 0:
        return None

    gap = compute_gap_percent(o, previous_close)
    if gap is None:
        return None

    net_vol = compute_net_volume(first_bar, previous_close)
    upper = bb["bb_upper"]
    lower = bb["bb_lower"]

    if o < previous_close and gap <= MIN_GAP_PCT_LONG and bb_px < lower:
        if c <= o:
            return None
        if net_vol <= 0:
            return None
        direction = "LONG"
    elif o > previous_close and gap >= MIN_GAP_PCT_SHORT and bb_px > upper:
        if c >= o:
            return None
        if net_vol >= 0:
            return None
        direction = "SHORT"
    else:
        return None

    vol = _f(first_bar.get("volume"))
    score = total_score(
        gap,
        net_vol,
        vol,
        relative_volume,
        o,
        h,
        l,
        gap_threshold=MIN_GAP_PCT_SHORT,
    )

    out: Dict[str, Any] = {
        "symbol": symbol,
        "future_symbol": future_symbol,
        "instrument_key": instrument_key,
        "direction": direction,
        "gap_percent": round(gap, 4),
        "previous_close": round(previous_close, 4),
        "first_15m_open": round(o, 4),
        "first_15m_high": round(h, 4),
        "first_15m_low": round(l, 4),
        "first_15m_close": round(c, 4),
        "first_15m_volume": round(vol, 2),
        "net_volume": round(net_vol, 2),
        "relative_volume": round(relative_volume, 4) if relative_volume is not None else None,
        "score": score,
        "bb_upper": upper,
        "bb_middle": bb["bb_middle"],
        "bb_lower": lower,
    }
    if include_volume_split:
        first_vol, vol_bought, vol_sold = volume_bought_sold(first_bar)
        out["first_15m_volume"] = first_vol
        out["volume_bought"] = vol_bought
        out["volume_sold"] = vol_sold
    return out
