"""
Smart Futures: index alignment (NIFTY + BANKNIFTY) and exit rules (divergence / VWAP / regime).

Used by the picker context and by /daily exit hints for open positions.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, List, Optional, Sequence, Tuple

import pytz

from backend.services.smart_futures_picker.indicators import (
    adx_14_last_two,
    divergence_bundle,
    session_vwap,
    wilder_atr,
    wilder_atr_14,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
NIFTY50_KEY = "NSE_INDEX|Nifty 50"
BANKNIFTY_KEY = "NSE_INDEX|Nifty Bank"


def _sort_candles(candles: Optional[List[dict]]) -> List[dict]:
    if not candles:
        return []
    return sorted(candles, key=lambda c: str(c.get("timestamp") or ""))


def _ist_date_from_ts(ts: str) -> Optional[date]:
    if not ts or len(ts) < 10:
        return None
    try:
        dt = datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S")
        return dt.replace(tzinfo=IST).date()
    except ValueError:
        try:
            return datetime.strptime(ts[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def m5_bars_for_session(candles: Optional[List[dict]], session_date: date) -> List[dict]:
    s = _sort_candles(candles)
    return [b for b in s if _ist_date_from_ts(str(b.get("timestamp") or "")) == session_date]


def session_last_close_and_vwap(m5: List[dict]) -> Tuple[Optional[float], Optional[float]]:
    if len(m5) < 10:
        return None, None
    highs = [float(b["high"]) for b in m5]
    lows = [float(b["low"]) for b in m5]
    closes = [float(b["close"]) for b in m5]
    vols = [float(b.get("volume") or 0) for b in m5]
    lc = closes[-1]
    vw = session_vwap(highs, lows, closes, vols)
    return float(lc), float(vw)


def index_session_long_short_flags(
    upstox: Any,
    session_date: date,
    *,
    range_end_date: Optional[date] = None,
    days_back: int = 5,
) -> Tuple[bool, bool]:
    """Fetch NIFTY50 + BANKNIFTY session 5m and return (supports_long, supports_short). Fail-closed."""
    end_d = range_end_date or session_date
    try:
        nm = upstox.get_historical_candles_by_instrument_key(
            NIFTY50_KEY, interval="minutes/5", days_back=days_back, range_end_date=end_d
        )
        bm = upstox.get_historical_candles_by_instrument_key(
            BANKNIFTY_KEY, interval="minutes/5", days_back=days_back, range_end_date=end_d
        )
        n_today = m5_bars_for_session(nm, session_date)
        b_today = m5_bars_for_session(bm, session_date)
        ok, lg, sh = index_alignment_supports(n_today, b_today)
        if not ok:
            return False, False
        return lg, sh
    except Exception as e:
        logger.warning("index_session_long_short_flags: %s", e)
        return False, False


def index_alignment_supports(
    nifty_m5: List[dict],
    bank_m5: List[dict],
) -> Tuple[bool, bool, bool]:
    """
    Returns ``(data_ok, supports_long, supports_short)`` using last close vs session VWAP on each index.
    Long: both closes above VWAP. Short: both below. Strict inequalities.
    """
    nc, nv = session_last_close_and_vwap(nifty_m5)
    bc, bv = session_last_close_and_vwap(bank_m5)
    if nc is None or nv is None or bc is None or bv is None:
        return False, False, False
    sup_long = nc > nv and bc > bv
    sup_short = nc < nv and bc < bv
    return True, sup_long, sup_short


def should_exit_position(
    side: str,
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    vwap: float,
    adx_curr: Optional[float],
    adx_prev: Optional[float],
    atr5: Optional[float],
    atr14: Optional[float],
    md: float,
    rd: float,
    sd: float,
) -> Tuple[bool, str]:
    """
    Exit if any: ADX falling, ATR(5)<ATR(14), opposite-side divergence cluster, price vs VWAP adverse.
    """
    reasons: List[str] = []
    sd_u = str(side or "").strip().upper()
    lc = float(closes[-1]) if closes else 0.0
    div_sum = float(md) + float(rd) + float(sd)

    if adx_curr is not None and adx_prev is not None and adx_curr < adx_prev:
        reasons.append("adx_falling")
    if atr5 is not None and atr14 is not None and atr14 > 0 and atr5 < atr14:
        reasons.append("atr_contracting")

    if sd_u == "LONG":
        if div_sum <= -0.5:
            reasons.append("bearish_divergence")
        if lc < float(vwap):
            reasons.append("below_vwap")
    elif sd_u == "SHORT":
        if div_sum >= 0.5:
            reasons.append("bullish_divergence")
        if lc > float(vwap):
            reasons.append("above_vwap")

    if not reasons:
        return False, ""
    return True, "|".join(reasons)


def exit_evaluation_from_m5_dicts(
    side: str,
    m5_today: List[dict],
) -> Tuple[bool, str]:
    """Build OHLC series from session 5m bars and run ``should_exit_position``."""
    if len(m5_today) < 15:
        return False, ""
    highs = [float(b["high"]) for b in m5_today]
    lows = [float(b["low"]) for b in m5_today]
    closes = [float(b["close"]) for b in m5_today]
    vols = [float(b.get("volume") or 0) for b in m5_today]
    vwap = session_vwap(highs, lows, closes, vols)
    atr14 = wilder_atr_14(highs, lows, closes)
    atr5 = wilder_atr(highs, lows, closes, 5)
    adx_c, adx_p = adx_14_last_two(highs, lows, closes)
    md, rd, sd = divergence_bundle(highs, lows, closes)
    return should_exit_position(
        side, highs, lows, closes, vwap, adx_c, adx_p, atr5, atr14, md, rd, sd
    )
