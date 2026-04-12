"""
Shared Smart Futures session 5m helpers (ATR ratio backfill).

Kept separate from ``job.py`` so routers (e.g. ``smart_futures_stub``) do not import the full picker job graph at module load.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import List, Optional

import pytz

from backend.services.smart_futures_picker.indicators import wilder_atr, wilder_atr_14
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


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


def compute_atr5_14_ratio_for_session(
    upstox: UpstoxService, fut_instrument_key: str, session_date: date
) -> Optional[float]:
    """
    ATR(5)/ATR(14) on the same session 5m window as the picker. Used to backfill rows
    that predate atr5_14_ratio persistence (e.g. GET /daily one-off fill).
    """
    try:
        m5_raw = upstox.get_historical_candles_by_instrument_key(
            fut_instrument_key, interval="minutes/5", days_back=2
        )
        m5 = _sort_candles(m5_raw)
        m5_today = [b for b in m5 if _ist_date_from_ts(str(b.get("timestamp") or "")) == session_date]
        if len(m5_today) < 20:
            m5_today = m5[-max(20, len(m5)) :] if len(m5) >= 20 else []
        if len(m5_today) < 15:
            return None
        highs = [float(b["high"]) for b in m5_today]
        lows = [float(b["low"]) for b in m5_today]
        closes = [float(b["close"]) for b in m5_today]
        atr = wilder_atr_14(highs, lows, closes)
        if atr is None or atr <= 0:
            return None
        atr5 = wilder_atr(highs, lows, closes, 5)
        if atr5 is None or atr5 <= 0:
            return None
        return float(atr5) / float(atr)
    except Exception as e:
        logger.debug("compute_atr5_14_ratio_for_session %s: %s", fut_instrument_key, e)
        return None
