"""Session 15m candle fetch for Open-Low backtest — disk cache first, no live shared-cache bleed."""
from __future__ import annotations

import logging
import time
from datetime import date, time as dt_time
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.volume_mismatch.candle_cache import VolumeMismatchCandleCache
from backend.services.volume_mismatch.candles import (
    _parse_ts,
    clear_candle_cache,
    is_first_15m_bar,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

MIN_SESSION_BARS = 6  # need post-entry bars for simulation


def native_first_15m_bar(
    candles: List[Dict[str, Any]],
    session_date: date,
) -> Optional[Dict[str, Any]]:
    """
    Strict 09:15–09:30 native ``minutes/15`` tip only.

    Never use the volume-mismatch 10m-from-5m aggregate (09:15–09:25) — that
    produces false OPEN≈LOW on symbols like DMART when the true 15m candle has a lower wick.
    """
    for c in sorted(candles, key=lambda x: str(x.get("timestamp") or "")):
        if is_first_15m_bar(c, session_date):
            return c
    return None


def _session_bar_count(candles: List[Dict[str, Any]], session_date: date) -> int:
    n = 0
    for c in candles:
        ts = _parse_ts(c.get("timestamp"))
        if ts is None:
            continue
        t = ts.astimezone(IST)
        if t.date() != session_date:
            continue
        if dt_time(9, 15) <= t.time() <= dt_time(15, 30):
            n += 1
    return n


def ensure_m15_for_session(
    upstox: Any,
    persistent: VolumeMismatchCandleCache,
    instrument_key: str,
    session_date: date,
    *,
    symbol_pause_sec: float = 0.12,
    days_back: int = 5,
    force_refetch: bool = False,
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Return merged 15m series (multi-day, includes warmup) with target session present.
    Uses disk cache; REST only on miss. Avoids fetch_candles_cached live-cache shortcut.
    """
    ik = (instrument_key or "").strip()
    if not ik:
        return [], False

    if not force_refetch:
        bars = list(persistent.get_m15_candles(ik))
        if native_first_15m_bar(bars, session_date) is not None:
            if _session_bar_count(bars, session_date) >= MIN_SESSION_BARS:
                return bars, False

    if symbol_pause_sec > 0:
        time.sleep(symbol_pause_sec)

    persistent._warm_m15_chunk(upstox, ik, session_date, days_back=days_back)
    bars = list(persistent.get_m15_candles(ik))
    return bars, True


def reset_fetch_caches() -> None:
    """Clear in-memory candle cache so historical range_end is never skipped."""
    clear_candle_cache()
