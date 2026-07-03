"""Shared cache-first 5m candle access for conviction board + setup radar."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import text

from backend.services.market_data import candle_cache
from backend.services.relative_strength_scanner import (
    CACHE_MAX_AGE_SEC,
    CANDLE_INTERVAL,
    MIN_BARS,
    _sorted_candles,
)


def candles_cache_only(instrument_key: str) -> Optional[List[Dict]]:
    ik = (instrument_key or "").strip()
    if not ik:
        return None
    cached = candle_cache.get_recent(ik, CANDLE_INTERVAL, CACHE_MAX_AGE_SEC)
    if cached and len(cached) >= MIN_BARS:
        return _sorted_candles(cached)
    return None


def load_instrument_atr_maps(db, symbols: Set[str]) -> Tuple[Dict[str, str], Dict[str, float]]:
    """Return (symbol -> instrument_key, symbol -> atr14_pct)."""
    if not symbols:
        return {}, {}
    rows = db.execute(
        text(
            """
            SELECT am.stock, am.currmth_future_instrument_key, h.atr14_pct
            FROM arbitrage_master am
            LEFT JOIN rs_scanner_history h
              ON h.symbol = am.stock AND h.date = CURRENT_DATE
            WHERE am.stock = ANY(:syms)
            """
        ),
        {"syms": list(symbols)},
    ).fetchall()
    ikeys: Dict[str, str] = {}
    atrs: Dict[str, float] = {}
    for r in rows:
        sym = r.stock
        if r.currmth_future_instrument_key:
            ikeys[sym] = str(r.currmth_future_instrument_key).strip()
        if r.atr14_pct is not None:
            atrs[sym] = float(r.atr14_pct)
    return ikeys, atrs
