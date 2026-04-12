"""Sector rotation score from Nifty sector index 1d / 5d returns (Upstox daily candles)."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

from backend.services.sector_movers import (
    SECTOR_STOCK_UNIVERSE,
    UPSTOX_SECTOR_INDEX_KEYS,
    equity_sector_index_instrument_key,
)
from backend.services.upstox_service import upstox_service

logger = logging.getLogger(__name__)

# stock (NSE symbol) -> sector label (legacy fallback when arbitrage_master has no sector_index)
_STOCK_TO_SECTOR: Dict[str, str] = {}
for _label, _yahoo_list in SECTOR_STOCK_UNIVERSE.items():
    for _yh in _yahoo_list:
        base = str(_yh or "").replace(".NS", "").strip().upper()
        if base:
            _STOCK_TO_SECTOR[base] = _label


def _parse_candle_dates(candles: List[dict]) -> List[Tuple[date, float]]:
    out: List[Tuple[date, float]] = []
    for c in candles or []:
        ts = str(c.get("timestamp") or "")
        cl = float(c.get("close") or 0)
        if len(ts) < 10 or cl <= 0:
            continue
        try:
            d = datetime.strptime(ts[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        out.append((d, cl))
    out.sort(key=lambda x: x[0])
    return out


def _daily_closes_for_instrument_key(instrument_key: str) -> Optional[List[Tuple[date, float]]]:
    ikey = str(instrument_key or "").strip()
    if not ikey or not upstox_service:
        return None
    try:
        candles = upstox_service.get_historical_candles_by_instrument_key(
            ikey, interval="days/1", days_back=30
        )
        parsed = _parse_candle_dates(candles or [])
        return parsed if len(parsed) >= 6 else None
    except Exception as e:
        logger.debug("sector daily closes failed key=%s: %s", ikey, e)
        return None


def _sector_daily_closes(label: str) -> Optional[List[Tuple[date, float]]]:
    ikey = UPSTOX_SECTOR_INDEX_KEYS.get(str(label or "").strip())
    if not ikey:
        return None
    return _daily_closes_for_instrument_key(ikey)


def sector_raw_returns_for_instrument_key(
    instrument_key: str,
) -> Tuple[Optional[float], Optional[float]]:
    """
    1d_return and 5d_return in % (close-to-close), latest bar vs prior / vs 5 bars back.
    """
    parsed = _daily_closes_for_instrument_key(instrument_key)
    if not parsed or len(parsed) < 2:
        return None, None
    _latest_d, latest_c = parsed[-1]
    prev = parsed[-2][1]
    d1 = (latest_c - prev) / prev * 100.0 if prev > 0 else None
    if len(parsed) >= 6:
        old_c = parsed[-6][1]
        d5 = (latest_c - old_c) / old_c * 100.0 if old_c > 0 else None
    else:
        d5 = None
    return d1, d5


def sector_raw_returns(label: str) -> Tuple[Optional[float], Optional[float]]:
    """1d / 5d returns by Nifty sector display label (maps via UPSTOX_SECTOR_INDEX_KEYS)."""
    ikey = UPSTOX_SECTOR_INDEX_KEYS.get(str(label or "").strip())
    if not ikey:
        return None, None
    return sector_raw_returns_for_instrument_key(ikey)


def compute_sector_score_from_instrument_key(instrument_key: Optional[str]) -> float:
    """
    sector_raw = (1d_return * 0.6) + (5d_return * 0.4); sector_score = clamp(sector_raw/5, -1, 1).
    """
    ikey = str(instrument_key or "").strip()
    if not ikey:
        return 0.0
    d1, d5 = sector_raw_returns_for_instrument_key(ikey)
    if d1 is None:
        logger.debug("sector_score: no index returns for instrument_key=%s (score=0)", ikey)
        return 0.0
    if d5 is None:
        d5 = d1
    sector_raw = (d1 * 0.6) + (d5 * 0.4)
    return max(-1.0, min(1.0, sector_raw / 5.0))


def resolve_sector_instrument_key_for_stock(
    stock: str, sector_index_from_master: Optional[str] = None
) -> Optional[str]:
    """
    Prefer arbitrage_master.sector_index (Upstox instrument key), then static equity map.
    """
    sym = str(stock or "").strip().upper()
    manual = str(sector_index_from_master or "").strip()
    if manual:
        return manual
    ek = equity_sector_index_instrument_key(sym)
    if ek:
        return ek
    lbl = _STOCK_TO_SECTOR.get(sym)
    if lbl:
        return UPSTOX_SECTOR_INDEX_KEYS.get(lbl)
    return None


def compute_sector_score_for_stock(
    stock: str, sector_instrument_key: Optional[str] = None
) -> float:
    """
    sector_raw = (1d_return * 0.6) + (5d_return * 0.4); sector_score = clamp(sector_raw/5, -1, 1).

    When ``sector_instrument_key`` is set (e.g. from arbitrage_master.sector_index), it is used
    for Upstox daily candles. Otherwise falls back to equity_sector_index_instrument_key / legacy
    sector label map. Unknown → 0.0.
    """
    sym = str(stock or "").strip().upper()
    ikey = resolve_sector_instrument_key_for_stock(sym, sector_instrument_key)
    if not ikey:
        logger.debug("sector_score: no sector instrument key for symbol=%s (score=0)", sym)
        return 0.0
    return compute_sector_score_from_instrument_key(ikey)
