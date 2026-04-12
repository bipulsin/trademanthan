"""
Sector score using daily closes on or before ``as_of_date`` (backtest-only; does not change sector_score.py).
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import List, Optional, Tuple

from backend.services.smart_futures_picker.sector_score import resolve_sector_instrument_key_for_stock
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)


def _parse_daily_closes_upto(candles: List[dict], as_of: date) -> List[Tuple[date, float]]:
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
        if d <= as_of:
            out.append((d, cl))
    out.sort(key=lambda x: x[0])
    return out


def sector_score_as_of(
    upstox: UpstoxService,
    stock: str,
    sector_index_from_master: Optional[str],
    as_of: date,
) -> float:
    """
    Same formula as compute_sector_score_from_instrument_key but using last daily bar on/before as_of.
    """
    ikey = resolve_sector_instrument_key_for_stock(stock, sector_index_from_master)
    if not ikey:
        return 0.0
    try:
        candles = upstox.get_historical_candles_by_instrument_key(
            ikey, interval="days/1", days_back=160
        )
    except Exception as e:
        logger.debug("sector_asof fetch failed %s: %s", ikey, e)
        return 0.0
    parsed = _parse_daily_closes_upto(candles or [], as_of)
    if len(parsed) < 2:
        return 0.0
    latest_c = parsed[-1][1]
    prev = parsed[-2][1]
    d1 = (latest_c - prev) / prev * 100.0 if prev > 0 else None
    if d1 is None:
        return 0.0
    if len(parsed) >= 6:
        old_c = parsed[-6][1]
        d5 = (latest_c - old_c) / old_c * 100.0 if old_c > 0 else None
    else:
        d5 = d1
    if d5 is None:
        d5 = d1
    sector_raw = (d1 * 0.6) + (d5 * 0.4)
    return max(-1.0, min(1.0, sector_raw / 5.0))
