"""arbitrage_master universe: current-month FUT or cash equity key."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.smart_futures_picker.position_sizing import get_futures_lot_size_by_instrument_key
from backend.services.trap_ce.universe import LotSizeLookup, resolve_eq

logger = logging.getLogger(__name__)


@dataclass
class HaVwapName:
    symbol: str
    instrument_key: str
    lot_size: int
    instrument: str  # "fut" | "cash"


def fut_lot_size(fut_instrument_key: str, lots: Optional[LotSizeLookup] = None) -> int:
    """Shares in 1 NSE FUT lot from Upstox instrument master (nse_instruments.json)."""
    ik = (fut_instrument_key or "").strip()
    if not ik:
        return 0
    lookup = lots or LotSizeLookup()
    return int(lookup.get(ik) or get_futures_lot_size_by_instrument_key(ik) or 0)


def load_universe(mode: str, *, lots: Optional[LotSizeLookup] = None) -> List[HaVwapName]:
    mode = (mode or "futures").strip().lower()
    lots = lots or LotSizeLookup()
    db = SessionLocal()
    try:
        if mode == "cash":
            rows = db.execute(
                text(
                    """
                    SELECT UPPER(TRIM(stock)) AS stock, stock_instrument_key, currmth_future_instrument_key
                    FROM arbitrage_master
                    WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                      AND stock_instrument_key IS NOT NULL
                      AND TRIM(stock_instrument_key) <> ''
                    ORDER BY 1
                    """
                )
            ).fetchall()
            out: List[HaVwapName] = []
            for r in rows:
                sym = str(r[0] or "").strip().upper()
                ik = str(r[1] or "").strip()
                fut_ik = str(r[2] or "").strip()
                if not sym:
                    continue
                if not ik:
                    eq = resolve_eq(sym)
                    if not eq:
                        continue
                    ik = eq[1]
                lot = fut_lot_size(fut_ik, lots)
                if lot <= 0:
                    logger.warning("ha_vwap cash skip %s: missing FUT lot size (fut_key=%s)", sym, fut_ik or "none")
                    continue
                out.append(HaVwapName(symbol=sym, instrument_key=ik, lot_size=int(lot), instrument="cash"))
            return out
        rows = db.execute(
            text(
                """
                SELECT UPPER(TRIM(stock)) AS stock, currmth_future_instrument_key
                FROM arbitrage_master
                WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                  AND currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                ORDER BY 1
                """
            )
        ).fetchall()
        out = []
        for r in rows:
            sym = str(r[0] or "").strip().upper()
            ik = str(r[1] or "").strip()
            if not sym or not ik:
                continue
            lot = fut_lot_size(ik, lots)
            if lot <= 0:
                logger.warning("ha_vwap futures %s: missing lot size on %s; using qty=1", sym, ik)
                lot = 1
            out.append(HaVwapName(symbol=sym, instrument_key=ik, lot_size=int(lot), instrument="fut"))
        return out
    finally:
        db.close()


def month_range(year: int, month: int, clip_from: date, clip_to: date) -> tuple[date, date]:
    import calendar

    last = calendar.monthrange(year, month)[1]
    d0 = date(year, month, 1)
    d1 = date(year, month, last)
    return max(d0, clip_from), min(d1, clip_to)
