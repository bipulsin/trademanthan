"""2:45 PM universe scan — NIFTY side + top/bottom 3 by magnitude."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.btst_backtest.data_access import BtstDataAccess

logger = logging.getLogger(__name__)


def load_fno_stock_universe() -> List[Dict[str, str]]:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT stock, stock_instrument_key
                FROM arbitrage_master
                WHERE stock_instrument_key IS NOT NULL
                  AND TRIM(stock_instrument_key) <> ''
                ORDER BY stock
                """
            )
        ).fetchall()
        return [
            {"symbol": (r.stock or "").strip().upper(), "instrument_key": (r.stock_instrument_key or "").strip()}
            for r in rows
            if (r.stock or "").strip() and (r.stock_instrument_key or "").strip()
        ]
    finally:
        db.close()


def nifty_scan_side(
    data: BtstDataAccess,
    trade_date: date,
    trading_days: List[date],
    nifty_key: str,
    *,
    flat_epsilon_pct: float = 0.0,
) -> Tuple[str, Optional[float]]:
    """
    Returns ('gainers'|'losers', nifty_change_pct).
    Flat/zero NIFTY change defaults to gainer side.
    """
    prev_close = data.previous_close(nifty_key, trade_date, trading_days)
    spot = data.spot_at(nifty_key, trade_date, "14:45")
    if prev_close is None or spot is None or prev_close <= 0:
        return "gainers", None
    chg = (spot - prev_close) / prev_close * 100.0
    if chg < -flat_epsilon_pct:
        return "losers", chg
    return "gainers", chg


def rank_candidates_for_side(
    universe: List[Dict[str, str]],
    data: BtstDataAccess,
    trade_date: date,
    trading_days: List[date],
    scan_side: str,
    *,
    top_n: int = 3,
    snapshot_hhmm: str = "14:45",
) -> List[Dict[str, Any]]:
    scored: List[Dict[str, Any]] = []
    for row in universe:
        sym = row["symbol"]
        ik = row["instrument_key"]
        prev_close = data.previous_close(ik, trade_date, trading_days)
        spot = data.spot_at(ik, trade_date, snapshot_hhmm)
        if prev_close is None or spot is None or prev_close <= 0:
            continue
        chg = (spot - prev_close) / prev_close * 100.0
        if scan_side == "gainers" and chg <= 0:
            continue
        if scan_side == "losers" and chg >= 0:
            continue
        ohlc = data.prev_trading_day_ohlc(ik, trade_date, trading_days)
        if not ohlc:
            continue
        m5 = data.get_candles_5m(ik, trade_date, days_back=5)
        direction = "bullish" if scan_side == "gainers" else "bearish"
        rank_type = "top_gainer" if scan_side == "gainers" else "top_loser"
        scored.append(
            {
                "stock_symbol": sym,
                "instrument_key": ik,
                "trade_date": trade_date,
                "change_pct_at_1445": chg,
                "rank_type": rank_type,
                "spot_price_1445": spot,
                "prev_day_ohlc": ohlc,
                "candles_5min": m5,
                "direction": direction,
                "magnitude": abs(chg),
            }
        )
    scored.sort(key=lambda x: x["magnitude"], reverse=True)
    return scored[:top_n]
