"""2:45 PM universe scan — rank top/bottom 3 gainers and losers independently."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List

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


def rank_candidates_for_side(
    universe: List[Dict[str, str]],
    data: BtstDataAccess,
    trade_date: date,
    trading_days: List[date],
    side: str,
    *,
    top_n: int = 3,
    snapshot_hhmm: str = "14:45",
) -> List[Dict[str, Any]]:
    """side: 'gainer' (positive change, CE) or 'loser' (negative change, PE)."""
    scored: List[Dict[str, Any]] = []
    for row in universe:
        sym = row["symbol"]
        ik = row["instrument_key"]
        if data.instrument_prefetch_failed(ik):
            continue
        prev_close = data.previous_close(ik, trade_date, trading_days)
        spot = data.spot_at(ik, trade_date, snapshot_hhmm)
        if prev_close is None or spot is None or prev_close <= 0:
            continue
        chg = (spot - prev_close) / prev_close * 100.0
        if side == "gainer" and chg <= 0:
            continue
        if side == "loser" and chg >= 0:
            continue
        ohlc = data.prev_trading_day_ohlc(ik, trade_date, trading_days)
        if not ohlc:
            continue
        m5 = data.m5_bars(ik)
        direction = "bullish" if side == "gainer" else "bearish"
        rank_type = "top_gainer" if side == "gainer" else "top_loser"
        scored.append(
            {
                "side": side,
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


def rank_both_sides(
    universe: List[Dict[str, str]],
    data: BtstDataAccess,
    trade_date: date,
    trading_days: List[date],
    *,
    top_n: int = 3,
    snapshot_hhmm: str = "14:45",
) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "gainer": rank_candidates_for_side(
            universe, data, trade_date, trading_days, "gainer", top_n=top_n, snapshot_hhmm=snapshot_hhmm
        ),
        "loser": rank_candidates_for_side(
            universe, data, trade_date, trading_days, "loser", top_n=top_n, snapshot_hhmm=snapshot_hhmm
        ),
    }
