"""
Futures Trading Report API (Daily Futures + Smart Futures sold trades), grouped by date.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.smart_futures_picker.position_sizing import (
    get_futures_lot_size_by_instrument_key,
)

router = APIRouter(prefix="/futures-reports", tags=["futures-reports"])


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _parse_ymd(v: Optional[str]) -> Optional[datetime.date]:
    if not v:
        return None
    return datetime.strptime(v, "%Y-%m-%d").date()


def _fetch_merged_sold_rows(
    db: Session,
    user_id: int,
    start_date: Optional[str],
    end_date: Optional[str],
    source: Optional[str],
) -> List[Dict[str, Any]]:
    sd = _parse_ymd(start_date)
    ed = _parse_ymd(end_date)

    out: List[Dict[str, Any]] = []
    source_norm = (source or "").strip().lower()

    if source_norm in ("", "all", "daily", "daily_futures"):
        daily_sql = """
            SELECT
                s.trade_date::date AS trade_date,
                'Daily Futures'::text AS source,
                COALESCE(t.future_symbol, t.underlying)::text AS symbol,
                t.lot_size::integer AS qty,
                t.entry_time::text AS entry_time,
                t.entry_price::numeric AS entry_price,
                t.exit_time::text AS exit_time,
                t.exit_price::numeric AS exit_price,
                t.pnl_rupees::numeric AS pnl
            FROM daily_futures_user_trade t
            JOIN daily_futures_screening s ON s.id = t.screening_id
            WHERE t.user_id = :u
              AND LOWER(TRIM(t.order_status)) = 'sold'
              AND (:sd IS NULL OR s.trade_date >= :sd)
              AND (:ed IS NULL OR s.trade_date <= :ed)
            ORDER BY s.trade_date DESC, t.updated_at DESC
        """
        rows = db.execute(text(daily_sql), {"u": user_id, "sd": sd, "ed": ed}).mappings().all()
        for r in rows:
            out.append(
                {
                    "date": str(r["trade_date"]),
                    "source": "Daily Futures",
                    "symbol": r["symbol"],
                    "qty": int(r["qty"]) if r["qty"] is not None else None,
                    "entry_time": r["entry_time"],
                    "entry_price": float(r["entry_price"]) if r["entry_price"] is not None else None,
                    "exit_time": r["exit_time"],
                    "exit_price": float(r["exit_price"]) if r["exit_price"] is not None else None,
                    "pnl": float(r["pnl"]) if r["pnl"] is not None else None,
                }
            )

    if source_norm in ("", "all", "smart", "smart_futures"):
        smart_sql = """
            SELECT
                d.session_date::date AS trade_date,
                'Smart Futures'::text AS source,
                COALESCE(d.fut_symbol, d.stock)::text AS symbol,
                COALESCE(NULLIF(d.calculated_lots, 0), 1)::integer AS lots,
                d.fut_instrument_key::text AS fut_instrument_key,
                COALESCE(TO_CHAR((d.entry_at AT TIME ZONE 'Asia/Kolkata'), 'HH24:MI'), '')::text AS entry_time,
                d.buy_price::numeric AS entry_price,
                COALESCE(TO_CHAR((d.sell_time AT TIME ZONE 'Asia/Kolkata'), 'HH24:MI'), '')::text AS exit_time,
                d.sell_price::numeric AS exit_price,
                CASE
                    WHEN d.buy_price IS NULL OR d.sell_price IS NULL THEN NULL
                    WHEN UPPER(COALESCE(d.side, '')) = 'SHORT'
                        THEN (d.buy_price - d.sell_price) * COALESCE(NULLIF(d.calculated_lots, 0), 1)
                    ELSE (d.sell_price - d.buy_price) * COALESCE(NULLIF(d.calculated_lots, 0), 1)
                END::numeric AS pnl
            FROM smart_futures_daily d
            WHERE LOWER(TRIM(COALESCE(d.order_status, ''))) = 'sold'
              AND (:sd IS NULL OR d.session_date >= :sd)
              AND (:ed IS NULL OR d.session_date <= :ed)
            ORDER BY d.session_date DESC, d.updated_at DESC
        """
        rows = db.execute(text(smart_sql), {"sd": sd, "ed": ed}).mappings().all()
        lot_size_cache: Dict[str, int] = {}
        for r in rows:
            lots = int(r["lots"]) if r["lots"] is not None else 1
            ikey = str(r.get("fut_instrument_key") or "").strip()
            if ikey:
                if ikey not in lot_size_cache:
                    try:
                        ls = int(get_futures_lot_size_by_instrument_key(ikey) or 0)
                    except Exception:
                        ls = 0
                    lot_size_cache[ikey] = ls
                ls = lot_size_cache.get(ikey, 0)
            else:
                ls = 0
            qty_units = lots * ls if ls > 0 else lots
            out.append(
                {
                    "date": str(r["trade_date"]),
                    "source": "Smart Futures",
                    "symbol": r["symbol"],
                    "qty": int(qty_units),
                    "entry_time": r["entry_time"],
                    "entry_price": float(r["entry_price"]) if r["entry_price"] is not None else None,
                    "exit_time": r["exit_time"],
                    "exit_price": float(r["exit_price"]) if r["exit_price"] is not None else None,
                    "pnl": float(r["pnl"]) if r["pnl"] is not None else None,
                }
            )

    out.sort(key=lambda x: (x.get("date") or "", x.get("source") or "", x.get("symbol") or ""), reverse=True)
    return out


@router.get("/trading-report")
def futures_trading_report(
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    source: Optional[str] = Query("all", description="all|daily|smart"),
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    rows = _fetch_merged_sold_rows(db, user.id, start_date, end_date, source)
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        grouped[r["date"]].append(r)

    data: List[Dict[str, Any]] = []
    total_pnl = 0.0
    for dt in sorted(grouped.keys(), reverse=True):
        day_rows = grouped[dt]
        day_pnl = 0.0
        daily_count = 0
        smart_count = 0
        for r in day_rows:
            p = r.get("pnl")
            if p is not None:
                day_pnl += float(p)
            if r.get("source") == "Daily Futures":
                daily_count += 1
            else:
                smart_count += 1
        total_pnl += day_pnl
        tot = len(day_rows)
        wins = sum(1 for r in day_rows if (r.get("pnl") is not None and float(r["pnl"]) > 0))
        losses = sum(1 for r in day_rows if (r.get("pnl") is not None and float(r["pnl"]) < 0))
        denom = wins + losses
        data.append(
            {
                "date": dt,
                "total_trades": tot,
                "daily_futures_trades": daily_count,
                "smart_futures_trades": smart_count,
                "wins": wins,
                "losses": losses,
                "win_rate": round((wins / denom) * 100, 2) if denom else 0.0,
                "total_pnl": round(day_pnl, 2),
            }
        )

    return {
        "success": True,
        "data": data,
        "summary": {
            "total_days": len(data),
            "total_trades": len(rows),
            "overall_pnl": round(total_pnl, 2),
        },
    }


@router.get("/daily-trades/{trade_date}")
def futures_daily_trades(
    trade_date: str,
    source: Optional[str] = Query("all", description="all|daily|smart"),
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    # reuse merged fetch with exact-date boundaries
    rows = _fetch_merged_sold_rows(db, user.id, trade_date, trade_date, source)
    rows = [r for r in rows if r.get("date") == trade_date]
    return {
        "success": True,
        "date": trade_date,
        "total_trades": len(rows),
        "trades": rows,
    }

