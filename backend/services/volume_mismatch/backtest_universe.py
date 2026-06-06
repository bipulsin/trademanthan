"""Historical front-month FUT universe for Volume Mismatch backtest."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.config import get_instruments_file_path
from backend.database import SessionLocal
from backend.services.smart_futures_backtest.april_2026_universe import (
    load_april_2026_futures_by_underlying,
    use_fixed_april_2026_futures,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
FRONT_MONTH_MAX_FORWARD_DAYS = 45

_fut_index_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None


def _expiry_ms_to_ist_date(ms: Any) -> Optional[date]:
    try:
        n = int(ms)
    except (TypeError, ValueError):
        return None
    if n > 1_000_000_000_000:
        n //= 1000
    try:
        return datetime.fromtimestamp(n, tz=timezone.utc).astimezone(IST).date()
    except (OSError, OverflowError, ValueError):
        return None


def _load_fut_by_underlying() -> Dict[str, List[Dict[str, Any]]]:
    global _fut_index_cache
    if _fut_index_cache is not None:
        return _fut_index_cache
    path = get_instruments_file_path()
    out: Dict[str, List[Dict[str, Any]]] = {}
    if not path.is_file():
        logger.warning("vm backtest universe: instruments file missing: %s", path)
        _fut_index_cache = out
        return out
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.warning("vm backtest universe: instruments read failed: %s", e)
        _fut_index_cache = out
        return out
    if not isinstance(data, list):
        _fut_index_cache = out
        return out
    for inst in data:
        if not isinstance(inst, dict):
            continue
        if str(inst.get("instrument_type") or "").upper() != "FUT":
            continue
        seg = str(inst.get("segment") or "").upper()
        if "NSE_FO" not in seg and "NFO" not in seg:
            continue
        und = str(inst.get("underlying_symbol") or "").strip().upper()
        if und:
            out.setdefault(und, []).append(inst)
    _fut_index_cache = out
    return out


def _arbitrage_stock_list() -> List[str]:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT UPPER(TRIM(stock)) AS stock
                FROM arbitrage_master
                WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                ORDER BY stock
                """
            )
        ).fetchall()
        return [str(r[0]).strip().upper() for r in rows if r and r[0]]
    finally:
        db.close()


def _resolve_front_month_fut(
    symbol: str,
    session_date: date,
    fut_by_und: Dict[str, List[Dict[str, Any]]],
) -> Optional[Tuple[str, str]]:
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    best_fut: Optional[Dict[str, Any]] = None
    best_exp: Optional[date] = None
    for inst in fut_by_und.get(sym) or []:
        exp = _expiry_ms_to_ist_date(inst.get("expiry"))
        if exp is None or exp < session_date:
            continue
        gap = (exp - session_date).days
        if gap > FRONT_MONTH_MAX_FORWARD_DAYS:
            continue
        if best_exp is None or exp < best_exp:
            best_exp = exp
            best_fut = inst
    if not best_fut:
        return None
    ts = str(best_fut.get("trading_symbol") or best_fut.get("tradingsymbol") or "").strip()
    ik = str(best_fut.get("instrument_key") or "").strip()
    if not ts or not ik:
        return None
    return ts, ik


def load_volume_mismatch_universe_for_session(session_date: date) -> List[Dict[str, str]]:
    """
    Stocks from arbitrage_master; FUT keys resolved per session date.

    Feb–Mar 2026 uses fixed April-2026 expiry (same as Smart Futures backtest).
    From Apr 2026 onward, front-month FUT from ``nse_instruments.json``.
    Falls back to ``arbitrage_master.currmth`` when instruments miss a symbol.
    """
    stocks = _arbitrage_stock_list()
    if not stocks:
        return []

    apr_map = load_april_2026_futures_by_underlying() if use_fixed_april_2026_futures(session_date) else {}
    fut_by_und = _load_fut_by_underlying()

    currmth_fallback: Dict[str, Tuple[str, str]] = {}
    if session_date >= date(2026, 4, 1):
        db = SessionLocal()
        try:
            rows = db.execute(
                text(
                    """
                    SELECT UPPER(TRIM(stock)), currmth_future_symbol, currmth_future_instrument_key
                    FROM arbitrage_master
                    WHERE currmth_future_instrument_key IS NOT NULL
                      AND TRIM(currmth_future_instrument_key) <> ''
                    """
                )
            ).fetchall()
            for r in rows:
                st = str(r[0] or "").strip().upper()
                if st:
                    currmth_fallback[st] = (str(r[1] or "").strip(), str(r[2] or "").strip())
        finally:
            db.close()

    out: List[Dict[str, str]] = []
    for sym in stocks:
        fut_sym = ""
        ik = ""
        if use_fixed_april_2026_futures(session_date):
            hit = apr_map.get(sym)
            if hit:
                fut_sym, ik = hit
        else:
            hit = _resolve_front_month_fut(sym, session_date, fut_by_und)
            if hit:
                fut_sym, ik = hit
            elif sym in currmth_fallback:
                fut_sym, ik = currmth_fallback[sym]
        if ik:
            out.append({"symbol": sym, "future_symbol": fut_sym or sym, "instrument_key": ik})
    return out
