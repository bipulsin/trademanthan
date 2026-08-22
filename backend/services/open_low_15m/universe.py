"""F&O universe from arbitrage_master with August-2026 FUT resolution."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.config import get_instruments_file_path
from backend.services.open_low_15m.config import DATE_FROM, DATE_TO, EXCLUDED_SYMBOLS
from backend.services.volume_mismatch.backtest_universe import (
    _arbitrage_stock_list,
    _load_fut_by_underlying,
    _resolve_front_month_fut,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

AUGUST_2026_EXPIRY = date(2026, 8, 27)  # typical NSE monthly; match by month/year


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


def _is_august_2026_expiry(ms: Any) -> bool:
    d = _expiry_ms_to_ist_date(ms)
    return d is not None and d.year == 2026 and d.month == 8


def _august_2026_futures_by_underlying() -> Dict[str, Tuple[str, str]]:
    path = get_instruments_file_path()
    out: Dict[str, Tuple[str, str]] = {}
    if not path.is_file():
        return out
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("open_low_15m: instruments read failed: %s", e)
        return out
    if not isinstance(data, list):
        return out
    for inst in data:
        if not isinstance(inst, dict):
            continue
        if str(inst.get("instrument_type") or "").upper() != "FUT":
            continue
        seg = str(inst.get("segment") or "").upper()
        if "NSE_FO" not in seg and "NFO" not in seg:
            continue
        if not _is_august_2026_expiry(inst.get("expiry")):
            continue
        und = str(inst.get("underlying_symbol") or "").strip().upper()
        ts = str(inst.get("trading_symbol") or inst.get("tradingsymbol") or "").strip()
        ik = str(inst.get("instrument_key") or "").strip()
        if und and ts and ik:
            out[und] = (ts, ik)
    return out


_aug_cache: Optional[Dict[str, Tuple[str, str]]] = None


def _aug_map() -> Dict[str, Tuple[str, str]]:
    global _aug_cache
    if _aug_cache is None:
        _aug_cache = _august_2026_futures_by_underlying()
    return _aug_cache


def use_august_2026_futures(session_date: date) -> bool:
    return DATE_FROM <= session_date <= DATE_TO


def load_open_low_universe_for_session(session_date: date) -> List[Dict[str, str]]:
    """Stocks from arbitrage_master; August-2026 FUT when in backtest window."""
    stocks = [s for s in _arbitrage_stock_list() if s not in EXCLUDED_SYMBOLS]
    if not stocks:
        return []

    aug_map = _aug_map() if use_august_2026_futures(session_date) else {}
    fut_by_und = _load_fut_by_underlying()

    out: List[Dict[str, str]] = []
    for sym in stocks:
        fut_sym = ""
        ik = ""
        if sym in aug_map:
            fut_sym, ik = aug_map[sym]
        else:
            hit = _resolve_front_month_fut(sym, session_date, fut_by_und)
            if hit:
                fut_sym, ik = hit
        if ik:
            out.append({"symbol": sym, "future_symbol": fut_sym or sym, "instrument_key": ik})
    return out
