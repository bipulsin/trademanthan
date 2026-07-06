"""Load F&O option contracts from nse_instruments.json for ATM resolution."""
from __future__ import annotations

import logging
from datetime import date, datetime
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from backend.services.oi_heatmap import load_nse_instruments_json

logger = logging.getLogger(__name__)


def _expiry_to_date(inst: Dict[str, Any]) -> Optional[date]:
    ex = inst.get("expiry")
    try:
        v = int(ex)
        if v > 1_000_000_000_000:
            v //= 1000
        return datetime.utcfromtimestamp(v).date()
    except (TypeError, ValueError):
        return None


def _is_stock_option(inst: Dict[str, Any]) -> bool:
    seg = str(inst.get("segment") or "").upper()
    it = str(inst.get("instrument_type") or "").upper()
    if "NSE_FO" not in seg and "NFO" not in seg:
        return False
    if it not in ("CE", "PE"):
        return False
    u = (inst.get("underlying_symbol") or inst.get("name") or "").strip().upper()
    if not u:
        return False
    return True


@lru_cache(maxsize=1)
def _options_by_underlying() -> Dict[str, List[Dict[str, Any]]]:
    raw = load_nse_instruments_json()
    out: Dict[str, List[Dict[str, Any]]] = {}
    for inst in raw:
        if not isinstance(inst, dict) or not _is_stock_option(inst):
            continue
        u = (inst.get("underlying_symbol") or inst.get("name") or "").strip().upper()
        out.setdefault(u, []).append(inst)
    return out


def front_monthly_expiry(session_date: date, underlying: str) -> Optional[date]:
    rows = _options_by_underlying().get(underlying.upper(), [])
    expiries = sorted(
        {d for r in rows if (d := _expiry_to_date(r)) is not None and d >= session_date}
    )
    return expiries[0] if expiries else None


def resolve_atm_option(
    stock_symbol: str,
    spot_price: float,
    session_date: date,
    option_type: str,
) -> Tuple[Optional[float], Optional[str], Optional[str], Optional[int]]:
    """
    Returns (atm_strike, option_symbol, numeric_instrument_key, lot_size).
    Uses strike ladder from nse_instruments.json — no chain API.
    """
    sym = stock_symbol.strip().upper()
    ot = option_type.strip().upper()
    if ot not in ("CE", "PE"):
        return None, None, None, None
    expiry = front_monthly_expiry(session_date, sym)
    if expiry is None:
        return None, None, None, None
    rows = [
        r
        for r in _options_by_underlying().get(sym, [])
        if _expiry_to_date(r) == expiry and str(r.get("instrument_type") or "").upper() == ot
    ]
    if not rows:
        return None, None, None, None
    strikes: List[Tuple[float, Dict[str, Any]]] = []
    for r in rows:
        try:
            sk = float(r.get("strike_price") or r.get("strike") or 0)
        except (TypeError, ValueError):
            continue
        if sk > 0:
            strikes.append((sk, r))
    if not strikes:
        return None, None, None, None
    atm_strike, row = min(strikes, key=lambda t: abs(t[0] - float(spot_price)))
    ik = (row.get("instrument_key") or "").strip()
    trading_sym = (row.get("trading_symbol") or row.get("tradingsymbol") or "").strip()
    lot = row.get("lot_size") or row.get("lotsize")
    try:
        lot_i = int(lot) if lot is not None else None
    except (TypeError, ValueError):
        lot_i = None
    return atm_strike, trading_sym or None, ik or None, lot_i
