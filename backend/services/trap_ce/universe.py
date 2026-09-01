"""Resolve NSE FUT instrument key + lot size for a Trap-CE CSV symbol.

Fallback order:
1. August-2026 FUT map (when session is in that window)
2. Front-month FUT within 45 days (volume-mismatch helper)
3. Any nearest listed NSE FUT on/after session_date in nse_instruments.json
   (no 45-day cap — dump often only has current/near month)
4. NSE_EQ cash key; qty = 1 share for INR risk (not FUT lot / board lot)

Cash lot is always 1 share so Risk ₹ = (entry − stop) points × 1.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from backend.config import get_instruments_file_path
from backend.services.open_low_15m.universe import _aug_map, use_august_2026_futures
from backend.services.trap_ce.config import EQ_LOT_QTY
from backend.services.volume_mismatch.backtest_universe import (
    _expiry_ms_to_ist_date,
    _load_fut_by_underlying,
    _resolve_front_month_fut,
)

_eq_index_cache: Optional[Dict[str, Dict[str, Any]]] = None


@dataclass
class ResolvedLeg:
    kind: str  # "fut" | "eq"
    trading_symbol: str
    instrument_key: str
    lot_size: int


def resolve_fut(symbol: str, session_date: date) -> Optional[Tuple[str, str]]:
    """Return (trading_symbol, instrument_key) for front-month FUT."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    if use_august_2026_futures(session_date):
        hit = _aug_map().get(sym)
        if hit:
            return hit
    return _resolve_front_month_fut(sym, session_date, _load_fut_by_underlying())


def _resolve_nearest_listed_fut(
    symbol: str,
    session_date: date,
    fut_by_und: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Optional[Tuple[str, str]]:
    """Nearest NSE FUT expiry on/after session_date (no 45-day cap)."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    fut_by_und = fut_by_und if fut_by_und is not None else _load_fut_by_underlying()
    best_fut: Optional[Dict[str, Any]] = None
    best_exp: Optional[date] = None
    for inst in fut_by_und.get(sym) or []:
        exp = _expiry_ms_to_ist_date(inst.get("expiry"))
        if exp is None or exp < session_date:
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


def _load_eq_by_symbol() -> Dict[str, Dict[str, Any]]:
    global _eq_index_cache
    if _eq_index_cache is not None:
        return _eq_index_cache
    out: Dict[str, Dict[str, Any]] = {}
    path = get_instruments_file_path()
    if not path.is_file():
        _eq_index_cache = out
        return out
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        _eq_index_cache = out
        return out
    if not isinstance(data, list):
        _eq_index_cache = out
        return out
    for inst in data:
        if not isinstance(inst, dict):
            continue
        itype = str(inst.get("instrument_type") or "").upper()
        seg = str(inst.get("segment") or "").upper()
        if itype not in ("EQ", "EQUITY") and "NSE_EQ" not in seg:
            continue
        if "NSE_EQ" not in seg:
            continue
        ts = str(inst.get("trading_symbol") or inst.get("tradingsymbol") or "").strip().upper()
        ik = str(inst.get("instrument_key") or "").strip()
        if ts and ik:
            out[ts] = inst
    _eq_index_cache = out
    return out


def resolve_eq(
    symbol: str,
    *,
    eq_by_symbol: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[Tuple[str, str]]:
    """Return (trading_symbol, instrument_key) for NSE_EQ cash."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    idx = eq_by_symbol if eq_by_symbol is not None else _load_eq_by_symbol()
    inst = idx.get(sym)
    if not inst:
        return None
    ts = str(inst.get("trading_symbol") or inst.get("tradingsymbol") or sym).strip()
    ik = str(inst.get("instrument_key") or "").strip()
    if not ik:
        return None
    return ts, ik


def resolve_leg(
    symbol: str,
    session_date: date,
    *,
    lots: Optional["LotSizeLookup"] = None,
    fut_by_und: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    eq_by_symbol: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[ResolvedLeg]:
    """FUT if any listed key exists; else EQ with qty=1."""
    fut = resolve_fut(symbol, session_date)
    if not fut:
        fut = _resolve_nearest_listed_fut(symbol, session_date, fut_by_und)
    if fut:
        fut_sym, ik = fut
        lot = (lots.get(ik) if lots else 0) or 0
        if lot <= 0:
            src = fut_by_und if fut_by_und is not None else _load_fut_by_underlying()
            for inst in src.get((symbol or "").strip().upper()) or []:
                if str(inst.get("instrument_key") or "") == ik:
                    try:
                        lot = int(inst.get("lot_size") or inst.get("lotSize") or 0)
                    except (TypeError, ValueError):
                        lot = 0
                    break
        if lot > 0:
            return ResolvedLeg(kind="fut", trading_symbol=fut_sym, instrument_key=ik, lot_size=lot)
    eq = resolve_eq(symbol, eq_by_symbol=eq_by_symbol)
    if not eq:
        return None
    eq_sym, ik = eq
    return ResolvedLeg(kind="eq", trading_symbol=eq_sym, instrument_key=ik, lot_size=EQ_LOT_QTY)


class LotSizeLookup:
    def __init__(self) -> None:
        self._by_key: Dict[str, int] = {}
        path = get_instruments_file_path()
        if not path.is_file():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(data, list):
            return
        for inst in data:
            if not isinstance(inst, dict):
                continue
            ik = str(inst.get("instrument_key") or "").strip()
            lot = inst.get("lot_size") or inst.get("lotSize")
            if ik and lot:
                try:
                    self._by_key[ik] = int(lot)
                except (TypeError, ValueError):
                    continue

    def get(self, instrument_key: str) -> int:
        return int(self._by_key.get(instrument_key or "", 0) or 0)
