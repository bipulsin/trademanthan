"""Universe: arbitrage_master sector_index grouping + per-date FUT/EQ resolution."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.breakfast_strategy.candles import bar_move_pct, bar_volume
from backend.services.nks_intraday_backtest import (
    InstrumentRef,
    _current_fut_lot_size,
    _expiry_ms_to_ist_date,
    _index_instruments,
    _load_instruments,
    resolve_instrument,
)
from backend.services.sector_movers import UPSTOX_SECTOR_INDEX_KEYS, normalize_sector_instrument_key

SECTOR_UNIVERSE: List[Tuple[str, str]] = [
    ("Nifty Private Bank", "NIFTY_PVT_BANK.NS"),
    ("Nifty IT", "^CNXIT"),
    ("Nifty Auto", "^CNXAUTO"),
    ("Nifty FMCG", "^CNXFMCG"),
    ("Nifty Metal", "^CNXMETAL"),
    ("Nifty Realty", "^CNXREALTY"),
    ("Nifty Energy", "^CNXENERGY"),
    ("Nifty Infra", "^CNXINFRA"),
    ("Nifty PSU Bank", "^CNXPSUBANK"),
    ("Nifty Healthcare", "NIFTY_HEALTHCARE.NS"),
    ("Nifty Consumer Durables", "NIFTY_CONSR_DURBL.NS"),
    ("Nifty Oil & Gas", "NIFTY_OIL_AND_GAS.NS"),
    ("Nifty Financial Services", "^CNXFIN"),
    ("Nifty Chemicals", "NIFTY_CHEMICALS.NS"),
    ("Nifty Services", "^CNXSERVICE"),
    ("Nifty Telecom", "NIFTY_MS_IT_TELCM.NS"),
]


@dataclass
class StockRow:
    stock: str
    display_symbol: str
    instrument_label: str
    sector: str
    sector_index: str
    instrument_key: str
    lot_size: int
    price_source: str  # futures | spot_proxy | FUT | EQ (legacy)


def sector_index_key_for_label(label: str) -> Optional[str]:
    return UPSTOX_SECTOR_INDEX_KEYS.get(str(label or "").strip())


def load_arbitrage_by_sector() -> Dict[str, List[Dict[str, str]]]:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT TRIM(stock) AS stock,
                       TRIM(sector) AS sector,
                       TRIM(sector_index) AS sector_index
                FROM arbitrage_master
                WHERE stock IS NOT NULL
                  AND sector_index IS NOT NULL
                  AND TRIM(sector_index) <> ''
                ORDER BY stock
                """
            )
        ).mappings().all()
    finally:
        db.close()
    out: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        raw = str(r.get("sector_index") or "").strip()
        key = normalize_sector_instrument_key(raw) or raw
        if not key:
            continue
        out.setdefault(key, []).append(
            {
                "stock": str(r.get("stock") or "").strip().upper(),
                "sector": str(r.get("sector") or "").strip(),
                "sector_index": key,
            }
        )
    return out


def build_instrument_indexes() -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Dict[str, Any]]]:
    return _index_instruments(_load_instruments())


def _nearest_listed_fut(
    symbol: str,
    session_date: date,
    *,
    fut_by_und: Dict[str, List[Dict[str, Any]]],
) -> Optional[InstrumentRef]:
    """Nearest expiry FUT on/after session_date (no 45-day cap — for backtest history)."""
    sym_u = (symbol or "").strip().upper()
    if not sym_u:
        return None
    best_fut: Optional[Dict[str, Any]] = None
    best_exp: Optional[date] = None
    for inst in fut_by_und.get(sym_u, []):
        exp = _expiry_ms_to_ist_date(inst.get("expiry"))
        if exp is None or exp < session_date:
            continue
        if best_exp is None or exp < best_exp:
            best_exp = exp
            best_fut = inst
    if not best_fut or not best_exp:
        return None
    fut_lot = int(best_fut.get("lot_size") or 0) or None
    cur_fut_lot = _current_fut_lot_size(sym_u, fut_by_und=fut_by_und)
    return InstrumentRef(
        source="FUT",
        trading_symbol=str(best_fut.get("trading_symbol") or best_fut.get("tradingsymbol") or ""),
        instrument_key=str(best_fut.get("instrument_key") or ""),
        expiry_date=best_exp,
        lot_size=fut_lot,
        fut_lot_size=fut_lot or cur_fut_lot,
    )


def format_instrument_label(symbol: str, ref: InstrumentRef) -> str:
    sym = (symbol or "").strip().upper()
    if ref.source == "EQ":
        return "SPOT"
    if ref.expiry_date:
        return f"{ref.expiry_date.strftime('%b').upper()}{ref.expiry_date.strftime('%y')} FUT"
    ts = (ref.trading_symbol or "").upper()
    m = re.search(r"FUT\s+(\d{1,2})\s+([A-Z]{3})\s+(\d{2})", ts)
    if m:
        return f"{m.group(2)}{m.group(3)} FUT"
    return "FUT"


def display_symbol_spot_proxy(symbol: str) -> str:
    return f"{(symbol or '').strip().upper()} SPOT*"


def resolve_eq_spot_with_fut_lot(
    symbol: str,
    *,
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
) -> Optional[InstrumentRef]:
    """EQ spot instrument with futures lot size for P&L (1-lot FUT equivalent)."""
    sym_u = (symbol or "").strip().upper()
    if not sym_u:
        return None
    eq = eq_by_symbol.get(sym_u)
    if not eq or not eq.get("instrument_key"):
        return None
    fut_lot = _current_fut_lot_size(sym_u, fut_by_und=fut_by_und)
    if not fut_lot or fut_lot <= 0:
        return None
    return InstrumentRef(
        source="EQ",
        trading_symbol=str(eq.get("trading_symbol") or sym_u),
        instrument_key=str(eq.get("instrument_key") or ""),
        expiry_date=None,
        lot_size=int(eq.get("lot_size") or 1) or 1,
        fut_lot_size=fut_lot,
    )


def display_symbol_for(symbol: str, ref: InstrumentRef) -> str:
    sym = (symbol or "").strip().upper()
    label = format_instrument_label(sym, ref)
    if label == "SPOT":
        return f"{sym} SPOT"
    return f"{sym} {label}"


def resolve_stock_instrument(
    symbol: str,
    session_date: date,
    *,
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
) -> Optional[InstrumentRef]:
    """Front-month FUT when within 45d; else nearest listed FUT; else EQ spot."""
    ref = resolve_instrument(symbol, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
    if ref and ref.source == "FUT" and ref.instrument_key:
        lot = int(ref.fut_lot_size or ref.lot_size or 0)
        if lot > 0:
            ref.fut_lot_size = lot
            return ref
    fut = _nearest_listed_fut(symbol, session_date, fut_by_und=fut_by_und)
    if fut and fut.instrument_key:
        lot = int(fut.fut_lot_size or fut.lot_size or 0)
        if lot > 0:
            fut.fut_lot_size = lot
            return fut
    if ref and ref.source == "EQ" and ref.instrument_key:
        lot = int(ref.fut_lot_size or ref.lot_size or 0)
        if lot and lot > 0:
            return ref
    return None


def resolve_stock_fut(
    symbol: str,
    session_date: date,
    *,
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
) -> Optional[InstrumentRef]:
    """Backward-compatible alias — prefers FUT but allows EQ spot."""
    return resolve_stock_instrument(
        symbol, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol
    )


def fo_eligible_sector_keys(
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    session_date: date,
    *,
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
) -> Set[str]:
    eligible: Set[str] = set()
    for skey, members in stocks_by_sector.items():
        for m in members:
            sym = m.get("stock") or ""
            if resolve_stock_instrument(sym, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol):
                eligible.add(skey)
                break
    return eligible


def rank_sectors(
    sector_bars: Dict[str, Dict[str, Any]],
    *,
    eligible_keys: Set[str],
    descending: bool,
) -> List[Tuple[str, float, float]]:
    rows: List[Tuple[str, float, float]] = []
    for label, _yahoo in SECTOR_UNIVERSE:
        ikey = sector_index_key_for_label(label)
        if not ikey or ikey not in eligible_keys:
            continue
        bar = sector_bars.get(ikey)
        if not bar:
            continue
        pct = bar_move_pct(bar)
        if pct is None:
            continue
        rows.append((ikey, float(pct), float(bar_volume(bar))))
    rows.sort(key=lambda x: (-x[1], -x[2]) if descending else (x[1], -x[2]))
    return rows


def pick_stocks_in_sector(
    members: List[Dict[str, str]],
    stock_bars: Dict[str, Dict[str, Any]],
    stock_move_pcts: Dict[str, float],
    *,
    session_date: date,
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
    long_side: bool,
    move_cap: float = 4.0,
    top_n: int = 2,
    session_rows: Optional[Dict[str, StockRow]] = None,
) -> List[StockRow]:
    candidates: List[Tuple[float, str, Dict[str, Any], InstrumentRef, Dict[str, str]]] = []
    for m in members:
        sym = str(m.get("stock") or "").upper()
        bar = stock_bars.get(sym)
        if not bar:
            continue
        pct = stock_move_pcts.get(sym)
        if pct is None:
            continue
        if long_side:
            if not (0.0 < pct < move_cap):
                continue
        elif not (-move_cap < pct < 0.0):
            continue
        tpl = (session_rows or {}).get(sym)
        if tpl:
            lot = int(tpl.lot_size or 0)
            if lot <= 0:
                continue
            candidates.append((pct, sym, bar, tpl, m))
            continue
        ref = resolve_stock_instrument(sym, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
        if not ref or not ref.instrument_key:
            continue
        lot = int(ref.fut_lot_size or 0)
        if lot <= 0:
            continue
        candidates.append((pct, sym, bar, ref, m))

    if long_side:
        candidates.sort(key=lambda x: (-x[0], -bar_volume(x[2])))
    else:
        candidates.sort(key=lambda x: (x[0], -bar_volume(x[2])))

    out: List[StockRow] = []
    seen: Set[str] = set()
    for pct, sym, _bar, ref_or_tpl, m in candidates:
        if sym in seen:
            continue
        seen.add(sym)
        if isinstance(ref_or_tpl, StockRow):
            tpl = ref_or_tpl
            out.append(
                StockRow(
                    stock=sym,
                    display_symbol=tpl.display_symbol,
                    instrument_label=tpl.instrument_label,
                    sector=str(m.get("sector") or ""),
                    sector_index=str(m.get("sector_index") or ""),
                    instrument_key=str(tpl.instrument_key),
                    lot_size=int(tpl.lot_size),
                    price_source=str(tpl.price_source),
                )
            )
        else:
            ref = ref_or_tpl
            label = format_instrument_label(sym, ref)
            out.append(
                StockRow(
                    stock=sym,
                    display_symbol=display_symbol_for(sym, ref),
                    instrument_label=label,
                    sector=str(m.get("sector") or ""),
                    sector_index=str(m.get("sector_index") or ""),
                    instrument_key=str(ref.instrument_key),
                    lot_size=int(ref.fut_lot_size or 0),
                    price_source="futures",
                )
            )
        if len(out) >= top_n:
            break
    return out
