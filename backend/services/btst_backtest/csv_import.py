"""Parse ChartInk-exported BTST candidate CSV uploads."""
from __future__ import annotations

import csv
import io
import re
from datetime import date, datetime
from typing import Any, Dict, List, Tuple

_DATE_ALIASES = frozenset({"trade_date", "trade date", "date", "date time", "datetime", "date_time"})
_SYMBOL_ALIASES = frozenset({"stock_symbol", "stock symbol", "symbol", "stock", "name"})
_SECTOR_ALIASES = frozenset({"sector"})
_CAP_ALIASES = frozenset({"marketcap", "market_cap", "mcap"})


def _norm_header(h: str) -> str:
    return re.sub(r"[\s_]+", " ", (h or "").strip().lower())


def _parse_date(raw: str) -> date:
    s = (raw or "").strip()
    if not s:
        raise ValueError("empty trade_date")
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")[:10]).date()
    except ValueError as exc:
        raise ValueError(f"unparseable date: {raw!r}") from exc


def _map_headers(fieldnames: List[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for h in fieldnames:
        nh = _norm_header(h)
        if nh in _DATE_ALIASES or "date" in nh:
            mapping["trade_date"] = h
        elif nh in _SYMBOL_ALIASES:
            mapping["stock_symbol"] = h
        elif nh in _SECTOR_ALIASES:
            mapping["sector"] = h
        elif nh in _CAP_ALIASES:
            mapping["marketcap"] = h
    return mapping


def parse_btst_csv(content: bytes | str) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Returns (rows, warnings). Each row: trade_date, stock_symbol, sector (optional).
    """
    text = content.decode("utf-8-sig") if isinstance(content, bytes) else content
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("CSV has no header row")
    colmap = _map_headers(list(reader.fieldnames))
    if "trade_date" not in colmap or "stock_symbol" not in colmap:
        raise ValueError("CSV must include trade_date (or Date Time) and stock_symbol columns")
    warnings: List[str] = []
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for i, raw in enumerate(reader, start=2):
        td_raw = (raw.get(colmap["trade_date"]) or "").strip()
        sym_raw = (raw.get(colmap["stock_symbol"]) or "").strip().upper()
        if not td_raw and not sym_raw:
            continue
        if not td_raw or not sym_raw:
            warnings.append(f"line {i}: skipped — missing date or symbol")
            continue
        try:
            td = _parse_date(td_raw)
        except ValueError as exc:
            warnings.append(f"line {i}: {exc}")
            continue
        sector = None
        if "sector" in colmap:
            sector = (raw.get(colmap["sector"]) or "").strip() or None
        key = (td, sym_raw)
        if key in seen:
            warnings.append(f"line {i}: duplicate {sym_raw} on {td} — keeping first")
            continue
        seen.add(key)
        out.append({"trade_date": td, "stock_symbol": sym_raw, "sector": sector})
    if not out:
        raise ValueError("No valid rows found in CSV")
    return out, warnings
