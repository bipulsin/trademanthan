"""TV symbol parse → fixed MCX underlyings + Upstox front-month FUT resolve.

No user mapping table required. Allowed underlyings:
  CRUDEOIL, NATURALGAS, COPPER, GOLDPETAL, SILVERMINI
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_EXCHANGE_PREFIX = re.compile(
    r"^(NSE|BSE|NFO|MCX|BINANCE|BYBIT|COINBASE|NYSE|NASDAQ|AMEX)\s*:\s*",
    re.I,
)
_CONT_FUT = re.compile(r"\d+!$")
# Trailing futures month letter + 4-digit year: Z2026, M2025, …
_LETTER_YEAR = re.compile(r"[A-Z]\d{4}$")
# Also accept letter + 2-digit year: Z26
_LETTER_YY = re.compile(r"[A-Z]\d{2}$")

# Canonical desk names (longest first for prefix match).
ALLOWED_UNDERLYINGS: Tuple[str, ...] = (
    "NATURALGAS",
    "SILVERMINI",
    "GOLDPETAL",
    "CRUDEOIL",
    "COPPER",
)

# Lookup keys tried against Upstox master (SILVERMINI → SILVERM on Upstox).
UPSTOX_RESOLVE_ALIASES: Dict[str, List[str]] = {
    "CRUDEOIL": ["CRUDEOIL"],
    "NATURALGAS": ["NATURALGAS"],
    "COPPER": ["COPPER"],
    "GOLDPETAL": ["GOLDPETAL"],
    "SILVERMINI": ["SILVERM", "SILVERMINI"],
}

# Exact underlying_symbol to keep (excludes NATGASMINI / CRUDEOILM / SILVERMIC).
PREFERRED_UNDERLYING_SYMBOL: Dict[str, str] = {
    "CRUDEOIL": "CRUDEOIL",
    "NATURALGAS": "NATURALGAS",
    "COPPER": "COPPER",
    "GOLDPETAL": "GOLDPETAL",
    "SILVERMINI": "SILVERM",
}


def _strip_to_core(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip().strip('"').strip("'").upper()
    if not s or ("{{" in s and "}}" in s):
        return None
    s = _EXCHANGE_PREFIX.sub("", s).strip()
    if ":" in s:
        s = s.split(":")[-1].strip()
    s = re.sub(r"\s+", "", s)
    if " FUT" in s:
        s = s.split(" FUT", 1)[0].strip()
    return s or None


def normalize_tv_ticker(raw: Optional[str]) -> Optional[str]:
    """Strip exchange / continuous / letter+year → best-effort core ticker."""
    s = _strip_to_core(raw)
    if not s:
        return None
    s = _CONT_FUT.sub("", s).replace("!", "").strip()
    if _LETTER_YEAR.search(s):
        s = _LETTER_YEAR.sub("", s)
    elif _LETTER_YY.search(s) and len(s) > 3:
        candidate = _LETTER_YY.sub("", s)
        if any(candidate == u or candidate.startswith(u) for u in ALLOWED_UNDERLYINGS):
            s = candidate
    return s or None


def parse_underlying(symbol_raw: str) -> Optional[str]:
    """
    Map TradingView symbol to one of ALLOWED_UNDERLYINGS, else None.

    Accepts:
      CRUDEOIL1!, MCX:CRUDEOIL1!, CRUDEOILZ2026, CRUDEOIL, NATURALGAS1!, …
    """
    core = _strip_to_core(symbol_raw)
    if not core:
        return None
    core = _CONT_FUT.sub("", core).replace("!", "").strip()
    if core in ALLOWED_UNDERLYINGS:
        return core
    for u in ALLOWED_UNDERLYINGS:
        if not core.startswith(u):
            continue
        rest = core[len(u) :]
        if rest == "":
            return u
        if _LETTER_YEAR.fullmatch(rest) or _LETTER_YY.fullmatch(rest):
            return u
        if re.fullmatch(r"[A-Z]", rest):
            return u
    return None


def resolve_mcx_instrument(upstox_symbol: str, exchange: str = "MCX") -> Optional[Dict[str, Any]]:
    """Front-month MCX FUT via Upstox complete master, preferring exact underlying_symbol."""
    sym = (upstox_symbol or "").strip().upper()
    if not sym:
        return None
    # Map alias key → preferred underlying filter (canonical desk names also work).
    prefer_und = PREFERRED_UNDERLYING_SYMBOL.get(sym, sym)
    try:
        from backend.services.divtest.instruments import _parse_expiry_ms, ensure_instrument_master

        rows = ensure_instrument_master()
        fut_rows: List[Dict[str, Any]] = []
        for r in rows:
            t = str(r.get("instrument_type") or "").upper()
            seg = str(r.get("segment") or "").upper()
            if "FUT" not in t or "MCX" not in seg or r.get("weekly"):
                continue
            und = str(r.get("underlying_symbol") or "").strip().upper()
            if und == prefer_und:
                fut_rows.append(r)

        futures: List[Dict[str, Any]] = []
        for r in fut_rows:
            exp_ms = _parse_expiry_ms(r.get("expiry"))
            if not exp_ms:
                continue
            futures.append(
                {
                    "instrument_key": r.get("instrument_key"),
                    "trading_symbol": r.get("trading_symbol"),
                    "segment": r.get("segment"),
                    "lot_size": int(r.get("lot_size") or r.get("minimum_lot") or 1),
                    "expiry": datetime.utcfromtimestamp(exp_ms / 1000).strftime("%Y-%m-%d"),
                    "expiry_ms": exp_ms,
                    "underlying_symbol": r.get("underlying_symbol"),
                }
            )
        futures.sort(key=lambda f: f["expiry_ms"])
        if not futures:
            return None
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        live = [f for f in futures if int(f.get("expiry_ms") or 0) >= now_ms - 7 * 86400_000]
        pick = (live or futures)[0]
        return {
            "instrument_key": pick.get("instrument_key"),
            "trading_symbol": pick.get("trading_symbol"),
            "lot_size": int(pick.get("lot_size") or 1),
            "segment": pick.get("segment"),
            "expiry": pick.get("expiry"),
            "resolved_as": prefer_und,
            "underlying_symbol": pick.get("underlying_symbol"),
        }
    except Exception as e:
        logger.warning("commodities_div MCX resolve failed for %s: %s", sym, e)
        return None


def resolve_underlying_instrument(canonical: str) -> Optional[Dict[str, Any]]:
    """Resolve desk canonical name → front-month MCX FUT."""
    # Prefer PREFERRED_UNDERLYING_SYMBOL path first.
    prefer = PREFERRED_UNDERLYING_SYMBOL.get(canonical, canonical)
    inst = resolve_mcx_instrument(prefer)
    if inst and inst.get("instrument_key"):
        inst["canonical"] = canonical
        return inst
    for name in UPSTOX_RESOLVE_ALIASES.get(canonical, [canonical]):
        if name == prefer:
            continue
        inst = resolve_mcx_instrument(name)
        if inst and inst.get("instrument_key"):
            inst["canonical"] = canonical
            return inst
    return None


def attach_instrument_fields(symbol_raw: str) -> Dict[str, Any]:
    """
    Parse TV symbol → fixed underlying + front-month MCX FUT.

    underlying_matched=False when symbol is not one of the five allowed names.
    """
    underlying = parse_underlying(symbol_raw)
    out: Dict[str, Any] = {
        "symbol_mapped": underlying or (normalize_tv_ticker(symbol_raw) or ""),
        "underlying_matched": underlying is not None,
        "mapping_found": underlying is not None,
        "instrument_key": None,
        "contract": None,
        "lot_size": None,
        "exchange": "MCX",
        "resolved_as": None,
    }
    if not underlying:
        return out
    out["symbol_mapped"] = underlying
    inst = resolve_underlying_instrument(underlying)
    if not inst:
        return out
    out["instrument_key"] = inst.get("instrument_key")
    out["contract"] = inst.get("trading_symbol")
    out["lot_size"] = inst.get("lot_size")
    out["resolved_as"] = inst.get("resolved_as")
    return out
