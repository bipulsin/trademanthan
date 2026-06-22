"""Resolve display symbols to Upstox instrument keys for the generic chart module."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.config import settings
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

_INSTRUMENT_TYPES = frozenset(
    {"FUT", "FUTURES", "FUTURE", "EQUITY", "EQ", "OPTION", "OPT", "INDEX"}
)


def normalize_instrument_type(value: Optional[str]) -> str:
    v = (value or "FUT").strip().upper()
    if v in ("FUTURES", "FUTURE"):
        return "FUT"
    if v in ("EQ",):
        return "EQUITY"
    if v in ("OPT",):
        return "OPTION"
    return v


def _fut_from_arbitrage_master(stock: str) -> Optional[Dict[str, Any]]:
    db = SessionLocal()
    try:
        sym_u = str(stock or "").strip().upper()
        if not sym_u:
            return None
        base_u = sym_u.split(" FUT")[0].strip() or sym_u
        row = db.execute(
            text(
                """
                SELECT stock, currmth_future_symbol, currmth_future_instrument_key
                FROM arbitrage_master
                WHERE (UPPER(TRIM(stock)) IN (:sym, :base)
                   OR UPPER(TRIM(currmth_future_symbol)) = :sym)
                  AND currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                LIMIT 1
                """
            ),
            {"sym": sym_u, "base": base_u},
        ).fetchone()
        if not row:
            return None
        return {
            "instrument_key": str(row[2] or "").strip(),
            "symbol": str(row[0] or stock).strip(),
            "display_symbol": str(row[1] or row[0] or stock).strip(),
            "exchange": "NSE",
            "instrument_type": "FUT",
        }
    finally:
        db.close()


def resolve_chart_instrument(
    symbol: str,
    instrument_type: Optional[str] = None,
    *,
    instrument_key: Optional[str] = None,
    exchange: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Resolve chart target to Upstox instrument_key + metadata.
    Prefer explicit instrument_key when provided (e.g. from Vajra row).
    """
    sym = (symbol or "").strip().upper()
    if not sym and not (instrument_key or "").strip():
        raise ValueError("symbol or instrument_key required")

    ik = (instrument_key or "").strip()
    it = normalize_instrument_type(instrument_type)
    if it not in _INSTRUMENT_TYPES:
        raise ValueError(f"Unsupported instrument_type: {instrument_type}")

    if ik:
        return {
            "instrument_key": ik.replace(":", "|"),
            "symbol": sym or ik.split("|")[-1][:20],
            "display_symbol": sym or ik,
            "exchange": (exchange or "NSE").upper(),
            "instrument_type": it,
        }

    if it == "FUT":
        hit = _fut_from_arbitrage_master(sym)
        if hit:
            return hit
        raise ValueError(f"No current-month future instrument_key for {sym}")

    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()

    if it == "EQUITY":
        key = ux.get_instrument_key(sym)
        if not key:
            raise ValueError(f"Equity instrument_key not found for {sym}")
        return {
            "instrument_key": key.replace(":", "|"),
            "symbol": sym,
            "display_symbol": sym,
            "exchange": (exchange or "NSE").upper(),
            "instrument_type": "EQUITY",
        }

    if it == "INDEX":
        key = f"NSE_INDEX|{sym}"
        return {
            "instrument_key": key,
            "symbol": sym,
            "display_symbol": sym,
            "exchange": "NSE",
            "instrument_type": "INDEX",
        }

    raise ValueError(f"Instrument resolution for {it} requires instrument_key")
