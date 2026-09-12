"""TV ticker normalize + empty mapping table + Upstox MCX resolve."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.commodities_div.schema import ensure_commodities_div_tables

logger = logging.getLogger(__name__)

_EXCHANGE_PREFIX = re.compile(
    r"^(NSE|BSE|NFO|MCX|BINANCE|BYBIT|COINBASE|NYSE|NASDAQ|AMEX)\s*:\s*",
    re.I,
)
_CONT_FUT = re.compile(r"\d+!$")


def normalize_tv_ticker(raw: Optional[str]) -> Optional[str]:
    """Best-effort display normalize: MCX:CRUDEOIL1! → CRUDEOIL. Not a mapping row."""
    if raw is None:
        return None
    s = str(raw).strip().strip('"').strip("'").upper()
    if not s or ("{{" in s and "}}" in s):
        return None
    s = _EXCHANGE_PREFIX.sub("", s).strip()
    if ":" in s:
        s = s.split(":")[-1].strip()
    s = _CONT_FUT.sub("", s).replace("!", "").strip()
    if " FUT" in s:
        s = s.split(" FUT", 1)[0].strip()
    s = re.sub(r"\s+", "", s)
    return s or None


def lookup_mapping(tv_or_normalized: str) -> Optional[Dict[str, str]]:
    """Lookup by exact tv_ticker (case-insensitive) or upstox_symbol."""
    ensure_commodities_div_tables()
    key = (tv_or_normalized or "").strip().upper()
    if not key:
        return None
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT id, tv_ticker, upstox_symbol, exchange, notes
                FROM commodities_div_symbol_map
                WHERE UPPER(TRIM(tv_ticker)) = :k
                   OR UPPER(TRIM(upstox_symbol)) = :k
                ORDER BY id
                LIMIT 1
                """
            ),
            {"k": key},
        ).mappings().first()
        return dict(row) if row else None
    finally:
        db.close()


def list_mappings() -> List[Dict[str, Any]]:
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, tv_ticker, upstox_symbol, exchange, notes,
                       created_at, updated_at
                FROM commodities_div_symbol_map
                ORDER BY tv_ticker
                """
            )
        ).mappings().all()
        out = []
        for r in rows:
            d = dict(r)
            for k in ("created_at", "updated_at"):
                if d.get(k) is not None:
                    d[k] = str(d[k])
            out.append(d)
        return out
    finally:
        db.close()


def upsert_mapping(
    *,
    tv_ticker: str,
    upstox_symbol: str,
    exchange: str = "MCX",
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    ensure_commodities_div_tables()
    tv = (tv_ticker or "").strip().upper()
    ups = (upstox_symbol or "").strip().upper()
    ex = (exchange or "MCX").strip().upper() or "MCX"
    if not tv or not ups:
        raise ValueError("tv_ticker and upstox_symbol required")
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                INSERT INTO commodities_div_symbol_map (tv_ticker, upstox_symbol, exchange, notes, updated_at)
                VALUES (:tv, :ups, :ex, :notes, NOW())
                ON CONFLICT (tv_ticker) DO UPDATE SET
                    upstox_symbol = EXCLUDED.upstox_symbol,
                    exchange = EXCLUDED.exchange,
                    notes = COALESCE(EXCLUDED.notes, commodities_div_symbol_map.notes),
                    updated_at = NOW()
                RETURNING id, tv_ticker, upstox_symbol, exchange, notes
                """
            ),
            {"tv": tv, "ups": ups, "ex": ex, "notes": notes},
        ).mappings().first()
        db.commit()
        return dict(row) if row else {}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def delete_mapping(map_id: int) -> bool:
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        r = db.execute(
            text("DELETE FROM commodities_div_symbol_map WHERE id = :id RETURNING id"),
            {"id": int(map_id)},
        ).first()
        db.commit()
        return bool(r)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def resolve_mapped_symbol(symbol_raw: str) -> Tuple[str, Optional[Dict[str, str]]]:
    """
    Returns (display_mapped_symbol, mapping_row_or_none).
    Prefer explicit map hit on raw or normalized; else best-effort normalize for display.
    """
    raw = (symbol_raw or "").strip()
    norm = normalize_tv_ticker(raw) or raw.upper()
    # Try raw first, then normalized, then map keys that match tv continuous forms
    for candidate in (raw.upper(), norm):
        hit = lookup_mapping(candidate)
        if hit:
            return str(hit["upstox_symbol"]).strip().upper(), hit
    # Also try matching map.tv_ticker after normalizing both sides
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text("SELECT id, tv_ticker, upstox_symbol, exchange, notes FROM commodities_div_symbol_map")
        ).mappings().all()
        for r in rows:
            tv_n = normalize_tv_ticker(r["tv_ticker"]) or str(r["tv_ticker"]).upper()
            if tv_n == norm:
                return str(r["upstox_symbol"]).strip().upper(), dict(r)
    finally:
        db.close()
    return norm, None


def resolve_mcx_instrument(upstox_symbol: str, exchange: str = "MCX") -> Optional[Dict[str, Any]]:
    """Front-month MCX FUT for upstox_symbol via Upstox complete instrument master (divtest path)."""
    sym = (upstox_symbol or "").strip().upper()
    if not sym:
        return None
    try:
        from backend.services.divtest.instruments import ensure_instrument_master, resolve_instrument

        # Prefer dedicated resolve; also filter by exchange/segment.
        resolved = resolve_instrument(sym)
        if not resolved.get("found"):
            return None
        current = resolved.get("current_future")
        futures = list(resolved.get("futures") or [])
        ex_u = (exchange or "MCX").upper()
        mcx_futs = [
            f
            for f in futures
            if ex_u in str(f.get("segment") or "").upper() or "MCX" in str(f.get("segment") or "").upper()
        ]
        pick = None
        if current and (
            "MCX" in str(current.get("segment") or "").upper() or not mcx_futs
        ):
            pick = current
        if mcx_futs:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            live = [f for f in mcx_futs if int(f.get("expiry_ms") or 0) >= now_ms - 7 * 86400_000]
            pick = (live or mcx_futs)[0]
        if not pick:
            # Fall back to any current_future from resolve_instrument
            pick = current
        if not pick:
            return None
        return {
            "instrument_key": pick.get("instrument_key"),
            "trading_symbol": pick.get("trading_symbol"),
            "lot_size": int(pick.get("lot_size") or 1),
            "segment": pick.get("segment"),
            "expiry": pick.get("expiry"),
        }
    except Exception as e:
        logger.warning("commodities_div MCX resolve failed for %s: %s", sym, e)
        return None


def attach_instrument_fields(symbol_raw: str) -> Dict[str, Any]:
    """Resolve mapping + instrument for a webhook symbol."""
    mapped, mapping = resolve_mapped_symbol(symbol_raw)
    out: Dict[str, Any] = {
        "symbol_mapped": mapped,
        "mapping_found": mapping is not None,
        "instrument_key": None,
        "contract": None,
        "lot_size": None,
        "exchange": (mapping or {}).get("exchange") or "MCX",
    }
    if not mapping:
        return out
    inst = resolve_mcx_instrument(mapped, str(mapping.get("exchange") or "MCX"))
    if not inst:
        return out
    out["instrument_key"] = inst.get("instrument_key")
    out["contract"] = inst.get("trading_symbol")
    out["lot_size"] = inst.get("lot_size")
    return out
