"""Resolve display symbols to Upstox instrument keys for the generic chart module."""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any, Dict, Optional, Set

from sqlalchemy import text

from backend.database import SessionLocal
from backend.config import settings
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

_INSTRUMENT_TYPES = frozenset(
    {"FUT", "FUTURES", "FUTURE", "EQUITY", "EQ", "OPTION", "OPT", "INDEX"}
)

_lot_index_lock = threading.Lock()
_lot_index: Dict[str, Any] = {
    "nse_mtime": None,
    "complete_mtime": None,
    "by_key": {},
    "by_symbol": {},
}


def _key_forms(instrument_key: Optional[str]) -> Set[str]:
    raw = str(instrument_key or "").strip()
    if not raw:
        return set()
    return {raw, raw.replace(":", "|"), raw.replace("|", ":")}


def _positive_lot(raw: Any) -> Optional[int]:
    """Positive whole-number lot from the master. Missing or zero stays unknown."""
    if raw is None or raw == "":
        return None
    try:
        number = float(raw)
    except (TypeError, ValueError):
        return None
    if not number.is_integer():
        return None
    lot = int(number)
    return lot if lot > 0 else None


def _row_lot_size(row: Dict[str, Any]) -> Optional[int]:
    if "FUT" not in str(row.get("instrument_type") or "").upper():
        return None
    for field in ("lot_size", "lotSize", "minimum_lot"):
        lot = _positive_lot(row.get(field))
        if lot is not None:
            return lot
    return None


def _read_json_rows(path: Path) -> list:
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, list) else []


def _nse_instruments_path() -> Path:
    from backend.config import get_instruments_file_path

    return get_instruments_file_path()


def _complete_master_path() -> Path:
    from backend.services.divtest.instruments import MASTER_PATH

    return MASTER_PATH


def _file_mtime(path: Path) -> Optional[float]:
    try:
        if path.is_file():
            return path.stat().st_mtime
    except OSError:
        return None
    return None


def _ingest_future_rows(
    rows: list,
    by_key: Dict[str, int],
    by_symbol: Dict[str, Set[int]],
) -> None:
    for row in rows:
        if not isinstance(row, dict):
            continue
        lot = _row_lot_size(row)
        if lot is None:
            continue
        for form in _key_forms(row.get("instrument_key")):
            by_key[form] = lot
        for field in ("trading_symbol", "tradingsymbol"):
            symbol = str(row.get(field) or "").strip().upper()
            if symbol:
                by_symbol.setdefault(symbol, set()).add(lot)


def _rebuild_future_lot_index() -> None:
    by_key: Dict[str, int] = {}
    by_symbol: Dict[str, Set[int]] = {}
    nse_path = _nse_instruments_path()
    complete_path = _complete_master_path()
    nse_mtime = _file_mtime(nse_path)
    complete_mtime = _file_mtime(complete_path)
    # Daily NSE file overwrites the complete master when both know the same contract.
    sources = []
    if complete_mtime is not None:
        sources.append(complete_path)
    if nse_mtime is not None:
        sources.append(nse_path)
    for path in sources:
        try:
            _ingest_future_rows(_read_json_rows(path), by_key, by_symbol)
        except Exception as exc:
            logger.warning("future lot index skipped %s: %s", path, exc)
    symbols = {
        symbol: next(iter(lots))
        for symbol, lots in by_symbol.items()
        if len(lots) == 1
    }
    _lot_index["nse_mtime"] = nse_mtime
    _lot_index["complete_mtime"] = complete_mtime
    _lot_index["by_key"] = by_key
    _lot_index["by_symbol"] = symbols
    logger.info("future lot index: %s instrument keys", len(by_key))


def _future_lot_index() -> Dict[str, Any]:
    nse_mtime = _file_mtime(_nse_instruments_path())
    complete_mtime = _file_mtime(_complete_master_path())
    with _lot_index_lock:
        if (
            _lot_index.get("built")
            and _lot_index["nse_mtime"] == nse_mtime
            and _lot_index["complete_mtime"] == complete_mtime
        ):
            return _lot_index
        _rebuild_future_lot_index()
        _lot_index["built"] = True
        return _lot_index


def lookup_future_lot_size(
    instrument_key: Optional[str] = None,
    trading_symbol: Optional[str] = None,
) -> Optional[int]:
    """
    Per-lot quantity for a futures contract from the Upstox instrument master.

    Matches instrument_key (``:`` and ``|``) or an exact trading symbol.
    Returns None when the contract is not a future or the master has no lot size.
    """
    try:
        index = _future_lot_index()
    except Exception as exc:
        logger.warning("future lot size lookup failed: %s", exc)
        return None
    by_key = index.get("by_key") or {}
    for form in _key_forms(instrument_key):
        lot = by_key.get(form)
        if lot:
            return int(lot)
    symbol = str(trading_symbol or "").strip().upper()
    if not symbol:
        return None
    lot = (index.get("by_symbol") or {}).get(symbol)
    return int(lot) if lot else None


def _attach_future_lot_size(meta: Dict[str, Any]) -> None:
    if meta.get("instrument_type") != "FUT":
        return
    meta["lot_size"] = lookup_future_lot_size(
        instrument_key=str(meta.get("instrument_key") or ""),
        trading_symbol=str(meta.get("display_symbol") or meta.get("symbol") or ""),
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
    meta = _resolve_chart_instrument(
        symbol,
        instrument_type,
        instrument_key=instrument_key,
        exchange=exchange,
    )
    _attach_future_lot_size(meta)
    return meta


def _resolve_chart_instrument(
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

    exch = (exchange or "NSE").strip().upper() or "NSE"

    if ik:
        return {
            "instrument_key": ik.replace(":", "|"),
            "symbol": sym or ik.split("|")[-1][:20],
            "display_symbol": sym or ik,
            "exchange": exch,
            "instrument_type": it,
        }

    if exch == "MCX":
        from backend.services.commodities_div.mapping import (
            attach_instrument_fields,
            resolve_underlying_instrument,
            parse_underlying,
        )

        underlying = parse_underlying(sym) or sym
        inst = resolve_underlying_instrument(underlying) if underlying else None
        if not inst or not inst.get("instrument_key"):
            attached = attach_instrument_fields(sym)
            if attached.get("instrument_key"):
                inst = attached
        if not inst or not inst.get("instrument_key"):
            raise ValueError(f"No MCX front-month future instrument_key for {sym}")
        return {
            "instrument_key": str(inst["instrument_key"]).replace(":", "|"),
            "symbol": str(inst.get("canonical") or underlying or sym).strip(),
            "display_symbol": str(
                inst.get("trading_symbol") or inst.get("contract") or underlying or sym
            ).strip(),
            "exchange": "MCX",
            "instrument_type": "FUT",
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
