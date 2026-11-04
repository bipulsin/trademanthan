"""Delta Exchange India helpers for Multi-Leg Bitcoin / Ethereum options.

Instrument journal names are BITCOIN / ETHEREUM. Delta symbols use BTC / ETH
(``C-BTC-99000-271126``). Public tickers need no API key.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

CRYPTO_INSTRUMENTS = ("BITCOIN", "ETHEREUM")
CRYPTO_TO_DELTA = {"BITCOIN": "BTC", "ETHEREUM": "ETH"}
DELTA_TO_CRYPTO = {v: k for k, v in CRYPTO_TO_DELTA.items()}
# Journal lot_size is Delta "lots"; PnL uses underlying qty = lots × contract size.
CRYPTO_LOT_CONTRACT_QTY = {"BITCOIN": 0.001, "ETHEREUM": 0.01}
DEFAULT_CRYPTO_LOT = 1


def contract_qty_per_lot(instrument: Any) -> Optional[float]:
    """Underlying qty for one filled lot (BTC 0.001, ETH 0.01)."""
    return CRYPTO_LOT_CONTRACT_QTY.get(str(instrument or "").strip().upper())


def effective_position_qty(instrument: Any, lot_size: Any) -> Optional[float]:
    """Lots × contract size for crypto; otherwise integer lot count."""
    try:
        lots = float(lot_size)
    except (TypeError, ValueError):
        return None
    if lots != lots or lots <= 0:
        return None
    per = contract_qty_per_lot(instrument)
    if per is not None:
        return lots * per
    # Equity / index / MCX keep whole-lot sizing.
    if abs(lots - round(lots)) > 1e-9:
        return None
    return float(int(round(lots)))

_TICKER_LOCK = threading.Lock()
_TICKER_CACHE: Dict[str, Any] = {"by_und": {}, "loaded_at": 0.0}
_TICKER_TTL = 120.0


def is_crypto_instrument(instrument: Any) -> bool:
    return str(instrument or "").strip().upper() in CRYPTO_TO_DELTA


def delta_underlying(instrument: Any) -> Optional[str]:
    return CRYPTO_TO_DELTA.get(str(instrument or "").strip().upper())


def is_delta_option_key(instrument_key: Any) -> bool:
    """True for Delta symbols like C-BTC-99000-271126 (not Upstox NSE_FO|…)."""
    key = str(instrument_key or "").strip().upper()
    if not key or "|" in key or ":" in key:
        return False
    parts = key.split("-")
    if len(parts) < 4:
        return False
    return parts[0] in ("C", "P") and parts[1] in DELTA_TO_CRYPTO


def constructed_crypto_option_key(
    instrument: str,
    expiry: date,
    strike: float,
    option_type: str,
) -> str:
    und = delta_underlying(instrument)
    if not und:
        raise ValueError(f"not a crypto instrument: {instrument}")
    right = str(option_type or "").strip().upper()
    prefix = "C" if right == "CE" else "P" if right == "PE" else None
    if prefix is None:
        raise ValueError("option_type must be CE or PE")
    strike_i = int(round(float(strike)))
    exp_code = expiry.strftime("%d%m%y")
    return f"{prefix}-{und}-{strike_i}-{exp_code}"


def parse_crypto_option_key(symbol: str) -> Optional[Dict[str, Any]]:
    parts = str(symbol or "").strip().upper().split("-")
    if len(parts) < 4 or parts[0] not in ("C", "P") or parts[1] not in DELTA_TO_CRYPTO:
        return None
    try:
        strike = float(parts[2])
        exp = datetime.strptime(parts[3], "%d%m%y").date()
    except ValueError:
        return None
    return {
        "instrument": DELTA_TO_CRYPTO[parts[1]],
        "delta_underlying": parts[1],
        "option_type": "CE" if parts[0] == "C" else "PE",
        "strike": strike,
        "expiry": exp,
        "symbol": str(symbol).strip(),
    }


def _ticker_rows(underlying: str) -> List[Dict[str, Any]]:
    und = str(underlying or "").strip().upper()
    if und not in ("BTC", "ETH"):
        return []
    now = time.monotonic()
    with _TICKER_LOCK:
        cached = _TICKER_CACHE["by_und"].get(und)
        if cached is not None and (now - float(_TICKER_CACHE["loaded_at"])) < _TICKER_TTL:
            return list(cached)
    try:
        from backend.services.crypto_options_report.delta_client import fetch_public_tickers

        payload = fetch_public_tickers(
            contract_types="call_options,put_options",
            underlying_asset_symbols=und,
        )
    except Exception as exc:
        logger.info("multi_leg crypto tickers failed for %s: %s", und, exc.__class__.__name__)
        return []
    rows = payload.get("result") if isinstance(payload, dict) and payload.get("ok") else []
    if not isinstance(rows, list):
        rows = []
    with _TICKER_LOCK:
        _TICKER_CACHE["by_und"][und] = rows
        _TICKER_CACHE["loaded_at"] = time.monotonic()
    return list(rows)


def listed_crypto_expiries(instrument: str, on_or_after: Optional[date] = None) -> List[date]:
    und = delta_underlying(instrument)
    if not und:
        return []
    dates = set()
    for row in _ticker_rows(und):
        parsed = parse_crypto_option_key(str(row.get("symbol") or ""))
        if not parsed:
            continue
        exp = parsed["expiry"]
        if on_or_after is not None and exp < on_or_after:
            continue
        dates.add(exp)
    return sorted(dates)


def resolve_crypto_option_contract(
    instrument: str,
    strike: Any,
    option_type: str,
    expiry: date,
) -> Dict[str, Any]:
    sym = str(instrument or "").strip().upper()
    right = str(option_type or "").strip().upper()
    try:
        strike_f = float(strike)
    except (TypeError, ValueError):
        strike_f = None
    if not is_crypto_instrument(sym) or right not in ("CE", "PE") or strike_f is None:
        raise ValueError("crypto leg needs BITCOIN/ETHEREUM, strike, CE/PE, and expiry")
    if not isinstance(expiry, date):
        raise ValueError("crypto leg expiry is required")
    key = constructed_crypto_option_key(sym, expiry, strike_f, right)
    # Prefer a live listed symbol when strike/expiry exist on Delta.
    und = delta_underlying(sym)
    for row in _ticker_rows(und or ""):
        parsed = parse_crypto_option_key(str(row.get("symbol") or ""))
        if not parsed:
            continue
        if (
            parsed["option_type"] == right
            and parsed["expiry"] == expiry
            and abs(float(parsed["strike"]) - strike_f) < 1e-6
        ):
            key = str(row.get("symbol") or key)
            break
    return {
        "instrument": sym,
        "strike": strike_f,
        "option_type": right,
        "expiry": expiry,
        "instrument_key": key,
        "lot_size": DEFAULT_CRYPTO_LOT,
        "resolved_from": "delta_india",
    }


def fetch_crypto_ltps(keys: Sequence[str]) -> Dict[str, Optional[float]]:
    from backend.services.crypto_options_report.delta_client import fetch_option_quotes

    clean = [str(k).strip() for k in keys if str(k or "").strip()]
    out: Dict[str, Optional[float]] = {k: None for k in clean}
    if not clean:
        return out
    quotes = fetch_option_quotes(clean)
    for key in clean:
        mark = (quotes.get(key) or {}).get("mark")
        out[key] = float(mark) if mark is not None and float(mark) > 0 else None
    return out


def fetch_crypto_deltas(keys: Sequence[str]) -> Dict[str, Optional[float]]:
    from backend.services.crypto_options_report.delta_client import fetch_option_quotes

    clean = [str(k).strip() for k in keys if str(k or "").strip()]
    out: Dict[str, Optional[float]] = {k: None for k in clean}
    if not clean:
        return out
    quotes = fetch_option_quotes(clean)
    for key in clean:
        delta = (quotes.get(key) or {}).get("delta")
        try:
            out[key] = float(delta) if delta is not None else None
        except (TypeError, ValueError):
            out[key] = None
    return out


def crypto_spot_ltps(instruments: Sequence[str]) -> Dict[str, Optional[float]]:
    """Spot from any live option ticker for that underlying."""
    wanted: List[str] = []
    for name in instruments:
        token = str(name or "").strip().upper()
        if token in CRYPTO_TO_DELTA and token not in wanted:
            wanted.append(token)
    out: Dict[str, Optional[float]] = {name: None for name in wanted}
    for name in wanted:
        und = CRYPTO_TO_DELTA[name]
        for row in _ticker_rows(und):
            raw = row.get("spot_price")
            try:
                spot = float(raw) if raw is not None else None
            except (TypeError, ValueError):
                spot = None
            if spot is not None and spot > 0:
                out[name] = spot
                break
    return out


def split_upstox_and_delta_keys(keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    upstox: List[str] = []
    delta: List[str] = []
    for key in keys:
        raw = str(key or "").strip()
        if not raw:
            continue
        if is_delta_option_key(raw):
            delta.append(raw)
        else:
            upstox.append(raw)
    return upstox, delta
