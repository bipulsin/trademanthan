"""Load the arbitrage_master universe and each front-month contract's own candles.

October (expired) contracts resolve through the existing expired-instrument API.
The November contract, still listed on the scan's last day, resolves from the
instruments file first and falls back to the same API.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.config import get_instruments_file_path, settings
from backend.services.mtf_swing.data import (
    _fetch_expired_rows,
    load_universe,
    resolve_expired_future,
)
from backend.services.upstox_service import UpstoxService, _aggregate_1m_to_n_minute

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def cache_dir() -> Path:
    mounted = Path("/home/ubuntu/trademanthan/data")
    base = mounted if mounted.is_dir() else repo_root() / "data"
    path = base / "ema_scan" / "cache"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _expiry_date(inst: Dict[str, Any]) -> Optional[date]:
    exp = inst.get("expiry")
    if isinstance(exp, str) and len(exp) >= 10 and exp[4] == "-":
        try:
            return date.fromisoformat(exp[:10])
        except ValueError:
            return None
    try:
        num = float(exp)
    except (TypeError, ValueError):
        return None
    if num > 10**12:
        num = num / 1000.0
    try:
        return datetime.fromtimestamp(num, tz=IST).date()
    except (OverflowError, OSError, ValueError):
        return None


def load_future_index() -> Dict[Tuple[str, date], Dict[str, str]]:
    """Map (underlying, expiry date) to the listed future in nse_instruments.json."""
    path = get_instruments_file_path()
    out: Dict[Tuple[str, date], Dict[str, str]] = {}
    if not path.is_file():
        logger.info("ema_scan instruments file missing: %s", path)
        return out
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.info("ema_scan instruments read failed: %s", exc)
        return out
    if not isinstance(data, list):
        return out
    for inst in data:
        if not isinstance(inst, dict):
            continue
        if inst.get("segment") != "NSE_FO" or inst.get("instrument_type") != "FUT":
            continue
        trading = str(inst.get("trading_symbol") or "").strip()
        if " FUT " not in trading:
            continue
        symbol = trading.split(" FUT ", 1)[0].strip().upper()
        expiry = _expiry_date(inst)
        key = str(inst.get("instrument_key") or "").strip()
        if not symbol or expiry is None or not key:
            continue
        out[(symbol, expiry)] = {"instrument_key": key, "trading_symbol": trading}
    return out


def resolve_contract(
    upstox: UpstoxService,
    symbol: str,
    expiry: date,
    underlying_key: str,
    fut_index: Dict[Tuple[str, date], Dict[str, str]],
) -> Dict[str, str]:
    symbol_u = symbol.upper()
    found = fut_index.get((symbol_u, expiry))
    if not found:
        # Listed expiry can sit on the holiday Tuesday while the tradable expiry
        # is the previous session. Accept a contract within three calendar days.
        nearest = None
        nearest_gap = 99
        for (sym, exp), row in fut_index.items():
            if sym != symbol_u:
                continue
            gap = abs((exp - expiry).days)
            if gap <= 3 and gap < nearest_gap:
                nearest = row
                nearest_gap = gap
        found = nearest
    if found and found.get("instrument_key"):
        return found
    resolved = resolve_expired_future(upstox, symbol, expiry, underlying_key)
    key = str(resolved.get("instrument_key") or "").strip()
    if not key:
        return {"instrument_key": "", "trading_symbol": ""}
    label = f"{symbol.upper()} {expiry.strftime('%d%b%y').upper()}"
    return {"instrument_key": key, "trading_symbol": label}


def _chunks(start: date, end: date, span_days: int = 28):
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=span_days))
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def _cache_file(instrument_key: str, kind: str, start: date, end: date) -> Path:
    safe = instrument_key.replace("|", "_").replace("/", "_").replace(":", "_")
    return cache_dir() / f"{safe}_{kind}_{start.isoformat()}_{end.isoformat()}.json"


def _read_cache(path: Path) -> Optional[List[Dict[str, Any]]]:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, list) else None


def _write_cache(path: Path, rows: List[Dict[str, Any]]) -> None:
    try:
        path.write_text(json.dumps(rows), encoding="utf-8")
    except Exception as exc:
        logger.info("ema_scan cache write failed %s: %s", path.name, exc)


def _normalize(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if "timestamp" not in row:
            continue
        try:
            out.append(
                {
                    "timestamp": row["timestamp"],
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue
    return out


def fetch_candles(
    upstox: UpstoxService,
    instrument_key: str,
    start: date,
    end: date,
    kind: str,
) -> List[Dict[str, Any]]:
    """``kind`` is ``5m`` or ``1d``. Cached on disk per contract and span."""
    path = _cache_file(instrument_key, kind, start, end)
    cached = _read_cache(path)
    if cached is not None:
        return cached
    expired = instrument_key.count("|") >= 2
    rows: List[Dict[str, Any]] = []
    if kind == "5m":
        if expired:
            for chunk_start, chunk_end in _chunks(start, end, 28):
                rows.extend(
                    _fetch_expired_rows(upstox, instrument_key, "5minute", chunk_start, chunk_end)
                )
            if len(rows) < 50:
                raw_1m: List[Dict[str, Any]] = []
                for chunk_start, chunk_end in _chunks(start, end, 20):
                    raw_1m.extend(
                        _fetch_expired_rows(
                            upstox, instrument_key, "1minute", chunk_start, chunk_end
                        )
                    )
                if raw_1m:
                    rows = _aggregate_1m_to_n_minute(raw_1m, 5)
        else:
            for _chunk_start, chunk_end in _chunks(start, end, 28):
                days_back = 28
                raw = upstox.get_historical_candles_by_instrument_key(
                    instrument_key,
                    interval="minutes/5",
                    days_back=days_back,
                    range_end_date=chunk_end,
                )
                rows.extend(raw or [])
    else:
        if expired:
            rows.extend(_fetch_expired_rows(upstox, instrument_key, "day", start, end))
        else:
            days_back = max(1, (end - start).days + 2)
            raw = upstox.get_historical_candles_by_instrument_key(
                instrument_key,
                interval="days/1",
                days_back=days_back,
                range_end_date=end,
            )
            rows.extend(raw or [])
    normalized = _normalize(rows)
    min_rows = 200 if kind == "5m" else 10
    if len(normalized) >= min_rows:
        _write_cache(path, normalized)
    return normalized


def make_upstox() -> UpstoxService:
    return UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)


def universe_rows() -> List[Dict[str, str]]:
    return load_universe()
