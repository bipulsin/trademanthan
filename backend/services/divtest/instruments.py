"""Instrument master download + name → key resolution (cash / futures / MCX)."""
from __future__ import annotations

import calendar
import gzip
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from backend.services.divtest.config import CACHE_DIR, get_settings

logger = logging.getLogger(__name__)

COMPLETE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
MASTER_PATH = CACHE_DIR / "complete.json"
MAX_AGE_SEC = 24 * 3600

_cache: Dict[str, Any] = {"rows": [], "loaded_at": 0.0}


def _parse_expiry_ms(expiry: Any) -> Optional[int]:
    if expiry is None or expiry == "":
        return None
    if isinstance(expiry, (int, float)):
        n = float(expiry)
        return int(n if n > 1e12 else n * 1000)
    s = str(expiry).strip()
    if s.isdigit():
        n = float(s)
        return int(n if n > 1e12 else n * 1000)
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return None


def ensure_instrument_master(force: bool = False) -> List[Dict[str, Any]]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    now = time.time()
    if not force and _cache["rows"] and now - float(_cache["loaded_at"]) < MAX_AGE_SEC:
        return _cache["rows"]
    if not force and MASTER_PATH.exists() and now - MASTER_PATH.stat().st_mtime < MAX_AGE_SEC:
        rows = json.loads(MASTER_PATH.read_text(encoding="utf-8"))
        _cache["rows"] = rows
        _cache["loaded_at"] = now
        return rows

    logger.info("Downloading Upstox instrument master…")
    resp = requests.get(COMPLETE_URL, timeout=120)
    resp.raise_for_status()
    raw = resp.content
    if COMPLETE_URL.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        text = gzip.decompress(raw).decode("utf-8")
    else:
        text = raw.decode("utf-8")
    rows = json.loads(text)
    MASTER_PATH.write_text(json.dumps(rows), encoding="utf-8")
    _cache["rows"] = rows
    _cache["loaded_at"] = now
    logger.info("Instrument master loaded: %s rows", len(rows))
    return rows


def _match_name(row: Dict[str, Any], symbol: str) -> bool:
    s = symbol.upper()
    fields = [row.get("name"), row.get("trading_symbol"), row.get("short_name"), row.get("underlying_symbol")]
    for f in fields:
        if not f:
            continue
        fu = str(f).upper()
        if fu == s or fu.startswith(f"{s} ") or fu.startswith(f"{s}-"):
            return True
    return False


def resolve_instrument(symbol: str) -> Dict[str, Any]:
    rows = ensure_instrument_master()
    s = str(symbol).upper().strip()
    cfg = get_settings()
    matched = [r for r in rows if _match_name(r, s)]
    if not matched:
        return {
            "symbol": s,
            "found": False,
            "cash": None,
            "futures": [],
            "is_commodity": False,
            "lot_size": cfg["equity_default_qty"],
            "notes": [f"No instrument master match for {s}"],
        }

    cash = next(
        (
            r
            for r in matched
            if (r.get("segment") == "NSE_EQ" or r.get("instrument_type") == "EQ")
            and str(r.get("trading_symbol") or "").upper() == s
        ),
        None,
    )
    if cash is None:
        cash = next(
            (r for r in matched if r.get("segment") == "NSE_EQ" or r.get("instrument_type") == "EQ"),
            None,
        )
    if cash is None:
        cash = next((r for r in matched if r.get("segment") == "BSE_EQ"), None)

    fut_rows = []
    for r in matched:
        t = str(r.get("instrument_type") or "").upper()
        seg = str(r.get("segment") or "").upper()
        if ("FUT" in t) and ("FO" in seg or "MCX" in seg or "NFO" in seg or "CDS" in seg):
            fut_rows.append(r)

    is_commodity = any("MCX" in str(r.get("segment") or r.get("exchange") or "").upper() for r in matched)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    futures = []
    for r in fut_rows:
        exp_ms = _parse_expiry_ms(r.get("expiry"))
        if not exp_ms or r.get("weekly"):
            continue
        futures.append(
            {
                "instrument_key": r.get("instrument_key"),
                "trading_symbol": r.get("trading_symbol"),
                "segment": r.get("segment"),
                "lot_size": int(r.get("lot_size") or r.get("minimum_lot") or 1),
                "expiry": datetime.utcfromtimestamp(exp_ms / 1000).strftime("%Y-%m-%d"),
                "expiry_ms": exp_ms,
            }
        )
    futures.sort(key=lambda f: f["expiry_ms"])
    live = [f for f in futures if f["expiry_ms"] >= now_ms - 7 * 86400_000]
    current = live[0] if live else None
    lot = (current and current["lot_size"]) or (
        int(cash.get("lot_size") or 1) if cash else cfg["equity_default_qty"]
    )

    return {
        "symbol": s,
        "found": True,
        "cash": {
            "instrument_key": cash.get("instrument_key"),
            "trading_symbol": cash.get("trading_symbol"),
            "segment": cash.get("segment"),
            "lot_size": int(cash.get("lot_size") or 1),
        }
        if cash
        else None,
        "futures": futures,
        "current_future": current,
        "is_commodity": is_commodity,
        "lot_size": lot,
        "notes": [],
    }


def pick_future_for_month(resolved: Dict[str, Any], yyyy_mm: str) -> Optional[Dict[str, Any]]:
    y, m = [int(x) for x in yyyy_mm.split("-")]
    month_start = int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp() * 1000)
    last_day = calendar.monthrange(y, m)[1]
    month_end = int(datetime(y, m, last_day, 23, 59, tzinfo=timezone.utc).timestamp() * 1000)
    mid = int(datetime(y, m, 15, tzinfo=timezone.utc).timestamp() * 1000)
    candidates = [f for f in (resolved.get("futures") or []) if f["expiry_ms"] >= month_start - 5 * 86400_000]
    if not candidates:
        return None
    by_exp = sorted(
        [f for f in candidates if mid <= f["expiry_ms"] <= month_end + 45 * 86400_000],
        key=lambda f: f["expiry_ms"],
    )
    if by_exp:
        return by_exp[0]
    after = sorted([f for f in candidates if f["expiry_ms"] >= month_start], key=lambda f: f["expiry_ms"])
    return after[0] if after else None
