"""
For backtest session dates in Feb–Mar 2026, resolve equity futures from the instruments file
by **April 2026 expiry** (not ``arbitrage_master.currmth``), so replay always uses the April series.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

from backend.config import get_instruments_file_path

logger = logging.getLogger(__name__)

# Inclusive window: these session dates use April-2026 contracts from instruments JSON.
APRIL_2026_FUT_SESSION_START = date(2026, 2, 1)
APRIL_2026_FUT_SESSION_END = date(2026, 3, 31)


def use_fixed_april_2026_futures(session_date: date) -> bool:
    return APRIL_2026_FUT_SESSION_START <= session_date <= APRIL_2026_FUT_SESSION_END


def _expiry_ms_to_utc_date(ms: object) -> Optional[date]:
    try:
        n = int(ms)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if n > 1_000_000_000_000:
        n //= 1000
    try:
        return datetime.fromtimestamp(n, tz=timezone.utc).date()
    except (OSError, OverflowError, ValueError):
        return None


def _is_april_2026_expiry(ms: object) -> bool:
    d = _expiry_ms_to_utc_date(ms)
    return d is not None and d.year == 2026 and d.month == 4


def load_april_2026_futures_by_underlying() -> Dict[str, Tuple[str, str]]:
    """
    underlying_symbol (NSE) -> (trading_symbol, instrument_key) for FUT with expiry in April 2026.

    If multiple rows per underlying, prefer a symbol containing ``APR`` (monthly naming).
    """
    path: Path = get_instruments_file_path()
    if not path.is_file():
        logger.warning("april_2026_universe: instruments file missing: %s", path)
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.warning("april_2026_universe: failed to read instruments: %s", e)
        return {}
    if not isinstance(data, list):
        return {}

    out: Dict[str, Tuple[str, str]] = {}
    for inst in data:
        if not isinstance(inst, dict):
            continue
        u = (inst.get("underlying_symbol") or "").strip().upper()
        if not u:
            continue
        if str(inst.get("instrument_type") or "").upper() != "FUT":
            continue
        seg = str(inst.get("segment") or "").upper()
        if "NSE_FO" not in seg and "NFO" not in seg:
            continue
        ex = inst.get("expiry")
        if ex is None or not _is_april_2026_expiry(ex):
            continue
        ts = (inst.get("trading_symbol") or inst.get("tradingsymbol") or "").strip()
        ik = (inst.get("instrument_key") or "").strip()
        if not ts or not ik:
            continue
        prev = out.get(u)
        if prev is None:
            out[u] = (ts, ik)
            continue
        # Prefer explicit APR in symbol when replacing a non-APR label
        prev_ts = prev[0]
        if "APR" in ts.upper() and "APR" not in prev_ts.upper():
            out[u] = (ts, ik)
    return out
