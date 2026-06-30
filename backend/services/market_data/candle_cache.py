"""Process-local cache of recently fetched intraday candles.

The centralized market-data refresh (``engine.refresh_arbitrage_master_market_data``)
already fetches 5m candle history for the whole current-month universe every 5
minutes. Other in-process consumers (e.g. the Relative Strength Scanner) can read
those candles from here instead of issuing their own duplicate Upstox requests,
which keeps total request volume — and 429 rate-limit pressure — down.

This is an in-memory cache shared across threads of the same process (APScheduler
jobs + uvicorn workers run in-process). It is intentionally simple: last-write
wins, with a per-entry timestamp so stale data can be rejected by the reader.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional, Tuple

_LOCK = threading.Lock()
# instrument_key -> (epoch_seconds, candles)
_CACHE: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}


def put(instrument_key: str, candles: Optional[List[Dict[str, Any]]]) -> None:
    """Store candles for an instrument (no-op for empty input)."""
    if not instrument_key or not candles:
        return
    with _LOCK:
        _CACHE[instrument_key] = (time.time(), list(candles))


def get(instrument_key: str, max_age_sec: float) -> Optional[List[Dict[str, Any]]]:
    """Return cached candles if present and fresher than ``max_age_sec``, else None."""
    if not instrument_key:
        return None
    with _LOCK:
        item = _CACHE.get(instrument_key)
    if not item:
        return None
    ts, candles = item
    if (time.time() - ts) > max_age_sec:
        return None
    return candles


def stats() -> Dict[str, Any]:
    """Lightweight introspection for diagnostics/logging."""
    with _LOCK:
        return {"entries": len(_CACHE)}
