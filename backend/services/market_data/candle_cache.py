"""Process-wide shared cache of recently fetched candles.

Many in-process jobs (centralized market-data refresh, Vajra, Smart Futures
picker, OI heatmap, sector scores, Relative Strength scanner, …) fetch candles
for overlapping instruments every few minutes. Without coordination they each
issue their own Upstox historical-candle requests for the *same* bars, which —
together with the shared per-user rate limit — caused the market-open 429 storm.

This cache lets the whole platform fetch each ``(instrument, interval)`` series
roughly once per TTL instead of once per job:

* Keyed by ``(instrument_key, interval)``; the value records the fetched date
  span so a cached *wider* window can serve any *narrower* request (filtered to
  the caller's exact ``from_date`` so indicator math is identical to a direct
  fetch).
* Callers decide freshness via ``max_age_sec`` at read time, so a tolerant reader
  (e.g. the RS scanner) and a strict reader (signal jobs) can share one entry.

In-memory, thread-safe, shared across the APScheduler jobs + uvicorn request
handlers that run in the same process.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# Canonical (widest) fetch window per interval. When the shared cache fetches one
# of these intervals it fetches at least this many days so engine/picker/RS share
# a single underlying fetch; each caller is still returned only its requested
# window. Intervals absent here are fetched at the caller's requested width.
# (A wider days_back is a single HTTP call — same request-count — so this is free
# on the rate-limit dimension.)
CANONICAL_DAYS_BACK: Dict[str, int] = {
    "minutes/1": 2,
    "minutes/5": 6,
    "minutes/15": 6,
    "days/1": 45,
}

_MAX_ENTRIES = 4000


@dataclass
class _Entry:
    fetched: float       # epoch seconds
    from_date: str       # "YYYY-MM-DD" (inclusive start of cached span)
    to_date: str         # "YYYY-MM-DD" (inclusive end of cached span)
    candles: List[Dict[str, Any]]


_LOCK = threading.Lock()
_CACHE: Dict[Tuple[str, str], _Entry] = {}

# Best-effort metrics.
_hits = 0
_misses = 0


def canonical_days_back(interval: str, requested_days_back: int) -> int:
    """Widest days_back to actually fetch for ``interval`` (>= requested)."""
    return max(int(requested_days_back), CANONICAL_DAYS_BACK.get(interval or "", 0))


def filter_from(candles: List[Dict[str, Any]], from_date: str) -> List[Dict[str, Any]]:
    """Return only candles whose IST date (timestamp[:10]) is >= ``from_date``."""
    if not from_date:
        return list(candles)
    out: List[Dict[str, Any]] = []
    for c in candles:
        ts = str(c.get("timestamp") or "")
        if ts[:10] >= from_date:
            out.append(c)
    return out


def _evict_if_needed() -> None:
    """Drop the oldest entries if the cache grows past the cap. Caller holds lock."""
    if len(_CACHE) <= _MAX_ENTRIES:
        return
    # Remove ~10% oldest by fetched time.
    victims = sorted(_CACHE.items(), key=lambda kv: kv[1].fetched)[: max(1, _MAX_ENTRIES // 10)]
    for key, _ in victims:
        _CACHE.pop(key, None)


def put(
    instrument_key: str,
    interval: str,
    from_date: str,
    to_date: str,
    candles: Optional[List[Dict[str, Any]]],
) -> None:
    """Store a fetched candle span (no-op for empty input)."""
    if not instrument_key or not interval or not candles:
        return
    with _LOCK:
        _CACHE[(instrument_key, interval)] = _Entry(
            fetched=time.time(),
            from_date=from_date or "",
            to_date=to_date or "",
            candles=list(candles),
        )
        _evict_if_needed()


def get(
    instrument_key: str,
    interval: str,
    from_date: str,
    max_age_sec: float,
) -> Optional[List[Dict[str, Any]]]:
    """Return cached candles covering ``[from_date, …]`` if fresh, else None.

    A cached entry serves the request when its span starts on/before ``from_date``
    (i.e. it covers the requested window); the result is filtered to ``from_date``
    so it matches a direct fetch for that window.
    """
    global _hits, _misses
    if not instrument_key or not interval:
        return None
    with _LOCK:
        entry = _CACHE.get((instrument_key, interval))
        if entry is None:
            _misses += 1
            return None
        if (time.time() - entry.fetched) > max_age_sec:
            _misses += 1
            return None
        if from_date and entry.from_date and entry.from_date > from_date:
            # Cached window starts later than requested -> does not cover it.
            _misses += 1
            return None
        candles = entry.candles
    _hits += 1
    return filter_from(candles, from_date)


def get_recent(
    instrument_key: str,
    interval: str,
    max_age_sec: float,
) -> Optional[List[Dict[str, Any]]]:
    """Return the full cached candle series for ``(instrument_key, interval)`` if
    fresh, ignoring the date window. For readers that just need the most recent
    bars (e.g. the RS scanner) regardless of exact span."""
    global _hits, _misses
    if not instrument_key or not interval:
        return None
    with _LOCK:
        entry = _CACHE.get((instrument_key, interval))
        if entry is None or (time.time() - entry.fetched) > max_age_sec:
            _misses += 1
            return None
        candles = list(entry.candles)
    _hits += 1
    return candles


def stats() -> Dict[str, Any]:
    """Lightweight introspection for diagnostics/logging."""
    with _LOCK:
        entries = len(_CACHE)
    total = _hits + _misses
    return {
        "entries": entries,
        "hits": _hits,
        "misses": _misses,
        "hit_rate": round(_hits / total, 3) if total else 0.0,
    }
