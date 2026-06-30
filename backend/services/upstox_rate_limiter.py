"""Process-wide budget for Upstox historical/intraday candle requests.

Upstox enforces per-user rate limits on the historical-candle APIs (documented
~50 req/s, 500 req/min, 2000 req/30-min — and as low as 10 req/s for the algo
retail category). Many in-process jobs (market-data refresh, Vajra, Smart Futures
picker, OI heatmap, …) each fetch candles concurrently; collectively they blow
the per-user budget and trigger a 429 storm, where every job then wastes time on
back-off retries.

This module provides a single shared limiter so all candle requests in the
process draw from one budget and are paced under the caps — turning chaotic 429
thrash into orderly, predictable throughput. It is intentionally simple and
self-contained (no external deps) and thread-safe for use from ThreadPoolExecutor
workers.

Scope: only the candle endpoints are gated (that is where the storm is); order,
position and quote calls are unaffected.
"""
from __future__ import annotations

import bisect
import logging
import threading
import time
from typing import List, Tuple

logger = logging.getLogger(__name__)


class SlidingWindowRateLimiter:
    """Enforces several (max_count, window_seconds) caps simultaneously.

    ``acquire`` blocks until a request slot is available under *all* configured
    windows, then records the grant. Returns the seconds it waited (for metrics).
    """

    def __init__(self, limits: List[Tuple[int, float]], min_interval: float = 0.0):
        # Keep only positive caps; sort by window for readability.
        self._limits = sorted(
            ((int(m), float(w)) for m, w in limits if int(m) > 0 and float(w) > 0),
            key=lambda x: x[1],
        )
        self._max_window = max((w for _, w in self._limits), default=0.0)
        # Minimum spacing between consecutive grants — evens out bursts so a batch
        # of worker threads can't fire N requests in the same instant and trip
        # Upstox's per-second limit.
        self._min_interval = max(0.0, float(min_interval))
        self._events: List[float] = []  # monotonic grant timestamps, ascending
        self._last_grant: float = 0.0
        self._lock = threading.Lock()

    def _wait_needed(self, now: float) -> float:
        """Seconds to wait before a slot frees up (0.0 if free now). Caller holds lock."""
        # Drop events older than the widest window.
        cutoff = now - self._max_window
        drop = bisect.bisect_left(self._events, cutoff)
        if drop:
            del self._events[:drop]

        wait = 0.0
        if self._min_interval > 0.0 and self._last_grant:
            wait = max(wait, self._last_grant + self._min_interval - now)
        for max_count, window in self._limits:
            start = now - window
            j = bisect.bisect_left(self._events, start)
            count = len(self._events) - j
            if count >= max_count:
                # The event at this index must exit its window before we may proceed.
                exit_event = self._events[len(self._events) - max_count]
                wait = max(wait, exit_event + window - now)
        return wait

    def acquire(self, max_wait: float = 240.0) -> float:
        """Block (up to ``max_wait`` s) until a slot is free; record + return wait."""
        if not self._limits:
            return 0.0
        start_ts = time.monotonic()
        while True:
            with self._lock:
                now = time.monotonic()
                wait = self._wait_needed(now)
                if wait <= 0.0:
                    self._events.append(now)
                    self._last_grant = now
                    return now - start_ts
            if (time.monotonic() - start_ts) + wait > max_wait:
                # Give up waiting; record the grant anyway so we don't busy-loop and
                # so the caller proceeds (Upstox 429 back-off remains the safety net).
                with self._lock:
                    g = time.monotonic()
                    self._events.append(g)
                    self._last_grant = g
                logger.warning(
                    "candle rate limiter: max_wait %.0fs exceeded; proceeding", max_wait
                )
                return time.monotonic() - start_ts
            time.sleep(min(wait, 0.25))


# --- process-wide singleton -------------------------------------------------

_LIMITER: SlidingWindowRateLimiter | None = None
_INIT_LOCK = threading.Lock()

# Lightweight metrics (best-effort, not strictly synchronized on read).
_acquired = 0
_total_wait = 0.0
_throttled = 0


def _build_limiter() -> SlidingWindowRateLimiter:
    from backend.config import settings

    per_sec = max(1, int(getattr(settings, "UPSTOX_CANDLE_RL_PER_SEC", 5)))
    configured_interval = float(getattr(settings, "UPSTOX_CANDLE_RL_MIN_INTERVAL", 0) or 0)
    # Even spacing derived from the per-second cap unless explicitly overridden (>0).
    min_interval = configured_interval if configured_interval > 0 else (1.0 / per_sec)
    return SlidingWindowRateLimiter(
        [
            (per_sec, 1.0),
            (getattr(settings, "UPSTOX_CANDLE_RL_PER_MIN", 120), 60.0),
            (getattr(settings, "UPSTOX_CANDLE_RL_PER_30MIN", 1500), 1800.0),
        ],
        min_interval=min_interval,
    )


def _get_limiter() -> SlidingWindowRateLimiter:
    global _LIMITER
    if _LIMITER is None:
        with _INIT_LOCK:
            if _LIMITER is None:
                _LIMITER = _build_limiter()
    return _LIMITER


def acquire_candle_slot() -> float:
    """Block until a candle-request slot is available under the shared budget.

    No-op (returns 0.0) when disabled via ``UPSTOX_CANDLE_RATE_LIMIT_ENABLED``.
    """
    global _acquired, _total_wait, _throttled
    try:
        from backend.config import settings

        if not getattr(settings, "UPSTOX_CANDLE_RATE_LIMIT_ENABLED", True):
            return 0.0
    except Exception:
        pass

    try:
        from backend.config import settings

        max_wait = float(getattr(settings, "UPSTOX_CANDLE_RL_MAX_WAIT", 240) or 240)
    except Exception:
        max_wait = 240.0
    waited = _get_limiter().acquire(max_wait=max_wait)
    _acquired += 1
    _total_wait += waited
    if waited > 0.01:
        _throttled += 1
    # Periodic visibility into how hard we are throttling.
    if _acquired % 500 == 0:
        logger.info(
            "candle rate limiter: %d requests paced, %d throttled, %.1fs total wait",
            _acquired, _throttled, _total_wait,
        )
    return waited


def stats() -> dict:
    return {"acquired": _acquired, "throttled": _throttled, "total_wait_sec": round(_total_wait, 1)}
