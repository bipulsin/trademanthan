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

Priority: ``scheduled_10m`` (and other scheduled warm executions) run with a
longer per-slot wait and block discretionary callers while active. Discretionary
callers also yield when the 30-min window is near cap (headroom reserved for the
next scheduled warm).
"""
from __future__ import annotations

import bisect
import logging
import threading
import time
from contextlib import contextmanager
from typing import Iterator, List, Tuple

logger = logging.getLogger(__name__)

# Executions that receive scheduled priority (long wait + block discretionary).
SCHEDULED_WARM_EXECUTIONS = frozenset(
    {
        "scheduled_10m",
        "scheduled_aux_0905",
        "scheduled_stock_next_vwap_ema_hourly",
    }
)


def is_scheduled_warm_execution(execution: str) -> bool:
    return str(execution or "") in SCHEDULED_WARM_EXECUTIONS


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

    def _prune(self, now: float) -> None:
        cutoff = now - self._max_window
        drop = bisect.bisect_left(self._events, cutoff)
        if drop:
            del self._events[:drop]

    def _wait_needed(self, now: float) -> float:
        """Seconds to wait before a slot frees up (0.0 if free now). Caller holds lock."""
        self._prune(now)

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

    def count_in_window(self, window_seconds: float) -> int:
        """Grants recorded in the last ``window_seconds`` (for headroom checks)."""
        with self._lock:
            now = time.monotonic()
            self._prune(now)
            start = now - float(window_seconds)
            j = bisect.bisect_left(self._events, start)
            return len(self._events) - j

    def acquire(self, max_wait: float = 90.0) -> Tuple[bool, float]:
        """Try to reserve a slot, waiting up to ``max_wait`` s.

        Returns ``(granted, waited_seconds)``. When the budget can't free a slot
        within ``max_wait`` the request is **denied** (``granted=False``) and *no*
        slot is consumed — the caller should skip the request entirely rather than
        sending it to Upstox. This sheds excess demand cleanly instead of bursting
        over the limit and triggering 429s.
        """
        if not self._limits:
            return True, 0.0
        start_ts = time.monotonic()
        while True:
            with self._lock:
                now = time.monotonic()
                wait = self._wait_needed(now)
                if wait <= 0.0:
                    self._events.append(now)
                    self._last_grant = now
                    return True, now - start_ts
            if (time.monotonic() - start_ts) + wait > max_wait:
                # Budget exhausted: deny without sending (caller skips this request).
                return False, time.monotonic() - start_ts
            time.sleep(min(wait, 0.25))


# --- process-wide singleton -------------------------------------------------

_LIMITER: SlidingWindowRateLimiter | None = None
_INIT_LOCK = threading.Lock()

# Lightweight metrics (best-effort, not strictly synchronized on read).
_acquired = 0
_total_wait = 0.0
_throttled = 0
_denied = 0
_denied_yield_scheduled = 0
_denied_yield_headroom = 0
_scheduled_acquired = 0

# Thread-local: worker threads inside a scheduled warm pool mark themselves.
_tls = threading.local()

# Active scheduled warm depth (main thread holds context; workers use tls).
_scheduled_warm_depth = 0
_scheduled_warm_depth_lock = threading.Lock()

# Process-wide max_wait override (legacy hourly path). Prefer candle_warm_execution().
_max_wait_override: float | None = None
_max_wait_override_lock = threading.Lock()


def set_candle_rl_max_wait_override(seconds: float | None) -> None:
    """Set/clear a process-wide acquire max_wait for candle slots (None = use default)."""
    global _max_wait_override
    with _max_wait_override_lock:
        _max_wait_override = None if seconds is None else float(seconds)


@contextmanager
def scheduled_candle_worker() -> Iterator[None]:
    """Mark the current thread as a scheduled warm worker (ThreadPoolExecutor child)."""
    prev = getattr(_tls, "scheduled_worker", False)
    _tls.scheduled_worker = True
    try:
        yield
    finally:
        _tls.scheduled_worker = prev


@contextmanager
def candle_warm_execution(execution: str) -> Iterator[None]:
    """Wrap a scheduled candle warm: priority mode + long max_wait for workers."""
    global _scheduled_warm_depth
    is_scheduled = is_scheduled_warm_execution(execution)
    if is_scheduled:
        with _scheduled_warm_depth_lock:
            _scheduled_warm_depth += 1
        set_candle_rl_max_wait_override(_scheduled_max_wait())
    try:
        yield
    finally:
        if is_scheduled:
            set_candle_rl_max_wait_override(None)
            with _scheduled_warm_depth_lock:
                _scheduled_warm_depth -= 1


def _scheduled_warm_active() -> bool:
    with _scheduled_warm_depth_lock:
        return _scheduled_warm_depth > 0


def _is_scheduled_worker() -> bool:
    return bool(getattr(_tls, "scheduled_worker", False))


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


def _scheduled_max_wait() -> float:
    from backend.config import settings

    return float(getattr(settings, "UPSTOX_CANDLE_RL_SCHEDULED_MAX_WAIT", 300) or 300)


def _default_max_wait() -> float:
    from backend.config import settings

    if _is_backtest_bulk_prefetch():
        return float(getattr(settings, "UPSTOX_BTST_PREFETCH_RL_MAX_WAIT", 300) or 300)
    return float(getattr(settings, "UPSTOX_CANDLE_RL_MAX_WAIT", 90) or 90)


def _headroom_exhausted() -> bool:
    """True when discretionary callers should yield to the next scheduled_10m."""
    from backend.config import settings

    cap = int(getattr(settings, "UPSTOX_CANDLE_RL_PER_30MIN", 1500))
    headroom = int(getattr(settings, "SCHEDULED_CANDLE_RL_HEADROOM", 220))
    if cap <= 0 or headroom <= 0:
        return False
    used = _get_limiter().count_in_window(1800.0)
    return used >= max(0, cap - headroom)


# Thread-local: BTST bulk prefetch waits longer for a slot instead of denying at 90s.
_bt_local = threading.local()


def set_backtest_bulk_prefetch_mode(enabled: bool) -> None:
    """When True, candle acquire waits up to 5 min (BTST bulk prefetch only)."""
    _bt_local.backtest_bulk = bool(enabled)


def _is_backtest_bulk_prefetch() -> bool:
    return bool(getattr(_bt_local, "backtest_bulk", False))


def acquire_candle_slot() -> bool:
    """Reserve a candle-request slot under the shared budget.

    Returns True if the caller may send the request, or False if the budget is
    exhausted and the request should be skipped (no Upstox call). Always returns
    True when disabled via ``UPSTOX_CANDLE_RATE_LIMIT_ENABLED``.
    """
    global _acquired, _total_wait, _throttled, _denied
    global _denied_yield_scheduled, _denied_yield_headroom, _scheduled_acquired
    try:
        from backend.config import settings

        if not getattr(settings, "UPSTOX_CANDLE_RATE_LIMIT_ENABLED", True):
            return True
    except Exception:
        pass

    scheduled_worker = _is_scheduled_worker()
    if _scheduled_warm_active() and not scheduled_worker:
        _denied += 1
        _denied_yield_scheduled += 1
        return False

    if not scheduled_worker and _headroom_exhausted():
        _denied += 1
        _denied_yield_headroom += 1
        return False

    try:
        with _max_wait_override_lock:
            override = _max_wait_override
        if override is not None and override > 0:
            max_wait = float(override)
        elif scheduled_worker:
            max_wait = _scheduled_max_wait()
        else:
            max_wait = _default_max_wait()
    except Exception:
        max_wait = _scheduled_max_wait() if scheduled_worker else 90.0

    granted, waited = _get_limiter().acquire(max_wait=max_wait)
    _total_wait += waited
    if not granted:
        _denied += 1
    else:
        _acquired += 1
        if scheduled_worker:
            _scheduled_acquired += 1
        if waited > 0.01:
            _throttled += 1
    # Periodic visibility into pacing + how much demand is being shed.
    if (_acquired + _denied) % 500 == 0:
        logger.info(
            "candle rate limiter: %d granted, %d denied(skipped), %d throttled, "
            "%.1fs total wait (yield_sched=%d yield_headroom=%d scheduled_grants=%d)",
            _acquired,
            _denied,
            _throttled,
            _total_wait,
            _denied_yield_scheduled,
            _denied_yield_headroom,
            _scheduled_acquired,
        )
    return granted


def stats() -> dict:
    out = {
        "acquired": _acquired,
        "denied": _denied,
        "throttled": _throttled,
        "total_wait_sec": round(_total_wait, 1),
        "denied_yield_to_scheduled": _denied_yield_scheduled,
        "denied_yield_headroom": _denied_yield_headroom,
        "scheduled_acquired": _scheduled_acquired,
        "scheduled_warm_active": _scheduled_warm_active(),
    }
    try:
        lim = _get_limiter()
        out["window_30min_used"] = lim.count_in_window(1800.0)
    except Exception:
        pass
    return out
