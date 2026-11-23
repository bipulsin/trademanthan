"""Dedicated OLH candle REST budgets (Phase A / Phase B / canary).

Why (2026-11-23 forensics)
-------------------------
Morning Phase A refill kept probing the full universe for missing 09:15 bars
through 09:25–09:29 and competed with Phase B on the **shared** process-wide
Upstox candle limiter (``UPSTOX_CANDLE_RL_*`` ≈ 5/s, 120/min, 1500/30m).
Each denied acquire logged ``CandleBudgetDenied``; ~**540** denials piled up
while Phase B's 20 qualifier 09:20 fetches returned ``bars=0`` → all cards
``09:20 data missing`` / DEGRADED.

Fix
---
- ``phase_b`` pool: separate SlidingWindowRateLimiter + semaphore. Phase A
  never draws from it. Active 09:25:03–09:29 IST (and force Phase B).
- ``canary`` pool: tiny dedicated budget so morning REST canaries never steal
  Phase A/B slots.
- ``phase_a`` / default: shared process limiter (unchanged for other jobs).
- Per-phase per-minute usage logs (granted / denied).

Limits (env-overridable via ``open_low_high.config`` / settings):
  Phase B: OLH_PHASE_B_RL_PER_SEC (default 4), PER_MIN (80), SEMAPHORE (4)
  Canary:  OLH_CANARY_RL_PER_SEC (1), PER_MIN (20), SEMAPHORE (1)
"""
from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, Iterator, Optional, Tuple

import pytz

from backend.services.upstox_rate_limiter import SlidingWindowRateLimiter

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

POOL_SHARED = "shared"
POOL_PHASE_A = "phase_a"
POOL_PHASE_B = "phase_b"
POOL_CANARY = "canary"

_tls = threading.local()
_pools: Dict[str, SlidingWindowRateLimiter] = {}
_semaphores: Dict[str, threading.BoundedSemaphore] = {}
_init_lock = threading.Lock()

# Metrics: pool -> minute_key -> {granted, denied}
_usage: Dict[str, Dict[str, Dict[str, int]]] = {}
_usage_lock = threading.Lock()
_last_usage_log_minute: Dict[str, str] = {}


def _cfg_int(name: str, default: int) -> int:
    try:
        from backend.services.open_low_high import config as cfg

        return int(getattr(cfg, name, default))
    except Exception:
        return default


def _build_pool(name: str) -> Tuple[SlidingWindowRateLimiter, threading.BoundedSemaphore]:
    if name == POOL_PHASE_B:
        per_sec = max(1, _cfg_int("PHASE_B_RL_PER_SEC", 4))
        per_min = max(per_sec, _cfg_int("PHASE_B_RL_PER_MIN", 80))
        sem_n = max(1, _cfg_int("PHASE_B_RL_SEMAPHORE", 4))
        max_wait = float(_cfg_int("PHASE_B_RL_MAX_WAIT_SEC", 45))
    elif name == POOL_CANARY:
        per_sec = max(1, _cfg_int("CANARY_RL_PER_SEC", 1))
        per_min = max(per_sec, _cfg_int("CANARY_RL_PER_MIN", 20))
        sem_n = max(1, _cfg_int("CANARY_RL_SEMAPHORE", 1))
        max_wait = float(_cfg_int("CANARY_RL_MAX_WAIT_SEC", 15))
    else:
        # phase_a uses shared limiter via acquire_candle_slot — no local pool.
        raise ValueError(f"no dedicated pool for {name}")
    lim = SlidingWindowRateLimiter(
        [(per_sec, 1.0), (per_min, 60.0)],
        min_interval=1.0 / float(per_sec),
    )
    # Stash max_wait on limiter for acquire helpers
    lim._olh_max_wait = max_wait  # type: ignore[attr-defined]
    return lim, threading.BoundedSemaphore(sem_n)


def _ensure_pool(name: str) -> Tuple[SlidingWindowRateLimiter, threading.BoundedSemaphore]:
    with _init_lock:
        if name not in _pools:
            lim, sem = _build_pool(name)
            _pools[name] = lim
            _semaphores[name] = sem
            logger.info(
                "olh candle budget pool=%s per_sec/min ready semaphore=%s",
                name,
                sem._initial_value if hasattr(sem, "_initial_value") else "?",  # noqa: SLF001
            )
        return _pools[name], _semaphores[name]


def current_pool() -> str:
    return str(getattr(_tls, "pool", None) or POOL_SHARED)


@contextmanager
def olh_candle_pool(pool: str) -> Iterator[str]:
    """Mark this thread's candle acquires as ``phase_a`` / ``phase_b`` / ``canary``."""
    prev = getattr(_tls, "pool", None)
    _tls.pool = str(pool or POOL_SHARED)
    try:
        yield _tls.pool
    finally:
        _tls.pool = prev


def _minute_key(now: Optional[datetime] = None) -> str:
    t = now or datetime.now(IST)
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    return t.strftime("%H:%M")


def _record(pool: str, granted: bool) -> None:
    mk = _minute_key()
    with _usage_lock:
        by_min = _usage.setdefault(pool, {})
        row = by_min.setdefault(mk, {"granted": 0, "denied": 0})
        if granted:
            row["granted"] += 1
        else:
            row["denied"] += 1
        # Log once per pool per minute when activity happens.
        last = _last_usage_log_minute.get(pool)
        if last != mk:
            # Flush previous minute if any
            if last and last in by_min:
                prev = by_min[last]
                logger.info(
                    "olh_candle_budget usage pool=%s minute=%s granted=%s denied=%s",
                    pool,
                    last,
                    prev.get("granted", 0),
                    prev.get("denied", 0),
                )
            _last_usage_log_minute[pool] = mk
        # Also log current rolling counts every 25 events in-minute
        total = int(row["granted"]) + int(row["denied"])
        if total > 0 and total % 25 == 0:
            logger.info(
                "olh_candle_budget usage pool=%s minute=%s granted=%s denied=%s",
                pool,
                mk,
                row["granted"],
                row["denied"],
            )


def usage_snapshot() -> Dict[str, Dict[str, Dict[str, int]]]:
    with _usage_lock:
        return {p: {m: dict(v) for m, v in mins.items()} for p, mins in _usage.items()}


def reset_pools_for_tests() -> None:
    """Test helper — clear dedicated limiters and metrics."""
    global _pools, _semaphores, _usage, _last_usage_log_minute
    with _init_lock:
        _pools = {}
        _semaphores = {}
    with _usage_lock:
        _usage = {}
        _last_usage_log_minute = {}
    _tls.pool = None


def release_olh_candle_slot() -> None:
    """Release Phase B / canary semaphore held for an in-flight HTTP call."""
    releasers = getattr(_tls, "sem_releasers", None)
    if not releasers:
        return
    while releasers:
        fn = releasers.pop()
        try:
            fn()
        except Exception:
            logger.debug("olh candle sem release failed", exc_info=True)


def acquire_olh_candle_slot() -> Optional[bool]:
    """Acquire for dedicated OLH pools.

    Returns:
      True / False when this thread is in ``phase_b`` or ``canary`` (handled here).
      None when pool is shared/phase_a — caller should use ``acquire_candle_slot``.

    On True, caller **must** invoke ``release_olh_candle_slot`` after the HTTP
    attempt (success or fail) so the concurrency semaphore is freed.
    """
    pool = current_pool()
    if pool not in (POOL_PHASE_B, POOL_CANARY):
        return None

    lim, sem = _ensure_pool(pool)
    max_wait = float(getattr(lim, "_olh_max_wait", 45.0))
    got_sem = sem.acquire(blocking=True, timeout=max_wait)
    if not got_sem:
        _record(pool, False)
        logger.warning(
            "olh_candle_budget denied pool=%s reason=semaphore",
            pool,
        )
        return False

    granted, waited = lim.acquire(max_wait=max_wait)
    if not granted:
        try:
            sem.release()
        except ValueError:
            pass
        _record(pool, False)
        logger.warning(
            "olh_candle_budget denied pool=%s reason=rate_limit waited=%.2f",
            pool,
            waited,
        )
        return False

    releasers = getattr(_tls, "sem_releasers", None)
    if releasers is None:
        releasers = []
        _tls.sem_releasers = releasers
    releasers.append(sem.release)
    _record(pool, True)
    return True


def note_shared_acquire(granted: bool, *, phase_hint: Optional[str] = None) -> None:
    """Record shared-limiter outcome under phase_a / shared for per-minute logs."""
    if current_pool() in (POOL_PHASE_B, POOL_CANARY):
        return
    pool = phase_hint or (
        POOL_PHASE_A if current_pool() == POOL_PHASE_A else POOL_SHARED
    )
    _record(pool, bool(granted))
