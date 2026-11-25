"""Dedicated OLH candle REST budgets (Phase A / Phase B / canary).

Why (2026-11-23 forensics)
-------------------------
Morning Phase A refill kept probing the full universe for missing 09:15 bars
through 09:25–09:29 and competed with Phase B on the **shared** process-wide
Upstox candle limiter (``UPSTOX_CANDLE_RL_*`` ≈ 5/s, 120/min, 1500/30m).
Each denied acquire logged ``CandleBudgetDenied``; ~**540** denials piled up
while Phase B's 20 qualifier 09:20 fetches returned ``bars=0`` → all cards
``09:20 data missing`` / DEGRADED.

Why (2026-11-25 forensics)
-------------------------
Phase A still drew from the **shared** pool. By 09:20 the 30-min shared window
was empty (PDC + FO/warm burned ~120/min from 09:00). ``phase_a`` logged
``granted=0 denied=tens`` while the dedicated **canary** pool still fetched
real 09:15 bars. Fix: dedicated Phase A pool + shared yields to OLH 09:10–09:30
+ auto-fallback to reserved lane when shared denies under Phase A TLS.

Fix
---
- ``phase_a`` / ``phase_b`` / ``canary`` pools: separate SlidingWindowRateLimiter
  + semaphore. Never draw from shared 30-min window.
- Shared / non-OLH callers yield during OLH priority window (see
  ``upstox_rate_limiter``).
- If shared denies while TLS pool is ``phase_a``, escalate to reserved
  ``phase_a`` lane immediately (do not wait for hard stop).
- Per-phase per-minute usage logs (granted / denied).

Limits (env-overridable via ``open_low_high.config`` / settings):
  Phase A: OLH_PHASE_A_RL_PER_SEC (5), PER_MIN (120), SEMAPHORE (6)
  Phase B: OLH_PHASE_B_RL_PER_SEC (4), PER_MIN (80), SEMAPHORE (4)
  Canary:  OLH_CANARY_RL_PER_SEC (1), PER_MIN (20), SEMAPHORE (1)
"""
from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Iterator, Optional, Tuple

import pytz

from backend.services.upstox_rate_limiter import SlidingWindowRateLimiter

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

POOL_SHARED = "shared"
POOL_PHASE_A = "phase_a"
POOL_PHASE_B = "phase_b"
POOL_CANARY = "canary"
DEDICATED_POOLS = frozenset({POOL_PHASE_A, POOL_PHASE_B, POOL_CANARY})

_tls = threading.local()
_pools: Dict[str, SlidingWindowRateLimiter] = {}
_semaphores: Dict[str, threading.BoundedSemaphore] = {}
_init_lock = threading.Lock()

# Metrics: pool -> minute_key -> {granted, denied}
_usage: Dict[str, Dict[str, Dict[str, int]]] = {}
_usage_lock = threading.Lock()
_last_usage_log_minute: Dict[str, str] = {}

# Fallback counters (shared deny → reserved phase_a)
_fallback_to_reserved = 0


def _cfg_int(name: str, default: int) -> int:
    try:
        from backend.services.open_low_high import config as cfg

        return int(getattr(cfg, name, default))
    except Exception:
        return default


def _build_pool(name: str) -> Tuple[SlidingWindowRateLimiter, threading.BoundedSemaphore]:
    if name == POOL_PHASE_A:
        per_sec = max(1, _cfg_int("PHASE_A_RL_PER_SEC", 5))
        per_min = max(per_sec, _cfg_int("PHASE_A_RL_PER_MIN", 120))
        sem_n = max(1, _cfg_int("PHASE_A_RL_SEMAPHORE", 6))
        max_wait = float(_cfg_int("PHASE_A_RL_MAX_WAIT_SEC", 60))
    elif name == POOL_PHASE_B:
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
        raise ValueError(f"no dedicated pool for {name}")
    lim = SlidingWindowRateLimiter(
        [(per_sec, 1.0), (per_min, 60.0)],
        min_interval=1.0 / float(per_sec),
    )
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


def reserved_budgets_ready() -> Dict[str, Any]:
    """Readiness helper — ensure Phase A/B/canary pools can be constructed."""
    out: Dict[str, Any] = {"ok": True, "pools": {}}
    for name in (POOL_PHASE_A, POOL_PHASE_B, POOL_CANARY):
        try:
            lim, sem = _ensure_pool(name)
            out["pools"][name] = {
                "ok": True,
                "semaphore": getattr(sem, "_initial_value", None),
                "max_wait": getattr(lim, "_olh_max_wait", None),
            }
        except Exception as exc:
            out["ok"] = False
            out["pools"][name] = {"ok": False, "error": str(exc)[:120]}
    return out


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
        last = _last_usage_log_minute.get(pool)
        if last != mk:
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
    global _pools, _semaphores, _usage, _last_usage_log_minute, _fallback_to_reserved
    with _init_lock:
        _pools = {}
        _semaphores = {}
    with _usage_lock:
        _usage = {}
        _last_usage_log_minute = {}
    _fallback_to_reserved = 0
    _tls.pool = None
    if hasattr(_tls, "sem_releasers"):
        _tls.sem_releasers = []
    if hasattr(_tls, "force_reserved"):
        _tls.force_reserved = False


def release_olh_candle_slot() -> None:
    """Release Phase A / B / canary semaphore held for an in-flight HTTP call."""
    releasers = getattr(_tls, "sem_releasers", None)
    if not releasers:
        return
    while releasers:
        fn = releasers.pop()
        try:
            fn()
        except Exception:
            logger.debug("olh candle sem release failed", exc_info=True)


def _acquire_dedicated(pool: str) -> bool:
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


def acquire_olh_candle_slot() -> Optional[bool]:
    """Acquire for dedicated OLH pools.

    Returns:
      True / False when this thread is in ``phase_a`` / ``phase_b`` / ``canary``.
      None when pool is shared — caller should use ``acquire_candle_slot``.

    On True, caller **must** invoke ``release_olh_candle_slot`` after the HTTP
    attempt (success or fail) so the concurrency semaphore is freed.
    """
    pool = current_pool()
    if pool not in DEDICATED_POOLS:
        # Explicit force-reserved (e.g. after shared deny under Phase A TLS).
        if getattr(_tls, "force_reserved", False):
            pool = POOL_PHASE_A
        else:
            return None
    return _acquire_dedicated(pool)


def fallback_phase_a_reserved_after_shared_deny() -> bool:
    """Shared denied under Phase A → immediately take reserved Phase A lane.

    Returns True if reserved slot granted (caller must release_olh_candle_slot).
    """
    global _fallback_to_reserved
    pool = current_pool()
    if pool not in (POOL_PHASE_A, POOL_SHARED):
        return False
    # Prefer phase_a TLS; if somehow on shared during phase_a window, still escalate.
    _tls.force_reserved = True
    try:
        ok = _acquire_dedicated(POOL_PHASE_A)
    finally:
        _tls.force_reserved = False
    if ok:
        _fallback_to_reserved += 1
        logger.warning(
            "olh_candle_budget fallback shared→reserved pool=phase_a count=%s",
            _fallback_to_reserved,
        )
        try:
            from backend.services.open_low_high.ohl_log import ohl_log

            ohl_log(
                "success",
                action="candle_budget_fallback_reserved",
                detail=f"shared_denied→phase_a_reserved count={_fallback_to_reserved}",
            )
        except Exception:
            pass
    return ok


def note_shared_acquire(granted: bool, *, phase_hint: Optional[str] = None) -> None:
    """Record shared-limiter outcome under phase_a / shared for per-minute logs."""
    if current_pool() in DEDICATED_POOLS:
        return
    pool = phase_hint or (
        POOL_PHASE_A if current_pool() == POOL_PHASE_A else POOL_SHARED
    )
    _record(pool, bool(granted))
