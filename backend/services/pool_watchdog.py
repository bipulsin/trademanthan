"""
Pool / DB connectivity watchdog — exits the process after sustained stress so Docker
restarts the app container with a fresh SQLAlchemy connection pool.

Configure via env:
  POOL_WATCHDOG_ENABLED=1
  POOL_WATCHDOG_INTERVAL_SEC=60
  POOL_WATCHDOG_STRESS_MINUTES=3
  POOL_WATCHDOG_DB_FAIL_MINUTES=3
  POOL_WATCHDOG_COOLDOWN_MINUTES=30
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

_POOL_WATCHDOG_ENABLED = os.getenv("POOL_WATCHDOG_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
_POOL_WATCHDOG_STRESS_MINUTES = max(1, int(os.getenv("POOL_WATCHDOG_STRESS_MINUTES", "3")))
_POOL_WATCHDOG_DB_FAIL_MINUTES = max(1, int(os.getenv("POOL_WATCHDOG_DB_FAIL_MINUTES", "3")))
_POOL_WATCHDOG_COOLDOWN_MINUTES = max(5, int(os.getenv("POOL_WATCHDOG_COOLDOWN_MINUTES", "30")))

_stress_since_mono: Optional[float] = None
_db_fail_since_mono: Optional[float] = None
_last_self_heal_mono: float = 0.0


@dataclass(frozen=True)
class WatchdogDecision:
    action: str  # "none" | "self_heal"
    reason: str = ""
    stressed_for_sec: float = 0.0
    db_fail_for_sec: float = 0.0


def _enabled() -> bool:
    return _POOL_WATCHDOG_ENABLED


def _cooldown_active(now_mono: float) -> bool:
    return (now_mono - _last_self_heal_mono) < (_POOL_WATCHDOG_COOLDOWN_MINUTES * 60)


def _db_probe_ok() -> bool:
    from backend.database import SessionLocal

    if SessionLocal is None:
        return False
    db = SessionLocal()
    try:
        db.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.warning("pool_watchdog: DB probe failed: %s", exc)
        return False
    finally:
        try:
            db.close()
        except Exception:
            pass


def evaluate_pool_watchdog(
    *,
    pool_stressed: bool,
    db_ok: bool,
    now_mono: float,
    stress_since_mono: Optional[float],
    db_fail_since_mono: Optional[float],
    last_self_heal_mono: float,
    enabled: bool = True,
    stress_minutes: int = _POOL_WATCHDOG_STRESS_MINUTES,
    db_fail_minutes: int = _POOL_WATCHDOG_DB_FAIL_MINUTES,
    cooldown_minutes: int = _POOL_WATCHDOG_COOLDOWN_MINUTES,
) -> WatchdogDecision:
    """Pure state machine for tests."""
    if not enabled:
        return WatchdogDecision(action="none")

    if (now_mono - last_self_heal_mono) < (cooldown_minutes * 60):
        return WatchdogDecision(action="none")

    stress_since = stress_since_mono
    db_fail_since = db_fail_since_mono

    if pool_stressed:
        if stress_since is None:
            stress_since = now_mono
    else:
        stress_since = None

    if not db_ok:
        if db_fail_since is None:
            db_fail_since = now_mono
    else:
        db_fail_since = None

    stressed_for = (now_mono - stress_since) if stress_since is not None else 0.0
    db_fail_for = (now_mono - db_fail_since) if db_fail_since is not None else 0.0

    if stressed_for >= stress_minutes * 60:
        return WatchdogDecision(
            action="self_heal",
            reason="pool_stressed",
            stressed_for_sec=stressed_for,
            db_fail_for_sec=db_fail_for,
        )
    if db_fail_for >= db_fail_minutes * 60:
        return WatchdogDecision(
            action="self_heal",
            reason="db_unreachable",
            stressed_for_sec=stressed_for,
            db_fail_for_sec=db_fail_for,
        )
    return WatchdogDecision(action="none", stressed_for_sec=stressed_for, db_fail_for_sec=db_fail_for)


def _apply_state_from_signals(pool_stressed: bool, db_ok: bool, now_mono: float) -> WatchdogDecision:
    global _stress_since_mono, _db_fail_since_mono

    decision = evaluate_pool_watchdog(
        pool_stressed=pool_stressed,
        db_ok=db_ok,
        now_mono=now_mono,
        stress_since_mono=_stress_since_mono,
        db_fail_since_mono=_db_fail_since_mono,
        last_self_heal_mono=_last_self_heal_mono,
        enabled=_enabled(),
    )

    if pool_stressed:
        if _stress_since_mono is None:
            _stress_since_mono = now_mono
    else:
        _stress_since_mono = None

    if not db_ok:
        if _db_fail_since_mono is None:
            _db_fail_since_mono = now_mono
    else:
        _db_fail_since_mono = None

    return decision


def _trigger_self_heal(reason: str, decision: WatchdogDecision) -> None:
    global _last_self_heal_mono
    _last_self_heal_mono = time.monotonic()
    logger.critical(
        "POOL_WATCHDOG self-heal: reason=%s stressed_for=%.0fs db_fail_for=%.0fs "
        "— exiting process so Docker restarts app with fresh DB pool",
        reason,
        decision.stressed_for_sec,
        decision.db_fail_for_sec,
    )
    time.sleep(1.5)
    os._exit(1)


def run_pool_watchdog_tick() -> None:
    """Called every minute from smart_future_algo scheduler."""
    if not _enabled():
        return

    from backend.database import get_db_pool_stats, log_db_pool_pressure

    now_mono = time.monotonic()
    pool = log_db_pool_pressure(logger, "watchdog")
    pool_stressed = bool(pool.get("stressed"))
    db_ok = _db_probe_ok()

    decision = _apply_state_from_signals(pool_stressed, db_ok, now_mono)
    if decision.action == "self_heal":
        _trigger_self_heal(decision.reason, decision)
        return

    if pool_stressed or not db_ok:
        logger.warning(
            "pool_watchdog: monitoring checked_out=%s/%s stressed=%s db_ok=%s "
            "stressed_for=%.0fs db_fail_for=%.0fs (threshold=%sm/%sm)",
            pool.get("checked_out"),
            pool.get("max_capacity"),
            pool_stressed,
            db_ok,
            decision.stressed_for_sec,
            decision.db_fail_for_sec,
            _POOL_WATCHDOG_STRESS_MINUTES,
            _POOL_WATCHDOG_DB_FAIL_MINUTES,
        )
