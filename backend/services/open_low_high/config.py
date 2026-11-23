"""Open Low High live pipeline knobs (env-overridable)."""
from __future__ import annotations

import os
from datetime import time


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


# --- Phase windows (buffer after candle close) ---
PHASE_A_BUFFER_SEC = _env_int("OLH_PHASE_A_BUFFER_SEC", 10)  # 09:20:00 + buffer → 09:20:10
# Give Upstox intraday time to publish the closed 09:20 5m (was 3s; too tight + REST thrash).
PHASE_B_BUFFER_SEC = _env_int("OLH_PHASE_B_BUFFER_SEC", 15)  # 09:25:00 + buffer → 09:25:15
PHASE_A_START = time(9, 20, max(0, min(59, PHASE_A_BUFFER_SEC)))
PHASE_B_START = time(9, 25, max(0, min(59, PHASE_B_BUFFER_SEC)))
PHASE_B_HARD_STOP = time(9, 29, 0)
# Hard stop for Phase A refill — must finish before Phase B owns REST.
PHASE_A_REFILL_HARD_STOP = time(9, 24, 50)

# FREEZE GATE: require coverage of symbols with usable 09:15 equity bars + Nifty present.
FREEZE_COVERAGE_MIN = _env_float("OLH_FREEZE_COVERAGE_MIN", 0.90)

# --- Dedicated REST budgets (Phase B / canary never share Phase A slots) ---
# 2026-11-23: shared limiter → 540 CandleBudgetDenied while Phase B starved.
PHASE_B_RL_PER_SEC = _env_int("OLH_PHASE_B_RL_PER_SEC", 4)
PHASE_B_RL_PER_MIN = _env_int("OLH_PHASE_B_RL_PER_MIN", 80)
PHASE_B_RL_SEMAPHORE = _env_int("OLH_PHASE_B_RL_SEMAPHORE", 4)
PHASE_B_RL_MAX_WAIT_SEC = _env_int("OLH_PHASE_B_RL_MAX_WAIT_SEC", 45)
CANARY_RL_PER_SEC = _env_int("OLH_CANARY_RL_PER_SEC", 1)
CANARY_RL_PER_MIN = _env_int("OLH_CANARY_RL_PER_MIN", 20)
CANARY_RL_SEMAPHORE = _env_int("OLH_CANARY_RL_SEMAPHORE", 1)
CANARY_RL_MAX_WAIT_SEC = _env_int("OLH_CANARY_RL_MAX_WAIT_SEC", 15)
# Legacy empty-Phase-A incomplete helper (miss rate among scanned).
MISSING_DATA_RATE_THRESHOLD = _env_float("OLH_MISSING_DATA_RATE_THRESHOLD", 0.35)

# WS trust: first 1m of a 5m bucket must start within this many seconds of bucket open.
# (Legacy soft check; completeness of all five 1m bars is authoritative — see candle_builder.)
WS_FIRST_TICK_TRUST_SEC = _env_int("OLH_WS_FIRST_TICK_TRUST_SEC", 5)
# One WS "try" = connect+subscribe + ≥N 1m bars in T seconds for the symbol.
WS_TRY_MIN_1M_BARS = _env_int("OLH_WS_TRY_MIN_1M_BARS", 1)
WS_TRY_WAIT_SEC = _env_float("OLH_WS_TRY_WAIT_SEC", 2.0)
WS_TRY_COUNT = _env_int("OLH_WS_TRY_COUNT", 2)

# Official I1 / market OHLC vs tick-built open cross-check (percent of price).
OFFICIAL_OPEN_TOLERANCE_PCT = _env_float("OLH_OFFICIAL_OPEN_TOLERANCE_PCT", 0.05)
# WS vs REST 09:15 compare sample size (equities) + always include Nifty.
WS_REST_0915_COMPARE_N = _env_int("OLH_WS_REST_0915_COMPARE_N", 10)

# 09:05 canary: token ok, Nifty tick, ≥ this share of equities with ≥1 WS 1m (post 09:15)
# Pre-open uses provider registration; after 09:15 uses bar share.
CANARY_EQUITY_SHARE = _env_float("OLH_CANARY_EQUITY_SHARE", 0.90)
WS_WARM_MIN_EQUITY_SHARE = _env_float("OLH_WS_WARM_MIN_EQUITY_SHARE", 0.15)
# 09:05 / 09:15 canary: fail loudly if EQ or INDEX ticks in last 60s are 0.
CANARY_REQUIRE_RECENT_TICKS = _env_bool("OLH_CANARY_REQUIRE_RECENT_TICKS", True)
CANARY_RECENT_TICK_WINDOW_SEC = _env_int("OLH_CANARY_RECENT_TICK_WINDOW_SEC", 60)

# REST concurrency / pacing inside Phase A/B pools.
REST_MAX_WORKERS = _env_int("OLH_REST_MAX_WORKERS", 4)
REST_PACING_SLEEP_SEC = _env_float("OLH_REST_PACING_SLEEP_SEC", 0.05)

# PDC EOD time (hour:minute IST) — not necessarily 15:30.
PDC_EOD_HOUR = _env_int("OLH_PDC_EOD_HOUR", 16)
PDC_EOD_MINUTE = _env_int("OLH_PDC_EOD_MINUTE", 5)

# Alerts (Telegram via TradeWithCTO channel when configured).
OLH_ALERTS_ENABLED = _env_bool("OLH_ALERTS_ENABLED", True)

# Deploy / restart forbidden window (IST).
DEPLOY_GUARD_START = time(8, 45, 0)
DEPLOY_GUARD_END = time(9, 35, 0)
DEPLOY_GUARD_FORCE = _env_bool("OLH_DEPLOY_GUARD_FORCE", False)  # allow override

# Persist OHL_Log outside ephemeral container FS when set / mounted.
OLH_LOG_HOST_DIR = (os.getenv("OLH_LOG_HOST_DIR") or "").strip()

# Host-persisted OHL logs (survive container rebuild); empty → try known mount paths.
OLH_LOG_HOST_DIR = (os.getenv("OLH_LOG_HOST_DIR") or "").strip()
OLH_FETCH_ATTEMPTS_ENABLED = _env_bool("OLH_FETCH_ATTEMPTS_ENABLED", True)
