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
PHASE_B_BUFFER_SEC = _env_int("OLH_PHASE_B_BUFFER_SEC", 3)  # 09:25:00 + buffer → 09:25:03
PHASE_A_START = time(9, 20, max(0, min(59, PHASE_A_BUFFER_SEC)))
PHASE_B_START = time(9, 25, max(0, min(59, PHASE_B_BUFFER_SEC)))
PHASE_B_HARD_STOP = time(9, 29, 0)

# FREEZE GATE: require coverage of symbols with usable 09:15 equity bars + Nifty present.
FREEZE_COVERAGE_MIN = _env_float("OLH_FREEZE_COVERAGE_MIN", 0.90)
# Legacy empty-Phase-A incomplete helper (miss rate among scanned).
MISSING_DATA_RATE_THRESHOLD = _env_float("OLH_MISSING_DATA_RATE_THRESHOLD", 0.35)

# WS trust: first 1m of a 5m bucket must start within this many seconds of bucket open.
WS_FIRST_TICK_TRUST_SEC = _env_int("OLH_WS_FIRST_TICK_TRUST_SEC", 5)
# One WS "try" = connect+subscribe + ≥N 1m bars in T seconds for the symbol.
WS_TRY_MIN_1M_BARS = _env_int("OLH_WS_TRY_MIN_1M_BARS", 1)
WS_TRY_WAIT_SEC = _env_float("OLH_WS_TRY_WAIT_SEC", 2.0)
WS_TRY_COUNT = _env_int("OLH_WS_TRY_COUNT", 2)

# 09:05 canary: token ok, Nifty tick, ≥ this share of equities with ≥1 WS 1m (post 09:15)
# Pre-open uses provider registration; after 09:15 uses bar share.
CANARY_EQUITY_SHARE = _env_float("OLH_CANARY_EQUITY_SHARE", 0.90)
WS_WARM_MIN_EQUITY_SHARE = _env_float("OLH_WS_WARM_MIN_EQUITY_SHARE", 0.15)

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
