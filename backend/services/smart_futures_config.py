"""
Central configuration for Smart Futures CMS pipeline (intraday).

All tunables live here — imported by picker, exit helpers, and API enrichment.
"""
from __future__ import annotations

from datetime import time
from typing import Dict, List, Tuple

# --- ADX (regime filter uses ADX_LENGTH; slow reference available for diagnostics) ---
ADX_LENGTH: int = 9
ADX_SLOW_LENGTH: int = 14
ADX_THRESHOLD: float = 20.0

# --- Time-of-day (IST bar time) ---
TIME_FILTER_ENABLED: bool = True
# Single continuous session on IST *bar end* time from the last 5m candle (no lunch gap).
TRADE_WINDOWS: List[Tuple[time, time]] = [
    (time(9, 30), time(15, 0)),
]

# --- CMS tier thresholds (applied to final_cms = Layer2 × vol/trend multipliers) ---
TIER1_THRESHOLD: float = 0.75
TIER2_THRESHOLD: float = 0.60
NEUTRAL_BAND: float = 0.60

# Sector index must agree with side by at least this magnitude
SECTOR_ALIGN_MIN: float = 0.05

# --- CMS weights (must sum to 1.0 when OI_IN_CMS_ENABLED is False) ---
CMS_WEIGHTS: Dict[str, float] = {
    "obv_slope": 0.25,
    "volume_surge": 0.20,
    "vwap_dev": 0.20,
    "ha_trend": 0.15,
    "renko_mom": 0.10,
    "ema_slope": 0.10,
}

# When True, oi_score uses 0.10 from volume_surge (see OI_CMS_WEIGHTS).
OI_IN_CMS_ENABLED: bool = False

OI_CMS_WEIGHTS: Dict[str, float] = {
    "obv_slope": 0.25,
    "volume_surge": 0.10,
    "vwap_dev": 0.20,
    "ha_trend": 0.15,
    "renko_mom": 0.10,
    "ema_slope": 0.10,
    "oi_score": 0.10,
}

# --- Risk / sizing ---
CAPITAL: float = 800_000.0
RISK_PCT: float = 1.0
MAX_OPEN_POSITIONS: int = 3

# Picker / futures_backtester: minimum LONG_BUILDUP slots when the budget (top_n) allows.
MIN_LONG_BUILDUP_SELECTION: int = 3

# Picker: per scan budget (same formula as backtester); long/short caps from buildup_selection_long_short_caps().
SMART_FUTURES_PICK_SELECTION_TOP_N: int = 5
# Hard cap of qualifying rows persisted per scan run.
SMART_FUTURES_MAX_PUBLISH_PER_SCAN: int = 5


def buildup_selection_long_short_caps(top_n: int) -> Tuple[int, int]:
    """
    Return (long_cap, short_cap) for LONG_BUILDUP vs SHORT_BUILDUP ranking.

    Long side: at least ``max(MIN_LONG_BUILDUP_SELECTION, top_n // 3)`` when possible, but never
    more than ``top_n`` total budget. Short side: ``top_n // 2``, trimmed so long_cap + short_cap <= top_n.
    """
    tn = max(0, int(top_n))
    if tn <= 0:
        return 0, 0
    n_long_target = max(int(MIN_LONG_BUILDUP_SELECTION), tn // 3)
    n_long = min(n_long_target, tn)
    n_short = min(tn // 2, max(0, tn - n_long))
    return n_long, n_short


# GAP5: floor(raw_lots * tier_sizing_mult); TIER1 uses higher mult
TIER1_SIZING_MULT: float = 2.0
TIER2_SIZING_MULT: float = 1.0

# --- Trailing stop (evaluated on /daily for bought rows) ---
TRAILING_STOP_ENABLED: bool = True
TRAIL_STAGE1_ATR_MULT: float = 1.5
TRAIL_STAGE2_ATR_MULT: float = 2.5
TRAIL_LOCK_ATR_MULT: float = 1.0

# --- Re-entry (per symbol per session; pairs with job exclusion rules) ---
REENTRY_ENABLED: bool = True
REENTRY_COOLDOWN_MINUTES: int = 10
REENTRY_NEUTRAL_RESET_THRESHOLD: float = 0.3
REENTRY_MAX_PER_SESSION: int = 1

# --- OI gate ---
OI_GATE_ENABLED: bool = True
OI_BLOCK_ON_CONFLICT: bool = True
OI_FETCH_INTERVAL_SECONDS: int = 60

# --- Legacy alias for diagnostics logs (prefer TIER2_THRESHOLD for entries) ---
CMS_FINAL_ENTRY_THRESHOLD: float = TIER2_THRESHOLD

# When True, only symbols in premarket Top N (see PREMKET_TOP_N in config) OR top OI-mover ranks qualify for CMS signals.
CMS_PRIORITY_FILTER_ENABLED: bool = False
CMS_PRIORITY_OI_MOVER_MAX_RANK: int = 20


def cms_weights_active() -> Dict[str, float]:
    return OI_CMS_WEIGHTS if OI_IN_CMS_ENABLED else CMS_WEIGHTS


def assert_cms_weights_sum(w: Dict[str, float]) -> None:
    s = sum(float(v) for v in w.values())
    if abs(s - 1.0) > 1e-6:
        raise ValueError(f"CMS weights must sum to 1.0, got {s}")
