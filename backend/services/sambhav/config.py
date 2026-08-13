"""Sambhav V1 constants — NIFTY 50, 10m bars, 30m horizon, NSE session."""

from __future__ import annotations

from datetime import time
from pathlib import Path

import pytz

IST = pytz.timezone("Asia/Kolkata")

INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
SYMBOL = "NIFTY"

TF_MINUTES = 10
HORIZON_MINUTES = 30
HORIZON_BARS = HORIZON_MINUTES // TF_MINUTES  # 3 × 10m bars

SESSION_START = time(9, 15)
SESSION_END = time(15, 30)

EXPECTED_1M_PER_10M = 10

STATUS_RESEARCH = "RESEARCH"
STATUS_VALIDATED = "VALIDATED"
STATUS_LIVE = "LIVE"

PRED_PENDING = "PENDING"
PRED_RESOLVED = "RESOLVED"

ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"

IMPORT_CHUNK_DAYS = 25

FEATURE_NAMES: tuple[str, ...] = (
    "ret_1",
    "ret_3",
    "ret_6",
    "log_range",
    "body_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "close_loc",
    "gap_from_prev",
    "ema9_slope",
    "ema21_slope",
    "ema9_vs_ema21",
    "close_vs_ema21",
    "ema_stack",
    "rsi14",
    "roc3",
    "roc6",
    "macd_hist",
    "atr14_pct",
    "realized_vol6",
    "high_low_pct",
    "range_expand",
    "vol_z20",
    "vol_ratio5",
    "dollar_vol_proxy",
    "tod_sin",
    "tod_cos",
    "mins_from_open",
    "mins_to_close",
    "is_open_bucket",
    "is_close_bucket",
)
