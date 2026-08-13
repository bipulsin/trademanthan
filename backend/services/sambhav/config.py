"""Sambhav V1 constants — NIFTY 50, 10m bars, 30m horizon, NSE session."""

from __future__ import annotations

import os
from datetime import date, time
from pathlib import Path

import pytz

IST = pytz.timezone("Asia/Kolkata")

INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
INSTRUMENT_DISPLAY = "NIFTY 50"
SYMBOL = "NIFTY"

TF_MINUTES = 10
HORIZON_MINUTES = 30
HORIZON_BARS = HORIZON_MINUTES // TF_MINUTES  # 3 × 10m bars

SESSION_START = time(9, 15)
SESSION_END = time(15, 30)

# V1 historical dataset is native Upstox V3 10-minute candles.
# 1-minute data may be added in a future Sambhav V2 feature-enhancement study.
EXPECTED_1M_PER_10M = 10  # unused by V1 importer; retained for optional V2 1m study
EXPECTED_10M_PER_SESSION = 38  # 09:15, 09:25, …, 15:25 IST (inspected Upstox V3)

# --- Sambhav V1 dataset definition ---
DATASET_VERSION_V1 = "sambhav_dataset_v1_20260813"
FEATURES_VERSION_V1 = "sambhav_features_v1"  # reserved; features not generated in this phase
MODEL_VERSION_XGB_V1 = "sambhav_xgb_v1"  # reserved; do not train in this phase

SESSION_TYPE_REGULAR = "REGULAR"
SESSION_TYPE_EXCLUDED_HOLIDAY = "EXCLUDED_HOLIDAY"
SESSION_TYPE_EXCLUDED_MUHURAT = "EXCLUDED_MUHURAT"
SESSION_TYPE_EXCLUDED_SPECIAL = "EXCLUDED_SPECIAL"
SESSION_TYPE_UNKNOWN = "UNKNOWN"

# Preserved in sambhav_10m_candles but excluded from V1 ML analysis.
V1_EXCLUDED_SPECIAL_DATES = frozenset(
    {
        date(2024, 3, 2),
        date(2024, 5, 18),
    }
)
V1_EXCLUDED_MUHURAT_DATES = frozenset(
    {
        date(2025, 10, 21),
    }
)

STATUS_RESEARCH = "RESEARCH"
STATUS_VALIDATED = "VALIDATED"
STATUS_LIVE = "LIVE"

PRED_PENDING = "PENDING"
PRED_RESOLVED = "RESOLVED"

ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"

# Upstox V3 minutes 1–15: max ~1 month per historical-candle request.
IMPORT_CHUNK_DAYS = int(os.getenv("SAMBHAV_HISTORICAL_CHUNK_DAYS", "31"))
# Unused by V1 (kept so the dormant 1m importer stays conservative if invoked).
IMPORT_CHUNK_DAYS_1M = 25

# V3 Historical Candle API interval (unit/value). Do not download 1-minute history for V1.
HISTORICAL_INTERVAL = "minutes/10"
HISTORICAL_SOURCE = "upstox_v3_10m"

# Conservative throttle — one shared delay, never hard-coded at call sites.
# Suggested 2–5 seconds. Do not attempt the maximum Upstox rate.
HISTORICAL_REQUEST_DELAY_SECONDS = float(
    os.getenv("SAMBHAV_HISTORICAL_REQUEST_DELAY_SECONDS", "2")
)
HISTORICAL_MAX_RETRIES = int(os.getenv("SAMBHAV_HISTORICAL_MAX_RETRIES", "5"))
HISTORICAL_BACKOFF_CAP_SECONDS = float(
    os.getenv("SAMBHAV_HISTORICAL_BACKOFF_CAP_SECONDS", "60")
)
HISTORICAL_REQUEST_TIMEOUT_SECONDS = float(
    os.getenv("SAMBHAV_HISTORICAL_REQUEST_TIMEOUT_SECONDS", "30")
)

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
