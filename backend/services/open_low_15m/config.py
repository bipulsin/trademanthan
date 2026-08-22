"""Open-Low 15m strategy backtest constants."""
from __future__ import annotations

from datetime import date, time

DATE_FROM = date(2026, 7, 22)
DATE_TO = date(2026, 8, 21)

EXCLUDED_SYMBOLS = frozenset({"EXIDEIND", "NUVAMA", "SAMMAANCAP"})

OPEN_LOW_TOL_PCT = 0.05  # open ≈ low within 0.05%
MAX_GAP_PCT = 2.0
ATR_LEN = 14
ATR5_LEN = 5
RANGE_ATR_MULT = 2.0  # "too big" candle

RISK_INR_MIN = 2000.0
RISK_INR_MAX = 10000.0

TP_R_LEVELS = {
    "TP1": 1.0,
    "TP2": 1.5,
    "TP3": 2.0,
    "TP4": 3.0,
}

TRAIL_STEP_R = 1.5  # profit step
TRAIL_MOVE_R = 1.0  # SL move per step

FORCE_EXIT_TIME = time(15, 15)
MARKET_OPEN = time(9, 15)

EMA_FAST = 5
EMA_SLOW = 10
ST_PERIOD = 10
ST_MULT = 3.0

ARTIFACT_NAME = "open_low_15m_backtest.json"
