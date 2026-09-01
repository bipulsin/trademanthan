"""Trap-CE backtest constants."""
from __future__ import annotations

from datetime import time

RISK_CAP_INR = 3000.0
FORCE_EXIT_TIME = time(15, 15)
MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)
BE_R = 1.0
TRAIL_ARM_R = 1.5
EMA_TRAIL_PERIOD = 10
BAR_MINUTES = 10
DEFAULT_CSV = "data/trap_ce/Backtest_Intraday_Trap_-_CE.csv"
SKIP_RISK_CAP = "skipped — risk cap"
SKIP_NO_LOT = "skipped — no lot size"
SKIP_NO_BARS = "skipped — no 10m bars"
SKIP_NO_TRIGGER = "skipped — trigger bar missing"
SKIP_NO_ENTRY = "skipped — next bar missing"
SKIP_NON_POSITIVE_R = "skipped — non-positive R"
SKIP_NO_FUT = "skipped — futures key missing"
SKIP_SHORT = "skipped — short not supported"
