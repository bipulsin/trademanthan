"""Breakfast Strategy — constants and date range."""
from __future__ import annotations

from datetime import date

DATE_FROM = date(2026, 7, 29)
DATE_TO = date(2026, 8, 28)

SLIPPAGE_PCT = 0.0003  # 0.03%
SL_PCT = 0.01
TP_PCT = 0.01
STOCK_MOVE_CAP_PCT = 4.0
SECTORS_TO_PICK = 1  # topmost (long) or bottommost (short) sector only
STOCKS_PER_SECTOR = 2
MAX_TRADES_PER_DAY = SECTORS_TO_PICK * STOCKS_PER_SECTOR

FIRST_BAR_END = (9, 20)  # 9:15–9:20 IST 5m bar end (signal / ranking bar)
ANCHOR_BAR_TIME = (9, 15)  # 9:15 stamp — anchor TP/SL/entry close when present
SIGNAL_BAR_TIME = (9, 20)  # 9:20 stamp — stock rank vs prev close
MONITOR_FROM = (9, 25)   # default when entry is on 9:20 bar close
MONITOR_FROM_AFTER_915 = (9, 20)  # when anchor is 9:15 bar close
TIME_EXIT = (10, 15)     # force exit at 10:15 5m bar close if TP/SL not hit
EOD_EXIT = (15, 15)      # legacy label only (unused for exit sim)

PNL_CAP_INR = 5000.0     # optional intraday profit lock (toggle on UI)

CANDLE_INTERVAL = "minutes/5"
CANDLE_DAYS_BACK = 40  # span for Jul–Aug window + buffer
FETCH_THROTTLE_SEC = 0.12

ARTIFACT_NAME = "breakfast_strategy_backtest.json"
OOS_SPOT_ARTIFACT_NAME = "breakfast_strategy_oos_spot_jun2026.json"
HISTORY_ARTIFACT_NAME = "breakfast_strategy_history.json"
