"""HA-VWAP 10m backtest constants."""
from __future__ import annotations

from datetime import date, time

FUTURES_FROM = date(2026, 7, 15)
FUTURES_TO = date(2026, 9, 4)
CASH_FROM = date(2026, 1, 1)
CASH_TO = date(2026, 7, 31)

MARKET_OPEN = time(9, 15)
SIGNAL_FROM = time(9, 45)
SIGNAL_TO = time(12, 45)
FORCE_EXIT_TIME = time(15, 15)
MARKET_CLOSE = time(15, 30)

BAR_MINUTES = 10
MAX_CONCURRENT = 999
TOP_N_BY_VOLUME = 999
SLIPPAGE = 0.0003  # raw 10m close × 1.0003 (fill on actual market, not HA)
TP_PCT = 0.008  # full exit at entry × 1.008

EMA_PERIOD = 20
ST_PERIOD = 10
ST_MULTIPLIER = 3.0
MACD_FAST = 104
MACD_SLOW = 48
MACD_SIGNAL = 36
CANDLE_DAYS_BACK = 15
HISTORY_SESSIONS = 8
SIGNALS_CSV = "signals.csv"

ARTIFACT_COMBINED = "ha_vwap_combined.json"
PUBLIC_ARTIFACT = "ha_vwap_data.json"
