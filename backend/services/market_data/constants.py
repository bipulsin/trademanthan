"""Market data service configuration."""
from __future__ import annotations

# Default staleness for 10m curr-month REST warm cycle (+ buffer).
# Stock/next LTP persist on a 30m WS path; readers typically use allow_stale=True.
DEFAULT_LTP_MAX_AGE_SEC = 720
DEFAULT_INDICATOR_MAX_AGE_SEC = 900

BATCH_QUOTE_CHUNK = 450
CANDLE_FETCH_WORKERS = 10
CANDLE_INTERVAL = "minutes/5"
CANDLE_DAYS_BACK = 3

DATA_SOURCE_REST = "upstox_rest"
DATA_SOURCE_WS = "upstox_ws"
DATA_SOURCE_DB = "arbitrage_master"

REFRESH_OK = "ok"
REFRESH_PARTIAL = "partial"
REFRESH_FAILED = "failed"
