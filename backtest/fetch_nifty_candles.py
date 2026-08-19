# TEMP: Upstox data fetch for backtest — not for live trading.
"""Fetch Nifty 50 15-minute candles for HA Momentum VWAP filter."""
from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.fetch_candles import BACKTEST_FROM, BACKTEST_TO, _setup_log, fetch_symbol, load_cached

NIFTY_KEY = "NSE_INDEX|Nifty 50"
NIFTY_SYMBOL = "NIFTY50"
logger = logging.getLogger("ha_nifty")


def main() -> None:
    _setup_log()
    from_d = date.fromisoformat(BACKTEST_FROM)
    to_d = date.fromisoformat(BACKTEST_TO)
    n = fetch_symbol(NIFTY_SYMBOL, NIFTY_KEY, from_d, to_d)
    cached = load_cached(NIFTY_SYMBOL)
    logger.info("NIFTY50 15m bars=%s cached=%s", n, bool(cached and cached.get("candles")))


if __name__ == "__main__":
    main()
