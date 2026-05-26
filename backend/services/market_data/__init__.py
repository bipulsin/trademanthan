"""
Centralized market data for arbitrage_master universe.

Algos should read LTP / session VWAP / EMA(5) via ``market_data.reads`` instead of
duplicate Upstox quote calls. Historical candle series for scoring remain per-algo.
"""

from backend.services.market_data.engine import refresh_arbitrage_master_market_data
from backend.services.market_data.reads import (
    get_ltp_for_instrument_key,
    get_ltps_for_instrument_keys,
    get_row_market_snapshot,
    is_market_data_fresh,
    ltp_map_with_fallback,
)

__all__ = [
    "refresh_arbitrage_master_market_data",
    "get_ltp_for_instrument_key",
    "get_ltps_for_instrument_keys",
    "get_row_market_snapshot",
    "is_market_data_fresh",
    "ltp_map_with_fallback",
]
