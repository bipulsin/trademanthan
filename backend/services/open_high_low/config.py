"""OpenHighLow backtest configuration (dates, square-off, costs)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, time
from typing import List, Optional


# ---- Top-level defaults (override via OHLConfig / CLI) ----
BACKTEST_START = date(2026, 10, 20)
BACKTEST_END = date(2026, 11, 16)
SQUARE_OFF_TIME = time(15, 15)  # close of 15:15–15:20 candle
BROKERAGE_PER_LOT = 0.0
SLIPPAGE_POINTS = 0.0
LOT_SIZE_FALLBACK = 1  # used when instruments / universe resolution has no lot

EXCLUDE_SYMBOLS = frozenset({"NIFTY", "BANKNIFTY"})

FIRST_CANDLE = time(9, 15)
SECOND_CANDLE = time(9, 20)
THIRD_CANDLE = time(9, 25)
SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)

TICK_EPS = 1e-6  # float-safe tick equality for open_eq_extreme


ATR_PERIOD = 14

TRADE_FIELDS: List[str] = [
    "date",
    "symbol",
    "contract",
    "expiry",
    "side",
    "lot_size",
    "prev_day_high",
    "prev_day_low",
    "first_open",
    "first_high",
    "first_low",
    "first_close",
    "first_vwap",
    "second_close",
    "entry_time",
    "entry_price",
    "atr",
    "initial_sl",
    "exit_time",
    "exit_price",
    "exit_reason",
    "final_sl",
    "points_pnl",
    "pct_return",
    "rupee_pnl",
    "r_multiple",
    "mfe",
    "mae",
    "holding_minutes",
    "open_eq_extreme",
]


@dataclass
class OHLConfig:
    start: date = BACKTEST_START
    end: date = BACKTEST_END
    square_off: time = SQUARE_OFF_TIME
    brokerage_per_lot: float = BROKERAGE_PER_LOT
    slippage_points: float = SLIPPAGE_POINTS
    lot_fallback: int = LOT_SIZE_FALLBACK
    exclude_symbols: frozenset = field(default_factory=lambda: EXCLUDE_SYMBOLS)
    symbols: Optional[List[str]] = None  # None = full universe
    use_cache: bool = True
    max_symbols: Optional[int] = None
