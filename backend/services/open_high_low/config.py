"""OpenHighLow backtest configuration (dates, square-off, costs, study knobs)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, time
from typing import List, Optional, Sequence, Tuple


# ---- Top-level defaults (override via OHLConfig / CLI) ----
# Prefer longest feasible Upstox stock-futures history; runner documents actual from-date.
BACKTEST_START = date(2025, 11, 1)
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
EMA_PERIOD_5M = 30
RVOL_LOOKBACK_SESSIONS = 20
MOVE_FILTER_PCT = 3.0  # signed move toward trade direction
SELECTION_TOP_N_PER_SIDE = 2
RANDOM_BASELINE_SAMPLES = 2000
RISK_RUPEES_DEFAULT = 5000.0
ATR_TRAIL_MULT = 1.0  # step size in ATR units (1×ATR → BE, 2×ATR → lock 1×ATR, …)
NIFTY_INDEX_KEY = "NSE_INDEX|Nifty 50"

SL_MODES: Tuple[str, ...] = ("fixed", "candle_trail", "atr_trail")
SELECTION_RULES: Tuple[str, ...] = ("ema", "range", "composite")

TRADE_FIELDS: List[str] = [
    "date",
    "symbol",
    "contract",
    "expiry",
    "side",
    "sl_mode",
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
    "rupee_pnl_fixed_risk",
    "fixed_risk_shares",
    "r_multiple",
    "mfe",
    "mae",
    "holding_minutes",
    "open_eq_extreme",
    # Features at entry decision (09:25)
    "ema30_5m",
    "entry_dist_ema30_pct",
    "entry_dist_ema30_atr",
    "prev_close",
    "move_prev_close_to_entry_pct",
    "gap_pct",
    "first_range_pct",
    "first_range_atr",
    "first_rvol_20d",
    "nifty_first_dir",
    "nifty_first_pct",
    "same_side_count_0925",
    # Selection flags (post-signal 2+2 rules)
    "selected_ema",
    "selected_range",
    "selected_composite",
    "passed_move_filter",
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
    sl_modes: Sequence[str] = field(default_factory=lambda: list(SL_MODES))
    atr_period: int = ATR_PERIOD
    atr_trail_mult: float = ATR_TRAIL_MULT
    ema_period: int = EMA_PERIOD_5M
    rvol_lookback: int = RVOL_LOOKBACK_SESSIONS
    move_filter_pct: float = MOVE_FILTER_PCT
    selection_top_n: int = SELECTION_TOP_N_PER_SIDE
    random_baseline_samples: int = RANDOM_BASELINE_SAMPLES
    risk_rupees: float = RISK_RUPEES_DEFAULT
    nifty_key: str = NIFTY_INDEX_KEY
    sl_path_samples: int = 5
    resume_months: bool = True
