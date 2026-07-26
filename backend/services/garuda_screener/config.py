"""Garuda screener constants."""
from __future__ import annotations

from dataclasses import dataclass

# Part 1 — imbalance
ATR_LEN = 14
RANGE_EXPANSION_MULT = 1.5
VOLUME_BREAKOUT_MULT = 1.5
CLOSE_POSITION_LONG = 0.65
CLOSE_POSITION_SHORT = 0.35
CONSEC_DIR_BARS = 3
GAP_HOLD_BARS = 3
GAP_ATR_MULT = 0.75
GAP_CUTOFF_HM = "10:00"
RS_WINDOW_BARS = 6  # ~1h on 10m; scales with bar minutes in backtest

# Part 2
DIRECTION_LOOKBACK = 3
ADX_LEN = 14
ADX_SLOPE_LOOKBACK = 3
ER_LEN = 10
ROC_LEN_PRIMARY = 3
ROC_LEN_ALT = 5
BETA_LOOKBACK_DAYS = 20

# Ranking
TOP_N = 6
RANK_METHOD = "avg_strength_momentum_pct"  # (strength_pct + momentum_pct_roc3) / 2

# Momentum comparison only (not a gate)
VWAP_SLOPE_THRESHOLD = 50.0

# FO universe exclusions (illiquid / non-standard)
EXCLUDED_SYMBOLS = frozenset({"EXIDEIND", "NUVAMA", "SAMMAANCAP"})

NIFTY_KEY = "NSE_INDEX|Nifty 50"


@dataclass(frozen=True)
class GarudaConfig:
    atr_len: int = ATR_LEN
    range_expansion_mult: float = RANGE_EXPANSION_MULT
    volume_breakout_mult: float = VOLUME_BREAKOUT_MULT
    close_position_long: float = CLOSE_POSITION_LONG
    close_position_short: float = CLOSE_POSITION_SHORT
    consec_dir_bars: int = CONSEC_DIR_BARS
    gap_hold_bars: int = GAP_HOLD_BARS
    gap_atr_mult: float = GAP_ATR_MULT
    rs_window_bars: int = RS_WINDOW_BARS
    direction_lookback: int = DIRECTION_LOOKBACK
    adx_slope_lookback: int = ADX_SLOPE_LOOKBACK
    er_len: int = ER_LEN
    roc_len_primary: int = ROC_LEN_PRIMARY
    roc_len_alt: int = ROC_LEN_ALT
    beta_lookback_days: int = BETA_LOOKBACK_DAYS
    top_n: int = TOP_N
