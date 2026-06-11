"""Volume Mismatch Futures configuration."""
from __future__ import annotations

MIN_GAP_PCT_LONG = -1.0
MIN_GAP_PCT_SHORT = 1.0
# Gap-up / flat / mild gap-down continuation (ICICIBANK -0.35% open, high rel vol).
MIN_GAP_PCT_MOMENTUM_LONG = -1.0  # same floor as classic; gap must be > this (mild gap-down OK)
MIN_GAP_PCT_MOMENTUM_LONG_CEIL = 1.0  # exclude strong gap-up (SHORT gap territory)
MIN_REL_VOL_MOMENTUM_LONG = 1.25
# Intraday discovery (monitor): breakout above first 15m high after VWAP reclaim (KOTAK-style).
MOMENTUM_DISCOVERY_MIN_REL_VOL = 1.15
MOMENTUM_DISCOVERY_START_MINUTES = 9 * 60 + 45  # after first 15m completes
MOMENTUM_DISCOVERY_END_MINUTES = 12 * 60
# Scoring + live scan default gap magnitude (same as SHORT minimum).
DEFAULT_GAP_THRESHOLD_PCT = MIN_GAP_PCT_SHORT
RELATIVE_VOLUME_LOOKBACK_SESSIONS = 20
SCAN_HOUR = 9
SCAN_MINUTE = 30
SCAN_SECOND = 30
FIRST_CANDLE_MINUTES = 15
PREFERRED_ENTRY_BUFFER_PCT = 0.05  # 0.05% above high / below low
