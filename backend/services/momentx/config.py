"""Single config object for MomentX thresholds, periods, and schedule knobs."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict


@dataclass(frozen=True)
class MomentXConfig:
    # --- Stage 1 (equity, 15m scan) ---
    sma_period: int = 200
    pct_change_min: float = 1.0
    pct_change_max: float = 3.0
    rsi_period: int = 14
    rsi_min: float = 55.0
    williams_period: int = 14
    williams_min: float = -25.0
    adx_period: int = 14
    adx_min: float = 30.0
    volume_sma_days: int = 5
    # Legacy flag (UI alias). Baseline is always the 5-session *full-day* average
    # of completed daily volumes (Chartink-style). Numerator remains session
    # cumulative volume since market open through the as-of clock.
    time_adjusted_volume: bool = False

    # --- Stage 1 bearish (equity, 15m scan) ---
    # Close/price above SMA200, day down ≥1%, oversold oscillators, same ADX floor.
    pct_change_bearish_max: float = -1.0  # pct_change <= this
    rsi_bearish_max: float = 40.0
    williams_bearish_max: float = -75.0

    # Candle fetch windows
    daily_days_back: int = 420  # calendar days for ~200 trading sessions + buffer
    minutes15_days_back: int = 12
    minutes15_min_bars: int = 100
    minutes5_days_back: int = 10
    minutes5_min_bars: int = 60

    # --- Stage 2 (futures, 5m) ---
    ema_fast: int = 5
    ema_cross: int = 10  # Active reactivation: EMA(5) cross above EMA(10)
    ema_mid: int = 15
    ema_slow: int = 50
    # Active → Locked after this many seconds continuously Active
    active_lock_after_sec: int = 300

    # Schedule (IST)
    scan_delay_sec: int = 8
    ema_delay_sec: int = 6
    market_open_hhmm: tuple = (9, 15)
    market_close_hhmm: tuple = (15, 30)
    first_15m_scan_hhmm: tuple = (9, 30)
    # No new Stage-1 / EMA scans at or after this time
    scan_cutoff_hhmm: tuple = (14, 45)
    # Force-lock every day-board card at or after this time
    lock_all_hhmm: tuple = (15, 0)

    # Batching
    candle_workers: int = 8
    poll_ready_sec: int = 5  # frontend poll hint

    def to_public_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # tuples → lists for JSON
        d["market_open_hhmm"] = list(self.market_open_hhmm)
        d["market_close_hhmm"] = list(self.market_close_hhmm)
        d["first_15m_scan_hhmm"] = list(self.first_15m_scan_hhmm)
        d["scan_cutoff_hhmm"] = list(self.scan_cutoff_hhmm)
        d["lock_all_hhmm"] = list(self.lock_all_hhmm)
        # Spec alias
        d["timeAdjustedVolume"] = self.time_adjusted_volume
        return d


CONFIG = MomentXConfig()
