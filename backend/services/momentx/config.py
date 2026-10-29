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
    # Literal rule (default): today cumulative vol vs plain 5-day full-day average.
    # When True: compare vs average of prior days' volume up to the same clock time.
    time_adjusted_volume: bool = False

    # Candle fetch windows
    daily_days_back: int = 420  # calendar days for ~200 trading sessions + buffer
    minutes15_days_back: int = 12
    minutes15_min_bars: int = 100
    minutes5_days_back: int = 10
    minutes5_min_bars: int = 60

    # --- Stage 2 (futures, 5m) ---
    ema_fast: int = 5
    ema_mid: int = 15
    ema_slow: int = 50

    # Schedule (IST)
    scan_delay_sec: int = 8
    ema_delay_sec: int = 6
    market_open_hhmm: tuple = (9, 15)
    market_close_hhmm: tuple = (15, 30)
    first_15m_scan_hhmm: tuple = (9, 30)

    # Batching
    candle_workers: int = 8
    poll_ready_sec: int = 5  # frontend poll hint

    def to_public_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # tuples → lists for JSON
        d["market_open_hhmm"] = list(self.market_open_hhmm)
        d["market_close_hhmm"] = list(self.market_close_hhmm)
        d["first_15m_scan_hhmm"] = list(self.first_15m_scan_hhmm)
        # Spec alias
        d["timeAdjustedVolume"] = self.time_adjusted_volume
        return d


CONFIG = MomentXConfig()
