"""MomentX — 15m equity screener + 5m futures EMA trigger desk.

Rules (Stage 1 — all must pass on equity):
  price < SMA(200 daily), day % change in [+1%, +3%], RSI(14) 15m > 55,
  Williams %R(14) 15m >= -25, ADX(14) 15m > 30,
  today volume > SMA(5) of prior 5 full-day volumes.

Stage 2 (Ready stocks' current-month future):
  EMA(5/15/50) on closed 5m candles; trigger when EMA5 > EMA15 and EMA5 > EMA50.

Config lives in ``momentx.config.CONFIG``. Schedulers skip weekends/holidays via
``should_skip_scheduled_market_jobs_ist``. Live LTP uses the shared Upstox feed
provider ``momentx``.
"""
from backend.services.momentx.config import CONFIG, MomentXConfig

__all__ = ["CONFIG", "MomentXConfig"]
