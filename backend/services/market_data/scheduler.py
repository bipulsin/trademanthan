"""APScheduler hooks for market data refresh."""
from __future__ import annotations

import logging
from typing import Optional

from backend.services.market_data.engine import (
    refresh_arbitrage_master_market_data,
    refresh_curr_month_aux_candles,
    refresh_stock_next_ltp_from_ws,
    refresh_stock_next_vwap_ema_hourly,
)

logger = logging.getLogger(__name__)


def _scheduler_window_ok() -> Optional[dict]:
    try:
        from backend.services.scheduler_window import is_allowed_scheduler_window_ist
        from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

        if should_skip_scheduled_market_jobs_ist():
            return {"success": True, "skipped": "non_trading_day"}
        if not is_allowed_scheduler_window_ist():
            return {"success": True, "skipped": "outside_scheduler_window"}
    except Exception:
        pass
    return None


def run_market_data_refresh_job() -> dict:
    """
    Curr-month REST candle + LTP warm (every 10 min, clock-aligned :05/:15/…).

    Stock/next LTP → ``run_stock_next_ws_ltp_job`` (30m).
    Stock/next VWAP/EMA5 → ``run_stock_next_vwap_ema_hourly_job`` (:08;
    paused unless ``STOCK_NEXT_VWAP_EMA_HOURLY_ENABLED``).
    """
    skipped = _scheduler_window_ok()
    if skipped is not None:
        return skipped

    return refresh_arbitrage_master_market_data(
        execution="scheduled_10m",
        fetch_candles=True,
        candle_legs=("currmth",),
        ltp_legs=("currmth",),
    )


def run_stock_next_ws_ltp_job() -> dict:
    """Stock + next-month LTP from WebSocket (every 30 min). Independent of VWAP/EMA job."""
    skipped = _scheduler_window_ok()
    if skipped is not None:
        return skipped

    return refresh_stock_next_ltp_from_ws(execution="scheduled_ws_ltp_30m")


def run_stock_next_vwap_ema_hourly_job() -> dict:
    """
    Stock + next-month REST 5m → VWAP/EMA5 at :08 IST hourly.

    Does not write LTP (WS @ 30m owns those columns). Schedule paused by
    default (``STOCK_NEXT_VWAP_EMA_HOURLY_ENABLED``); function kept for rollback.
    """
    skipped = _scheduler_window_ok()
    if skipped is not None:
        return skipped

    return refresh_stock_next_vwap_ema_hourly(
        execution="scheduled_stock_next_vwap_ema_hourly"
    )


def run_curr_month_aux_candle_warm_job() -> dict:
    """
    Morning one-shot: curr-month ``days/1`` into shared candle_cache (VM BB / prev close).

    Opening 10m range is built from the 10m 5m warm — no minutes/15 fetch.
    """
    skipped = _scheduler_window_ok()
    if skipped is not None:
        return skipped

    return refresh_curr_month_aux_candles(
        execution="scheduled_aux_0905",
        intervals=[("days/1", 45)],
    )
