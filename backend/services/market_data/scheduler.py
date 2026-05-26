"""APScheduler hook for 5-minute market data refresh."""
from __future__ import annotations

import logging

from backend.services.market_data.engine import refresh_arbitrage_master_market_data

logger = logging.getLogger(__name__)


def run_market_data_refresh_job() -> dict:
    """Scheduled job entry — same contract as other smart_future_algo jobs."""
    try:
        from backend.services.scheduler_window import is_allowed_scheduler_window_ist
        from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

        if should_skip_scheduled_market_jobs_ist():
            return {"success": True, "skipped": "non_trading_day"}
        if not is_allowed_scheduler_window_ist():
            return {"success": True, "skipped": "outside_scheduler_window"}
    except Exception:
        pass

    return refresh_arbitrage_master_market_data(execution="scheduled_5m")
