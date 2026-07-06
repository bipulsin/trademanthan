"""Thin Upstox candle fetch layer for CSV-fed BTST backtest."""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from enum import Enum
from typing import List, Optional, Tuple

from backend.config import settings
from backend.services.btst_backtest.timing import bars_on_session, close_at_or_before, next_trading_day
from backend.services.upstox_service import UpstoxService, _upstox_v3_max_calendar_span_days

logger = logging.getLogger(__name__)


class FetchOutcome(str, Enum):
    OK = "ok"
    EMPTY = "empty"
    FAILED = "failed"


class BtstDataAccess:
    """On-demand historical candles — no bulk universe prefetch."""

    M5_INTERVAL = "minutes/5"
    DAILY_INTERVAL = "days/1"

    def __init__(self, *, throttle_sec: float = 0.05, retries: int = 3):
        self.ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        self.ux.reload_token_from_storage()
        self.throttle_sec = throttle_sec
        self.retries = retries

    def _sleep(self) -> None:
        if self.throttle_sec > 0:
            time.sleep(self.throttle_sec)

    def _days_back(self, interval: str, days: int) -> int:
        cap = _upstox_v3_max_calendar_span_days(interval)
        if cap is not None:
            return min(max(1, days), cap)
        return max(1, days)

    def fetch_candles(
        self,
        instrument_key: str,
        interval: str,
        range_end: date,
        days_back: int,
    ) -> Tuple[FetchOutcome, List[dict]]:
        for attempt in range(self.retries):
            self._sleep()
            candles = self.ux.get_historical_candles_by_instrument_key(
                instrument_key,
                interval=interval,
                days_back=self._days_back(interval, days_back),
                range_end_date=range_end,
            )
            if candles is not None:
                return (
                    FetchOutcome.EMPTY if len(candles) == 0 else FetchOutcome.OK,
                    list(candles),
                )
            time.sleep(min(2 ** attempt, 15))
        return FetchOutcome.FAILED, []

    def equity_m5(self, instrument_key: str, trade_date: date) -> Tuple[FetchOutcome, List[dict]]:
        return self.fetch_candles(instrument_key, self.M5_INTERVAL, trade_date, days_back=8)

    def equity_daily(self, instrument_key: str, trade_date: date) -> Tuple[FetchOutcome, List[dict]]:
        return self.fetch_candles(instrument_key, self.DAILY_INTERVAL, trade_date, days_back=15)

    def spot_at(self, instrument_key: str, trade_date: date, hhmm: str) -> Optional[float]:
        outcome, bars = self.equity_m5(instrument_key, trade_date)
        if outcome == FetchOutcome.FAILED:
            return None
        return close_at_or_before(bars, trade_date, hhmm)

    def previous_close(self, instrument_key: str, trade_date: date) -> Optional[float]:
        outcome, daily = self.equity_daily(instrument_key, trade_date)
        if outcome == FetchOutcome.FAILED:
            return None
        best_d = None
        best_close = None
        for c in daily:
            ts = str(c.get("timestamp") or "")[:10]
            try:
                d = date.fromisoformat(ts)
            except ValueError:
                continue
            if d < trade_date and (best_d is None or d > best_d):
                best_d = d
                best_close = float(c.get("close") or 0)
        return best_close

    def option_premium_candles(
        self,
        option_key: str,
        trade_date: date,
    ) -> Tuple[FetchOutcome, List[dict]]:
        """5m premium bars covering entry day afternoon + next session open."""
        nd = next_trading_day(trade_date)
        outcome, bars = self.fetch_candles(option_key, self.M5_INTERVAL, nd, days_back=5)
        if outcome == FetchOutcome.FAILED:
            return outcome, []
        if outcome == FetchOutcome.EMPTY:
            return outcome, []
        keep = []
        for c in bars:
            sd = str(c.get("timestamp") or "")
            try:
                d = date.fromisoformat(sd[:10])
            except ValueError:
                continue
            if d == trade_date or d == nd:
                keep.append(c)
        return FetchOutcome.OK if keep else FetchOutcome.EMPTY, keep
