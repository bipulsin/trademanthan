"""Swappable historical data access for BTST backtest (Upstox implementation)."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

from backend.config import settings
from backend.services.btst_backtest.timing import bars_on_session, bar_minutes, close_at_or_before
from backend.services.btst_backtest import progress as btst_progress
from backend.services.upstox_rate_limiter import set_backtest_bulk_prefetch_mode
from backend.services.upstox_service import UpstoxService, _upstox_v3_max_calendar_span_days

logger = logging.getLogger(__name__)


class FetchOutcome(str, Enum):
    OK = "ok"
    EMPTY = "empty"
    FAILED = "failed"


@dataclass
class PrefetchStats:
    ok: int = 0
    empty: int = 0
    failed: int = 0
    failed_keys: Optional[Set[str]] = None

    def __post_init__(self) -> None:
        if self.failed_keys is None:
            self.failed_keys = set()


class BtstDataAccess:
    """Prefetch once per instrument per run window; never cache failed fetches."""

    M5_INTERVAL = "minutes/5"
    DAILY_INTERVAL = "days/1"

    def __init__(self, *, throttle_sec: float = 0.05, prefetch_retries: int = 5):
        self.ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        self.ux.reload_token_from_storage()
        self.throttle_sec = throttle_sec
        self.prefetch_retries = prefetch_retries
        self._m5_series: Dict[str, List[dict]] = {}
        self._daily_series: Dict[str, List[dict]] = {}
        self._m5_outcome: Dict[str, FetchOutcome] = {}
        self._daily_outcome: Dict[str, FetchOutcome] = {}
        # On-demand option premium cache (successful fetches only)
        self._option_m5: Dict[Tuple[str, date], List[dict]] = {}
        self._option_outcome: Dict[Tuple[str, date], FetchOutcome] = {}

    def _sleep(self) -> None:
        if self.throttle_sec > 0:
            time.sleep(self.throttle_sec)

    def _days_back_for_window(self, start: date, end: date, interval: str) -> int:
        span = max(1, (end - start).days + 8)
        cap = _upstox_v3_max_calendar_span_days(interval)
        if cap is not None:
            span = min(span, cap)
        return max(1, span)

    def _fetch_candles_with_retry(
        self,
        instrument_key: str,
        interval: str,
        range_end: date,
        days_back: int,
    ) -> Tuple[FetchOutcome, List[dict]]:
        for attempt in range(self.prefetch_retries):
            self._sleep()
            candles = self.ux.get_historical_candles_by_instrument_key(
                instrument_key,
                interval=interval,
                days_back=max(1, days_back),
                range_end_date=range_end,
            )
            if candles is not None:
                return (
                    FetchOutcome.EMPTY if len(candles) == 0 else FetchOutcome.OK,
                    list(candles),
                )
            wait = min(2 ** attempt, 30)
            logger.warning(
                "BTST fetch failed %s %s (attempt %s/%s), retry in %ss",
                instrument_key,
                interval,
                attempt + 1,
                self.prefetch_retries,
                wait,
            )
            time.sleep(wait)
        return FetchOutcome.FAILED, []

    def prefetch_universe(
        self,
        universe: List[Dict[str, str]],
        window_start: date,
        window_end: date,
    ) -> PrefetchStats:
        """Bulk-fetch daily + 5m once per underlying for the whole window."""
        stats = PrefetchStats()
        days_back_m5 = self._days_back_for_window(window_start, window_end, self.M5_INTERVAL)
        days_back_daily = self._days_back_for_window(
            window_start - timedelta(days=20), window_end, self.DAILY_INTERVAL
        )
        set_backtest_bulk_prefetch_mode(True)
        try:
            for i, row in enumerate(universe):
                ik = row["instrument_key"]
                if ik in self._m5_outcome and ik in self._daily_outcome:
                    continue
                btst_progress.set_prefetch(i + 1, len(universe), instrument=row.get("symbol"))
                if (i + 1) % 10 == 0 or i + 1 == len(universe):
                    logger.info("BTST prefetch %s/%s instruments", i + 1, len(universe))
                m5_out, m5_bars = self._fetch_candles_with_retry(ik, self.M5_INTERVAL, window_end, days_back_m5)
                self._m5_outcome[ik] = m5_out
                self._m5_series[ik] = m5_bars
                if m5_out == FetchOutcome.FAILED:
                    stats.failed += 1
                    stats.failed_keys.add(ik)
                elif m5_out == FetchOutcome.EMPTY:
                    stats.empty += 1
                else:
                    stats.ok += 1

                d_out, d_bars = self._fetch_candles_with_retry(
                    ik, self.DAILY_INTERVAL, window_end, days_back_daily
                )
                self._daily_outcome[ik] = d_out
                self._daily_series[ik] = d_bars
                if d_out == FetchOutcome.FAILED:
                    stats.failed_keys.add(ik)
        finally:
            set_backtest_bulk_prefetch_mode(False)
        logger.info(
            "BTST prefetch done: ok=%s empty=%s failed_keys=%s",
            stats.ok,
            stats.empty,
            len(stats.failed_keys),
        )
        return stats

    def instrument_prefetch_failed(self, instrument_key: str) -> bool:
        m5 = self._m5_outcome.get(instrument_key)
        daily = self._daily_outcome.get(instrument_key)
        return m5 == FetchOutcome.FAILED or daily == FetchOutcome.FAILED

    def m5_bars(self, instrument_key: str) -> List[dict]:
        return list(self._m5_series.get(instrument_key) or [])

    def daily_bars(self, instrument_key: str) -> List[dict]:
        return list(self._daily_series.get(instrument_key) or [])

    def session_has_equity_bars(self, instrument_key: str, trade_date: date) -> bool:
        if self.instrument_prefetch_failed(instrument_key):
            return False
        return len(bars_on_session(self.m5_bars(instrument_key), trade_date)) > 0

    def market_session_data_status(
        self,
        universe: List[Dict[str, str]],
        trade_date: date,
    ) -> str:
        """
        Returns 'ok', 'api_fetch_failed', or 'no_session_bars'.
        """
        if not universe:
            return "api_fetch_failed"
        any_prefetch_ok = False
        any_session = False
        all_failed = True
        for row in universe[:30]:
            ik = row["instrument_key"]
            if self.instrument_prefetch_failed(ik):
                continue
            all_failed = False
            any_prefetch_ok = True
            if self.session_has_equity_bars(ik, trade_date):
                any_session = True
                break
        if all_failed and not any_prefetch_ok:
            return "api_fetch_failed"
        if not any_session:
            return "no_session_bars"
        return "ok"

    def prev_trading_day_ohlc(
        self,
        instrument_key: str,
        trade_date: date,
        trading_days: List[date],
    ) -> Optional[Dict[str, float]]:
        if self.instrument_prefetch_failed(instrument_key):
            return None
        daily = self.daily_bars(instrument_key)
        idx = trading_days.index(trade_date) if trade_date in trading_days else -1
        if idx <= 0:
            session_bars = []
            for c in daily:
                ts = str(c.get("timestamp") or "")[:10]
                try:
                    d = date.fromisoformat(ts)
                except ValueError:
                    continue
                if d < trade_date:
                    session_bars.append((d, c))
            if not session_bars:
                return None
            session_bars.sort(key=lambda x: x[0])
            _, bar = session_bars[-1]
        else:
            prev_d = trading_days[idx - 1]
            bar = None
            for c in daily:
                ts = str(c.get("timestamp") or "")[:10]
                try:
                    d = date.fromisoformat(ts)
                except ValueError:
                    continue
                if d == prev_d:
                    bar = c
                    break
            if bar is None:
                return None
        return {
            "high": float(bar.get("high") or 0),
            "low": float(bar.get("low") or 0),
            "close": float(bar.get("close") or 0),
        }

    def previous_close(
        self,
        instrument_key: str,
        trade_date: date,
        trading_days: List[date],
    ) -> Optional[float]:
        ohlc = self.prev_trading_day_ohlc(instrument_key, trade_date, trading_days)
        if not ohlc:
            return None
        return float(ohlc["close"])

    def spot_at(self, instrument_key: str, trade_date: date, hhmm: str) -> Optional[float]:
        if self.instrument_prefetch_failed(instrument_key):
            return None
        return close_at_or_before(self.m5_bars(instrument_key), trade_date, hhmm)

    def get_option_m5(self, option_key: str, trade_date: date, *, days_back: int = 5) -> Tuple[FetchOutcome, List[dict]]:
        cache_key = (option_key, trade_date)
        if cache_key in self._option_outcome:
            return self._option_outcome[cache_key], list(self._option_m5.get(cache_key) or [])
        set_backtest_bulk_prefetch_mode(True)
        try:
            outcome, bars = self._fetch_candles_with_retry(
                option_key, self.M5_INTERVAL, trade_date, days_back
            )
        finally:
            set_backtest_bulk_prefetch_mode(False)
        if outcome != FetchOutcome.FAILED:
            self._option_m5[cache_key] = bars
        self._option_outcome[cache_key] = outcome
        return outcome, list(self._option_m5.get(cache_key) or [])

    def option_premium_history_usable(self, option_key: str, trade_date: date) -> Tuple[FetchOutcome, bool]:
        outcome, m5 = self.get_option_m5(option_key, trade_date, days_back=5)
        if outcome == FetchOutcome.FAILED:
            return outcome, False
        session = bars_on_session(m5, trade_date)
        if not session:
            return outcome, False
        for c in session:
            tm = bar_minutes(c.get("timestamp"))
            if tm is not None and 15 * 60 <= tm <= 15 * 60 + 15:
                return outcome, True
        return outcome, False
