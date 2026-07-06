"""Swappable historical data access for BTST backtest (Upstox implementation)."""
from __future__ import annotations

import logging
import time
from datetime import date
from typing import Dict, List, Optional, Tuple

from backend.config import settings
from backend.services.btst_backtest.timing import bars_on_session, close_at_or_before
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)


class BtstDataAccess:
    """Thin candle fetch layer with in-memory cache for one backtest run."""

    def __init__(self, *, throttle_sec: float = 0.12):
        self.ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        self.ux.reload_token_from_storage()
        self._m5_cache: Dict[Tuple[str, date], List[dict]] = {}
        self._daily_cache: Dict[Tuple[str, date], List[dict]] = {}
        self.throttle_sec = throttle_sec

    def _sleep(self) -> None:
        if self.throttle_sec > 0:
            time.sleep(self.throttle_sec)

    def get_candles_5m(
        self,
        instrument_key: str,
        range_end_date: date,
        *,
        days_back: int = 5,
    ) -> List[dict]:
        eff_days = max(1, int(days_back))
        key = (instrument_key, range_end_date)
        if key in self._m5_cache:
            return self._m5_cache[key]
        self._sleep()
        candles = self.ux.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="minutes/5",
            days_back=eff_days,
            range_end_date=range_end_date,
        )
        out = candles or []
        self._m5_cache[key] = out
        return out

    def get_candles_daily(
        self,
        instrument_key: str,
        range_end_date: date,
        *,
        days_back: int = 15,
    ) -> List[dict]:
        key = (instrument_key, range_end_date)
        if key in self._daily_cache:
            return self._daily_cache[key]
        self._sleep()
        candles = self.ux.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="days/1",
            days_back=max(1, days_back),
            range_end_date=range_end_date,
        )
        out = candles or []
        self._daily_cache[key] = out
        return out

    def prev_trading_day_ohlc(
        self,
        instrument_key: str,
        trade_date: date,
        trading_days: List[date],
    ) -> Optional[Dict[str, float]]:
        idx = trading_days.index(trade_date) if trade_date in trading_days else -1
        if idx <= 0:
            daily = self.get_candles_daily(instrument_key, trade_date, days_back=20)
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
            daily = self.get_candles_daily(instrument_key, trade_date, days_back=20)
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

    def session_has_equity_bars(self, instrument_key: str, trade_date: date) -> bool:
        m5 = self.get_candles_5m(instrument_key, trade_date, days_back=3)
        return len(bars_on_session(m5, trade_date)) > 0

    def spot_at(self, instrument_key: str, trade_date: date, hhmm: str) -> Optional[float]:
        m5 = self.get_candles_5m(instrument_key, trade_date, days_back=5)
        return close_at_or_before(m5, trade_date, hhmm)

    def option_premium_history_usable(
        self,
        option_key: str,
        trade_date: date,
    ) -> bool:
        """True when 5m premium bars exist for the session (full data mode)."""
        m5 = self.get_candles_5m(option_key, trade_date, days_back=5)
        session = bars_on_session(m5, trade_date)
        if not session:
            return False
        from backend.services.btst_backtest.timing import bar_minutes

        for c in session:
            tm = bar_minutes(c.get("timestamp"))
            if tm is not None and 15 * 60 <= tm <= 15 * 60 + 15:
                return True
        return False
