"""Candle helpers for Volume Mismatch — batch fetch + first 15m bar extraction."""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Per-job cache: instrument_key -> (interval, days_back) -> candles
_candle_cache: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}


def clear_candle_cache() -> None:
    global _candle_cache
    _candle_cache = {}


def _parse_ts(ts: Any) -> Optional[datetime]:
    from backend.services.upstox_service import _parse_ts_to_aware_ist

    return _parse_ts_to_aware_ist(ts)


def _cache_key(interval: str, days_back: int, range_end: Optional[date]) -> str:
    return f"{interval}|{days_back}|{range_end or 'today'}"


def fetch_candles_cached(
    upstox: Any,
    instrument_key: str,
    interval: str,
    days_back: int,
    *,
    range_end_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    ik = (instrument_key or "").strip()
    if not ik:
        return []
    ck = _cache_key(interval, days_back, range_end_date)
    bucket = _candle_cache.setdefault(ik, {})
    if ck in bucket:
        return bucket[ck]
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            ik,
            interval=interval,
            days_back=days_back,
            range_end_date=range_end_date,
        )
        out = list(raw or [])
    except Exception as e:
        logger.debug("VM candles %s %s: %s", ik, interval, e)
        out = []
    bucket[ck] = out
    return out


def batch_fetch_candles(
    upstox: Any,
    instrument_keys: Sequence[str],
    interval: str,
    days_back: int,
    *,
    range_end_date: Optional[date] = None,
    max_workers: int = 24,
) -> Dict[str, List[Dict[str, Any]]]:
    keys = list(dict.fromkeys(k for k in instrument_keys if str(k).strip()))
    if not keys:
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {}
    workers = min(max(1, max_workers), len(keys))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {
            pool.submit(
                fetch_candles_cached,
                upstox,
                ik,
                interval,
                days_back,
                range_end_date=range_end_date,
            ): ik
            for ik in keys
        }
        for fut in as_completed(futs):
            ik = futs[fut]
            try:
                out[ik] = fut.result()
            except Exception as e:
                logger.debug("VM batch candle %s: %s", ik, e)
                out[ik] = []
    return out


def is_first_15m_bar(candle: Dict[str, Any], session_date: date) -> bool:
    ts = _parse_ts(candle.get("timestamp"))
    if ts is None:
        return False
    return ts.astimezone(IST).date() == session_date and ts.hour == 9 and ts.minute == 15


def first_15m_bar_for_session(
    candles: Sequence[Dict[str, Any]],
    session_date: date,
) -> Optional[Dict[str, Any]]:
    for c in sorted(candles, key=lambda x: str(x.get("timestamp") or "")):
        if is_first_15m_bar(c, session_date):
            return c
    return None


def first_15m_volumes_by_session(
    candles: Sequence[Dict[str, Any]],
    *,
    before_date: Optional[date] = None,
    max_sessions: int = 20,
) -> List[Tuple[date, float]]:
    """Collect first 15m volume per session (newest first), excluding before_date."""
    by_date: Dict[date, float] = {}
    for c in candles:
        ts = _parse_ts(c.get("timestamp"))
        if ts is None:
            continue
        d = ts.astimezone(IST).date()
        if before_date and d >= before_date:
            continue
        if ts.hour != 9 or ts.minute != 15:
            continue
        try:
            vol = float(c.get("volume") or 0)
        except (TypeError, ValueError):
            vol = 0.0
        if d not in by_date:
            by_date[d] = vol
    ordered = sorted(by_date.items(), key=lambda x: x[0], reverse=True)
    return ordered[:max_sessions]


# Backtest: daily history for prev close + BB (20,2 needs ~20 sessions + holiday buffer).
BB_DAILY_DAYS_BACK = 35
FIRST_15M_DAYS_BACK = 1


def fetch_first_15m_bar_for_session(
    upstox: Any,
    instrument_key: str,
    session_date: date,
    *,
    persistent_cache: Any = None,
) -> Tuple[Optional[Dict[str, Any]], bool]:
    """
    Minimal 15m fetch: one session day only (09:15 bar for ``session_date``).

    Returns (bar, api_fetched). With ``persistent_cache``, reads disk first.
    """
    ik = (instrument_key or "").strip()
    if not ik:
        return None, False
    if persistent_cache is not None:
        return persistent_cache.get_first_15m_bar(upstox, ik, session_date)
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            ik,
            interval="minutes/15",
            days_back=FIRST_15M_DAYS_BACK,
            range_end_date=session_date,
        )
    except Exception as e:
        logger.debug("VM first 15m %s %s: %s", ik, session_date, e)
        return None, True
    return first_15m_bar_for_session(raw or [], session_date), True


def _daily_candle_date(candle: Dict[str, Any]) -> Optional[date]:
    ts = _parse_ts(candle.get("timestamp"))
    return ts.astimezone(IST).date() if ts is not None else None


def _merge_daily_candles(
    existing: Sequence[Dict[str, Any]],
    fresh: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    by_date: Dict[date, Dict[str, Any]] = {}
    for c in list(existing) + list(fresh):
        d = _daily_candle_date(c)
        if d is not None:
            by_date[d] = c
    return [by_date[d] for d in sorted(by_date)]


class BacktestDailyCache:
    """In-memory daily series per instrument — reused across backtest session days."""

    def __init__(self, persistent_cache: Any = None) -> None:
        self._bars: Dict[str, List[Dict[str, Any]]] = {}
        self._range_end: Dict[str, date] = {}
        self.persistent = persistent_cache

    def _closes_before(self, bars: Sequence[Dict[str, Any]], session_date: date) -> int:
        n = 0
        for c in bars:
            d = _daily_candle_date(c)
            if d is None or d >= session_date:
                continue
            try:
                cl = float(c.get("close") or 0)
            except (TypeError, ValueError):
                continue
            if cl > 0:
                n += 1
        return n

    def _ensure(
        self,
        upstox: Any,
        instrument_key: str,
        session_date: date,
        *,
        min_closes: int,
    ) -> tuple[List[Dict[str, Any]], bool]:
        """Return (daily bars, True if an Upstox fetch was performed)."""
        ik = (instrument_key or "").strip()
        if not ik:
            return [], False
        if self.persistent is not None:
            bars, fetched = self.persistent.ensure_daily(
                upstox, ik, session_date, min_closes=min_closes
            )
            self._bars[ik] = bars
            self._range_end[ik] = session_date
            return bars, fetched
        existing = self._bars.get(ik) or []
        cached_end = self._range_end.get(ik)
        if (
            cached_end is not None
            and session_date <= cached_end
            and previous_day_close(existing, session_date) is not None
            and self._closes_before(existing, session_date) >= min_closes
        ):
            return existing, False

        if cached_end is not None and session_date > cached_end:
            gap_days = (session_date - cached_end).days
            days_back = min(max(gap_days + 5, 5), BB_DAILY_DAYS_BACK)
        else:
            days_back = BB_DAILY_DAYS_BACK

        try:
            fresh = upstox.get_historical_candles_by_instrument_key(
                ik,
                interval="days/1",
                days_back=days_back,
                range_end_date=session_date,
            )
        except Exception as e:
            logger.debug("VM daily cache %s %s: %s", ik, session_date, e)
            fresh = []
        merged = _merge_daily_candles(existing, fresh or [])
        self._bars[ik] = merged
        self._range_end[ik] = session_date
        if (
            previous_day_close(merged, session_date) is not None
            and self._closes_before(merged, session_date) >= min_closes
        ):
            return merged, True
        if days_back >= BB_DAILY_DAYS_BACK:
            return merged, True
        try:
            fresh = upstox.get_historical_candles_by_instrument_key(
                ik,
                interval="days/1",
                days_back=BB_DAILY_DAYS_BACK,
                range_end_date=session_date,
            )
        except Exception as e:
            logger.debug("VM daily cache retry %s %s: %s", ik, session_date, e)
            fresh = []
        merged = _merge_daily_candles(merged, fresh or [])
        self._bars[ik] = merged
        return merged, True

    def previous_close(
        self,
        upstox: Any,
        instrument_key: str,
        session_date: date,
    ) -> tuple[Optional[float], bool]:
        bars, fetched = self._ensure(upstox, instrument_key, session_date, min_closes=1)
        return previous_day_close(bars, session_date), fetched

    def daily_bars_for_bb(
        self,
        upstox: Any,
        instrument_key: str,
        session_date: date,
        *,
        min_closes: int = 20,
    ) -> tuple[List[Dict[str, Any]], bool]:
        return self._ensure(upstox, instrument_key, session_date, min_closes=min_closes)


def previous_day_close(
    daily_candles: Sequence[Dict[str, Any]],
    session_date: date,
) -> Optional[float]:
    """Last completed daily close strictly before session_date."""
    best_ts: Optional[datetime] = None
    best_close: Optional[float] = None
    for c in daily_candles:
        ts = _parse_ts(c.get("timestamp"))
        if ts is None:
            continue
        d = ts.astimezone(IST).date()
        if d >= session_date:
            continue
        try:
            cl = float(c.get("close") or 0)
        except (TypeError, ValueError):
            continue
        if cl <= 0:
            continue
        if best_ts is None or ts > best_ts:
            best_ts = ts
            best_close = cl
    return best_close


def last_two_completed_5m_bars(
    candles_5m: Sequence[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Return (current_completed, previous_completed) 5m bars."""
    if not candles_5m:
        return None, None
    now_ist = now or datetime.now(IST)
    if now_ist.tzinfo is None:
        now_ist = IST.localize(now_ist)
    else:
        now_ist = now_ist.astimezone(IST)

    completed: List[Dict[str, Any]] = []
    for c in sorted(candles_5m, key=lambda x: str(x.get("timestamp") or "")):
        ts = _parse_ts(c.get("timestamp"))
        if ts is None:
            continue
        end = ts + timedelta(minutes=5)
        if now_ist >= end:
            completed.append(c)
    if not completed:
        return None, None
    if len(completed) == 1:
        return completed[-1], None
    return completed[-1], completed[-2]
