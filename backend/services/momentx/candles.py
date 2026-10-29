"""Candle fetch helpers for MomentX (Upstox + shared rate limiter)."""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dt_time
from typing import Any, Dict, List, Optional, Sequence

import pytz

from backend.services.momentx.config import CONFIG

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Process-local daily cache: instrument_key -> (cache_date_iso, candles)
_DAILY_CACHE: Dict[str, tuple] = {}


def clear_daily_cache() -> None:
    _DAILY_CACHE.clear()


def _upstox():
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    return UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)


def _parse_ts(ts: Any) -> Optional[datetime]:
    from backend.services.upstox_service import _parse_ts_to_aware_ist

    return _parse_ts_to_aware_ist(ts)


def sort_candles(candles: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = [c for c in (candles or []) if isinstance(c, dict)]
    rows.sort(key=lambda c: _parse_ts(c.get("timestamp")) or datetime.min.replace(tzinfo=IST))
    return rows


def drop_forming_candle(candles: Sequence[Dict[str, Any]], interval_minutes: int) -> List[Dict[str, Any]]:
    """Drop the last bar if it has not yet closed (based on interval end)."""
    rows = sort_candles(candles)
    if not rows or interval_minutes <= 0:
        return rows
    last = rows[-1]
    ts = _parse_ts(last.get("timestamp"))
    if ts is None:
        return rows
    # Upstox timestamps are candle *start*; closed when start + interval <= now.
    from datetime import timedelta

    end = ts + timedelta(minutes=int(interval_minutes))
    now = datetime.now(IST)
    if end > now:
        return rows[:-1]
    return rows


def fetch_candles(instrument_key: str, interval: str, days_back: int) -> List[Dict[str, Any]]:
    ik = (instrument_key or "").strip()
    if not ik:
        return []
    try:
        from backend.services.upstox_rate_limiter import scheduled_candle_worker

        with scheduled_candle_worker():
            raw = _upstox().get_historical_candles_by_instrument_key(
                ik, interval=interval, days_back=int(days_back)
            )
        return sort_candles(raw or [])
    except Exception as e:
        logger.info("momentx candles fetch failed %s %s: %s", ik, interval, e)
        return []


def fetch_daily_cached(instrument_key: str) -> List[Dict[str, Any]]:
    """Daily candles cached once per IST calendar day."""
    ik = (instrument_key or "").strip()
    if not ik:
        return []
    today = datetime.now(IST).date().isoformat()
    hit = _DAILY_CACHE.get(ik)
    if hit and hit[0] == today:
        return list(hit[1])
    rows = fetch_candles(ik, "days/1", CONFIG.daily_days_back)
    # Daily bars for "today" may be incomplete — keep them for cumulative volume,
    # but SMA/prevClose logic excludes today separately.
    _DAILY_CACHE[ik] = (today, rows)
    return list(rows)


def batch_fetch(
    instrument_keys: Sequence[str],
    interval: str,
    days_back: int,
    *,
    max_workers: Optional[int] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    keys = list(dict.fromkeys(k for k in instrument_keys if str(k).strip()))
    if not keys:
        return {}
    workers = min(max(1, int(max_workers or CONFIG.candle_workers)), len(keys))
    out: Dict[str, List[Dict[str, Any]]] = {}

    def _one(ik: str) -> tuple:
        return ik, fetch_candles(ik, interval, days_back)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_one, ik): ik for ik in keys}
        for fut in as_completed(futs):
            try:
                ik, rows = fut.result()
                out[ik] = rows
            except Exception as e:
                ik = futs[fut]
                logger.info("momentx batch candle failed %s: %s", ik, e)
                out[ik] = []
    return out


def candle_date(c: Dict[str, Any]) -> Optional[date]:
    ts = _parse_ts(c.get("timestamp"))
    return ts.astimezone(IST).date() if ts else None


def candle_time(c: Dict[str, Any]) -> Optional[dt_time]:
    ts = _parse_ts(c.get("timestamp"))
    return ts.astimezone(IST).time() if ts else None


def splits_ohlcv(candles: Sequence[Dict[str, Any]]) -> tuple:
    opens, highs, lows, closes, volumes = [], [], [], [], []
    for c in candles:
        try:
            opens.append(float(c["open"]))
            highs.append(float(c["high"]))
            lows.append(float(c["low"]))
            closes.append(float(c["close"]))
            volumes.append(float(c.get("volume") or 0))
        except (KeyError, TypeError, ValueError):
            continue
    return opens, highs, lows, closes, volumes
