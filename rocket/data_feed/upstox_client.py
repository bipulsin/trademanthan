"""Upstox historical candle client — reuses ``backend.services.upstox_service.UpstoxService``.

Adds token-bucket pacing (~3 req/s by default) and Parquet cache; does not reimplement
HTTP/auth beyond a thin wrapper around the production client.

Upstox v2 native intervals: 1minute, 30minute, day, week, month.
3/5/15minute are aggregated locally from 1minute (same approach as production).
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pytz

from rocket.config.constants import INTERVAL_TO_UPSTOX, UPSTOX_V2_INTERVALS
from rocket.config.settings import PROJECT_ROOT, get_settings
from rocket.data_feed.cache_manager import CacheManager

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Ensure project root is importable for backend.*
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Intervals available natively on Upstox historical v2
_V2_NATIVE = frozenset({"1minute", "30minute", "day"})
_AGG_FROM_1M = {
    "3minute": 3,
    "5minute": 5,
    "15minute": 15,
}


class TokenBucket:
    """Simple token-bucket rate limiter (thread-safe)."""

    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(0.1, float(rate_per_sec))
        self.capacity = float(capacity if capacity is not None else max(1.0, self.rate))
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> float:
        waited = 0.0
        with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.updated
                self.updated = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return waited
                need = (tokens - self.tokens) / self.rate
                time.sleep(need)
                waited += need


def _aggregate_1m(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
    x = df.copy()
    x = x.set_index("timestamp").sort_index()
    rule = f"{int(minutes)}min"
    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    if "oi" in x.columns:
        agg["oi"] = "last"
    out = x.resample(rule, label="left", closed="left").agg(agg).dropna(subset=["open", "close"])
    out = out.reset_index()
    return out


class UpstoxCandleClient:
    """
    Fetches historical candles via production UpstoxService.

    Interval CLI names: 1minute, 3minute, 5minute, 15minute, 30minute, day.
    """

    def __init__(
        self,
        *,
        rate_per_sec: Optional[float] = None,
        cache_dir: Optional[Path] = None,
        access_token: Optional[str] = None,
    ):
        settings = get_settings()
        self.bucket = TokenBucket(rate_per_sec or settings.rocket_rate_limit_per_sec)
        self.cache = CacheManager(cache_dir or settings.rocket_cache_dir)
        self._ux = None
        self._access_token = access_token or settings.UPSTOX_ACCESS_TOKEN

    def _service(self):
        if self._ux is None:
            from backend.services.upstox_service import UpstoxService

            settings = get_settings()
            self._ux = UpstoxService(
                settings.UPSTOX_API_KEY,
                settings.UPSTOX_API_SECRET,
                access_token=self._access_token,
            )
        return self._ux

    @staticmethod
    def normalize_interval(interval: str) -> str:
        iv = (interval or "").strip().lower().replace(" ", "")
        aliases = {
            "1m": "1minute",
            "3m": "3minute",
            "5m": "5minute",
            "15m": "15minute",
            "30m": "30minute",
            "1d": "day",
            "daily": "day",
        }
        iv = aliases.get(iv, iv)
        if iv not in UPSTOX_V2_INTERVALS:
            raise ValueError(f"Unsupported interval {interval!r}; choose from {sorted(UPSTOX_V2_INTERVALS)}")
        return iv

    def _fetch_v2_rows(self, instrument_key: str, v2_interval: str, to_s: str, from_s: str) -> List[dict]:
        self.bucket.acquire()
        ux = self._service()
        raw = ux._fetch_historical_v2_candles(  # noqa: SLF001 — deliberate reuse
            instrument_key, v2_interval, to_s, from_s
        )
        return list(raw or [])

    def fetch_candles(
        self,
        instrument_key: str,
        interval: str,
        from_date: date,
        to_date: date,
        *,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        iv = self.normalize_interval(interval)
        from_s = from_date.isoformat()
        to_s = to_date.isoformat()
        if use_cache:
            cached = self.cache.load(instrument_key, iv, from_s, to_s)
            if cached is not None:
                return self._normalize_df(cached)

        if iv in _V2_NATIVE:
            rows = self._fetch_v2_rows(instrument_key, iv, to_s, from_s)
            df = self._normalize_df(pd.DataFrame(rows))
        elif iv in _AGG_FROM_1M:
            one = None
            if use_cache:
                one = self.cache.load(instrument_key, "1minute", from_s, to_s)
            if one is None:
                rows_1m = self._fetch_v2_rows(instrument_key, "1minute", to_s, from_s)
                one = self._normalize_df(pd.DataFrame(rows_1m))
                if use_cache and not one.empty:
                    save_1m = one.copy()
                    save_1m["timestamp"] = save_1m["timestamp"].astype(str)
                    self.cache.save(
                        instrument_key,
                        "1minute",
                        from_s,
                        to_s,
                        save_1m.to_dict(orient="records"),
                    )
            else:
                one = self._normalize_df(one)
            df = _aggregate_1m(one, _AGG_FROM_1M[iv])
        else:
            self.bucket.acquire()
            ux = self._service()
            upstox_iv = INTERVAL_TO_UPSTOX[iv]
            days_back = max(1, (to_date - from_date).days + 2)
            raw = ux.get_historical_candles_by_instrument_key(
                instrument_key,
                upstox_iv,
                days_back,
                range_end_date=to_date,
            )
            df = self._normalize_df(pd.DataFrame(list(raw or [])))

        if use_cache and not df.empty:
            save_df = df.copy()
            save_df["timestamp"] = save_df["timestamp"].astype(str)
            self.cache.save(instrument_key, iv, from_s, to_s, save_df.to_dict(orient="records"))
        return df

    @staticmethod
    def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
        out = df.copy()
        if "timestamp" not in out.columns and len(out.columns) >= 6:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
        out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
        out = out.dropna(subset=["timestamp"])
        out["timestamp"] = out["timestamp"].dt.tz_convert(IST)
        for col in ("open", "high", "low", "close", "volume"):
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")
        if "oi" not in out.columns:
            out["oi"] = 0.0
        else:
            out["oi"] = pd.to_numeric(out["oi"], errors="coerce").fillna(0.0)
        out = out.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
        return out

    def fetch_universe(
        self,
        instrument_keys: List[str],
        interval: str,
        from_date: date,
        to_date: date,
        *,
        sleep_on_error: float = 1.0,
    ) -> Dict[str, pd.DataFrame]:
        result: Dict[str, pd.DataFrame] = {}
        total = len(instrument_keys)
        for i, ik in enumerate(instrument_keys, 1):
            try:
                df = self.fetch_candles(ik, interval, from_date, to_date)
                result[ik] = df
                logger.info("[%s/%s] %s bars=%s", i, total, ik, len(df))
            except Exception as exc:
                logger.error("[%s/%s] fetch failed %s: %s", i, total, ik, exc)
                result[ik] = pd.DataFrame()
                time.sleep(sleep_on_error)
        return result
