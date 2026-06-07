"""Persistent on-disk candle cache for Volume Mismatch backtest only."""
from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pytz

from backend.services.volume_mismatch.candles import (
    BB_DAILY_DAYS_BACK,
    FIRST_15M_DAYS_BACK,
    _merge_daily_candles,
    first_15m_bar_for_session,
    previous_day_close,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

M15_MAX_SPAN_DAYS = 31
DAILY_EXTRA_BUFFER_DAYS = 15


def default_cache_dir() -> Path:
    """``data/volume_mismatch_candle_cache/`` — bind-mounted on paperclip."""
    ec2 = Path("/home/ubuntu/trademanthan/data/volume_mismatch_candle_cache")
    if Path("/home/ubuntu/trademanthan/data").is_dir():
        ec2.mkdir(parents=True, exist_ok=True)
        return ec2
    root = Path(__file__).resolve().parents[3]
    for rel in ("data/volume_mismatch_candle_cache", "backend/data/volume_mismatch_candle_cache"):
        p = root / rel
        p.mkdir(parents=True, exist_ok=True)
        return p
    p = root / "data" / "volume_mismatch_candle_cache"
    p.mkdir(parents=True, exist_ok=True)
    return p


def sanitize_instrument_key(instrument_key: str) -> str:
    return (instrument_key or "").strip().replace("|", "__")


def _parse_ts(ts: Any) -> Optional[datetime]:
    from backend.services.upstox_service import _parse_ts_to_aware_ist

    return _parse_ts_to_aware_ist(ts)


def _m15_session_dates(candles: Sequence[Dict[str, Any]]) -> Set[date]:
    out: Set[date] = set()
    for c in candles:
        ts = _parse_ts(c.get("timestamp"))
        if ts is None:
            continue
        if ts.hour == 9 and ts.minute == 15:
            out.add(ts.astimezone(IST).date())
    return out


def _merge_m15_candles(
    existing: Sequence[Dict[str, Any]],
    fresh: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    by_ts: Dict[str, Dict[str, Any]] = {}
    for c in list(existing) + list(fresh):
        key = str(c.get("timestamp") or "")
        if key:
            by_ts[key] = c
    return [by_ts[k] for k in sorted(by_ts)]


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, default=str)
    tmp.replace(path)


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.is_file():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.debug("VM candle cache read %s: %s", path, e)
        return None


def _chunk_session_dates(dates: Sequence[date], max_span: int) -> List[Tuple[date, date]]:
    """Split sorted session dates into (chunk_start, chunk_end) calendar windows."""
    if not dates:
        return []
    ordered = sorted(set(dates))
    chunks: List[Tuple[date, date]] = []
    chunk_start = ordered[0]
    chunk_end = ordered[0]
    for d in ordered[1:]:
        if (d - chunk_start).days <= max_span:
            chunk_end = d
        else:
            chunks.append((chunk_start, chunk_end))
            chunk_start = d
            chunk_end = d
    chunks.append((chunk_start, chunk_end))
    return chunks


class VolumeMismatchCandleCache:
    """Disk-backed candle store with in-memory layer for backtest reruns."""

    def __init__(self, cache_dir: Optional[Path] = None) -> None:
        self.cache_dir = cache_dir or default_cache_dir()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._daily: Dict[str, List[Dict[str, Any]]] = {}
        self._m15: Dict[str, List[Dict[str, Any]]] = {}
        self._daily_loaded: Set[str] = set()
        self._m15_loaded: Set[str] = set()
        self._locks: Dict[str, threading.Lock] = {}
        self.stats: Dict[str, int] = {
            "daily_disk_hit": 0,
            "daily_api": 0,
            "m15_disk_hit": 0,
            "m15_api": 0,
        }
        self._day_stats: Dict[str, int] = {}

    def reset_day_stats(self) -> None:
        self._day_stats = {
            "daily_disk_hit": 0,
            "daily_api": 0,
            "m15_disk_hit": 0,
            "m15_api": 0,
        }

    def day_stats(self) -> Dict[str, int]:
        return dict(self._day_stats)

    def _lock_for(self, instrument_key: str) -> threading.Lock:
        ik = (instrument_key or "").strip()
        if ik not in self._locks:
            self._locks[ik] = threading.Lock()
        return self._locks[ik]

    def _ik_dir(self, instrument_key: str) -> Path:
        return self.cache_dir / sanitize_instrument_key(instrument_key)

    def _daily_path(self, instrument_key: str) -> Path:
        return self._ik_dir(instrument_key) / "daily.json"

    def _m15_path(self, instrument_key: str) -> Path:
        return self._ik_dir(instrument_key) / "m15.json"

    def _load_daily(self, instrument_key: str) -> List[Dict[str, Any]]:
        ik = (instrument_key or "").strip()
        if not ik:
            return []
        if ik in self._daily_loaded:
            return self._daily.get(ik) or []
        self._daily_loaded.add(ik)
        doc = _read_json(self._daily_path(ik))
        bars = list(doc.get("candles") or []) if doc else []
        self._daily[ik] = bars
        return bars

    def _load_m15(self, instrument_key: str) -> List[Dict[str, Any]]:
        ik = (instrument_key or "").strip()
        if not ik:
            return []
        if ik in self._m15_loaded:
            return self._m15.get(ik) or []
        self._m15_loaded.add(ik)
        doc = _read_json(self._m15_path(ik))
        bars = list(doc.get("candles") or []) if doc else []
        self._m15[ik] = bars
        return bars

    def _save_daily(self, instrument_key: str, bars: List[Dict[str, Any]], range_end: date) -> None:
        ik = (instrument_key or "").strip()
        if not ik:
            return
        self._daily[ik] = bars
        _atomic_write_json(
            self._daily_path(ik),
            {
                "instrument_key": ik,
                "interval": "days/1",
                "range_end": range_end.isoformat(),
                "updated_at": datetime.now(IST).isoformat(),
                "candles": bars,
            },
        )

    def _save_m15(self, instrument_key: str, bars: List[Dict[str, Any]]) -> None:
        ik = (instrument_key or "").strip()
        if not ik:
            return
        self._m15[ik] = bars
        _atomic_write_json(
            self._m15_path(ik),
            {
                "instrument_key": ik,
                "interval": "minutes/15",
                "updated_at": datetime.now(IST).isoformat(),
                "candles": bars,
            },
        )

    def _closes_before(self, bars: Sequence[Dict[str, Any]], session_date: date) -> int:
        n = 0
        for c in bars:
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
            if cl > 0:
                n += 1
        return n

    def _daily_sufficient(
        self,
        bars: Sequence[Dict[str, Any]],
        session_date: date,
        *,
        min_closes: int,
    ) -> bool:
        return (
            previous_day_close(bars, session_date) is not None
            and self._closes_before(bars, session_date) >= min_closes
        )

    def ensure_daily(
        self,
        upstox: Any,
        instrument_key: str,
        session_date: date,
        *,
        min_closes: int = 1,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """Return (daily bars, True if Upstox was called)."""
        ik = (instrument_key or "").strip()
        if not ik:
            return [], False
        with self._lock_for(ik):
            bars = self._load_daily(ik)
            if self._daily_sufficient(bars, session_date, min_closes=min_closes):
                self.stats["daily_disk_hit"] += 1
                self._day_stats["daily_disk_hit"] = self._day_stats.get("daily_disk_hit", 0) + 1
                return bars, False

            days_back = BB_DAILY_DAYS_BACK
            try:
                fresh = upstox.get_historical_candles_by_instrument_key(
                    ik,
                    interval="days/1",
                    days_back=days_back,
                    range_end_date=session_date,
                )
            except Exception as e:
                logger.debug("VM cache daily %s %s: %s", ik, session_date, e)
                fresh = []
            merged = _merge_daily_candles(bars, fresh or [])
            self._save_daily(ik, merged, session_date)
            self.stats["daily_api"] += 1
            self._day_stats["daily_api"] = self._day_stats.get("daily_api", 0) + 1
            if self._daily_sufficient(merged, session_date, min_closes=min_closes):
                return merged, True
            try:
                fresh = upstox.get_historical_candles_by_instrument_key(
                    ik,
                    interval="days/1",
                    days_back=BB_DAILY_DAYS_BACK,
                    range_end_date=session_date,
                )
            except Exception as e:
                logger.debug("VM cache daily retry %s %s: %s", ik, session_date, e)
                fresh = []
            merged = _merge_daily_candles(merged, fresh or [])
            self._save_daily(ik, merged, session_date)
            self.stats["daily_api"] += 1
            self._day_stats["daily_api"] = self._day_stats.get("daily_api", 0) + 1
            return merged, True

    def get_m15_candles(self, instrument_key: str) -> List[Dict[str, Any]]:
        """Cached 15m series for relative-volume lookback (disk + memory)."""
        return self._load_m15((instrument_key or "").strip())

    def get_first_15m_bar(
        self,
        upstox: Any,
        instrument_key: str,
        session_date: date,
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        """Return (09:15 bar, True if Upstox was called)."""
        ik = (instrument_key or "").strip()
        if not ik:
            return None, False
        with self._lock_for(ik):
            bars = self._load_m15(ik)
            hit = first_15m_bar_for_session(bars, session_date)
            if hit is not None:
                self.stats["m15_disk_hit"] += 1
                self._day_stats["m15_disk_hit"] = self._day_stats.get("m15_disk_hit", 0) + 1
                return hit, False
            try:
                raw = upstox.get_historical_candles_by_instrument_key(
                    ik,
                    interval="minutes/15",
                    days_back=FIRST_15M_DAYS_BACK,
                    range_end_date=session_date,
                )
            except Exception as e:
                logger.debug("VM cache m15 %s %s: %s", ik, session_date, e)
                raw = []
            merged = _merge_m15_candles(bars, raw or [])
            self._save_m15(ik, merged)
            self.stats["m15_api"] += 1
            self._day_stats["m15_api"] = self._day_stats.get("m15_api", 0) + 1
            return first_15m_bar_for_session(merged, session_date), True

    def _warm_daily_one(
        self,
        upstox: Any,
        instrument_key: str,
        range_end: date,
        days_back: int,
    ) -> bool:
        ik = (instrument_key or "").strip()
        if not ik:
            return False
        bars = self._load_daily(ik)
        if bars and self._daily_sufficient(bars, range_end, min_closes=BB_DAILY_DAYS_BACK):
            return False
        try:
            fresh = upstox.get_historical_candles_by_instrument_key(
                ik,
                interval="days/1",
                days_back=days_back,
                range_end_date=range_end,
            )
        except Exception as e:
            logger.debug("VM warm daily %s: %s", ik, e)
            fresh = []
        merged = _merge_daily_candles(bars, fresh or [])
        self._save_daily(ik, merged, range_end)
        self.stats["daily_api"] += 1
        return True

    def _warm_m15_chunk(
        self,
        upstox: Any,
        instrument_key: str,
        chunk_end: date,
        days_back: int,
    ) -> bool:
        ik = (instrument_key or "").strip()
        if not ik:
            return False
        bars = self._load_m15(ik)
        try:
            fresh = upstox.get_historical_candles_by_instrument_key(
                ik,
                interval="minutes/15",
                days_back=days_back,
                range_end_date=chunk_end,
            )
        except Exception as e:
            logger.debug("VM warm m15 %s %s: %s", ik, chunk_end, e)
            fresh = []
        merged = _merge_m15_candles(bars, fresh or [])
        self._save_m15(ik, merged)
        self.stats["m15_api"] += 1
        return True

    def warm_for_backtest(
        self,
        upstox: Any,
        ik_session_dates: Dict[str, Set[date]],
        *,
        from_date: date,
        to_date: date,
        max_workers: int = 4,
    ) -> Dict[str, Any]:
        """
        Pre-fetch daily + m15 candles for all instruments in the backtest universe.

        After warm-up, per-day scans should hit disk/memory only (zero Upstox calls).
        """
        t0 = time.monotonic()
        keys = [k for k in ik_session_dates if str(k).strip()]
        if not keys:
            return {"elapsed_sec": 0.0, "instruments": 0}

        for ik in keys:
            self._load_daily(ik)
            self._load_m15(ik)

        span_days = max(1, (to_date - from_date).days)
        daily_days_back = span_days + BB_DAILY_DAYS_BACK + DAILY_EXTRA_BUFFER_DAYS

        daily_jobs: List[str] = []
        for ik in keys:
            bars = self._daily.get(ik) or []
            if not self._daily_sufficient(bars, to_date, min_closes=BB_DAILY_DAYS_BACK):
                daily_jobs.append(ik)

        m15_by_ik: Dict[str, List[Tuple[date, int]]] = {}
        for ik, dates in ik_session_dates.items():
            if not str(ik).strip():
                continue
            bars = self._m15.get(ik) or []
            cached_dates = _m15_session_dates(bars)
            missing = sorted(d for d in dates if d not in cached_dates)
            jobs: List[Tuple[date, int]] = []
            for chunk_start, chunk_end in _chunk_session_dates(missing, M15_MAX_SPAN_DAYS):
                days_back = min(
                    max((chunk_end - chunk_start).days + 3, FIRST_15M_DAYS_BACK),
                    M15_MAX_SPAN_DAYS,
                )
                jobs.append((chunk_end, days_back))
            if jobs:
                m15_by_ik[ik] = jobs

        api_before = dict(self.stats)
        workers = max(1, max_workers)

        def _run_daily(ik: str) -> None:
            with self._lock_for(ik):
                self._warm_daily_one(upstox, ik, to_date, daily_days_back)

        def _run_m15_all(ik: str, jobs: List[Tuple[date, int]]) -> None:
            with self._lock_for(ik):
                for chunk_end, days_back in jobs:
                    self._warm_m15_chunk(upstox, ik, chunk_end, days_back)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = [pool.submit(_run_daily, ik) for ik in daily_jobs]
            futs += [pool.submit(_run_m15_all, ik, jobs) for ik, jobs in m15_by_ik.items()]
            for fut in as_completed(futs):
                try:
                    fut.result()
                except Exception as e:
                    logger.debug("VM warm task failed: %s", e)

        elapsed = round(time.monotonic() - t0, 2)
        m15_chunks = sum(len(j) for j in m15_by_ik.values())
        return {
            "elapsed_sec": elapsed,
            "instruments": len(keys),
            "daily_fetches": len(daily_jobs),
            "m15_chunks": m15_chunks,
            "daily_api": self.stats["daily_api"] - api_before.get("daily_api", 0),
            "m15_api": self.stats["m15_api"] - api_before.get("m15_api", 0),
            "cache_dir": str(self.cache_dir),
        }


def collect_instrument_session_dates(
    session_days: Sequence[date],
    load_universe,
) -> Dict[str, Set[date]]:
    """Map instrument_key -> session dates where it appears in the backtest universe."""
    out: Dict[str, Set[date]] = {}
    for sd in session_days:
        for u in load_universe(sd):
            ik = str(u.get("instrument_key") or "").strip()
            if ik:
                out.setdefault(ik, set()).add(sd)
    return out


def cache_dir_size_bytes(cache_dir: Optional[Path] = None) -> int:
    root = cache_dir or default_cache_dir()
    if not root.is_dir():
        return 0
    total = 0
    for p in root.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except OSError:
                pass
    return total
