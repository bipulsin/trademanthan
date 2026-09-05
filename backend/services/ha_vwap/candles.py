"""Session-aligned 10m bars (Upstox 5m paired) with disk cache under data/ha_vwap/."""
from __future__ import annotations

import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.services.ha_vwap.config import CANDLE_DAYS_BACK
from backend.services.trap_ce.candles import load_cached_10m, save_cached_10m, session_10m_from_5m


def default_cache_dir() -> Path:
    for p in (
        Path("/home/ubuntu/trademanthan/data/ha_vwap/cache"),
        Path("/home/ubuntu/twcto/data/ha_vwap/cache"),
    ):
        if p.parent.parent.is_dir() or p.parent.is_dir():
            try:
                p.mkdir(parents=True, exist_ok=True)
                if p.parent.exists():
                    return p
            except OSError:
                continue
    if Path("/home/ubuntu/trademanthan/data").is_dir():
        p = Path("/home/ubuntu/trademanthan/data/ha_vwap/cache")
        p.mkdir(parents=True, exist_ok=True)
        return p
    root = Path(__file__).resolve().parents[3]
    p = root / "data" / "ha_vwap" / "cache"
    p.mkdir(parents=True, exist_ok=True)
    return p


def default_out_dir() -> Path:
    for base in (Path("/home/ubuntu/twcto"), Path("/home/ubuntu/trademanthan")):
        if base.is_dir():
            p = base / "data" / "ha_vwap"
            p.mkdir(parents=True, exist_ok=True)
            return p
    root = Path(__file__).resolve().parents[3]
    p = root / "data" / "ha_vwap"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _cache_window_from_5m(
    cache_dir: Path,
    instrument_key: str,
    session_date: date,
    raw_5m: List[Dict[str, Any]],
    days_back: int,
) -> List[Dict[str, Any]]:
    wanted = None
    d = session_date
    end = session_date - timedelta(days=max(days_back, 2) + 3)
    while d >= end:
        if d.weekday() < 5:
            bars = session_10m_from_5m(raw_5m or [], d)
            if bars:
                save_cached_10m(cache_dir, instrument_key, d, bars)
            if d == session_date:
                wanted = bars
        d -= timedelta(days=1)
    return wanted or []


def fetch_session_10m(
    upstox: Any,
    instrument_key: str,
    session_date: date,
    *,
    cache_dir: Optional[Path] = None,
    symbol_pause_sec: float = 0.12,
    days_back: int = CANDLE_DAYS_BACK,
) -> List[Dict[str, Any]]:
    cache_dir = cache_dir or default_cache_dir()
    cached = load_cached_10m(cache_dir, instrument_key, session_date)
    if cached:
        return cached
    if symbol_pause_sec > 0:
        time.sleep(symbol_pause_sec)
    raw = upstox.get_historical_candles_by_instrument_key(
        instrument_key,
        interval="minutes/5",
        days_back=days_back,
        range_end_date=session_date,
    )
    bars = _cache_window_from_5m(cache_dir, instrument_key, session_date, raw or [], days_back)
    return bars
