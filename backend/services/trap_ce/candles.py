"""Session 10m bars: Upstox 5m → Kavach session-aligned 10m (09:15, 09:25, …).

Do not use clock-floor 1m→10m (9:15 would bucket as 9:10). Native ``minutes/10``
is not on the Upstox V2 1m-aggregate path; V3 ``minutes/10`` exists for NSE but
5m pairing matches existing Kavach/open-low session boundaries.
"""
from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz

from backend.services.kavach_10m import aggregate_10m_bars
from backend.services.trap_ce.config import BAR_MINUTES, MARKET_CLOSE, MARKET_OPEN
from backend.services.upstox_service import _parse_ts_to_aware_ist

IST = pytz.timezone("Asia/Kolkata")


def default_cache_dir() -> Path:
    ec2 = Path("/home/ubuntu/trademanthan/data/trap_ce_candle_cache")
    if Path("/home/ubuntu/trademanthan/data").is_dir():
        ec2.mkdir(parents=True, exist_ok=True)
        return ec2
    root = Path(__file__).resolve().parents[3]
    p = root / "data" / "trap_ce_candle_cache"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _cache_path(cache_dir: Path, instrument_key: str, session_date: date) -> Path:
    safe = (instrument_key or "").replace("|", "__")
    return cache_dir / f"{safe}_{session_date.isoformat()}.json"


def session_10m_from_5m(candles_5m: List[Dict[str, Any]], session_date: date) -> List[Dict[str, Any]]:
    """Aggregate 5m → 10m and keep the IST session; timestamp = bar start."""
    out: List[Dict[str, Any]] = []
    for b in aggregate_10m_bars(candles_5m or []):
        end = b.get("bar_end")
        if end is None:
            ts = _parse_ts_to_aware_ist(b.get("timestamp"))
            if ts is None:
                continue
            end = ts.astimezone(IST) + timedelta(minutes=5)
        if end.tzinfo is None:
            end = IST.localize(end)
        else:
            end = end.astimezone(IST)
        start = end - timedelta(minutes=BAR_MINUTES)
        if start.date() != session_date:
            continue
        if start.time() < MARKET_OPEN or start.time() > MARKET_CLOSE:
            continue
        out.append(
            {
                "timestamp": start.isoformat(),
                "open": float(b.get("open") or 0),
                "high": float(b.get("high") or 0),
                "low": float(b.get("low") or 0),
                "close": float(b.get("close") or 0),
                "volume": float(b.get("volume") or 0),
                "bar_start": start,
                "bar_end": end,
            }
        )
    out.sort(key=lambda x: x["bar_start"])
    return out


def load_cached_10m(cache_dir: Path, instrument_key: str, session_date: date) -> Optional[List[Dict[str, Any]]]:
    path = _cache_path(cache_dir, instrument_key, session_date)
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    bars = raw.get("bars") if isinstance(raw, dict) else raw
    if not isinstance(bars, list) or not bars:
        return None
    hydrated = _hydrate_cached_10m(bars, session_date)
    return hydrated or None


def _hydrate_cached_10m(bars: List[Dict[str, Any]], session_date: date) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for b in bars:
        ts = _parse_ts_to_aware_ist(b.get("timestamp") or b.get("bar_start"))
        if ts is None:
            continue
        ts = ts.astimezone(IST)
        if ts.date() != session_date:
            continue
        end_raw = b.get("bar_end")
        end = _parse_ts_to_aware_ist(end_raw) if end_raw else ts + timedelta(minutes=BAR_MINUTES)
        if end is None:
            end = ts + timedelta(minutes=BAR_MINUTES)
        end = end.astimezone(IST)
        out.append(
            {
                "timestamp": ts.isoformat(),
                "open": float(b.get("open") or 0),
                "high": float(b.get("high") or 0),
                "low": float(b.get("low") or 0),
                "close": float(b.get("close") or 0),
                "volume": float(b.get("volume") or 0),
                "bar_start": ts,
                "bar_end": end,
            }
        )
    out.sort(key=lambda x: x["bar_start"])
    return out


def save_cached_10m(
    cache_dir: Path,
    instrument_key: str,
    session_date: date,
    bars: List[Dict[str, Any]],
) -> None:
    path = _cache_path(cache_dir, instrument_key, session_date)
    payload = {
        "instrument_key": instrument_key,
        "session_date": session_date.isoformat(),
        "bars": [
            {
                "timestamp": b["bar_start"].isoformat() if hasattr(b.get("bar_start"), "isoformat") else b.get("timestamp"),
                "open": b.get("open"),
                "high": b.get("high"),
                "low": b.get("low"),
                "close": b.get("close"),
                "volume": b.get("volume"),
                "bar_end": b["bar_end"].isoformat() if hasattr(b.get("bar_end"), "isoformat") else str(b.get("bar_end") or ""),
            }
            for b in bars
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def fetch_session_10m(
    upstox: Any,
    instrument_key: str,
    session_date: date,
    *,
    cache_dir: Optional[Path] = None,
    symbol_pause_sec: float = 0.12,
    days_back: int = 2,
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
    bars = session_10m_from_5m(raw or [], session_date)
    if bars:
        save_cached_10m(cache_dir, instrument_key, session_date, bars)
    return bars
