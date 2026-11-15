"""Candles for one contract, keeping volume and open interest.

Cached apart from the EMA scan. An empty or short response is not cached, so a
rate-limit miss is fetched again on the next run.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.services.ema_scan.fetch import (
    _chunks,
    load_future_index,
    make_upstox,
    repo_root,
    resolve_contract,
)
from backend.services.mtf_swing.data import _fetch_expired_rows
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)


def cache_dir() -> Path:
    mounted = Path("/home/ubuntu/trademanthan/data")
    base = mounted if mounted.is_dir() else repo_root() / "data"
    path = base / "big_move" / "cache"
    path.mkdir(parents=True, exist_ok=True)
    return path


def data_dir() -> Path:
    mounted = Path("/home/ubuntu/trademanthan/data")
    base = mounted if mounted.is_dir() else repo_root() / "data"
    path = base / "big_move"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _cache_file(instrument_key: str, kind: str, start: date, end: date) -> Path:
    safe = instrument_key.replace("|", "_").replace("/", "_").replace(":", "_").replace(" ", "_")
    return cache_dir() / f"{safe}_{kind}_{start.isoformat()}_{end.isoformat()}.json"


def _normalize(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_ts: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict) or "timestamp" not in row:
            continue
        try:
            item = {
                "timestamp": row["timestamp"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume") or 0.0),
            }
        except (KeyError, TypeError, ValueError):
            continue
        oi = row.get("oi")
        if oi not in (None, ""):
            try:
                item["oi"] = float(oi)
            except (TypeError, ValueError):
                pass
        by_ts[str(item["timestamp"])] = item
    return [by_ts[key] for key in sorted(by_ts)]


def _covers(rows: List[Dict[str, Any]], end: date) -> bool:
    latest = ""
    for row in rows:
        stamp = str(row.get("timestamp") or "")[:10]
        if stamp > latest:
            latest = stamp
    if len(latest) < 10:
        return False
    try:
        return date.fromisoformat(latest) >= end - timedelta(days=6)
    except ValueError:
        return False


def fetch_candles(
    upstox: UpstoxService,
    instrument_key: str,
    start: date,
    end: date,
    kind: str,
) -> List[Dict[str, Any]]:
    """``kind`` is ``5m`` or ``1d``. One contract only — nothing is stitched."""
    if not instrument_key or end < start:
        return []
    path = _cache_file(instrument_key, kind, start, end)
    if path.is_file():
        try:
            cached = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            cached = None
        if isinstance(cached, list) and cached:
            return cached
    expired = instrument_key.count("|") >= 2
    rows: List[Dict[str, Any]] = []
    if kind == "5m":
        if expired:
            for chunk_start, chunk_end in _chunks(start, end, 28):
                rows.extend(_fetch_expired_rows(upstox, instrument_key, "5minute", chunk_start, chunk_end))
        else:
            for _chunk_start, chunk_end in _chunks(start, end, 28):
                raw = upstox.get_historical_candles_by_instrument_key(
                    instrument_key,
                    interval="minutes/5",
                    days_back=28,
                    range_end_date=chunk_end,
                )
                rows.extend(raw or [])
    elif expired:
        rows.extend(_fetch_expired_rows(upstox, instrument_key, "day", start, end))
    else:
        days_back = max(1, (end - start).days + 2)
        raw = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="days/1",
            days_back=days_back,
            range_end_date=end,
        )
        rows.extend(raw or [])
    normalized = _normalize(rows)
    span = (end - start).days
    if kind == "5m":
        enough = len(normalized) >= (1500 if span > 60 else 80)
    else:
        enough = len(normalized) >= (40 if span > 60 else 8)
    if normalized and enough and _covers(normalized, end):
        try:
            path.write_text(json.dumps(normalized), encoding="utf-8")
        except Exception as exc:
            logger.info("big_move cache write failed %s: %s", path.name, exc)
    return normalized
