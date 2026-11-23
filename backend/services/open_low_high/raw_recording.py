"""Record raw Upstox WS frames + REST responses 09:00–09:35 IST to a persistent volume.

Retention: 14 days (configurable). Size estimate documented in ``estimate_daily_bytes``.
Staging-safe: no-op when ``OLH_RAW_RECORD_ENABLED`` is false (default).
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import threading
import time
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pytz

from backend.services.open_low_high.clock import olh_now
from backend.services.open_low_high.config import (
    OLH_RAW_RECORD_DIR,
    OLH_RAW_RECORD_ENABLED,
    OLH_RAW_RECORD_END,
    OLH_RAW_RECORD_RETENTION_DAYS,
    OLH_RAW_RECORD_START,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Rough size model (145 EQ + Nifty + ~145 FUT, full mode, 09:00–09:35):
# ~80–150 msg/s peak × ~400–1200 B decoded JSON after MessageToDict ≈ 2–8 MB/min
# × 35 min ≈ 70–280 MB/day WS; REST canary/phase samples ≈ 2–10 MB/day.
# Estimate mid: ~150 MB/day × 14d ≈ ~2.1 GB volume.
ESTIMATE_WS_MB_PER_DAY = 150
ESTIMATE_REST_MB_PER_DAY = 5

_lock = threading.RLock()
_ws_fh = None  # type: ignore
_ws_path: Optional[Path] = None
_ws_day: Optional[date] = None
_bytes_written_today = 0


def estimate_daily_bytes() -> Dict[str, Any]:
    return {
        "ws_mb_per_day_est": ESTIMATE_WS_MB_PER_DAY,
        "rest_mb_per_day_est": ESTIMATE_REST_MB_PER_DAY,
        "retention_days": OLH_RAW_RECORD_RETENTION_DAYS,
        "volume_mb_est": (ESTIMATE_WS_MB_PER_DAY + ESTIMATE_REST_MB_PER_DAY)
        * OLH_RAW_RECORD_RETENTION_DAYS,
        "window": f"{OLH_RAW_RECORD_START.strftime('%H:%M')}-{OLH_RAW_RECORD_END.strftime('%H:%M')} IST",
        "note": "Mid-case for 145+Nifty+FUT full mode; peak open can 2×.",
    }


def recording_root() -> Path:
    root = Path(OLH_RAW_RECORD_DIR or "data/olh_raw_recordings")
    root.mkdir(parents=True, exist_ok=True)
    return root


def in_record_window(now: Optional[datetime] = None) -> bool:
    t = now or olh_now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    tt = t.time()
    return OLH_RAW_RECORD_START <= tt < OLH_RAW_RECORD_END


def _close_ws_fh() -> None:
    global _ws_fh, _ws_path, _ws_day
    if _ws_fh is not None:
        try:
            _ws_fh.close()
        except Exception:
            pass
    _ws_fh = None
    _ws_path = None
    _ws_day = None


def _ensure_ws_fh(day: date):
    global _ws_fh, _ws_path, _ws_day, _bytes_written_today
    if _ws_fh is not None and _ws_day == day:
        return _ws_fh
    _close_ws_fh()
    day_dir = recording_root() / day.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / "ws_frames.jsonl.gz"
    _ws_fh = gzip.open(path, "at", encoding="utf-8")
    _ws_path = path
    _ws_day = day
    _bytes_written_today = path.stat().st_size if path.exists() else 0
    return _ws_fh


def record_ws_frame(decoded: Dict[str, Any], *, now: Optional[datetime] = None) -> bool:
    """Append one decoded FeedResponse dict (JSON line, gzip)."""
    if not OLH_RAW_RECORD_ENABLED:
        return False
    t = now or olh_now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    if not in_record_window(t):
        return False
    try:
        with _lock:
            fh = _ensure_ws_fh(t.date())
            line = json.dumps(
                {"ts": t.isoformat(), "frame": decoded},
                default=str,
                separators=(",", ":"),
            )
            fh.write(line + "\n")
            fh.flush()
            global _bytes_written_today
            _bytes_written_today += len(line) + 1
        return True
    except Exception:
        logger.debug("olh raw_recording ws failed", exc_info=True)
        return False


def record_rest_response(
    *,
    url: str,
    status_code: Any,
    headers: Optional[Dict[str, Any]] = None,
    body: Any = None,
    label: str = "",
    now: Optional[datetime] = None,
) -> bool:
    """Persist REST status + headers + first 2KB of body."""
    if not OLH_RAW_RECORD_ENABLED:
        return False
    t = now or olh_now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    if not in_record_window(t):
        return False
    try:
        body_s = body if isinstance(body, (str, bytes)) else json.dumps(body, default=str)
        if isinstance(body_s, bytes):
            body_preview = body_s[:2048].decode("utf-8", errors="replace")
        else:
            body_preview = str(body_s)[:2048]
        hdrs = {}
        if headers:
            for k, v in list(headers.items())[:40]:
                hdrs[str(k)] = str(v)[:200]
        day_dir = recording_root() / t.date().isoformat()
        day_dir.mkdir(parents=True, exist_ok=True)
        path = day_dir / "rest_responses.jsonl"
        row = {
            "ts": t.isoformat(),
            "label": label,
            "url": str(url)[:500],
            "status": status_code,
            "headers": hdrs,
            "body_preview": body_preview,
            "body_preview_bytes": len(body_preview),
        }
        with _lock:
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, default=str) + "\n")
        return True
    except Exception:
        logger.debug("olh raw_recording rest failed", exc_info=True)
        return False


def prune_old_recordings(*, retention_days: Optional[int] = None) -> Dict[str, Any]:
    days = int(retention_days if retention_days is not None else OLH_RAW_RECORD_RETENTION_DAYS)
    root = recording_root()
    cutoff = (olh_now().date() - timedelta(days=days))
    removed = []
    for child in sorted(root.iterdir()) if root.exists() else []:
        if not child.is_dir():
            continue
        try:
            d = date.fromisoformat(child.name)
        except ValueError:
            continue
        if d < cutoff:
            # Remove files then dir
            for p in child.rglob("*"):
                if p.is_file():
                    try:
                        p.unlink()
                    except Exception:
                        pass
            try:
                child.rmdir()
                removed.append(child.name)
            except Exception:
                removed.append(f"{child.name}?")
    return {"ok": True, "removed": removed, "cutoff": cutoff.isoformat(), "retention_days": days}


def recording_status() -> Dict[str, Any]:
    return {
        "enabled": OLH_RAW_RECORD_ENABLED,
        "dir": str(recording_root()),
        "in_window": in_record_window(),
        "bytes_written_today": _bytes_written_today,
        "estimate": estimate_daily_bytes(),
        "ws_path": str(_ws_path) if _ws_path else None,
    }
