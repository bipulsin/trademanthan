"""Candle fetch helpers (Upstox + demo synthetic)."""
from __future__ import annotations

import calendar
import logging
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def month_bounds(yyyy_mm: str) -> tuple[date, date]:
    y, m = [int(x) for x in yyyy_mm.split("-")]
    start = date(y, m, 1)
    end = date(y, m, calendar.monthrange(y, m)[1])
    return start, end


def months_back(count: int) -> List[str]:
    out: List[str] = []
    now = datetime.now(timezone.utc)
    y, m = now.year, now.month
    for _ in range(int(count)):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return out


def generate_demo_candles(yyyy_mm: str, timeframe_id: str, seed: int = 1) -> List[Dict[str, Any]]:
    start, end = month_bounds(yyyy_mm)
    step = 60 if timeframe_id == "1hr" else (15 if timeframe_id == "15min" else 10)
    times: List[datetime] = []
    t = datetime(start.year, start.month, start.day, 3, 45, tzinfo=timezone.utc)
    end_dt = datetime(end.year, end.month, end.day, 10, 0, tzinfo=timezone.utc)
    while t <= end_dt:
        if t.weekday() < 5:
            times.append(t)
        t = t + timedelta(minutes=step)
        if t.hour > 10 or (t.hour == 10 and t.minute > 0):
            t = t + timedelta(days=1)
            t = t.replace(hour=3, minute=45, second=0, microsecond=0)

    pattern: List[float] = []
    p = 100.0 + (seed % 17)

    def push(n: int, delta: float) -> None:
        nonlocal p
        for _ in range(n):
            p += delta
            pattern.append(p)

    push(25, 0.25)
    push(12, -1.6)
    push(10, 1.3)
    push(15, -1.15)
    push(1, -2.2)
    push(18, 2.1)
    push(8, 0.9)
    push(10, -0.85)
    push(12, 1.35)
    push(16, -1.55)
    for i in range(20):
        push(1, 0.35 if i % 2 == 0 else -0.3)

    scale = 1 + (seed % 5) * 0.05
    base = 900 + (seed % 40) * 5
    candles = []
    for i, ts in enumerate(times):
        raw = pattern[i % len(pattern)]
        cycle = i // len(pattern)
        close = base + raw * scale + cycle * 3
        prev = (
            close
            if i == 0
            else base + pattern[(i - 1) % len(pattern)] * scale + ((i - 1) // len(pattern)) * 3
        )
        open_ = prev
        candles.append(
            {
                "timestamp": ts.isoformat(),
                "open": open_,
                "high": max(open_, close) + 0.8,
                "low": min(open_, close) - 0.8,
                "close": close,
                "volume": 1000 + (i % 50) * 10,
            }
        )
    return candles


def fetch_month_candles_upstox(
    instrument_key: str, interval: str, yyyy_mm: str
) -> List[Dict[str, Any]]:
    """Fetch one calendar month via existing UpstoxService."""
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    start, end = month_bounds(yyyy_mm)
    days = (end - start).days + 2
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    if hasattr(ux, "reload_token_from_storage"):
        try:
            ux.reload_token_from_storage()
        except Exception:
            pass
    raw = ux.get_historical_candles_by_instrument_key(
        instrument_key,
        interval=interval,
        days_back=days,
        range_end_date=end,
    )
    if not raw:
        return []
    # Filter to month
    out = []
    for c in raw:
        ts = str(c.get("timestamp") or "")
        if len(ts) >= 7 and ts[:7] == yyyy_mm:
            out.append(
                {
                    "timestamp": c.get("timestamp"),
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                    "volume": float(c.get("volume") or 0),
                }
            )
    out.sort(key=lambda x: str(x["timestamp"]))
    return out
