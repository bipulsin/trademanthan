"""Historical OHLCV for generic security chart (Upstox REST only)."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz

from backend.config import settings
from backend.services.upstox_service import UpstoxService
from backend.services.vajra.timeframes import fetch_config

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

CHART_TIMEFRAMES = ("5m", "15m", "30m", "1hr", "1d")
DEFAULT_CHART_TF = "5m"


def normalize_chart_timeframe(tf: Optional[str]) -> str:
    v = (tf or DEFAULT_CHART_TF).strip().lower()
    if v in ("1h", "60m"):
        v = "1hr"
    if v not in CHART_TIMEFRAMES:
        raise ValueError(f"Invalid timeframe: {tf}")
    return v


def _ts_to_unix(time_val: Any, *, daily: bool) -> Optional[int]:
    if time_val is None:
        return None
    if isinstance(time_val, (int, float)):
        ts = float(time_val)
        if ts > 1e12:
            ts /= 1000.0
        return int(ts)
    if isinstance(time_val, str):
        s = time_val.strip()
        try:
            if daily and len(s) >= 10:
                dt = datetime.strptime(s[:10], "%Y-%m-%d")
                return int(IST.localize(dt.replace(hour=0, minute=0, second=0)).timestamp())
            from dateutil import parser

            dt = parser.parse(s)
            if dt.tzinfo is None:
                dt = IST.localize(dt)
            return int(dt.timestamp())
        except Exception:
            return None
    return None


def candles_to_lightweight(
    raw: List[dict],
    *,
    timeframe: str,
) -> List[Dict[str, float]]:
    daily = timeframe == "1d"
    out: List[Dict[str, float]] = []
    for c in sorted(raw, key=lambda x: x.get("timestamp") or ""):
        t = _ts_to_unix(c.get("timestamp"), daily=daily)
        if t is None:
            continue
        o, h, l, cl = (
            float(c.get("open") or 0),
            float(c.get("high") or 0),
            float(c.get("low") or 0),
            float(c.get("close") or 0),
        )
        vol = float(c.get("volume") or 0)
        if h <= 0 or l <= 0 or cl <= 0:
            continue
        out.append(
            {
                "time": t,
                "open": o,
                "high": h,
                "low": l,
                "close": cl,
                "volume": vol,
            }
        )
    return out


def fetch_chart_candles(
    instrument_key: str,
    timeframe: Optional[str] = None,
) -> Dict[str, Any]:
    tf = normalize_chart_timeframe(timeframe)
    cfg = fetch_config(tf)
    interval = str(cfg["interval"])
    days_back = int(cfg["days_back"])

    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()
    raw = ux.get_historical_candles_by_instrument_key(
        instrument_key.replace(":", "|"),
        interval=interval,
        days_back=days_back,
    ) or []
    bars = candles_to_lightweight(raw, timeframe=tf)
    return {
        "timeframe": tf,
        "interval": interval,
        "bars": bars,
        "count": len(bars),
    }
