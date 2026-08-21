"""10m candle helpers — Upstox / cache only (never Dhan)."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pytz

from backend.services.kavach_10m import aggregate_10m_bars
from backend.services.relative_strength_scanner import _sorted_candles
from backend.services.vajra.indicators import cumulative_vwap, ema_series

IST = pytz.timezone("Asia/Kolkata")


def _to_ist(dt: Any) -> Optional[datetime]:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
    try:
        parsed = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return IST.localize(parsed)
    return parsed.astimezone(IST)


def fetch_5m_candles(
    instrument_key: str,
    session_date: date,
    *,
    lookback_days: int = 8,
) -> List[Dict[str, Any]]:
    """Fetch 5m candles via Upstox (historical). Never uses Dhan."""
    from backend.services.upstox_service import UpstoxService

    svc = UpstoxService()
    raw = svc.get_historical_candles_by_instrument_key(
        instrument_key,
        interval="minutes/5",
        days_back=lookback_days,
        range_end_date=session_date,
    )
    if not raw:
        return []
    return _sorted_candles(raw)


def day_bars_10m_with_indicators(
    candles_5m: List[Dict[str, Any]],
    session_date: date,
) -> List[Dict[str, Any]]:
    """Aggregate 5m→10m for session_date and attach EMA5/10 + session VWAP."""
    if not candles_5m:
        return []
    bars10 = aggregate_10m_bars(candles_5m)
    day = session_date.isoformat()
    day_bars: List[Dict[str, Any]] = []
    for b in bars10:
        be = b.get("bar_end")
        be_ist = _to_ist(be)
        if be_ist is None or be_ist.strftime("%Y-%m-%d") != day:
            continue
        day_bars.append({**b, "bar_end": be_ist})
    if not day_bars:
        return []
    closes = [float(x["close"]) for x in day_bars]
    highs = [float(x["high"]) for x in day_bars]
    lows = [float(x["low"]) for x in day_bars]
    vols = [float(x.get("volume") or 0) for x in day_bars]
    ema5s = ema_series(closes, 5)
    ema10s = ema_series(closes, 10)
    try:
        vwaps = cumulative_vwap(highs, lows, closes, vols)
    except Exception:
        typical = [(h + l + c) / 3.0 for h, l, c in zip(highs, lows, closes)]
        vwaps = []
        cum_pv = 0.0
        cum_v = 0.0
        for tp, v in zip(typical, vols):
            cum_pv += tp * max(v, 1e-9)
            cum_v += max(v, 1e-9)
            vwaps.append(cum_pv / cum_v)
    out = []
    for i, b in enumerate(day_bars):
        out.append(
            {
                **b,
                "ema5": ema5s[i],
                "ema10": ema10s[i],
                "vwap": vwaps[i] if i < len(vwaps) else None,
            }
        )
    return out
