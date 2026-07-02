"""Volume metrics for Kavach RS scanner — closed-bar ratio + time-of-day cumulative."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.vajra.indicators import ema_series

IST = pytz.timezone("Asia/Kolkata")
BAR_MINUTES = 5
VOLUME_EMA_PERIOD = 20
TOD_LOOKBACK_SESSIONS = 5


def _parse_ist(ts: Any) -> Optional[datetime]:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        try:
            dt = datetime.strptime(str(ts)[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None
    return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def last_closed_bar_index(candles: List[Dict], *, now: Optional[datetime] = None) -> int:
    """Index of the last fully closed 5m bar, or -1 if none."""
    if not candles:
        return -1
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    for i in range(len(candles) - 1, -1, -1):
        dt = _parse_ist(candles[i].get("timestamp"))
        if dt is None:
            continue
        bar_end = dt + timedelta(minutes=BAR_MINUTES)
        if bar_end <= now:
            return i
    return -1


def closed_bar_volume_ratio(
    volumes: List[float], *, closed_idx: int, ema_period: int = VOLUME_EMA_PERIOD
) -> Tuple[float, float, float]:
    """Return (closed_volume, vol_ema_at_close, ratio) using only closed bars."""
    if closed_idx < 0 or not volumes:
        return 0.0, 0.0, 0.0
    slice_v = volumes[: closed_idx + 1]
    if not slice_v:
        return 0.0, 0.0, 0.0
    closed_vol = slice_v[-1]
    ema_s = ema_series(slice_v, ema_period)
    vol_ema = ema_s[-1] if ema_s else 0.0
    ratio = (closed_vol / vol_ema) if vol_ema > 0 else 0.0
    return closed_vol, vol_ema, ratio


def cumulative_volume_tod_ratio(
    candles: List[Dict], *, closed_idx: int, now: Optional[datetime] = None
) -> Optional[float]:
    """Cumulative session volume at current clock time vs avg of prior sessions."""
    if closed_idx < 0:
        return None
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    today = now.strftime("%Y-%m-%d")
    closed_dt = _parse_ist(candles[closed_idx].get("timestamp"))
    if closed_dt is None:
        return None
    cutoff = closed_dt.time()

    by_date: Dict[str, List[Tuple[datetime, float]]] = {}
    for i, c in enumerate(candles[: closed_idx + 1]):
        dt = _parse_ist(c.get("timestamp"))
        if dt is None:
            continue
        d = dt.strftime("%Y-%m-%d")
        by_date.setdefault(d, []).append((dt, _f(c.get("volume"))))

    if today not in by_date:
        return None

    def cum_to_cutoff(day: str) -> float:
        total = 0.0
        for dt, vol in by_date.get(day, []):
            if dt.time() <= cutoff:
                total += vol
        return total

    today_cum = cum_to_cutoff(today)
    prior_days = sorted(d for d in by_date if d < today)
    if not prior_days:
        return None
    lookback = prior_days[-TOD_LOOKBACK_SESSIONS:]
    priors = [cum_to_cutoff(d) for d in lookback if cum_to_cutoff(d) > 0]
    if not priors:
        return None
    avg_prior = sum(priors) / len(priors)
    if avg_prior <= 0:
        return None
    return today_cum / avg_prior


def volume_participation_label(
    bar_ratio: float, tod_ratio: Optional[float]
) -> str:
    """Primary label: time-of-day when available, else closed-bar ratio."""
    if tod_ratio is not None:
        if tod_ratio >= 1.2:
            return "High"
        if tod_ratio >= 0.65:
            return "Average"
        return "Low"
    if bar_ratio >= 1.2:
        return "High"
    if bar_ratio >= 0.65:
        return "Average"
    return "Low"
