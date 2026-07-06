"""IST session timing helpers for BTST backtest orchestration."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import List, Optional, Tuple

import pytz

IST = pytz.timezone("Asia/Kolkata")


def parse_hhmm(s: str) -> Tuple[int, int]:
    h, m = str(s).strip().split(":")[:2]
    return int(h), int(m)


def ist_dt(trade_date: date, hhmm: str) -> datetime:
    h, m = parse_hhmm(hhmm)
    return IST.localize(datetime.combine(trade_date, time(h, m)))


def last_n_trading_days(n: int, *, end: Optional[date] = None) -> List[date]:
    end = end or datetime.now(IST).date()
    out: List[date] = []
    d = end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return list(reversed(out))


def next_trading_day(d: date) -> date:
    n = d + timedelta(days=1)
    while n.weekday() >= 5:
        n += timedelta(days=1)
    return n


def bar_session_date(ts: str) -> Optional[date]:
    if not ts:
        return None
    try:
        s = str(ts).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.astimezone(IST).date()
    except (TypeError, ValueError):
        return None


def bar_minutes(ts: str) -> Optional[int]:
    try:
        s = str(ts).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        dt = dt.astimezone(IST)
        return dt.hour * 60 + dt.minute
    except (TypeError, ValueError):
        return None


def bars_on_session(candles: List[dict], trade_date: date) -> List[dict]:
    out = []
    for c in candles or []:
        sd = bar_session_date(c.get("timestamp"))
        if sd == trade_date:
            out.append(c)
    return sorted(out, key=lambda x: str(x.get("timestamp") or ""))


def close_at_or_before(candles: List[dict], trade_date: date, hhmm: str) -> Optional[float]:
    """Last 5m bar close at or before hh:mm on trade_date."""
    h, m = parse_hhmm(hhmm)
    target = h * 60 + m
    best = None
    best_t = -1
    for c in bars_on_session(candles, trade_date):
        tm = bar_minutes(c.get("timestamp"))
        if tm is None or tm > target:
            continue
        if tm >= best_t:
            best_t = tm
            best = c
    if best is None:
        return None
    return float(best.get("close") or 0)


def cumulative_volume_through(candles: List[dict], trade_date: date, hhmm: str) -> float:
    h, m = parse_hhmm(hhmm)
    target = h * 60 + m
    total = 0.0
    for c in bars_on_session(candles, trade_date):
        tm = bar_minutes(c.get("timestamp"))
        if tm is None or tm > target:
            continue
        total += float(c.get("volume") or 0)
    return total
