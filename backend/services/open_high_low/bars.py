"""Candle bar helpers for OpenHighLow."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import List, Optional, Sequence

import pytz

IST = pytz.timezone("Asia/Kolkata")


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    contract: str = ""

    def __post_init__(self) -> None:
        self.ts = ensure_ist(self.ts)
        self.open = float(self.open)
        self.high = float(self.high)
        self.low = float(self.low)
        self.close = float(self.close)
        self.volume = float(self.volume or 0.0)
        self.contract = str(self.contract or "")

    @property
    def session_date(self) -> date:
        return self.ts.date()

    @property
    def typical(self) -> float:
        return (self.high + self.low + self.close) / 3.0


def ensure_ist(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return IST.localize(ts)
    return ts.astimezone(IST)


def parse_ts(raw) -> Optional[datetime]:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return ensure_ist(raw)
    s = str(raw).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return ensure_ist(datetime.fromisoformat(s))
    except ValueError:
        return None


def bar_at(bars: Sequence[Bar], session: date, t: time) -> Optional[Bar]:
    for b in bars:
        if b.session_date == session and b.ts.time() == t:
            return b
    return None


def session_bars(bars: Sequence[Bar], session: date) -> List[Bar]:
    out = [b for b in bars if b.session_date == session]
    out.sort(key=lambda b: b.ts)
    return out


def first_vwap(bars: Sequence[Bar], session: date, asof: time = time(9, 15)) -> Optional[float]:
    """VWAP from 09:15 through close of the candle that starts at ``asof``."""
    day = session_bars(bars, session)
    cum_tpv = 0.0
    cum_v = 0.0
    for b in day:
        if b.ts.time() < time(9, 15):
            continue
        if b.ts.time() > asof:
            break
        vol = b.volume if b.volume > 0 else 1.0
        cum_tpv += b.typical * vol
        cum_v += vol
        if b.ts.time() == asof:
            return cum_tpv / cum_v if cum_v > 0 else None
    return None
