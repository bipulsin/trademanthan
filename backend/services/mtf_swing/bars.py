"""Session bars, ISO weeks, and trading-day filters."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Iterable, List, Optional, Sequence, Set, Tuple

import pytz

IST = pytz.timezone("Asia/Kolkata")
SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)
WeekKey = Tuple[int, int]


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    contract: str = ""

    def __post_init__(self) -> None:
        self.ts = ensure_ist(self.ts)
        self.open = float(self.open)
        self.high = float(self.high)
        self.low = float(self.low)
        self.close = float(self.close)
        self.contract = str(self.contract or "")

    @property
    def session_date(self) -> date:
        return self.ts.date()


def ensure_ist(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return IST.localize(ts)
    return ts.astimezone(IST)


def week_key(d: date) -> WeekKey:
    iso = d.isocalendar()
    return (int(iso[0]), int(iso[1]))


def next_week_key(d: date) -> WeekKey:
    return week_key(d + timedelta(days=7))


def is_trading_day(d: date, holidays: Set[date]) -> bool:
    return d.weekday() < 5 and d not in holidays


def in_session(ts: datetime) -> bool:
    t = ensure_ist(ts).time()
    return SESSION_OPEN <= t <= SESSION_CLOSE


def filter_session_bars(bars: Sequence[Bar], holidays: Set[date]) -> List[Bar]:
    out: List[Bar] = []
    for b in bars:
        if not is_trading_day(b.session_date, holidays):
            continue
        if not in_session(b.ts):
            continue
        out.append(b)
    out.sort(key=lambda b: b.ts)
    return out


def shift_bar(bar: Bar, delta: float) -> Bar:
    if not delta:
        return bar
    return Bar(
        ts=bar.ts,
        open=bar.open + delta,
        high=bar.high + delta,
        low=bar.low + delta,
        close=bar.close + delta,
        contract=bar.contract,
    )


def back_adjust_to_last(bars: Sequence[Bar]) -> List[Bar]:
    """Panama back-adjust so the last bar stays raw and roll gaps are removed.

    Gap at a contract change is ``new.open - prev.close``, added to every earlier bar.
    A constant shift does not change later comparisons; only the roll jump is removed.
    """
    n = len(bars)
    if n == 0:
        return []
    adj = [0.0] * n
    cumulative = 0.0
    for i in range(n - 1, 0, -1):
        prev, cur = bars[i - 1], bars[i]
        if prev.contract and cur.contract and prev.contract != cur.contract:
            cumulative += cur.open - prev.close
        adj[i - 1] = cumulative
    return [shift_bar(b, adj[i]) for i, b in enumerate(bars)]


def roll_gap(prev: Optional[Bar], bar: Bar) -> float:
    if prev is None:
        return 0.0
    if prev.contract and bar.contract and prev.contract != bar.contract:
        return bar.open - prev.close
    return 0.0


def last_index_by_date(bars: Sequence[Bar]) -> dict:
    last = {}
    for i, b in enumerate(bars):
        last[b.session_date] = i
    return last


def last_index_by_week(bars: Sequence[Bar]) -> dict:
    last = {}
    for i, b in enumerate(bars):
        last[week_key(b.session_date)] = i
    return last


def bars_for_week(bars: Sequence[Bar], key: WeekKey) -> List[Bar]:
    return [b for b in bars if week_key(b.session_date) == key]


def parse_ts(raw) -> Optional[datetime]:
    if raw is None or raw == "":
        return None
    if isinstance(raw, datetime):
        return ensure_ist(raw)
    s = str(raw).strip().replace("Z", "+00:00")
    if not s:
        return None
    try:
        if "T" not in s and " " in s:
            s = s.replace(" ", "T", 1)
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return ensure_ist(dt)


def iso(ts: Optional[datetime]) -> str:
    if ts is None:
        return ""
    return ensure_ist(ts).isoformat()
