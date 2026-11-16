"""OpenHighLow signal detection (bullish / bearish setup)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time
from typing import List, Optional, Sequence

from backend.services.open_high_low.bars import Bar, bar_at, first_vwap, session_bars
from backend.services.open_high_low.config import (
    FIRST_CANDLE,
    SECOND_CANDLE,
    THIRD_CANDLE,
    TICK_EPS,
)


@dataclass
class Setup:
    side: str  # BULLISH | BEARISH
    session: date
    first: Bar
    second: Bar
    third: Bar
    vwap: float
    prev_high: float
    prev_low: float
    open_eq_extreme: str  # YES | NO


def _near(a: float, b: float, eps: float = TICK_EPS) -> bool:
    return abs(float(a) - float(b)) <= eps


def open_eq_extreme_flag(side: str, first: Bar) -> str:
    if side == "BULLISH":
        return "YES" if _near(first.open, first.low) else "NO"
    if side == "BEARISH":
        return "YES" if _near(first.open, first.high) else "NO"
    return "NO"


def detect_setup(
    bars_5m: Sequence[Bar],
    session: date,
    prev_high: float,
    prev_low: float,
) -> Optional[Setup]:
    """At most one side per day. Bullish checked first; if both qualify, bullish wins
    (spec: cannot be both sides same day — prefer first match / bullish)."""
    first = bar_at(bars_5m, session, FIRST_CANDLE)
    second = bar_at(bars_5m, session, SECOND_CANDLE)
    third = bar_at(bars_5m, session, THIRD_CANDLE)
    if first is None or second is None or third is None:
        return None

    vwap = first_vwap(bars_5m, session, FIRST_CANDLE)
    if vwap is None:
        return None

    bullish = (
        first.close > prev_high
        and first.close > vwap
        and second.close > first.high
    )
    bearish = (
        first.close < prev_low
        and first.close < vwap
        and first.close < first.open  # red
        and second.close < first.low
    )

    if bullish and bearish:
        # Spec: cannot be both; take neither (ambiguous).
        return None
    if bullish:
        return Setup(
            side="BULLISH",
            session=session,
            first=first,
            second=second,
            third=third,
            vwap=vwap,
            prev_high=prev_high,
            prev_low=prev_low,
            open_eq_extreme=open_eq_extreme_flag("BULLISH", first),
        )
    if bearish:
        return Setup(
            side="BEARISH",
            session=session,
            first=first,
            second=second,
            third=third,
            vwap=vwap,
            prev_high=prev_high,
            prev_low=prev_low,
            open_eq_extreme=open_eq_extreme_flag("BEARISH", first),
        )
    return None


def bars_from_entry(bars_5m: Sequence[Bar], session: date, entry_time: time) -> List[Bar]:
    day = session_bars(bars_5m, session)
    return [b for b in day if b.ts.time() >= entry_time]
