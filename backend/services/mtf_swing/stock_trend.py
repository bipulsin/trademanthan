"""Per-stock weekly EMA trend filter (v3). No look-ahead.

At each screening Friday (last session of the week), classify the symbol as
Bullish / Bearish / Neutral from weekly closes available up through that
session. Requires ``warmup_weeks`` (default 80) completed weeks before the
first label; shorter history → TREND_UNKNOWN.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Sequence, Tuple

from backend.services.mtf_swing.bars import Bar
from backend.services.mtf_swing.regime import ema, weekly_closes


@dataclass(frozen=True)
class TrendPoint:
    screen_date: date
    week: Tuple[int, int]
    weekly_close: float
    ema20: float
    trend_state: str  # Bullish | Bearish | Neutral


def classify_stock_trend(close: float, ema_val: float) -> str:
    if close > ema_val:
        return "Bullish"
    if close < ema_val:
        return "Bearish"
    return "Neutral"


def build_stock_trend_series(
    bars: Sequence[Bar],
    *,
    ema_length: int = 20,
    warmup_weeks: int = 80,
) -> List[TrendPoint]:
    """One trend label per completed week using only closes up to that week."""
    weeks = weekly_closes(bars)
    closes = [c for _, _, c in weeks]
    emas = ema(closes, int(ema_length))
    out: List[TrendPoint] = []
    need = max(int(ema_length), int(warmup_weeks))
    for i, (d, wk, close) in enumerate(weeks):
        ma = emas[i]
        if ma is None:
            continue
        if i + 1 < need:
            continue
        out.append(
            TrendPoint(
                screen_date=d,
                week=wk,
                weekly_close=float(close),
                ema20=float(ma),
                trend_state=classify_stock_trend(float(close), float(ma)),
            )
        )
    return out


def trend_by_screen_date(points: Sequence[TrendPoint]) -> Dict[str, TrendPoint]:
    return {p.screen_date.isoformat(): p for p in points}


def trend_allows(side: str, trend_state: str) -> bool:
    if trend_state == "Bullish":
        return side == "LONG"
    if trend_state == "Bearish":
        return side == "SHORT"
    return False
