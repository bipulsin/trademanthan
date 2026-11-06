"""MTF Swing defaults. See README.md for resolved ambiguities."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List, Tuple


BACKTEST_START = date(2025, 11, 1)
BACKTEST_END = date(2026, 10, 31)
WARMUP_DAYS = 21
# Sessions after the cutoff used only to finish open trades (TIME / SL / T1).
TAIL_SESSIONS = 10
MAX_SESSIONS = 10
CLUSTER_PCT = 0.003  # 0.3% of the lower price
QTY_LOTS = 1

TIMEFRAMES: Tuple[str, ...] = ("60m", "15m")
INTERVAL = {"60m": "hours/1", "15m": "minutes/15"}

TRADE_FIELDS = [
    "trade_id",
    "timeframe",
    "month",
    "symbol",
    "side",
    "screen_date",
    "entry_time",
    "entry_price",
    "entry_type",
    "lot_size",
    "qty_lots",
    "sl",
    "t1",
    "exit_time",
    "exit_price",
    "exit_reason",
    "sessions_held",
    "gross_pnl",
    "charges",
    "net_pnl",
    "r_multiple",
    "run_ts",
]

DISCARD_FIELDS = [
    "discard_id",
    "timeframe",
    "month",
    "symbol",
    "side",
    "screen_date",
    "reason",
    "detail",
    "event_time",
    "run_ts",
]


def month_windows() -> List[Tuple[date, date, str]]:
    """Forward months Nov-2025 through Oct-2026 inclusive. Label is YYYY-MM."""
    out: List[Tuple[date, date, str]] = []
    y, m = 2025, 11
    for _ in range(12):
        start = date(y, m, 1)
        if m == 12:
            end = date(y, 12, 31)
            ny, nm = y + 1, 1
        else:
            end = date(y, m + 1, 1)
            from datetime import timedelta

            end = end - timedelta(days=1)
            ny, nm = y, m + 1
        out.append((start, end, f"{y:04d}-{m:02d}"))
        y, m = ny, nm
    return out


@dataclass
class MtfSwingConfig:
    entry_timing: str = "next_open"  # "next_open" | "close"
    max_sessions: int = MAX_SESSIONS
    cluster_pct: float = CLUSTER_PCT
    qty_lots: int = QTY_LOTS
    brokerage: float = 0.0
    charges: float = 0.0
    slippage_points: float = 0.0
    timeframe: str = "15m"

    def charge_inr(self) -> float:
        return float(self.brokerage) + float(self.charges)
