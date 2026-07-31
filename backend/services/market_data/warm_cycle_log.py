"""Ring buffer of centralized candle-warm cycle outcomes for the deny monitor UI."""
from __future__ import annotations

import threading
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")
_LOCK = threading.Lock()
_CYCLES: Deque[Dict[str, Any]] = deque(maxlen=72)  # ~12h of 10m cycles


def record_warm_cycle(summary: Dict[str, Any]) -> None:
    """Append one market_data refresh summary (mutates a copy; safe across threads)."""
    if not isinstance(summary, dict):
        return
    row = dict(summary)
    row.setdefault(
        "recorded_at_ist",
        datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
    )
    with _LOCK:
        _CYCLES.appendleft(row)


def recent_warm_cycles(limit: int = 36) -> List[Dict[str, Any]]:
    n = max(1, min(int(limit or 36), 72))
    with _LOCK:
        return [dict(x) for x in list(_CYCLES)[:n]]


def latest_warm_cycle() -> Optional[Dict[str, Any]]:
    with _LOCK:
        return dict(_CYCLES[0]) if _CYCLES else None
