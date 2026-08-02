"""Ring buffer of RS score-cycle skip outcomes for the deny-monitor UI."""
from __future__ import annotations

import threading
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")
_LOCK = threading.Lock()
_CYCLES: Deque[Dict[str, Any]] = deque(maxlen=72)


def record_rs_score_cycle(summary: Dict[str, Any]) -> None:
    row = dict(summary)
    row.setdefault("recorded_at_ist", datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"))
    with _LOCK:
        _CYCLES.appendleft(row)


def recent_rs_score_cycles(limit: int = 36) -> List[Dict[str, Any]]:
    with _LOCK:
        return list(_CYCLES)[: max(1, min(72, int(limit)))]


def latest_rs_score_cycle() -> Optional[Dict[str, Any]]:
    with _LOCK:
        return dict(_CYCLES[0]) if _CYCLES else None
