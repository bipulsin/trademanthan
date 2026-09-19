"""Tarang-local Upstox REST rate budget (does not change CommDiv).

Phase 1: REST Option Greek / quotes only. WS share is documented in risk.default.json;
Tarang may later call ``set_feed_provider_keys('tarang', keys)`` with a hard cap —
CommDiv provider keys are never cleared by Tarang.
"""
from __future__ import annotations

import threading
import time
from typing import Optional


class TarangUpstoxRateBudget:
    """Simple token-bucket style limiter for Tarang REST calls."""

    def __init__(self, per_sec: float = 2.0, per_min: float = 60.0) -> None:
        self.per_sec = max(0.1, float(per_sec))
        self.per_min = max(1.0, float(per_min))
        self._lock = threading.Lock()
        self._sec_ts: list[float] = []
        self._min_ts: list[float] = []
        self._priority = "low"  # vs CommDiv

    def acquire(self, timeout: float = 30.0) -> bool:
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                now = time.monotonic()
                self._sec_ts = [t for t in self._sec_ts if now - t < 1.0]
                self._min_ts = [t for t in self._min_ts if now - t < 60.0]
                if len(self._sec_ts) < self.per_sec and len(self._min_ts) < self.per_min:
                    self._sec_ts.append(now)
                    self._min_ts.append(now)
                    return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.05)


_BUDGET: Optional[TarangUpstoxRateBudget] = None
_BUDGET_LOCK = threading.Lock()


def get_tarang_upstox_budget() -> TarangUpstoxRateBudget:
    global _BUDGET
    with _BUDGET_LOCK:
        if _BUDGET is None:
            _BUDGET = TarangUpstoxRateBudget()
        return _BUDGET
