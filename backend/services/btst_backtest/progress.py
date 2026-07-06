"""In-memory BTST backtest progress (single worker process)."""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

_lock = threading.Lock()
_state: Dict[str, Any] = {
    "phase": "idle",
    "mode": None,
    "active_run_id": None,
    "started_at": None,
    "last_activity_at": None,
    "prefetch_done": 0,
    "prefetch_total": 0,
    "days_done": 0,
    "days_total": 0,
    "message": "",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def reset_for_job(mode: str) -> None:
    with _lock:
        _state.update(
            {
                "phase": "starting",
                "mode": mode,
                "active_run_id": None,
                "started_at": _utc_now(),
                "last_activity_at": _utc_now(),
                "prefetch_done": 0,
                "prefetch_total": 0,
                "days_done": 0,
                "days_total": 0,
                "message": "Starting backtest…",
            }
        )


def set_run_created(
    run_id: int,
    *,
    days_total: int,
    prefetch_total: int,
    window_start: str,
    window_end: str,
) -> None:
    with _lock:
        _state.update(
            {
                "phase": "prefetch",
                "active_run_id": run_id,
                "days_total": int(days_total),
                "prefetch_total": int(prefetch_total),
                "prefetch_done": 0,
                "days_done": 0,
                "last_activity_at": _utc_now(),
                "message": (
                    f"Prefetching {prefetch_total} instruments "
                    f"({window_start} → {window_end})"
                ),
            }
        )


def set_prefetch(done: int, total: int, *, instrument: Optional[str] = None) -> None:
    with _lock:
        _state["phase"] = "prefetch"
        _state["prefetch_done"] = int(done)
        _state["prefetch_total"] = int(total)
        _state["last_activity_at"] = _utc_now()
        sym = f" ({instrument})" if instrument else ""
        _state["message"] = f"Prefetch {done}/{total}{sym}"


def set_screening(day_index: int, days_total: int, trade_date: str) -> None:
    with _lock:
        _state["phase"] = "screening"
        _state["days_done"] = int(day_index)
        _state["days_total"] = int(days_total)
        _state["last_activity_at"] = _utc_now()
        _state["message"] = f"Screening day {day_index}/{days_total}: {trade_date}"


def set_idle() -> None:
    with _lock:
        _state.update(
            {
                "phase": "idle",
                "mode": None,
                "message": "",
            }
        )


def snapshot() -> Dict[str, Any]:
    with _lock:
        return dict(_state)
