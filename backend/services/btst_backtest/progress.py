"""In-memory BTST CSV run progress (single worker process)."""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

_lock = threading.Lock()
_state: Dict[str, Any] = {
    "phase": "idle",
    "active_run_id": None,
    "started_at": None,
    "last_activity_at": None,
    "rows_done": 0,
    "rows_total": 0,
    "current_symbol": None,
    "message": "",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def reset_for_job(rows_total: int, csv_filename: str = "") -> None:
    with _lock:
        _state.update(
            {
                "phase": "processing",
                "active_run_id": None,
                "started_at": _utc_now(),
                "last_activity_at": _utc_now(),
                "rows_done": 0,
                "rows_total": int(rows_total),
                "current_symbol": None,
                "message": f"Processing {rows_total} CSV rows" + (
                    f" ({csv_filename})" if csv_filename else ""
                ),
            }
        )


def set_run_created(run_id: int) -> None:
    with _lock:
        _state["active_run_id"] = run_id
        _state["last_activity_at"] = _utc_now()


def set_row(done: int, total: int, *, symbol: Optional[str] = None) -> None:
    with _lock:
        _state["rows_done"] = int(done)
        _state["rows_total"] = int(total)
        _state["current_symbol"] = symbol
        _state["last_activity_at"] = _utc_now()
        sym = f" {symbol}" if symbol else ""
        _state["message"] = f"Row {done}/{total}{sym}"


def set_idle() -> None:
    with _lock:
        _state.update({"phase": "idle", "message": "", "current_symbol": None})


def snapshot() -> Dict[str, Any]:
    with _lock:
        return dict(_state)
