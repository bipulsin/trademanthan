"""Process flag: force Phase A onto REST reserved budget (skip WS tries).

Set at 09:16 when WS ticks=0, or when Phase A ends bars=0 while canary healthy.
"""
from __future__ import annotations

import threading
from typing import Any, Dict, Optional

_lock = threading.Lock()
_state: Dict[str, Any] = {
    "force_rest_reserved": False,
    "reason": None,
    "set_at": None,
}


def set_force_rest_reserved(reason: str, *, at: Optional[str] = None) -> Dict[str, Any]:
    with _lock:
        _state["force_rest_reserved"] = True
        _state["reason"] = str(reason)[:200]
        _state["set_at"] = at
        return dict(_state)


def clear_force_rest_reserved() -> None:
    with _lock:
        _state["force_rest_reserved"] = False
        _state["reason"] = None
        _state["set_at"] = None


def is_force_rest_reserved() -> bool:
    with _lock:
        return bool(_state["force_rest_reserved"])


def status() -> Dict[str, Any]:
    with _lock:
        return dict(_state)
