"""In-memory MomentX scan + Ready-to-Trade state (thread-safe)."""
from __future__ import annotations

import copy
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")
_LOCK = threading.RLock()

_STATE: Dict[str, Any] = {
    "scan_at": None,
    "ema_at": None,
    "market_status": "unknown",
    "universe_size": 0,
    "qualified_count": 0,  # currently active (filters pass)
    "day_count": 0,  # all cards kept for the IST trading day
    "board_date": None,  # IST YYYY-MM-DD; cleared on new session day
    "scan_duration_sec": None,
    "skipped": [],
    "ready": [],  # day board: active + inactive (grey) cards
    "last_error": None,
    "scan_running": False,
    "ema_running": False,
    # When True, cards that fail Daily Vol > SMA(Vol,5) are Filters Off.
    # When False, volume is ignored for Active / Filters Off (other gates remain).
    "volume_filter_enabled": True,
}


def _now_iso() -> str:
    return datetime.now(IST).isoformat()


def snapshot() -> Dict[str, Any]:
    with _LOCK:
        return copy.deepcopy(_STATE)


def set_market_status(status: str) -> None:
    with _LOCK:
        _STATE["market_status"] = status


def set_scan_running(flag: bool) -> None:
    with _LOCK:
        _STATE["scan_running"] = bool(flag)


def set_ema_running(flag: bool) -> None:
    with _LOCK:
        _STATE["ema_running"] = bool(flag)


def set_last_error(error: Optional[str]) -> None:
    with _LOCK:
        _STATE["last_error"] = error


def clear_day_board(*, reason: Optional[str] = None) -> None:
    """Drop all Ready cards (new IST trading day / market open)."""
    with _LOCK:
        _STATE["ready"] = []
        _STATE["qualified_count"] = 0
        _STATE["day_count"] = 0
        _STATE["board_date"] = None
        if reason:
            _STATE["last_error"] = None


def ensure_board_date(today_iso: str) -> bool:
    """If the board is from a prior IST day, clear it. Returns True if cleared."""
    with _LOCK:
        prev = _STATE.get("board_date")
        if prev and prev != today_iso:
            _STATE["ready"] = []
            _STATE["qualified_count"] = 0
            _STATE["day_count"] = 0
            _STATE["board_date"] = today_iso
            return True
        if not prev:
            _STATE["board_date"] = today_iso
        return False


def replace_ready(
    cards: List[Dict[str, Any]],
    *,
    scan_at: Optional[str] = None,
    universe_size: int = 0,
    skipped: Optional[List[str]] = None,
    duration_sec: Optional[float] = None,
    error: Optional[str] = None,
    board_date: Optional[str] = None,
) -> None:
    with _LOCK:
        ready = list(cards)
        _STATE["ready"] = ready
        _STATE["qualified_count"] = sum(1 for c in ready if str(c.get("status") or "") == "active")
        _STATE["day_count"] = len(ready)
        _STATE["universe_size"] = int(universe_size)
        _STATE["skipped"] = list(skipped or [])
        _STATE["scan_duration_sec"] = duration_sec
        _STATE["last_error"] = error
        if board_date is not None:
            _STATE["board_date"] = board_date
        if scan_at is not None:
            _STATE["scan_at"] = scan_at
        else:
            _STATE["scan_at"] = _now_iso()


def set_ready_cards(cards: List[Dict[str, Any]]) -> None:
    """Replace day-board cards without touching scan metadata."""
    with _LOCK:
        ready = list(cards)
        _STATE["ready"] = ready
        _STATE["qualified_count"] = sum(1 for c in ready if str(c.get("status") or "") == "active")
        _STATE["day_count"] = len(ready)


def update_ema_fields(updates: Dict[str, Dict[str, Any]], *, ema_at: Optional[str] = None) -> None:
    """Merge EMA/LTP/trigger fields keyed by stock (upper)."""
    with _LOCK:
        by_stock = {str(c.get("stock") or "").upper(): c for c in _STATE["ready"]}
        for stock, patch in (updates or {}).items():
            key = str(stock or "").upper()
            card = by_stock.get(key)
            if not card:
                continue
            card.update(patch)
        _STATE["ema_at"] = ema_at or _now_iso()


def update_ltp(future_key: str, ltp: float) -> None:
    ik = (future_key or "").strip().replace(":", "|")
    if not ik or ltp <= 0:
        return
    with _LOCK:
        for card in _STATE["ready"]:
            key = str(card.get("future_instrument_key") or "").replace(":", "|")
            if key == ik:
                prev = card.get("ltp")
                card["ltp"] = float(ltp)
                card["ltp_at"] = _now_iso()
                if prev is not None:
                    try:
                        card["ltp_dir"] = "up" if float(ltp) > float(prev) else (
                            "down" if float(ltp) < float(prev) else "flat"
                        )
                    except (TypeError, ValueError):
                        card["ltp_dir"] = "flat"
                break


def volume_filter_enabled() -> bool:
    with _LOCK:
        return bool(_STATE.get("volume_filter_enabled", True))


def set_volume_filter_enabled(enabled: bool) -> bool:
    with _LOCK:
        _STATE["volume_filter_enabled"] = bool(enabled)
        return bool(_STATE["volume_filter_enabled"])


def ready_cards() -> List[Dict[str, Any]]:
    with _LOCK:
        return copy.deepcopy(_STATE["ready"])


def ready_future_keys() -> List[str]:
    with _LOCK:
        keys = []
        seen = set()
        for card in _STATE["ready"]:
            ik = str(card.get("future_instrument_key") or "").strip()
            if not ik or ik in seen:
                continue
            seen.add(ik)
            keys.append(ik)
        return keys
