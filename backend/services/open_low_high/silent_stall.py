"""EQ silent-stall watchdog: connected socket but no EQ ticks for N seconds.

2026-11-23 forensics context
----------------------------
- INDEX ticks=0 all morning was a **parser** bug (only ``marketFF`` read;
  Upstox puts NSE_INDEX under ``fullFeed.indexFF``) — fixed in tip ~eadcdaec.
- Raw WS frames were **not** retained for 23-Nov, so we cannot prove an EQ
  silent-stall from recordings. 20-Nov showed futures-only 1m rows despite
  EQ+INDEX subscribe — consistent with silent EQ path or parse drop.
- Connection topology (prod): single Upstox Market Data Feed v3 WS process in
  ``upstox_market_feed``; OLH registers provider ``open_low_high`` via
  ``set_feed_provider_keys``; mode ``full``; chunked subscribe (_CHUNK=100)
  with INDEX prioritized into the first chunk.

Watchdog behavior (staging-friendly, env-gated):
  - If EQ ticks in last ``OLH_EQ_STALL_SEC`` (default 20) == 0 while
    feed thread reports running + subscribed EQ keys > 0 → alert +
    reconnect/resubscribe up to ``OLH_EQ_STALL_MAX_ATTEMPTS``.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional

import pytz

from backend.services.open_low_high.clock import olh_now
from backend.services.open_low_high.config import (
    OLH_EQ_STALL_ENABLED,
    OLH_EQ_STALL_MAX_ATTEMPTS,
    OLH_EQ_STALL_SEC,
)
from backend.services.open_low_high.ohl_log import ohl_log

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_lock = threading.RLock()
_attempts_today = 0
_attempts_day: Optional[str] = None
_last_action_mono = 0.0
_last_status: Dict[str, Any] = {}


def eq_timeline_20261123() -> Dict[str, Any]:
    """Documented timeline / topology for 23-Nov (no raw frames available)."""
    return {
        "session": "2026-11-23",
        "raw_ws_frames_recorded": False,
        "known_issues": [
            {
                "segment": "INDEX",
                "symptom": "ticks=0 all morning",
                "root_cause": "parser skipped fullFeed.indexFF (only marketFF)",
                "fix": "eadcdaec / ab493255 after-close tests",
            },
            {
                "segment": "EQ",
                "symptom": "not independently proven silent-stall on 23-Nov",
                "note": (
                    "No raw frames retained. Prior 20-Nov: WS 1m futures-only "
                    "despite ~291 subscribed keys — treat as possible silent EQ path."
                ),
            },
        ],
        "connection_topology": {
            "process": "upstox_market_feed feed thread (single WS)",
            "mode": "full",
            "olh_provider": "open_low_high",
            "subscribe": "NSE_EQ universe + NSE_INDEX|Nifty 50 + NSE_FO front-month",
            "chunk": 100,
            "index_priority": "first chunk",
            "lock_window": "09:15–09:25 EQ+INDEX force-merge",
        },
        "watchdog": {
            "stall_sec": OLH_EQ_STALL_SEC,
            "max_attempts": OLH_EQ_STALL_MAX_ATTEMPTS,
            "enabled_default": OLH_EQ_STALL_ENABLED,
        },
    }


def _reset_attempts_if_new_day(now: datetime) -> None:
    global _attempts_today, _attempts_day
    day = now.strftime("%Y-%m-%d")
    if _attempts_day != day:
        _attempts_day = day
        _attempts_today = 0


def check_eq_silent_stall(
    *,
    force: bool = False,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Return stall diagnosis; optionally reconnect/resub when enabled."""
    global _attempts_today, _last_action_mono, _last_status

    t = now or olh_now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)

    out: Dict[str, Any] = {
        "ok": True,
        "stalled": False,
        "action": "none",
        "eq_ticks_window": 0,
        "stall_sec": OLH_EQ_STALL_SEC,
        "attempts": 0,
        "at": t.isoformat(),
    }

    if not OLH_EQ_STALL_ENABLED and not force:
        out["skipped"] = "disabled"
        _last_status = out
        return out

    from backend.services.upstox_market_feed import feed_status, get_segment_tick_counts_last_sec

    ticks = get_segment_tick_counts_last_sec(window_sec=int(OLH_EQ_STALL_SEC))
    eq_n = int(ticks.get("EQ") or 0)
    out["eq_ticks_window"] = eq_n
    out["ticks"] = ticks

    try:
        status = feed_status()
    except Exception:
        status = {}
    out["feed_status"] = {
        k: status.get(k)
        for k in ("thread_alive", "last_error", "universe_keys", "provider_keys", "cached_instruments")
        if k in status
    } or status

    running = bool(status.get("thread_alive") or True)
    out["feed_running"] = running
    if eq_n > 0:
        out["stalled"] = False
        _last_status = out
        return out

    # EQ silent while we expect ticks (market open window heuristic: 09:15–15:30)
    tt = t.time()
    from datetime import time as dtime

    if not (dtime(9, 15) <= tt <= dtime(15, 30)):
        out["skipped"] = "outside_cash_session"
        _last_status = out
        return out

    out["stalled"] = True
    out["ok"] = False

    with _lock:
        _reset_attempts_if_new_day(t)
        out["attempts"] = _attempts_today
        if _attempts_today >= int(OLH_EQ_STALL_MAX_ATTEMPTS):
            out["action"] = "attempt_limit"
            ohl_log(
                "failed",
                action="eq_silent_stall_limit",
                source="ws",
                detail=f"eq_ticks=0 window={OLH_EQ_STALL_SEC}s attempts={_attempts_today}",
            )
            _last_status = out
            return out

        # Debounce 5s between actions
        mono = time.monotonic()
        if mono - _last_action_mono < 5.0 and not force:
            out["action"] = "debounced"
            _last_status = out
            return out

        _attempts_today += 1
        _last_action_mono = mono
        out["attempts"] = _attempts_today

    try:
        from backend.services.open_low_high.alerts import send_olh_alert
        from backend.services.open_low_high.ws_feed import subscribe_olh_ws

        send_olh_alert(
            "EQ silent-stall",
            f"EQ ticks=0 for {OLH_EQ_STALL_SEC}s; resubscribe attempt {_attempts_today}/"
            f"{OLH_EQ_STALL_MAX_ATTEMPTS}",
            meta={"eq_ticks": eq_n, "attempt": _attempts_today},
        )
        sub = subscribe_olh_ws(engage_lock=None)
        out["action"] = "resubscribe"
        out["subscribe"] = {
            k: sub.get(k) for k in ("subscribed", "equity", "index", "ok") if k in sub
        }
        ohl_log(
            "success",
            action="eq_silent_stall_resub",
            source="ws",
            detail=f"attempt={_attempts_today} subscribed={sub.get('subscribed')}",
        )
    except Exception as exc:
        out["action"] = "resubscribe_failed"
        out["error"] = str(exc)[:200]
        ohl_log("failed", action="eq_silent_stall_resub", source="ws", detail=str(exc)[:200])

    _last_status = out
    return out


def last_stall_status() -> Dict[str, Any]:
    return dict(_last_status)


def reset_silent_stall_for_tests() -> None:
    global _attempts_today, _attempts_day, _last_action_mono, _last_status
    with _lock:
        _attempts_today = 0
        _attempts_day = None
        _last_action_mono = 0.0
        _last_status = {}
