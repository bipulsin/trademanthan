"""Independent OLH morning watchdog — intended as a **separate process**.

Checks at 09:21 / 09:26 / 09:30 IST (trading days only). Does not share the
APScheduler instance with the app; run via ``scripts/olh_watchdog.py``.
"""
from __future__ import annotations

import logging
from datetime import datetime, time
from typing import Any, Dict, List, Optional

import pytz

from backend.services.open_low_high.clock import olh_now
from backend.services.open_low_high.ohl_log import ohl_log

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

WATCHDOG_TIMES = (time(9, 21, 0), time(9, 26, 0), time(9, 30, 0))


def _label_for(tt: time) -> str:
    return f"{tt.hour:02d}{tt.minute:02d}"


def run_watchdog_check(
    *,
    label: Optional[str] = None,
    now: Optional[datetime] = None,
    send_alert: bool = True,
) -> Dict[str, Any]:
    """One-shot coverage/health pulse suitable for an external cron/process."""
    from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

    t = now or olh_now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)

    lbl = label or _label_for(t.time())
    out: Dict[str, Any] = {
        "ok": True,
        "label": lbl,
        "at": t.isoformat(),
        "session": t.date().isoformat(),
        "red": False,
        "checks": {},
    }

    if should_skip_scheduled_market_jobs_ist(t):
        out["skipped"] = "holiday_or_weekend"
        ohl_log("skipped", action=f"watchdog_{lbl}", detail="holiday_or_weekend")
        return out

    failures: List[str] = []

    # Phase / screen coverage
    try:
        from backend.services.open_low_high.persistence import get_day_meta, list_screen_for_date

        meta = get_day_meta(t.date()) or {}
        rows = list_screen_for_date(t.date()) or []
        n = len(rows)
        out["checks"]["screen"] = {
            "rows": n,
            "status": meta.get("status"),
            "frozen": meta.get("frozen"),
            "degraded": meta.get("degraded"),
            "phase_a_at": meta.get("phase_a_at"),
            "phase_b_at": meta.get("phase_b_at"),
        }
        if t.time() >= time(9, 21) and n == 0 and not meta.get("phase_a_at"):
            failures.append("no_phase_a_rows")
        if t.time() >= time(9, 30) and not meta.get("frozen") and not meta.get("phase_b_at"):
            failures.append("no_phase_b_by_0930")
    except Exception as exc:
        out["checks"]["screen"] = {"error": str(exc)[:160]}
        failures.append("screen_query")

    # WS ticks
    try:
        from backend.services.upstox_market_feed import get_segment_tick_counts_last_sec

        ticks = get_segment_tick_counts_last_sec(60)
        out["checks"]["ws_ticks_60s"] = ticks
        if int(ticks.get("EQ") or 0) == 0 and t.time() < time(9, 30):
            failures.append("eq_ticks_zero")
        if int(ticks.get("INDEX") or 0) == 0 and t.time() < time(9, 30):
            failures.append("index_ticks_zero")
    except Exception as exc:
        out["checks"]["ws_ticks_60s"] = {"error": str(exc)[:120]}
        failures.append("ws_ticks")

    # Silent-stall pulse
    try:
        from backend.services.open_low_high.silent_stall import check_eq_silent_stall

        stall = check_eq_silent_stall(force=False, now=t)
        out["checks"]["eq_stall"] = {
            "stalled": stall.get("stalled"),
            "action": stall.get("action"),
            "eq_ticks_window": stall.get("eq_ticks_window"),
        }
        if stall.get("stalled"):
            failures.append("eq_silent_stall")
    except Exception as exc:
        out["checks"]["eq_stall"] = {"error": str(exc)[:120]}

    out["failures"] = failures
    out["red"] = bool(failures)
    out["ok"] = not out["red"]

    ohl_log(
        "success" if out["ok"] else "failed",
        action=f"watchdog_{lbl}",
        detail=f"red={out['red']} failures={','.join(failures) or 'none'}",
    )

    if out["red"] and send_alert:
        try:
            from backend.services.open_low_high.alerts import send_olh_alert

            send_olh_alert(
                f"Watchdog RED {lbl} {t.date().isoformat()}",
                f"failures={failures}",
                meta={"label": lbl},
            )
        except Exception:
            logger.debug("watchdog alert failed", exc_info=True)

    return out


def nearest_watchdog_slot(now: Optional[datetime] = None) -> time:
    t = now or olh_now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    tt = t.time()
    best = WATCHDOG_TIMES[0]
    for slot in WATCHDOG_TIMES:
        if tt >= slot:
            best = slot
    return best
