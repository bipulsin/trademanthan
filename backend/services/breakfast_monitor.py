"""Breakfast preflight (09:00) and post-session (09:25) Telegram monitoring for @TradeWithCTO."""
from __future__ import annotations

import json
import logging
import os
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.breakfast_prev_close_scheduler import breakfast_prev_close_scheduler_status
from backend.services.breakfast_strategy.live_persist import fetch_session_lock
from backend.services.breakfast_strategy.live_scheduler import breakfast_live_scheduler_status
from backend.services.breakfast_strategy.live_tick import (
    _warmup_instrument_keys,
    get_breakfast_session_monitor_stats,
    get_last_warmup_result,
    get_live_tick_snapshot,
)
from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist
from backend.services.telegram_trade_channel import send_trade_with_cto_channel_message

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_EXPECTED_LIVE_JOBS = frozenset(
    {
        "breakfast_ws_warmup_910",
        "breakfast_live_tick_16",
        "breakfast_live_tick_17",
        "breakfast_live_tick_18",
        "breakfast_live_tick_19",
        "breakfast_live_freeze_92005",
    }
)
_EXPECTED_PREV_CLOSE_JOBS = frozenset(
    {
        "breakfast_prev_close_1600",
        "breakfast_prev_close_1630",
        "breakfast_prev_close_0905",
    }
)

_SCHEDULER: Optional[BackgroundScheduler] = None


def _now_ist() -> datetime:
    return datetime.now(IST)


def _backend_health_ok() -> bool:
    url = os.getenv("BACKEND_HEALTH_URL", "http://127.0.0.1:8000/scan/health")
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return False
        data = r.json()
        return str(data.get("status") or "").lower() == "healthy"
    except Exception as e:
        logger.warning("Breakfast monitor health check failed: %s", e)
        return False


def _estimate_warmup_instruments(now: Optional[datetime] = None) -> int:
    now = now or _now_ist()
    try:
        return len(_warmup_instrument_keys(now.date()))
    except Exception as e:
        logger.warning("Breakfast monitor instrument estimate failed: %s", e)
        return 0


def collect_breakfast_preflight_status(now: Optional[datetime] = None) -> Dict[str, Any]:
    """In-process checks for 09:00 Telegram preflight."""
    now = now or _now_ist()
    live = breakfast_live_scheduler_status()
    prev = breakfast_prev_close_scheduler_status()
    live_jobs = set(live.get("job_ids") or [])
    prev_jobs = set(prev.get("job_ids") or [])
    warmup = get_last_warmup_result()
    est_n = _estimate_warmup_instruments(now)
    warmup_done = bool(warmup and warmup.get("ok"))
    warmup_n = int(warmup.get("instrument_count") or 0) if warmup_done else est_n

    issues: List[str] = []
    if not live.get("running"):
        issues.append("live scheduler not running")
    elif _EXPECTED_LIVE_JOBS - live_jobs:
        issues.append(f"live jobs missing: {sorted(_EXPECTED_LIVE_JOBS - live_jobs)}")
    if not prev.get("running"):
        issues.append("prev-close scheduler not running")
    elif _EXPECTED_PREV_CLOSE_JOBS - prev_jobs:
        issues.append(f"prev-close jobs missing: {sorted(_EXPECTED_PREV_CLOSE_JOBS - prev_jobs)}")
    if not _backend_health_ok():
        issues.append("health endpoint not healthy")

    return {
        "session_date": now.date().isoformat(),
        "time_ist": now.strftime("%H:%M"),
        "live_scheduler": live,
        "prev_close_scheduler": prev,
        "health_ok": not any("health" in i for i in issues),
        "warmup_done": warmup_done,
        "warmup_instrument_count": warmup_n,
        "estimated_instrument_count": est_n,
        "issues": issues,
        "ok": not issues,
    }


def format_preflight_telegram(status: Dict[str, Any]) -> str:
    sd = status.get("session_date") or "?"
    icon = "✓" if status.get("ok") else "⚠️"
    lines = [f"Breakfast {sd} preflight {icon}"]

    live_ok = status.get("live_scheduler", {}).get("running")
    prev_ok = status.get("prev_close_scheduler", {}).get("running")
    lines.append(f"• Live scheduler: {'running' if live_ok else 'DOWN'} (9:10 warmup, 9:16–9:19 ticks, 9:20:05 freeze)")
    lines.append(f"• Prev-close scheduler: {'running' if prev_ok else 'DOWN'}")
    lines.append(f"• Health: {'healthy' if status.get('health_ok') else 'unhealthy'}")

    if status.get("warmup_done"):
        n = status.get("warmup_instrument_count") or 0
        lines.append(f"• WS warmup: done ({n} instruments)")
    else:
        n = status.get("estimated_instrument_count") or status.get("warmup_instrument_count") or 0
        lines.append(f"• WS warmup: scheduled 09:10 IST (~{n} instruments)")

    if status.get("issues"):
        lines.append(f"• Issues: {'; '.join(status['issues'])}")
    else:
        lines.append("• Startup errors: none detected")

    return "\n".join(lines)


def _tick_source_breakdown(tick_sources: List[Dict[str, Any]]) -> Tuple[Dict[str, int], Dict[str, int]]:
    by_source: Counter[str] = Counter()
    fallback_reasons: Counter[str] = Counter()
    for row in tick_sources:
        minute = row.get("minute")
        if minute is not None and int(minute) not in (16, 17, 18, 19, 20):
            continue
        src = str(row.get("source") or "unknown")
        by_source[src] += 1
        if src == "rest_fallback":
            reason = str(row.get("reason") or "unknown").strip() or "unknown"
            fallback_reasons[reason] += 1
    return dict(by_source), dict(fallback_reasons)


def _cross_check_from_lock(lock_row: Optional[Dict[str, Any]], snapshot: Optional[Dict[str, Any]]) -> str:
    for src in (snapshot, lock_row):
        if not src:
            continue
        payload = src
        if lock_row and src is lock_row:
            raw = lock_row.get("payload_json")
            if isinstance(raw, str):
                try:
                    payload = json.loads(raw)
                except Exception:
                    payload = lock_row
            elif isinstance(raw, dict):
                payload = raw
        status = payload.get("cross_check_status")
        if status:
            return str(status)
    return "n/a"


def assess_production_stability(
    *,
    lock_row: Optional[Dict[str, Any]],
    by_source: Dict[str, int],
    fallback_reasons: Dict[str, int],
) -> Tuple[str, str]:
    """Return (verdict_short, detail) for items 1–4 closure."""
    lock_status = str((lock_row or {}).get("lock_status") or "missing").lower()
    failure = (lock_row or {}).get("failure_reason")
    ws = int(by_source.get("ws") or 0)
    rest = int(by_source.get("rest_fallback") or 0)
    total = ws + rest
    ws_pct = (100.0 * ws / total) if total else 0.0

    blockers: List[str] = []
    if lock_status != "locked":
        blockers.append(f"freeze {lock_status}" + (f" ({failure})" if failure else ""))
    if total and ws_pct < 70.0:
        blockers.append(f"WS only {ws_pct:.0f}% ({ws}/{total})")
    if fallback_reasons.get("ws_feed_down"):
        blockers.append("ws_feed_down fallbacks")

    if not blockers and lock_status == "locked" and (not total or ws_pct >= 85.0):
        return "OK", "Items 1–4 closable — lock OK, WS-primary ticks stable"
    if not blockers and lock_status == "locked":
        return "MARGINAL", "Lock OK; review REST fallback mix before closing items 1–4"
    return "NEEDS ITERATION", "; ".join(blockers) or "incomplete session data"


def build_post_session_report(session_date: Optional[str] = None) -> str:
    now = _now_ist()
    sd = str(session_date or now.date().isoformat())[:10]
    stats = get_breakfast_session_monitor_stats(sd)
    tick_sources = stats.get("tick_sources") or []
    by_source, fallback_reasons = _tick_source_breakdown(tick_sources)
    ws = int(by_source.get("ws") or 0)
    rest = int(by_source.get("rest_fallback") or 0)
    total = ws + rest
    ws_pct = (100.0 * ws / total) if total else 0.0

    lock_row = fetch_session_lock(sd)
    snapshot = get_live_tick_snapshot()
    cross = _cross_check_from_lock(lock_row, snapshot)

    lines = [f"Breakfast {sd} report"]
    if total:
        lines.append(f"• Tick sources 9:16–9:20: WS {ws_pct:.0f}% ({ws}/{total}), REST {100 - ws_pct:.0f}%")
        if fallback_reasons:
            top = ", ".join(f"{k}({v})" for k, v in sorted(fallback_reasons.items(), key=lambda x: -x[1])[:5])
            lines.append(f"• REST reasons: {top}")
    else:
        lines.append("• Tick sources 9:16–9:20: no in-process data (check docker logs)")

    if lock_row:
        ls = lock_row.get("lock_status")
        fr = lock_row.get("failure_reason")
        sc = lock_row.get("signal_count")
        fr_txt = f", reason={fr}" if fr else ""
        lines.append(f"• Freeze: {ls}, signals={sc}{fr_txt}")
    else:
        lines.append("• Freeze: no breakfast_session_lock row")

    lines.append(f"• REST-vs-WS @9:20:05: {cross}")

    repicks = stats.get("repicks") or []
    if repicks:
        mins = ", ".join(f":{int(r.get('minute', 0)):02d}" for r in repicks)
        lines.append(f"• Sector re-picks: {len(repicks)} ({mins})")
    else:
        lines.append("• Sector re-picks: 0")

    verdict, detail = assess_production_stability(
        lock_row=lock_row, by_source=by_source, fallback_reasons=fallback_reasons
    )
    lines.append(f"• Stability: {verdict} — {detail}")
    return "\n".join(lines)


def run_breakfast_morning_telegram_ping() -> None:
    if should_skip_scheduled_market_jobs_ist():
        logger.info("Breakfast morning Telegram: skip non-trading day")
        return
    status = collect_breakfast_preflight_status()
    text = format_preflight_telegram(status)
    ok = send_trade_with_cto_channel_message(text)
    logger.info("Breakfast 09:00 Telegram sent=%s ok=%s", ok, status.get("ok"))


def run_breakfast_post_session_telegram_report() -> None:
    if should_skip_scheduled_market_jobs_ist():
        logger.info("Breakfast post-session Telegram: skip non-trading day")
        return
    text = build_post_session_report()
    ok = send_trade_with_cto_channel_message(text)
    logger.info("Breakfast 09:25 Telegram report sent=%s", ok)


def start_breakfast_monitor_scheduler() -> None:
    global _SCHEDULER
    if _SCHEDULER is not None:
        return
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    sch.add_job(
        run_breakfast_morning_telegram_ping,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=0, second=0, timezone="Asia/Kolkata"),
        id="breakfast_monitor_morning_900",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    sch.add_job(
        run_breakfast_post_session_telegram_report,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=25, second=0, timezone="Asia/Kolkata"),
        id="breakfast_monitor_post_session_925",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    sch.start()
    _SCHEDULER = sch
    logger.info("Breakfast monitor scheduler started (09:00 preflight, 09:25 report IST weekdays)")


def stop_breakfast_monitor_scheduler() -> None:
    global _SCHEDULER
    if _SCHEDULER:
        try:
            _SCHEDULER.shutdown(wait=False)
        finally:
            _SCHEDULER = None
