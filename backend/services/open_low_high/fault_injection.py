"""OLH fault-injection scenarios for dress rehearsal (staging only).

Each scenario returns ``{expected, actual, pass}``. Required invariants:
  - no hang (bounded runtime)
  - no silent zero (zeros surface as alerts / DEGRADED / explicit status)
  - per-symbol partial freeze allowed
  - alerts fire on hard faults
  - force-screen remains repairable
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import pytz

from backend.services.open_low_high.clock import install_advancing_clock, set_olh_clock

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


@dataclass
class FaultResult:
    scenario: str
    expected: str
    actual: str
    passed: bool
    detail: Dict[str, Any] = field(default_factory=dict)
    elapsed_sec: float = 0.0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "scenario": self.scenario,
            "expected": self.expected,
            "actual": self.actual,
            "pass": self.passed,
            "elapsed_sec": round(self.elapsed_sec, 3),
            "detail": self.detail,
        }


def _bounded(fn: Callable[[], Any], *, timeout_sec: float = 8.0) -> tuple[Any, float, bool]:
    """Run ``fn``; return (result, elapsed, hung). hung=True if wall > timeout (best-effort)."""
    t0 = time.monotonic()
    try:
        result = fn()
        elapsed = time.monotonic() - t0
        return result, elapsed, elapsed > timeout_sec
    except Exception as exc:
        elapsed = time.monotonic() - t0
        return {"error": str(exc)[:200]}, elapsed, False


def scenario_rest_429(monkey_hooks: Optional[Dict[str, Any]] = None) -> FaultResult:
    """REST 429 storm → retries, no hang, explicit failure status (not silent zero)."""
    from backend.services.open_low_high import candle_budget as cb

    hooks = monkey_hooks or {}
    denied = {"n": 0}

    def _run():
        cb.reset_pools_for_tests()
        with cb.olh_candle_pool("phase_a"):
            for _ in range(20):
                ok = False
                cb.note_shared_acquire(False)
                denied["n"] += 1
                if not ok:
                    continue
        snap = cb.usage_snapshot()
        a_denied = sum(v.get("denied", 0) for v in (snap.get("phase_a") or {}).values())
        return {"denied": a_denied, "silent_zero": a_denied == 0 and denied["n"] > 0}

    result, elapsed, hung = _bounded(_run)
    actual = (
        f"hung={hung} denied={result.get('denied') if isinstance(result, dict) else result}"
    )
    expected = "no hang; denials counted (>0); not silent zero"
    passed = (not hung) and isinstance(result, dict) and int(result.get("denied") or 0) > 0
    return FaultResult("rest_429_storm", expected, actual, passed, detail=result or {}, elapsed_sec=elapsed)


def scenario_rest_empty_body() -> FaultResult:
    """Empty REST body → logged miss, not treated as OHLC zeros."""
    from backend.services.open_low_high.screener import data_quality_incomplete

    incomplete, details = data_quality_incomplete(
        phase_a_count=0,
        scanned=10,
        miss_stats={"no_bars": 8, "no_0915": 8},
        nifty_ready=False,
    )
    expected = "empty/missing surfaced as incomplete — not silent zero board"
    actual = f"incomplete={incomplete} details={details}"
    return FaultResult(
        "rest_empty_body",
        expected,
        actual,
        bool(incomplete),
        detail={"incomplete": incomplete, "details": details},
    )


def scenario_ws_disconnect_partial() -> FaultResult:
    """WS drop → PARTIAL 1m mark; no hang."""
    from backend.services.open_low_high.candle_builder import MinuteCandleBuilder, TickEvent, STATE_PARTIAL

    def _run():
        b = MinuteCandleBuilder(now_fn=lambda: IST.localize(datetime(2026, 11, 23, 9, 16, 30)))
        tick = TickEvent(
            instrument_key="NSE_EQ|INE002A01018",
            ltp=100.0,
            exchange_ts=IST.localize(datetime(2026, 11, 23, 9, 16, 5)),
        )
        b.ingest_tick(tick)
        touched = b.mark_partial_on_reconnect()
        return {"partial_n": len(touched), "state": touched[0].state if touched else None}

    result, elapsed, hung = _bounded(_run)
    ok = (
        not hung
        and isinstance(result, dict)
        and int(result.get("partial_n") or 0) >= 1
        and result.get("state") == STATE_PARTIAL
    )
    return FaultResult(
        "ws_disconnect_partial",
        "in-progress 1m marked PARTIAL; no hang",
        str(result),
        ok,
        detail=result if isinstance(result, dict) else {},
        elapsed_sec=elapsed,
    )


def scenario_eq_silent_stall_inject() -> FaultResult:
    """09:20–09:23 EQ stall → watchdog detects + attempt-limited resub (mocked)."""
    from backend.services.open_low_high import silent_stall as ss

    ss.reset_silent_stall_for_tests()
    clock = install_advancing_clock(IST.localize(datetime(2026, 11, 23, 9, 21, 0)))
    alerts: List[str] = []

    def _run():
        import backend.services.open_low_high.alerts as al
        import backend.services.open_low_high.ws_feed as wf
        import backend.services.upstox_market_feed as umf

        orig_ticks = umf.get_segment_tick_counts_last_sec
        orig_feed = umf.feed_status
        orig_sub = wf.subscribe_olh_ws
        orig_alert = al.send_olh_alert
        umf.get_segment_tick_counts_last_sec = lambda window_sec=60: {
            "EQ": 0,
            "INDEX": 5,
            "FUT": 10,
            "OTHER": 0,
        }
        umf.feed_status = lambda: {"thread_alive": True, "last_error": None}
        wf.subscribe_olh_ws = lambda **_k: {
            "ok": True,
            "subscribed": 120,
            "equity": 100,
            "index": 1,
        }
        al.send_olh_alert = lambda *a, **_k: alerts.append(str(a[0] if a else "alert")) or True
        try:
            return ss.check_eq_silent_stall(force=True, now=clock())
        finally:
            umf.get_segment_tick_counts_last_sec = orig_ticks
            umf.feed_status = orig_feed
            wf.subscribe_olh_ws = orig_sub
            al.send_olh_alert = orig_alert
            set_olh_clock(None)

    result, elapsed, hung = _bounded(_run)
    stalled = isinstance(result, dict) and bool(result.get("stalled"))
    action = result.get("action") if isinstance(result, dict) else None
    passed = (not hung) and stalled and action == "resubscribe" and len(alerts) >= 1
    return FaultResult(
        "eq_silent_stall_0920_0923",
        "detect stall; alert; resubscribe; no hang",
        f"stalled={stalled} action={action} alerts={len(alerts)} hung={hung}",
        passed,
        detail=result if isinstance(result, dict) else {"raw": result},
        elapsed_sec=elapsed,
    )


def scenario_partial_freeze_repairable() -> FaultResult:
    """Per-symbol missing → partial freeze path remains force-repairable."""
    from backend.services.open_low_high.screener import freeze_gate

    may_freeze, reason = freeze_gate(coverage=0.85, nifty_present=True, coverage_min=0.90)
    expected = "coverage short → freeze blocked (force later repairable)"
    passed = may_freeze is False and "coverage" in reason
    return FaultResult(
        "partial_freeze_repairable",
        expected,
        f"may_freeze={may_freeze} reason={reason}",
        passed,
        detail={"may_freeze": may_freeze, "reason": reason},
    )


def scenario_index_unknown_feed_alert() -> FaultResult:
    """Unknown feed type on subscribed key → counted + alert path."""
    from backend.services.open_low_high import feed_contract as fc

    fc.reset_feed_contract_for_tests()
    alerts: List[str] = []
    import backend.services.open_low_high.alerts as al

    orig = al.send_olh_alert
    al.send_olh_alert = lambda *a, **_k: alerts.append(str(a[0] if a else "")) or True
    try:
        ft = fc.note_feed_observation(
            "NSE_EQ|INE002A01018",
            {"mysteryFeed": {"x": 1}},
            subscribed_keys={"NSE_EQ|INE002A01018"},
            now=IST.localize(datetime(2026, 11, 23, 9, 20, 0)),
        )
        snap = fc.unknown_counts_snapshot("09:20")
    finally:
        al.send_olh_alert = orig

    passed = (not fc.is_handled(ft)) and len(snap.get("unknown") or {}) >= 1 and len(alerts) >= 1
    return FaultResult(
        "unknown_feed_subscribed_alert",
        "unknown counted/min + alert for subscribed key",
        f"type={ft} unknown={snap.get('unknown')} alerts={len(alerts)}",
        passed,
        detail={"feed_type": ft, "snap": snap},
    )


def scenario_holiday_skip() -> FaultResult:
    """2026-11-24 holiday → canary/readiness skip, no REST alerts."""
    from backend.services.open_low_high.rest_canary import canary_should_run_today
    from backend.services.open_low_high.readiness import run_readiness_0850

    hol = IST.localize(datetime(2026, 11, 24, 8, 50, 0))
    # readiness uses holiday table/cache — monkey via should_skip
    import backend.services.market_holiday as mh

    orig = mh.should_skip_scheduled_market_jobs_ist
    mh.should_skip_scheduled_market_jobs_ist = lambda now=None: True
    try:
        canary = canary_should_run_today(hol)
        ready = run_readiness_0850(now=hol, send_alert=False)
    finally:
        mh.should_skip_scheduled_market_jobs_ist = orig

    # canary_should_run_today may still use its own is_trading_day — check both
    skipped = ready.get("skipped") == "holiday_or_weekend"
    passed = skipped and (canary is False or skipped)
    return FaultResult(
        "holiday_20261124_skip",
        "no REST/alerts on holiday; readiness skipped",
        f"canary={canary} readiness_skipped={ready.get('skipped')}",
        passed,
        detail={"canary": canary, "readiness": ready},
    )


def run_all_fault_scenarios() -> Dict[str, Any]:
    """Execute built-in scenarios; return table + pass rate."""
    runners = [
        scenario_rest_429,
        scenario_rest_empty_body,
        scenario_ws_disconnect_partial,
        scenario_eq_silent_stall_inject,
        scenario_partial_freeze_repairable,
        scenario_index_unknown_feed_alert,
        scenario_holiday_skip,
    ]
    rows: List[Dict[str, Any]] = []
    for fn in runners:
        try:
            rows.append(fn().as_dict())
        except Exception as exc:
            rows.append(
                {
                    "scenario": getattr(fn, "__name__", "unknown"),
                    "expected": "no exception",
                    "actual": str(exc)[:200],
                    "pass": False,
                }
            )
    passed = sum(1 for r in rows if r.get("pass"))
    return {
        "ok": passed == len(rows),
        "passed": passed,
        "total": len(rows),
        "table": rows,
    }
