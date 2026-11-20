"""Phase A / Phase B orchestration for Open Low High (early grey cards + qualifier-only B)."""
from __future__ import annotations

import logging
import threading
import time as time_mod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz

from backend.services.open_high_low.bars import bar_at
from backend.services.open_high_low.data import front_month_expiry, load_universe
from backend.services.open_low_high.ohl_log import ohl_log
from backend.services.open_low_high.persistence import (
    ensure_tables,
    is_day_frozen,
    list_screen_for_date,
    sort_screen_rows,
    upsert_day_meta,
    upsert_screen_rows,
)
from backend.services.open_low_high.signals import evaluate_phase_b

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

from backend.services.open_low_high.config import (
    FREEZE_COVERAGE_MIN,
    PHASE_A_START,
    PHASE_B_HARD_STOP,
    PHASE_B_START,
    REST_MAX_WORKERS,
    REST_PACING_SLEEP_SEC,
)

PHASE_A_END = time(9, 23, 0)
PHASE_A_REFILL = time(9, 24, 0)

_phase_a_lock = threading.Lock()
_phase_b_lock = threading.Lock()
_last_phase_a: Dict[str, Any] = {}
_last_phase_b: Dict[str, Any] = {}


def is_phase_a_busy() -> bool:
    """True when Phase A holds ``_phase_a_lock`` (non-blocking probe)."""
    acquired = _phase_a_lock.acquire(blocking=False)
    if acquired:
        _phase_a_lock.release()
        return False
    return True


def _now() -> datetime:
    return datetime.now(IST)


def _today() -> date:
    return _now().date()


def phase_a_window_status(now: Optional[datetime] = None) -> str:
    """waiting | open | past for Phase A primary window 09:20:10–09:23."""
    t = now or _now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    tt = t.time()
    if tt < PHASE_A_START:
        return "waiting"
    if tt <= PHASE_A_END:
        return "open"
    return "past"


def phase_b_window_status(now: Optional[datetime] = None) -> str:
    """waiting | open | past for Phase B 09:25:03–09:29."""
    t = now or _now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    tt = t.time()
    if tt < PHASE_B_START:
        return "waiting"
    if tt <= PHASE_B_HARD_STOP:
        return "open"
    return "past"


def past_phase_b_deadline(now: Optional[datetime] = None) -> bool:
    t = now or _now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    return t.time() >= PHASE_B_HARD_STOP


def run_phase_a(
    *,
    force: bool = False,
    session: Optional[date] = None,
    max_workers: int = REST_MAX_WORKERS,
    refill_only_symbols: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Phase A on fresh 09:15 5m candle — persist grey cards immediately (not frozen)."""
    from backend.services.open_low_high.screener import (
        _apply_sector_badges,
        _evaluate_nifty_header_cue,
        _nifty_data_ready,
        _screen_one,
        _upstox,
    )

    ensure_tables()
    trade_date = session or _today()
    now = _now()
    win = phase_a_window_status(now)

    if is_day_frozen(trade_date) and not force:
        rows = list_screen_for_date(trade_date)
        return {
            "ok": True,
            "status": "frozen",
            "trade_date": trade_date.isoformat(),
            "phase_a": len(rows),
            "frozen": True,
            "message": "Already frozen — Phase A skipped.",
        }

    if win == "waiting" and not force and refill_only_symbols is None:
        return {
            "ok": True,
            "status": "waiting",
            "trade_date": trade_date.isoformat(),
            "window": win,
            "message": "Waiting for Phase A window 09:20:10 IST.",
        }

    if not _phase_a_lock.acquire(blocking=False):
        return {"ok": False, "status": "busy", "message": "Phase A already running."}

    try:
        upstox = _upstox()
        universe = load_universe()
        if refill_only_symbols is not None:
            want = {s.upper() for s in refill_only_symbols}
            universe = [u for u in universe if str(u.get("symbol") or "").upper() in want]
        expiry = front_month_expiry(trade_date)
        ohl_log(
            "success",
            action="phase_a_start",
            detail=f"symbols={len(universe)} force={force} refill={refill_only_symbols is not None}",
        )
        results: List[Dict[str, Any]] = []
        miss_stats: Dict[str, int] = {
            "no_equity_key": 0,
            "no_bars": 0,
            "no_0915": 0,
            "no_prev": 0,
        }
        miss_lock = threading.Lock()

        def _one(urow: Dict[str, str]) -> Optional[Dict[str, Any]]:
            local_miss: Dict[str, int] = {}
            try:
                got = _screen_one(
                    upstox,
                    urow,
                    trade_date,
                    expiry,
                    local_miss,
                    phase_a_only=True,
                )
            except Exception:
                logger.exception("OHL phase_a symbol failed %s", urow.get("symbol"))
                ohl_log(
                    "failed",
                    action="phase_a_screen",
                    symbol=str(urow.get("symbol") or ""),
                    detail="exception",
                )
                return None
            if local_miss:
                with miss_lock:
                    for k, v in local_miss.items():
                        miss_stats[k] = int(miss_stats.get(k) or 0) + int(v)
            return got

        with ThreadPoolExecutor(max_workers=max(1, min(max_workers, 8))) as pool:
            futs = [pool.submit(_one, u) for u in universe]
            for fut in as_completed(futs):
                try:
                    got = fut.result()
                except Exception:
                    continue
                if got:
                    results.append(got)
                time_mod.sleep(float(REST_PACING_SLEEP_SEC))

        existing = list_screen_for_date(trade_date)
        if refill_only_symbols is not None:
            by_key = {
                (str(r.get("symbol") or "").upper(), str(r.get("side") or "").upper()): r
                for r in existing
            }
            for r in results:
                by_key[
                    (str(r.get("symbol") or "").upper(), str(r.get("side") or "").upper())
                ] = r
            results = list(by_key.values())
        elif not results and existing:
            # Transient empty pass — do not wipe board.
            ohl_log(
                "skipped",
                action="phase_a_persist",
                detail=f"empty_pass_kept_existing={len(existing)}",
            )
            return {
                "ok": True,
                "status": "phase_a",
                "trade_date": trade_date.isoformat(),
                "phase_a": len(existing),
                "message": f"Phase A empty pass — kept existing {len(existing)} cards.",
                "kept_existing": True,
            }

        # Sector badges optional at Phase A (not frozen yet).
        tagged = []
        for c in results:
            card = dict(c)
            card["active"] = False
            card["phase_b_ok"] = False
            card["frozen"] = False
            tagged.append(card)
        try:
            tagged = _apply_sector_badges(tagged)
            for c in tagged:
                c["frozen"] = False  # badges helper marks frozen=True
        except Exception:
            pass
        tagged = sort_screen_rows(tagged)

        nifty_cue = _evaluate_nifty_header_cue(upstox, trade_date)
        # Phase A nifty cue: 09:20 not required yet.
        nifty_ok = _nifty_data_ready(nifty_cue, require_0920=False)
        from backend.services.open_low_high.screener import (
            equity_coverage_from_miss,
            freeze_gate,
        )

        coverage = equity_coverage_from_miss(scanned=len(universe), miss_stats=miss_stats)
        may_freeze, gate_reason = freeze_gate(coverage=coverage, nifty_present=nifty_ok)

        upsert_screen_rows(trade_date, tagged, frozen=False)
        bullish = [c for c in tagged if c.get("side") == "BULLISH"]
        bearish = [c for c in tagged if c.get("side") == "BEARISH"]
        phase_ts = _now()
        degraded = (not may_freeze) and (len(tagged) == 0 or not nifty_ok)
        status = "degraded" if degraded else "phase_a"
        day_meta = {
            "window": win,
            "expiry": expiry.isoformat(),
            "phase": "A",
            "phase_a": len(tagged),
            "active": 0,
            "wait": 0,
            "candidates": len(tagged),
            "coverage": round(coverage, 4),
            "freeze_gate": gate_reason,
            "nifty_ready": nifty_ok,
            "degraded": degraded,
            **nifty_cue,
        }
        upsert_day_meta(
            trade_date,
            status=status,
            frozen=False,
            degraded=degraded,
            bullish_count=len(bullish),
            bearish_count=len(bearish),
            phase_a_at=phase_ts,
            notes=f"phase_a={len(tagged)} scanned={len(universe)} cov={coverage:.2%} miss={miss_stats}",
            meta=day_meta,
        )
        payload = {
            "ok": True,
            "status": status,
            "trade_date": trade_date.isoformat(),
            "window": win,
            "frozen": False,
            "degraded": degraded,
            "coverage": round(coverage, 4),
            "bullish": bullish,
            "bearish": bearish,
            "phase_a": len(tagged),
            "scanned": len(universe),
            "miss_stats": miss_stats,
            "nifty_gap": nifty_cue.get("nifty_gap"),
            "nifty_bias": nifty_cue.get("nifty_bias"),
            "nifty_show_on": nifty_cue.get("nifty_show_on"),
            "message": (
                f"Phase A complete — {len(tagged)} grey candidates on board "
                f"(Phase B from {PHASE_B_START.strftime('%H:%M:%S')} IST)."
            ),
        }
        global _last_phase_a
        _last_phase_a = dict(payload)
        _last_phase_a["at"] = phase_ts.isoformat()
        ohl_log(
            "success" if not degraded else "failed",
            action="phase_a_done",
            detail=f"phase_a={len(tagged)} scanned={len(universe)} cov={coverage:.2%} degraded={degraded}",
        )
        try:
            from backend.services.open_low_high.alerts import alert_degraded, alert_phase_stats

            alert_phase_stats(
                "Phase A",
                trade_date=trade_date.isoformat(),
                phase_a=len(tagged),
                coverage=coverage,
                nifty_ready=nifty_ok,
                status=status,
                extra=f"miss={miss_stats} gate={gate_reason}",
            )
            if len(tagged) == 0:
                alert_degraded(
                    "Phase A=0 after scan",
                    trade_date=trade_date.isoformat(),
                    coverage=round(coverage, 4),
                    nifty_ready=nifty_ok,
                )
            elif not nifty_ok:
                alert_degraded(
                    "Nifty null at Phase A",
                    trade_date=trade_date.isoformat(),
                    phase_a=len(tagged),
                )
        except Exception:
            logger.debug("OLH phase_a alert failed", exc_info=True)
        logger.info(
            "OHL phase_a done phase_a=%s scanned=%s cov=%.2f degraded=%s",
            len(tagged),
            len(universe),
            coverage,
            degraded,
        )
        return payload
    except Exception as exc:
        logger.exception("OHL phase_a failed")
        ohl_log("failed", action="phase_a_done", detail=str(exc)[:160])
        return {"ok": False, "status": "error", "message": str(exc)}
    finally:
        _phase_a_lock.release()


def run_phase_a_refill(
    *,
    force: bool = False,
    session: Optional[date] = None,
    max_workers: int = 6,
) -> Dict[str, Any]:
    """09:24: Phase A only for symbols whose 09:15 OHLC is still null/empty."""
    from backend.services.open_high_low.bars import bar_at
    from backend.services.open_low_high.screener import (
        FIRST_CANDLE,
        _resolve_equity_key,
        _session_5m_bars_try,
        _upstox,
    )

    ensure_tables()
    trade_date = session or _today()
    if is_day_frozen(trade_date) and not force:
        return {"ok": True, "status": "frozen", "filled": 0, "message": "Frozen — refill skipped."}

    upstox = _upstox()
    universe = load_universe()
    existing = list_screen_for_date(trade_date)
    have_0915: Set[str] = set()
    for r in existing:
        if r.get("first_open") is not None and r.get("first_close") is not None:
            have_0915.add(str(r.get("symbol") or "").upper())

    # Strict subset: 09:15 OHLC still null/empty (filter rejects with bars are skipped).
    missing: Set[str] = set()
    for urow in universe:
        sym = str(urow.get("symbol") or "").upper()
        if not sym or sym in have_0915:
            continue
        ik = _resolve_equity_key(upstox, urow, sym)
        if not ik:
            missing.add(sym)
            ohl_log("skipped", action="phase_a_refill_probe", symbol=sym, detail="no_equity_key")
            continue
        bars, _src = _session_5m_bars_try(
            upstox, ik, trade_date, symbol=sym, action="phase_a_refill_probe"
        )
        first = bar_at(bars, trade_date, FIRST_CANDLE) if bars else None
        if first is None:
            missing.add(sym)
            ohl_log("skipped", action="phase_a_refill_probe", symbol=sym, detail="still_null_0915")
        else:
            ohl_log(
                "skipped",
                action="phase_a_refill_probe",
                symbol=sym,
                source=_src,
                detail="has_0915_not_phase_a_qualifier",
            )
    ohl_log(
        "success",
        action="phase_a_refill_start",
        detail=f"missing={len(missing)} have={len(have_0915)}",
    )
    if not missing:
        return {
            "ok": True,
            "status": "phase_a_refill",
            "filled": 0,
            "still_missing": 0,
            "message": "Phase A refill — nothing missing.",
        }

    before = len(existing)
    result = run_phase_a(
        force=True,
        session=trade_date,
        max_workers=max_workers,
        refill_only_symbols=missing,
    )
    after_rows = list_screen_for_date(trade_date)
    filled = max(0, len(after_rows) - before)
    # Recompute still missing among the refill set.
    have2 = {
        str(r.get("symbol") or "").upper()
        for r in after_rows
        if r.get("first_close") is not None
    }
    still = sorted(s for s in missing if s not in have2)
    result["status"] = "phase_a_refill"
    result["filled"] = filled
    result["still_missing"] = len(still)
    result["still_missing_symbols"] = still[:40]
    result["message"] = (
        f"Phase A refill — filled≈{filled}, still missing 09:15={len(still)}."
    )
    ohl_log(
        "success",
        action="phase_a_refill_done",
        detail=f"filled={filled} still={len(still)}",
    )
    return result


def _try_0920_bar(
    upstox: Any,
    equity_ik: str,
    session: date,
    *,
    symbol: str,
    source: str,
) -> Optional[Any]:
    from backend.services.open_low_high.screener import SECOND_CANDLE, _fetch_rest_5m, _fetch_ws_5m_bars

    if source == "ws":
        bars, trust = _fetch_ws_5m_bars(equity_ik, session)
        if trust in ("UNTRUSTED", "PARTIAL"):
            bars = []  # Phase B: TRUSTED only else REST
    else:
        bars = _fetch_rest_5m(upstox, equity_ik, session)
    b = bar_at(bars, session, SECOND_CANDLE) if bars else None
    ohl_log(
        "success" if b is not None else "failed",
        action="phase_b_0920",
        symbol=symbol,
        source=source,
        detail="got 09:20" if b is not None else "miss 09:20",
    )
    return b


def run_phase_b(
    *,
    force: bool = False,
    session: Optional[date] = None,
    max_workers: int = REST_MAX_WORKERS,
) -> Dict[str, Any]:
    """Phase B only on Phase A qualifiers — WS→REST→wait30→REST until done or 09:29."""
    from backend.services.open_high_low.bars import bar_at
    from backend.services.open_low_high.screener import (
        FIRST_CANDLE,
        SECOND_CANDLE,
        _apply_sector_badges,
        _evaluate_nifty_header_cue,
        _fetch_daily_prev,
        _freeze_fut_entry,
        _session_5m_bars,
        _upstox,
    )
    from backend.services.open_low_high.signals import next_pivot_targets, open_extreme_badge

    ensure_tables()
    trade_date = session or _today()
    now = _now()
    win = phase_b_window_status(now)

    if is_day_frozen(trade_date) and not force:
        rows = list_screen_for_date(trade_date)
        return {
            "ok": True,
            "status": "frozen",
            "trade_date": trade_date.isoformat(),
            "phase_a": len(rows),
            "active": sum(1 for r in rows if r.get("active")),
            "frozen": True,
            "message": "Already frozen.",
        }

    if win == "waiting" and not force:
        return {
            "ok": True,
            "status": "waiting",
            "trade_date": trade_date.isoformat(),
            "message": "Waiting for Phase B window 09:25:03 IST.",
        }

    if not _phase_b_lock.acquire(blocking=False):
        return {"ok": False, "status": "busy", "message": "Phase B already running."}

    try:
        phase_a_rows = list_screen_for_date(trade_date)
        if not phase_a_rows:
            # Never lock an empty Phase A as a valid frozen day (19-Nov scenario).
            msg = "Phase B — no Phase A qualifiers on board."
            ohl_log("skipped", action="phase_b_start", detail="no_phase_a_rows")
            failures = ["No Phase A qualifiers to confirm."]
            past = past_phase_b_deadline(now)
            status = "degraded" if past else "phase_a"
            upsert_day_meta(
                trade_date,
                status=status,
                frozen=False,
                degraded=bool(past),
                notes=msg,
                meta={
                    "phase": "B",
                    "phase_b_failures": failures,
                    "degraded": bool(past),
                    "failure_banner": (
                        "DEGRADED — Phase A=0 at hard stop; not frozen." if past else None
                    ),
                },
            )
            if past:
                try:
                    from backend.services.open_low_high.alerts import alert_degraded

                    alert_degraded(msg, trade_date=trade_date.isoformat(), phase_a=0)
                except Exception:
                    pass
            return {
                "ok": True,
                "status": status,
                "phase_a": 0,
                "active": 0,
                "frozen": False,
                "degraded": bool(past),
                "phase_b_failures": len(failures),
                "failure_banner": (
                    "DEGRADED — Phase A=0 at hard stop; not frozen." if past else None
                ),
                "message": msg,
            }

        upstox = _upstox()
        ohl_log(
            "success",
            action="phase_b_start",
            detail=f"qualifiers={len(phase_a_rows)} force={force}",
        )

        # Map symbol → 09:20 equity bar via WS → REST → (wait 30s) → REST (cohort-level).
        seconds: Dict[str, Any] = {}
        already_done: Dict[str, Dict[str, Any]] = {}
        pending: List[Tuple[str, str]] = []  # (symbol, equity_ik)
        for row in phase_a_rows:
            sym = str(row.get("symbol") or "").upper()
            equity_ik = str(row.get("equity_instrument_key") or "").strip()
            # Prior Phase B tick already evaluated this qualifier — keep as-is.
            if row.get("second_close") is not None and (
                row.get("active")
                or row.get("wait_list")
                or row.get("phase_b_ok")
                or (
                    row.get("phase_b_fail_reason")
                    and row.get("phase_b_fail_reason") != "09:20 data missing"
                    and row.get("phase_b_fail_reason") != "Phase B pending"
                )
            ):
                already_done[sym] = dict(row)
                ohl_log("skipped", action="phase_b_0920", symbol=sym, detail="already_evaluated")
                continue
            if not equity_ik:
                ohl_log("failed", action="phase_b_0920", symbol=sym, detail="no equity key")
                continue
            pending.append((sym, equity_ik))

        def _fill(source: str, cohort: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
            left: List[Tuple[str, str]] = []
            for sym, ik in cohort:
                if past_phase_b_deadline() and not force:
                    left.append((sym, ik))
                    ohl_log(
                        "skipped",
                        action="phase_b_0920",
                        symbol=sym,
                        source=source,
                        detail="past_hard_stop",
                    )
                    continue
                b = _try_0920_bar(upstox, ik, trade_date, symbol=sym, source=source)
                if b is not None:
                    seconds[sym] = b
                else:
                    left.append((sym, ik))
                time_mod.sleep(0.02)
            return left

        pending = _fill("ws", pending)
        if pending:
            pending = _fill("rest", pending)
        if pending and not (past_phase_b_deadline() and not force):
            ohl_log(
                "success",
                action="phase_b_wait30",
                detail=f"waiting 30s for {len(pending)} symbols",
            )
            time_mod.sleep(30)
            if not (past_phase_b_deadline() and not force):
                pending = _fill("rest", pending)

        updated: List[Dict[str, Any]] = []
        still_missing: List[str] = []
        fail_details: List[str] = []

        for row in phase_a_rows:
            sym = str(row.get("symbol") or "").upper()
            if sym in already_done:
                updated.append(already_done[sym])
                continue
            fut_ik = str(row.get("instrument_key") or "").strip()
            side = str(row.get("side") or "").upper()
            card = dict(row)
            second = seconds.get(sym)

            if second is None:
                still_missing.append(sym)
                fail_details.append(f"{sym}: 09:20 missing")
                card["phase_b_fail_reason"] = "09:20 data missing"
                card["active"] = False
                card["phase_b_ok"] = False
                updated.append(card)
                continue

            phase_b = evaluate_phase_b(
                side,
                first_close=float(card["first_close"]),
                second_open=float(second.open),
                second_high=float(second.high),
                second_low=float(second.low),
                second_close=float(second.close),
            )
            phase_b_ok = bool(phase_b.ok)
            wait_list = bool(getattr(phase_b, "wait_list", False)) and not phase_b_ok
            card["second_close"] = float(second.close)
            card["active"] = phase_b_ok
            card["phase_b_ok"] = phase_b_ok
            card["wait_list"] = wait_list
            card["phase_b_fail_reason"] = (
                None if phase_b_ok else (phase_b.fail_reason or "Phase B failed")
            )
            card["wick_body_ratio"] = getattr(phase_b, "wick_body_ratio", None)
            card["olh_badge"] = open_extreme_badge(
                side,
                first_open=float(card["first_open"]),
                first_high=float(card["first_high"]),
                first_low=float(card["first_low"]),
            )

            if fut_ik:
                fut_bars = _session_5m_bars(upstox, fut_ik, trade_date)
                fut_first = bar_at(fut_bars, trade_date, FIRST_CANDLE) if fut_bars else None
                fut_second = bar_at(fut_bars, trade_date, SECOND_CANDLE) if fut_bars else None
                if fut_first is not None:
                    card["sl"] = float(fut_first.open)
                fut_second_close = float(fut_second.close) if fut_second is not None else None
                fut_first_close = float(fut_first.close) if fut_first is not None else None
                fut_entry_snap, entry_method = _freeze_fut_entry(
                    side=side,
                    fut_ik=fut_ik,
                    session=trade_date,
                    fut_second_close=fut_second_close,
                    fut_first_close=fut_first_close,
                )
                card["entry"] = fut_entry_snap
                card["entry_method"] = entry_method
                fut_prev_h, fut_prev_l, fut_prev_c = _fetch_daily_prev(upstox, fut_ik, trade_date)
                if (
                    fut_entry_snap is not None
                    and fut_prev_h is not None
                    and fut_prev_l is not None
                    and fut_prev_c is not None
                ):
                    try:
                        t1, t2 = next_pivot_targets(
                            side,
                            prev_high=float(fut_prev_h),
                            prev_low=float(fut_prev_l),
                            prev_close=float(fut_prev_c),
                            reference=float(fut_entry_snap),
                        )
                        card["t1"], card["t2"] = t1, t2
                    except Exception:
                        pass

            ohl_log(
                "success" if phase_b_ok or wait_list else "failed",
                action="phase_b_eval",
                symbol=sym,
                detail=f"ok={phase_b_ok} wait={wait_list} reason={card.get('phase_b_fail_reason')}",
            )
            updated.append(card)

        tagged = _apply_sector_badges(updated)
        complete = len(still_missing) == 0
        past = past_phase_b_deadline() or force

        nifty_cue = _evaluate_nifty_header_cue(upstox, trade_date)
        from backend.services.open_low_high.screener import _nifty_data_ready, freeze_gate

        nifty_ok = _nifty_data_ready(nifty_cue, require_0920=True)
        # Coverage among Phase A cohort that received 09:20.
        cohort_n = max(len(tagged), 1)
        got_0920 = cohort_n - len(still_missing)
        coverage = float(got_0920) / float(cohort_n) if tagged else 0.0
        # Also require Nifty; when Phase A empty already handled above.
        may_freeze, gate_reason = freeze_gate(
            coverage=coverage if tagged else 0.0,
            nifty_present=nifty_ok,
            coverage_min=FREEZE_COVERAGE_MIN,
        )
        # Freeze only when complete (or past deadline) AND freeze gate passes.
        want_close = complete or past
        should_freeze = bool(want_close and may_freeze and len(tagged) > 0)
        degraded = bool(want_close and not should_freeze)

        if should_freeze:
            for c in tagged:
                c["frozen"] = True
            upsert_screen_rows(trade_date, tagged, frozen=True)
        else:
            for c in tagged:
                c["frozen"] = False
            # Idempotent: never wipe non-empty board with empty.
            if tagged:
                upsert_screen_rows(trade_date, tagged, frozen=False)

        bullish = [c for c in tagged if c.get("side") == "BULLISH"]
        bearish = [c for c in tagged if c.get("side") == "BEARISH"]
        active_n = sum(1 for c in tagged if c.get("active"))
        wait_n = sum(1 for c in tagged if c.get("wait_list") and not c.get("active"))
        non_active_n = len(tagged) - active_n - wait_n

        failure_banner = None
        status = "phase_b"
        if degraded:
            status = "degraded"
            failure_banner = (
                f"DEGRADED — freeze gate failed ({gate_reason}); "
                f"coverage={coverage:.0%} nifty={nifty_ok}; repairable, not frozen."
            )
            ohl_log("failed", action="phase_b_degraded", detail=failure_banner)
        elif should_freeze and still_missing:
            failure_banner = (
                f"Phase B hard stop 09:29 — missing 09:20 data for "
                f"{len(still_missing)} symbol(s): {', '.join(still_missing[:12])}"
                + ("…" if len(still_missing) > 12 else "")
            )
            status = "phase_b_partial"
            ohl_log("failed", action="phase_b_hard_stop", detail=failure_banner)
        elif should_freeze:
            status = "frozen"

        day_meta = {
            "window": win,
            "phase": "B",
            "phase_a": len(tagged),
            "active": active_n,
            "wait": wait_n,
            "non_active": non_active_n,
            "candidates": wait_n + non_active_n,
            "still_missing_0920": still_missing,
            "phase_b_failures": fail_details,
            "failure_banner": failure_banner,
            "coverage": round(coverage, 4),
            "freeze_gate": gate_reason,
            "nifty_ready": nifty_ok,
            "degraded": degraded,
            **nifty_cue,
        }
        upsert_day_meta(
            trade_date,
            status=status if status != "frozen" else "frozen",
            frozen=bool(should_freeze),
            degraded=degraded,
            bullish_count=len(bullish),
            bearish_count=len(bearish),
            phase_b_at=_now(),
            notes=(
                f"phase_b active={active_n} wait={wait_n} missing_0920={len(still_missing)} "
                f"freeze={should_freeze} degraded={degraded} cov={coverage:.2%}"
            ),
            meta=day_meta,
        )
        payload = {
            "ok": True,
            "status": status,
            "trade_date": trade_date.isoformat(),
            "window": win,
            "frozen": bool(should_freeze),
            "degraded": degraded,
            "coverage": round(coverage, 4),
            "bullish": bullish,
            "bearish": bearish,
            "phase_a": len(tagged),
            "active": active_n,
            "wait": wait_n,
            "non_active": non_active_n,
            "phase_b_failures": len(fail_details),
            "still_missing": still_missing,
            "failure_banner": failure_banner,
            "nifty_gap": nifty_cue.get("nifty_gap"),
            "nifty_bias": nifty_cue.get("nifty_bias"),
            "nifty_show_on": nifty_cue.get("nifty_show_on"),
            "message": failure_banner
            or (
                f"Phase B {'frozen' if should_freeze else 'in progress'} — "
                f"{active_n} active, {wait_n} wait, {non_active_n} non-active."
            ),
        }
        global _last_phase_b
        _last_phase_b = dict(payload)
        _last_phase_b["at"] = _now().isoformat()
        try:
            from backend.services.open_low_high.alerts import alert_degraded, alert_phase_stats

            alert_phase_stats(
                "Phase B",
                trade_date=trade_date.isoformat(),
                phase_a=len(tagged),
                active=active_n,
                wait=wait_n,
                coverage=coverage,
                nifty_ready=nifty_ok,
                status=status,
            )
            if degraded:
                alert_degraded(
                    failure_banner or "DEGRADED",
                    trade_date=trade_date.isoformat(),
                    coverage=round(coverage, 4),
                    nifty_ready=nifty_ok,
                )
            if not nifty_ok:
                alert_degraded(
                    "Nifty null at Phase B",
                    trade_date=trade_date.isoformat(),
                    phase_a=len(tagged),
                )
        except Exception:
            logger.debug("OLH phase_b alert failed", exc_info=True)
        logger.info(
            "OHL phase_b done status=%s active=%s missing=%s freeze=%s degraded=%s",
            status,
            active_n,
            len(still_missing),
            should_freeze,
            degraded,
        )
        return payload
    except Exception as exc:
        logger.exception("OHL phase_b failed")
        ohl_log("failed", action="phase_b_done", detail=str(exc)[:160])
        upsert_day_meta(
            trade_date,
            status="error",
            frozen=False,
            notes=str(exc)[:500],
            meta={"failure_banner": f"Phase B error: {exc}"},
        )
        return {
            "ok": False,
            "status": "error",
            "message": str(exc),
            "failure_banner": f"Phase B error: {exc}",
        }
    finally:
        _phase_b_lock.release()
