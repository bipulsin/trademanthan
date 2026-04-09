"""Orchestration jobs for Smart Futures."""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, List

import pytz

from backend.services.smart_futures import data_service
from backend.services.smart_futures import repository
from backend.services.smart_futures.order_manager import audit, place_entry, place_exit
from backend.services.smart_futures.scanner import scan_symbol
from backend.services.smart_futures.session_calendar import effective_session_date_ist
from backend.services.smart_futures.signal_engine import should_exit_position

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def run_smart_futures_scan_job(force: bool = False) -> Dict[str, Any]:
    """
    Periodic job: fetch universe, run scanner, persist top candidates.
    Does not auto-place orders (only when Live + manual or future auto hook).
    """
    now = datetime.now(IST)
    hm = now.hour * 60 + now.minute
    # Market-ish window: skip heavy work outside 9:15–15:30 IST (unless force=True for admin one-off).
    if not force and (hm < 9 * 60 + 15 or hm > 15 * 60 + 30):
        logger.info("smart_futures scan skipped (outside window) ist=%s", now.isoformat())
        return {"skipped": True, "reason": "outside_window"}
    if force:
        logger.info("smart_futures scan running with force=True (window check bypassed) ist=%s", now.isoformat())

    rows = repository.fetch_future_symbols()
    max_n = data_service._max_symbols()
    rows = rows[:max_n]
    cfg = repository.get_config()
    # Layer 1 (bull/bear prefilter) must pass before Renko; Layer 2 = structure + score; persist only structure_pass.
    logger.info(
        "smart_futures scan begin symbols=%s (cap %s) ist=%s — sequential Upstox calls per symbol, may take many minutes",
        len(rows),
        max_n,
        now.isoformat(),
    )
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        sym = row["symbol"]
        ik = row["instrument_key"]
        if i % 10 == 0:
            logger.info("smart_futures scan progress %s/%s %s", i + 1, len(rows), sym)
        try:
            r = scan_symbol(sym, ik, now, cfg)
            if r is None:
                continue
            r.pop("meta", None)
            r.pop("prefilter_reason", None)
            r.pop("layer1_side", None)
            if r.get("structure_pass"):
                out.append(r)
        except Exception as e:
            logger.warning("smart_futures scan error %s: %s", sym, e)
        if i % 20 == 0 and i > 0:
            time.sleep(0.15)

    out.sort(key=lambda x: (-(x.get("score") or 0), x.get("symbol") or ""))
    out = out[:50]
    session_d = effective_session_date_ist()
    repository.replace_candidates_session(session_d, out)
    logger.info("smart_futures scan stored %s candidates for %s", len(out), session_d)
    return {"ok": True, "count": len(out), "session_date": str(session_d)}


def run_smart_futures_exit_check_job() -> Dict[str, Any]:
    """Every ~60s: evaluate 1m Renko exit for open positions (square-off only if Live)."""
    cfg = repository.get_config()
    session_d = effective_session_date_ist()
    positions = repository.list_open_positions(session_d)
    _sync_exit_flags(session_d, positions)
    return {"ok": True, "positions_checked": len(positions), "live": cfg["live_enabled"]}


def _sync_exit_flags(session_d, positions) -> None:
    """Update exit_ready on smart_futures_candidate rows (best-effort)."""
    from sqlalchemy import text
    from backend.database import engine

    for pos in positions:
        ik = pos["instrument_key"]
        mb = float(pos["main_brick_size"] or 0.0)
        dr = pos["direction"]
        try:
            flag = should_exit_position(ik, dr, mb)
        except Exception:
            flag = False
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE smart_futures_candidate
                        SET exit_ready = :ex, updated_at = CURRENT_TIMESTAMP
                        WHERE session_date = :d AND instrument_key = :ik
                        """
                    ),
                    {"ex": flag, "d": session_d, "ik": ik},
                )
        except Exception as e:
            logger.debug("sync exit flag: %s", e)


def force_exit_all_smart_futures_positions(user_id=None) -> Dict[str, Any]:
    """3:15 PM — close all open Smart Futures positions if Live."""
    cfg = repository.get_config()
    session_d = effective_session_date_ist()
    positions = repository.list_open_positions(session_d)
    if not cfg.get("live_enabled"):
        logger.info("smart_futures force exit skipped (live off)")
        return {"ok": True, "closed": 0, "reason": "live_disabled"}

    closed = 0
    for pos in positions:
        lots = int(pos["lots_open"])
        if lots <= 0:
            continue
        res = place_exit(
            user_id=user_id,
            instrument_key=pos["instrument_key"],
            direction=pos["direction"],
            quantity_lots=lots,
            tag="SMART_FUTURES_FORCE_EOD",
        )
        oid = (res.get("data") or {}).get("data", {}) if isinstance(res.get("data"), dict) else None
        order_id = res.get("order_id")
        if res.get("success"):
            audit(user_id, pos["id"], "EXIT_FULL", order_id, lots)
            repository.close_position(pos["id"])
            closed += 1
    logger.info("smart_futures force_exit closed=%s", closed)
    return {"ok": True, "closed": closed}


def auto_execute_entry_signals() -> Dict[str, Any]:
    """
    Optional: when live + entry_signal rows exist, place orders (disabled by default).
    Spec: no auto execution when Live is No — when Yes, user may click ORDER; this hook is for future automation.
    """
    return {"ok": True, "skipped": True}
