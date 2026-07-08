"""Live Kavach recompute for locked checklist symbols (10m candle cache).

Uses closed 10-minute bars aggregated from 5m cache — see kavach_10m.py for
chart parity notes. Every recompute is persisted to rs_live_kavach_audit.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict, Optional

import pytz
from sqlalchemy import text

from backend.services.daily_checklist import _auto_fields_from_rs
from backend.services.kavach_10m import metrics_from_10m_candles
from backend.services.rs_conviction_candles import candles_cache_only, load_instrument_atr_maps
from backend.services.rs_live_kavach_audit import last_audit_state, latest_audit_pair, persist_live_kavach_audit, prune_old_audit_rows
from backend.services.relative_strength_scanner import RANKING_BEARISH, RANKING_BULLISH

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _ranking_for_direction(direction: str) -> str:
    return RANKING_BEARISH if direction == "SHORT" else RANKING_BULLISH


def _latest_nifty_pct(db) -> float:
    row = db.execute(
        text(
            """
            SELECT nifty_percent FROM relative_strength_snapshot
            WHERE scan_time = (SELECT MAX(scan_time) FROM relative_strength_snapshot)
            LIMIT 1
            """
        )
    ).fetchone()
    if row and row.nifty_percent is not None:
        return float(row.nifty_percent)
    return 0.0


def recompute_locked_symbol(
    db,
    symbol: str,
    direction: str,
    *,
    session_date: Optional[str] = None,
    persist_audit: bool = True,
) -> Optional[Dict[str, Any]]:
    """Return checklist auto fields + indicator_as_of from live 10m evaluation, or None."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    ikey_map, _ = load_instrument_atr_maps(db, {sym})
    ikey = ikey_map.get(sym)
    if not ikey:
        return None
    candles = candles_cache_only(ikey)
    if not candles:
        return None
    ranking = _ranking_for_direction(direction)
    nifty_pct = _latest_nifty_pct(db)
    metrics = metrics_from_10m_candles(candles, ranking_type=ranking, nifty_pct=nifty_pct)
    if not metrics:
        return None

    sd = session_date or metrics["bar_evaluated_at"].astimezone(IST).strftime("%Y-%m-%d")
    prev_state = last_audit_state(db, sd, sym) if persist_audit else None
    if persist_audit:
        try:
            persist_live_kavach_audit(
                db,
                symbol=sym,
                lock_direction=direction,
                metrics=metrics,
                prev_kavach_state=prev_state,
            )
            prune_old_audit_rows(db)
        except Exception as exc:
            logger.debug("live kavach audit persist skipped: %s", exc)

    row = SimpleNamespace(**metrics, symbol=sym, scan_time=metrics["bar_evaluated_at"])
    fields = _auto_fields_from_rs(row, direction, live_map={})
    fields["chart_reversed"] = _chart_reversed(metrics.get("kavach_state"), direction)
    computed_at = metrics["bar_evaluated_at"]
    return {
        "fields": fields,
        "indicator_as_of": computed_at,
        "source": "live_recompute_10m",
        "kavach_state": metrics.get("kavach_state"),
        "prev_kavach_state": prev_state,
        "metrics": metrics,
    }


def _chart_reversed(kavach_state: Optional[str], lock_direction: str) -> bool:
    from backend.services.kavach_engine import BEARISH_STATES, BULLISH_STATES

    k = (kavach_state or "").upper()
    lock = (lock_direction or "LONG").upper()
    if lock == "SHORT":
        return k in BULLISH_STATES
    return k in BEARISH_STATES


def is_indicator_stale(
    indicator_as_of: Optional[datetime],
    latest_rs_scan: Optional[datetime],
    *,
    stale_minutes: int = 10,
) -> bool:
    """True when indicator data is too old vs latest RS batch or wall clock."""
    now = datetime.now(IST)
    if indicator_as_of is None:
        return True
    ia = indicator_as_of.astimezone(IST) if indicator_as_of.tzinfo else indicator_as_of.replace(tzinfo=IST)
    age_min = (now - ia).total_seconds() / 60.0
    if age_min > stale_minutes:
        return True
    if latest_rs_scan is not None:
        ls = latest_rs_scan.astimezone(IST) if latest_rs_scan.tzinfo else latest_rs_scan.replace(tzinfo=IST)
        if ls - ia > timedelta(minutes=stale_minutes):
            return True
    return False
