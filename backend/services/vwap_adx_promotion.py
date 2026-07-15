"""VWAP-slope + ADX independent lock-promotion path (live, additive).

Promotes symbols into ``daily_snapshot`` when slope ≥50 sustains for N consecutive
5m bars with Kavach ADX(14,14) >20 — parallel to RS-rank morning/intraday lock.
Does not change RS-rank logic, Confidence grading, or entry gates.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pytz
from sqlalchemy import text

from backend.services.daily_checklist_snapshot import (
    PROMOTION_CUTOFF_MIN,
    _log_membership,
    _upsert_snapshot_row,
    get_locked_symbol_rows,
    is_snapshot_locked,
)
from backend.services.kavach_momentum_ignition_validate import THRESHOLD_VWAP_SLOPE
from backend.services.kavach_volume import last_closed_bar_index
from backend.services.rs_vwap_quality import (
    consecutive_steep_adx_window,
    signed_vwap_slope_atr,
)
from backend.services.rs_conviction_signals import normalized_vwap_slope
from backend.services.rs_conviction_config import get_config

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

RULE_VWAP_ADX = "vwap_adx_promotion"
LOCKED_BY = "vwap_adx_promotion"
# Sentinel rank so VWAP+ names sort after RS Top-5 and are distinguishable.
VWAP_ADX_RANK = 90


def vwap_adx_promotion_enabled() -> bool:
    return os.environ.get("VWAP_ADX_PROMOTION_ENABLED", "true").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def vwap_adx_promotion_max() -> int:
    try:
        return max(0, int(os.environ.get("VWAP_ADX_PROMOTION_MAX", "4") or "4"))
    except (TypeError, ValueError):
        return 4


def vwap_adx_persist_bars() -> int:
    try:
        return max(1, int(os.environ.get("VWAP_ADX_PROMOTION_BARS", "3") or "3"))
    except (TypeError, ValueError):
        return 3


def vwap_persist_score_bump() -> int:
    """Additive Trade Score bump when steep slope persists ≥3 bars."""
    try:
        return max(0, int(os.environ.get("VWAP_PERSIST_SCORE_BUMP", "5") or "5"))
    except (TypeError, ValueError):
        return 5


def _side_from_long_short(direction: str) -> str:
    return "BEAR" if (direction or "").upper() in ("SHORT", "BEAR", "BEARISH") else "BULL"


def latest_lock_entry_rule(db, session_date: str, symbol: str) -> Optional[str]:
    """Most recent membership event rule if still an entry (None if removed / absent)."""
    row = db.execute(
        text(
            """
            SELECT event_type, rule
            FROM rs_lock_membership_audit
            WHERE session_date = CAST(:d AS date)
              AND UPPER(symbol) = :sym
            ORDER BY event_at DESC, id DESC
            LIMIT 1
            """
        ),
        {"d": session_date, "sym": (symbol or "").upper()},
    ).fetchone()
    if not row:
        return None
    if (row.event_type or "").lower() != "entry":
        return None
    return str(row.rule or "") or None


def vwap_adx_promoted_symbols(db, session_date: str) -> Set[str]:
    """Locked symbols whose latest audit entry is this promotion path."""
    locked = {str(r.symbol).upper() for r in get_locked_symbol_rows(db, session_date)}
    out: Set[str] = set()
    for sym in locked:
        if latest_lock_entry_rule(db, session_date, sym) == RULE_VWAP_ADX:
            out.add(sym)
    return out


def is_vwap_adx_lock(db, session_date: str, symbol: str) -> bool:
    return latest_lock_entry_rule(db, session_date, symbol) == RULE_VWAP_ADX


def promote_from_vwap_adx(
    db,
    session_date: str,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Scan universe (cache-only) and promote up to MAX extra slots.

    Requires: not locked, slope≥50 for N consecutive closed 5m bars, ADX>20 on
    those bars, same slope direction throughout. Cap enforced on concurrent
    ``vwap_adx_promotion`` members still in lock.
    """
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)

    empty: Dict[str, Any] = {
        "promoted": [],
        "skipped": True,
        "reason": "ok",
        "enabled": vwap_adx_promotion_enabled(),
        "cap": vwap_adx_promotion_max(),
        "slots_used": 0,
    }
    if not vwap_adx_promotion_enabled():
        return {**empty, "reason": "flag_off"}
    if not is_snapshot_locked(db, session_date):
        return {**empty, "reason": "not_locked"}
    if (now.hour * 60 + now.minute) > PROMOTION_CUTOFF_MIN:
        return {**empty, "reason": "past_cutoff"}

    cap = vwap_adx_promotion_max()
    n_bars = vwap_adx_persist_bars()
    already = vwap_adx_promoted_symbols(db, session_date)
    slots_used = len(already)
    empty["slots_used"] = slots_used
    if slots_used >= cap:
        return {**empty, "skipped": True, "reason": "cap_full"}

    from backend.services.kavach_universe_vwap_scan import _atr_map, _universe_keys
    from backend.services.rs_conviction_candles import candles_cache_only

    locked = {str(r.symbol).upper() for r in get_locked_symbol_rows(db, session_date)}
    universe = _universe_keys(db)
    atrs = _atr_map(db, [s for s, _ in universe])
    cfg = get_config()
    candidates: List[Dict[str, Any]] = []

    for sym, ik in universe:
        if sym in locked:
            continue
        candles = candles_cache_only(ik)
        if not candles or len(candles) < 40:
            continue
        atr = float(atrs.get(sym) or 1.0)
        # Fast reject: latest closed bar must be steep
        closed_idx = last_closed_bar_index(candles, now=now)
        if closed_idx < 0:
            continue
        from backend.services.rs_vwap_quality import _parse_ts

        as_of = _parse_ts(candles[closed_idx].get("timestamp"))
        if as_of is None:
            continue
        sliced = [c for c in candles if (_parse_ts(c.get("timestamp")) or as_of) <= as_of]
        if len(sliced) < 20:
            continue
        score = float(normalized_vwap_slope(sliced, atr, cfg))
        if score < THRESHOLD_VWAP_SLOPE:
            continue
        signed = float(signed_vwap_slope_atr(sliced, atr))
        if signed == 0:
            continue

        window = consecutive_steep_adx_window(
            candles,
            atr_daily_pct=atr,
            n_bars=n_bars,
            now=now,
            cfg=cfg,
        )
        if not window.get("ok"):
            continue
        candidates.append(
            {
                "symbol": sym,
                "direction": window["direction"],  # LONG/SHORT
                "side": _side_from_long_short(window["direction"]),
                "slope_scores": window.get("slope_scores"),
                "adx_values": window.get("adx_values"),
                "bar_timestamps": window.get("bar_timestamps"),
                "mean_slope": sum(window.get("slope_scores") or [0])
                / max(1, len(window.get("slope_scores") or [1])),
            }
        )

    # Strongest mean slope first
    candidates.sort(key=lambda c: (-float(c.get("mean_slope") or 0), c["symbol"]))
    remaining = cap - slots_used
    promoted: List[Dict[str, Any]] = []

    for cand in candidates[:remaining]:
        sym = cand["symbol"]
        side = cand["side"]
        _upsert_snapshot_row(
            db,
            session_date,
            sym,
            side,
            VWAP_ADX_RANK,
            None,
            now,
            refresh_locked_at=True,
        )
        detail = {
            "locked_by": LOCKED_BY,
            "vwap_slope_scores": cand.get("slope_scores"),
            "adx_14": cand.get("adx_values"),
            "direction": cand.get("direction"),
            "bar_timestamps": cand.get("bar_timestamps"),
            "persist_bars": n_bars,
            "threshold_slope": THRESHOLD_VWAP_SLOPE,
            "adx_min": 20.0,
        }
        _log_membership(
            db,
            session_date,
            symbol=sym,
            direction=side,
            event_type="entry",
            rule=RULE_VWAP_ADX,
            rank=VWAP_ADX_RANK,
            detail=detail,
            event_at=now,
        )
        promoted.append(
            {
                "symbol": sym,
                "direction": cand["direction"],
                "side": side,
                "slope_scores": cand.get("slope_scores"),
                "adx_14": cand.get("adx_values"),
            }
        )
        locked.add(sym)
        logger.info(
            "vwap_adx_promotion: entry %s %s slopes=%s adx=%s",
            sym,
            cand["direction"],
            cand.get("slope_scores"),
            cand.get("adx_values"),
        )

    return {
        "promoted": promoted,
        "skipped": False,
        "reason": "ok",
        "enabled": True,
        "cap": cap,
        "slots_used": slots_used + len(promoted),
        "candidates_seen": len(candidates),
    }
