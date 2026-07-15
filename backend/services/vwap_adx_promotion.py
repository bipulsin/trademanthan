"""VWAP-slope + ADX independent lock-promotion path (live, additive).

Promotes symbols into ``daily_snapshot`` when slope ≥50 sustains for N consecutive
5m bars with Kavach ADX(14,14) >20 — parallel to RS-rank morning/intraday lock.
Does not change RS-rank logic, Confidence grading, or entry gates.

Untraded slot expiry (same feature flag): if a ``vwap_adx_promotion`` member has
no open trade and slope stays below ``VWAP_ADX_EXPIRY_SLOPE_THRESHOLD`` (default 35,
deliberately softer than entry 50) for N consecutive 5m bars, remove it with
``rule=vwap_adx_slope_expiry`` so the slot frees the same poll cycle.

Expiry threshold is NOT the entry bar (50): VWAP slope naturally flattens later
in the session even when the underlying move has not stopped (2026-07-15 review);
re-using 50 would eject still-trending names on afternoon decay.
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
    _delete_snapshot_row,
    _log_membership,
    _upsert_snapshot_row,
    get_locked_symbol_rows,
    is_snapshot_locked,
)
from backend.services.kavach_momentum_ignition_validate import THRESHOLD_VWAP_SLOPE
from backend.services.kavach_volume import last_closed_bar_index
from backend.services.rs_conviction_config import get_config
from backend.services.rs_conviction_signals import normalized_vwap_slope
from backend.services.rs_vwap_quality import (
    consecutive_slope_below,
    consecutive_steep_adx_window,
    signed_vwap_slope_atr,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

RULE_VWAP_ADX = "vwap_adx_promotion"
RULE_VWAP_ADX_SLOPE_EXPIRY = "vwap_adx_slope_expiry"
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


def vwap_adx_expiry_slope_threshold() -> float:
    """Softer than entry 50 — see module docstring (afternoon slope decay)."""
    try:
        return float(os.environ.get("VWAP_ADX_EXPIRY_SLOPE_THRESHOLD", "35") or "35")
    except (TypeError, ValueError):
        return 35.0


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


def _has_open_trade(db, session_date: str, symbol: str) -> bool:
    row = db.execute(
        text(
            """
            SELECT 1 AS ok
            FROM kavach_checklist_trades
            WHERE session_date = CAST(:d AS date)
              AND UPPER(symbol) = :sym
              AND status = 'OPEN'
            LIMIT 1
            """
        ),
        {"d": session_date, "sym": (symbol or "").upper()},
    ).fetchone()
    return row is not None


def expire_untraded_vwap_adx(
    db,
    session_date: str,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Remove untraded VWAP+ADX promotions when slope stays soft for N bars.

    Does not touch RS-rank locks, R1/R2, or open-trade EXIT NOW.
    """
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)

    expired: List[Dict[str, Any]] = []
    if not vwap_adx_promotion_enabled():
        return {"expired": expired, "reason": "flag_off"}
    if not is_snapshot_locked(db, session_date):
        return {"expired": expired, "reason": "not_locked"}

    n_bars = vwap_adx_persist_bars()
    # Deliberately softer than entry THRESHOLD_VWAP_SLOPE (50): afternoon slope
    # flattening is normal and must not free slots while the move is still alive.
    thr = vwap_adx_expiry_slope_threshold()
    promoted = vwap_adx_promoted_symbols(db, session_date)
    if not promoted:
        return {"expired": expired, "reason": "none", "threshold": thr, "n_bars": n_bars}

    from backend.services.daily_checklist_snapshot import _load_candles_for_symbol
    from backend.services.kavach_universe_vwap_scan import _atr_map

    atrs = _atr_map(db, list(promoted))
    cfg = get_config()
    by_sym = {str(r.symbol).upper(): r for r in get_locked_symbol_rows(db, session_date)}

    for sym in sorted(promoted):
        if _has_open_trade(db, session_date, sym):
            continue
        row = by_sym.get(sym)
        if row is None:
            continue
        side = (row.direction or "").upper()
        if side not in ("BULL", "BEAR"):
            continue
        candles = _load_candles_for_symbol(db, sym)
        if not candles or len(candles) < 20:
            continue
        atr = float(atrs.get(sym) or 1.0)
        check = consecutive_slope_below(
            candles,
            atr_daily_pct=atr if atr > 0 else 1.0,
            threshold=thr,
            n_bars=n_bars,
            now=now,
            cfg=cfg,
        )
        if not check.get("below"):
            continue
        detail = {
            "reason": "slope_fade_below_expiry_threshold",
            "expiry_slope_threshold": thr,
            "entry_slope_threshold": THRESHOLD_VWAP_SLOPE,
            "n_bars": n_bars,
            "slope_scores": check.get("slope_scores"),
            "bar_timestamps": check.get("bar_timestamps"),
            # Soft expiry vs R1's harder N×10m VWAP-opposite-side break.
            "note": "Softer than entry 50; afternoon slope decay must not eject live trends.",
        }
        _delete_snapshot_row(db, session_date, sym, side)
        _log_membership(
            db,
            session_date,
            symbol=sym,
            direction=side,
            event_type="remove",
            rule=RULE_VWAP_ADX_SLOPE_EXPIRY,
            rank=int(row.rank) if row.rank is not None else VWAP_ADX_RANK,
            detail=detail,
            event_at=now,
        )
        expired.append(
            {
                "symbol": sym,
                "direction": side,
                "slope_scores": check.get("slope_scores"),
                "threshold": thr,
            }
        )
        logger.info(
            "vwap_adx_slope_expiry: remove %s slopes=%s thr=%s (untraded)",
            sym,
            check.get("slope_scores"),
            thr,
        )

    return {
        "expired": expired,
        "reason": "ok",
        "threshold": thr,
        "n_bars": n_bars,
    }


def promote_from_vwap_adx(
    db,
    session_date: str,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Expire faded untraded slots, then scan/promote up to MAX extra slots.

    Requires: not locked, slope≥50 for N consecutive closed 5m bars, ADX>20 on
    those bars, same slope direction throughout. Cap enforced on concurrent
    ``vwap_adx_promotion`` members still in lock (after same-cycle expiry).
    """
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)

    empty: Dict[str, Any] = {
        "promoted": [],
        "expired": [],
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

    # Free faded untraded slots even past promotion cutoff / when cap was full.
    expiry = expire_untraded_vwap_adx(db, session_date, now=now)
    expired = expiry.get("expired") or []
    empty["expired"] = expired

    if (now.hour * 60 + now.minute) > PROMOTION_CUTOFF_MIN:
        already = vwap_adx_promoted_symbols(db, session_date)
        return {
            **empty,
            "skipped": True,
            "reason": "past_cutoff",
            "slots_used": len(already),
        }

    cap = vwap_adx_promotion_max()
    n_bars = vwap_adx_persist_bars()
    already = vwap_adx_promoted_symbols(db, session_date)
    slots_used = len(already)
    empty["slots_used"] = slots_used
    if slots_used >= cap:
        return {
            **empty,
            "skipped": True,
            "reason": "cap_full",
            "expired": expired,
        }

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
                "direction": window["direction"],
                "side": _side_from_long_short(window["direction"]),
                "slope_scores": window.get("slope_scores"),
                "adx_values": window.get("adx_values"),
                "bar_timestamps": window.get("bar_timestamps"),
                "mean_slope": sum(window.get("slope_scores") or [0])
                / max(1, len(window.get("slope_scores") or [1])),
            }
        )

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
        "expired": expired,
        "skipped": False,
        "reason": "ok",
        "enabled": True,
        "cap": cap,
        "slots_used": slots_used + len(promoted),
        "candidates_seen": len(candidates),
        "expiry_threshold": vwap_adx_expiry_slope_threshold(),
    }
