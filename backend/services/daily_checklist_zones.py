"""Zone 1 session-bar signals + READY downgrades for Daily RS Checklist UI."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

ROTATION_OVERLAP_MAX = int(os.getenv("RS_ROTATION_CHIP_OVERLAP_MAX", "2"))
IMBALANCE_THRESHOLD = int(os.getenv("RS_DIRECTION_IMBALANCE_REMOVALS", "3"))
IMBALANCE_LOOKBACK_MIN = int(os.getenv("RS_DIRECTION_IMBALANCE_LOOKBACK_MIN", "60"))

STATE_READY = "READY"
STATE_RECHECK = "READY(RECHECK)"
STATE_WAIT = "WAIT FOR PULLBACK"


def _parse_ist(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        dt = val
    else:
        try:
            dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def rotation_chip(rotation_day: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Show ROTATION DAY chip when overlaps are thin."""
    rot = rotation_day or {}
    if (rot.get("rotation_day_type") or "").upper() != "ROTATION":
        return None
    bull = int(rot.get("bull_overlap") or 0)
    bear = int(rot.get("bear_overlap") or 0)
    if bull + bear > ROTATION_OVERLAP_MAX:
        return None
    return {
        "active": True,
        "label": "ROTATION DAY",
        "subtitle": (
            "Yesterday's leaders/laggards not carrying over. "
            "Prefer 1st-pullback setups only."
        ),
        "bull_overlap": bull,
        "bear_overlap": bear,
    }


def removal_counts_last_hour(
    removals: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
    lookback_min: int = IMBALANCE_LOOKBACK_MIN,
) -> Dict[str, int]:
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    cutoff = now - timedelta(minutes=lookback_min)
    counts = {"BULL": 0, "BEAR": 0}
    for r in removals or []:
        rule = (r.get("rule_tag") or r.get("rule") or "").upper()
        if rule not in ("R1", "R2"):
            continue
        at = _parse_ist(r.get("at"))
        if at is None or at < cutoff:
            continue
        side = (r.get("direction") or "").upper()
        if side in ("LONG", "BULL"):
            counts["BULL"] += 1
        elif side in ("SHORT", "BEAR"):
            counts["BEAR"] += 1
    return counts


def direction_imbalance(
    removals: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
    threshold: int = IMBALANCE_THRESHOLD,
) -> Optional[Dict[str, Any]]:
    counts = removal_counts_last_hour(removals, now=now)
    bull, bear = counts["BULL"], counts["BEAR"]
    if abs(bull - bear) < threshold:
        return None
    unstable = "BEAR" if bear > bull else "BULL"
    n = max(bull, bear)
    return {
        "active": True,
        "unstable_direction": unstable,  # BULL or BEAR
        "bull_removals": bull,
        "bear_removals": bear,
        "label": f"⚠ {unstable} unstable · {n} removals last hour",
    }


def compromised_lock_banner(locked_by: Optional[str]) -> Optional[Dict[str, Any]]:
    if (locked_by or "").strip().lower() != "manual":
        return None
    return {
        "active": True,
        "locked_by": "manual",
        "label": (
            "⚠ Morning lock was manually recovered today. "
            "Apply extra caution to all setups."
        ),
    }


def apply_zone_downgrades(
    stocks: List[Dict[str, Any]],
    *,
    imbalance: Optional[Dict[str, Any]],
    compromised: Optional[Dict[str, Any]],
) -> None:
    """In-place READY→WAIT downgrades for Zone 1 rules (display + Take Trade)."""
    unstable_side = None
    if imbalance and imbalance.get("active"):
        unstable_side = (imbalance.get("unstable_direction") or "").upper()

    for s in stocks:
        st = s.get("trade_state")
        if st not in (STATE_READY, STATE_RECHECK):
            continue
        direction = (s.get("direction") or "LONG").upper()
        side = "BEAR" if direction == "SHORT" else "BULL"

        if unstable_side and side == unstable_side:
            s["trade_state"] = STATE_WAIT
            s["trade_state_reason"] = f"{unstable_side} direction unstable"
            badges = list(s.get("gate_badges") or [])
            tag = "DIRECTION UNSTABLE"
            if tag not in badges:
                badges.append(tag)
            s["gate_badges"] = badges
            s["zone_downgrade"] = "direction_imbalance"
            continue

        # Compromised morning lock: only morning-lock (non-promoted) names
        if compromised and compromised.get("active"):
            promo_at = s.get("promoted_at")
            if not promo_at:
                s["trade_state"] = STATE_WAIT
                s["trade_state_reason"] = "Morning lock manually recovered — extra caution"
                s["zone_downgrade"] = "compromised_lock"


def build_zone1_obs(
    *,
    rotation_day: Optional[Dict[str, Any]],
    removals: List[Dict[str, Any]],
    locked_by: Optional[str],
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    rot = rotation_chip(rotation_day)
    imb = direction_imbalance(removals, now=now)
    comp = compromised_lock_banner(locked_by)
    return {
        "rotation_chip": rot,
        "direction_imbalance": imb,
        "compromised_lock": comp,
        "session_window_text": "Entry 09:45–14:30 · Square-off 15:15",
    }


def morning_locked_symbols(db, session_date: str) -> Dict[str, Dict[str, Any]]:
    """Symbols currently on daily_snapshot with rank (for provenance)."""
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(symbol) AS symbol, direction, rank
                FROM daily_snapshot
                WHERE snapshot_date = CAST(:d AS date)
                """
            ),
            {"d": session_date},
        ).fetchall()
        return {
            str(r.symbol).upper(): {
                "direction": r.direction,
                "rank": int(r.rank) if r.rank is not None else None,
            }
            for r in rows
        }
    except Exception as exc:
        logger.debug("morning_locked_symbols skipped: %s", exc)
        return {}
