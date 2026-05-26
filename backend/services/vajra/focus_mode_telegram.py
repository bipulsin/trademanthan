"""Server-side Telegram: one consolidated Focus Mode workflow alert per batch."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.vajra.stable_execution_tables import ensure_vajra_stable_execution_table

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

ALERT_WORKFLOW_STATES = frozenset({"PREPARE", "EXECUTABLE", "EXIT_RISK", "ACTIVE"})
MIN_INTERVAL_SEC = 300  # at most one consolidated message per 5 min per user/session


def _stock(row: Dict[str, Any]) -> str:
    return str(row.get("stock") or row.get("security") or "").strip().upper()


def _focus_universe(
    rows: List[Dict[str, Any]],
    sticky_top3: List[Dict[str, Any]],
    frozen: List[str],
) -> Set[str]:
    syms: Set[str] = set()
    for s in frozen or []:
        if s:
            syms.add(str(s).strip().upper())
    for r in sticky_top3 or []:
        sym = _stock(r)
        if sym:
            syms.add(sym)
    if not syms:
        for r in rows or []:
            if r.get("sticky_leader"):
                sym = _stock(r)
                if sym:
                    syms.add(sym)
    return syms


def _load_dedup(user_id: int, session_date: date) -> Dict[str, Any]:
    db = SessionLocal()
    try:
        ensure_vajra_stable_execution_table(db)
        r = db.execute(
            text(
                """
                SELECT focus_alert_dedup FROM vajra_stable_execution_state
                WHERE user_id = :uid AND session_date = :sd
                """
            ),
            {"uid": user_id, "sd": session_date},
        ).fetchone()
        if not r or not r[0]:
            return {"sent_keys": [], "last_sent_at": None}
        raw = r[0]
        if isinstance(raw, str):
            return json.loads(raw) if raw else {"sent_keys": [], "last_sent_at": None}
        return dict(raw) if isinstance(raw, dict) else {"sent_keys": [], "last_sent_at": None}
    except Exception:
        return {"sent_keys": [], "last_sent_at": None}
    finally:
        db.close()


def _save_dedup(user_id: int, session_date: date, dedup: Dict[str, Any]) -> None:
    db = SessionLocal()
    try:
        ensure_vajra_stable_execution_table(db)
        db.execute(
            text(
                """
                UPDATE vajra_stable_execution_state
                SET focus_alert_dedup = CAST(:d AS jsonb), updated_at = NOW()
                WHERE user_id = :uid AND session_date = :sd
                """
            ),
            {"uid": user_id, "sd": session_date, "d": json.dumps(dedup)},
        )
        db.commit()
    except Exception as e:
        db.rollback()
        logger.debug("focus_alert_dedup save: %s", e)
    finally:
        db.close()


def _username(user_id: int) -> str:
    try:
        from backend.models.user import User

        db = SessionLocal()
        try:
            u = db.query(User).filter(User.id == user_id).first()
            if u:
                return str(u.username or u.full_name or u.email or f"user#{user_id}")
        finally:
            db.close()
    except Exception:
        pass
    return f"user#{user_id}"


def _format_plan_line(row: Dict[str, Any]) -> str:
    sym = _stock(row)
    wf = row.get("execution_workflow_state") or "—"
    prior = row.get("prior_execution_workflow_state") or "—"
    setup = row.get("setup_type") or "—"
    grade = row.get("quality_grade") or "—"
    plan = row.get("trade_plan") or {}
    entry = plan.get("entry_condition") or row.get("execution_events", [{}])[0].get("message", "")
    if len(entry) > 120:
        entry = entry[:117] + "..."
    sg = row.get("sector_in_top_gainers_rank")
    sl = row.get("sector_in_top_losers_rank")
    badge = f"S{sg}" if sg else (f"W{sl}" if sl else "")
    badge_s = f" [{badge}]" if badge else ""
    return (
        f"• {sym}{badge_s}: {prior} → {wf} | {setup} | {grade}\n"
        f"  {entry}"
    )


def build_focus_transition_batch(
    rows: List[Dict[str, Any]],
    focus_syms: Set[str],
    dedup_sent: Set[str],
) -> List[Dict[str, Any]]:
    """Rows with workflow transition in focus universe, not yet alerted for this state."""
    eligible: List[Dict[str, Any]] = []
    seen_sym: Set[str] = set()
    for r in rows or []:
        sym = _stock(r)
        if sym not in focus_syms or sym in seen_sym:
            continue
        wf = str(r.get("execution_workflow_state") or "").upper()
        prior = str(r.get("prior_execution_workflow_state") or "").upper()
        if wf not in ALERT_WORKFLOW_STATES:
            continue
        key = f"{sym}:{wf}"
        if key in dedup_sent:
            continue
        transitioned = prior != wf
        first_actionable = wf in ("PREPARE", "EXECUTABLE") and (not prior or prior == "WAIT")
        if transitioned or first_actionable:
            eligible.append(r)
            seen_sym.add(sym)
    return eligible


def format_consolidated_telegram(
    eligible: List[Dict[str, Any]],
    *,
    market_bias: Optional[str] = None,
    username: Optional[str] = None,
) -> str:
    now = datetime.now(IST).strftime("%H:%M IST")
    lines = [
        f"Vajra Focus — workflow update ({now})",
    ]
    if username:
        lines.append(f"Trader: {username}")
    if market_bias:
        lines.append(f"Market: {market_bias}")
    lines.append(f"{len(eligible)} setup(s) — discretionary review only:")
    lines.append("")
    for r in eligible:
        lines.append(_format_plan_line(r))
    lines.append("")
    lines.append("Not auto-trades. Confirm on chart before orders.")
    return "\n".join(lines)


def maybe_send_focus_mode_telegram(
    user_id: int,
    rows: List[Dict[str, Any]],
    *,
    session_date: date,
    focus_mode_enabled: bool,
    sticky_top3: List[Dict[str, Any]],
    frozen_focus_stocks: List[str],
    market_bias: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Send ONE Telegram message listing all new focus-universe workflow transitions.
    Returns {sent, message, count, eligible_symbols}.
    """
    out = {"sent": False, "message": None, "count": 0, "eligible_symbols": []}
    if not focus_mode_enabled:
        return out

    import os

    if os.getenv("VAJRA_FOCUS_TELEGRAM", "1").strip().lower() in ("0", "false", "no"):
        return out

    focus_syms = _focus_universe(rows, sticky_top3, frozen_focus_stocks)
    if not focus_syms:
        return out

    dedup = _load_dedup(user_id, session_date)
    sent_keys = set(dedup.get("sent_keys") or [])
    last_at = dedup.get("last_sent_at")

    if last_at:
        try:
            last_dt = datetime.fromisoformat(str(last_at).replace("Z", "+00:00"))
            if last_dt.tzinfo is None:
                last_dt = IST.localize(last_dt)
            elapsed = (datetime.now(IST) - last_dt.astimezone(IST)).total_seconds()
            if elapsed < MIN_INTERVAL_SEC:
                return out
        except Exception:
            pass

    eligible = build_focus_transition_batch(rows, focus_syms, sent_keys)
    if not eligible:
        return out

    msg = format_consolidated_telegram(
        eligible,
        market_bias=market_bias,
        username=_username(user_id),
    )
    try:
        from backend.services.telegram_trade_channel import send_trade_with_cto_channel_message

        ok = send_trade_with_cto_channel_message(msg)
    except Exception as e:
        logger.warning("focus telegram send failed: %s", e)
        ok = False

    if ok:
        for r in eligible:
            sym = _stock(r)
            wf = str(r.get("execution_workflow_state") or "").upper()
            sent_keys.add(f"{sym}:{wf}")
        dedup["sent_keys"] = list(sent_keys)
        dedup["last_sent_at"] = datetime.now(IST).isoformat()
        _save_dedup(user_id, session_date, dedup)
        out["sent"] = True
        out["message"] = msg
        out["count"] = len(eligible)
        out["eligible_symbols"] = [_stock(r) for r in eligible]

    return out
