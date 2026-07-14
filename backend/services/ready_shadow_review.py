"""READY shadow consistency log review — query, classify, export.

Read-only diagnostics over ``kavach_ready_consistency_log`` plus editable
review columns in ``kavach_ready_consistency_review``.
"""
from __future__ import annotations

import csv
import io
import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.daily_checklist_trade_state import ensure_ready_consistency_log

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

OUTCOME_CHOICES = (
    "correct exclusion",
    "wrong exclusion",
    "unclear",
)

_REVIEW_ENSURED = False

# Stable export field order (self-describing names for Claude handoff).
EXPORT_FIELDS = [
    "log_id",
    "session_date",
    "logged_at",
    "symbol",
    "direction",
    "rendered_state",
    "rendered_ready",
    "pre_gate_state",
    "symbol_in_lock",
    "lock_rank",
    "lock_direction",
    "lock_mismatch",
    "vwap_slope_score",
    "steep_ok",
    "flip_flop",
    "whipsaw_crosses",
    "quality_pass",
    "vwap_gate_enabled",
    "shadow_would_exclude",
    "shadow_would_include",
    "vwap_gate_applied",
    "needs_classification",
    "outcome_classification",
    "note",
    "reviewed_at",
]


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def ensure_review_table() -> None:
    global _REVIEW_ENSURED
    if _REVIEW_ENSURED:
        return
    ensure_ready_consistency_log()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_ready_consistency_review (
                    log_id INTEGER PRIMARY KEY,
                    outcome_classification TEXT,
                    note TEXT,
                    reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
    _REVIEW_ENSURED = True


def _is_ready_state(st: Optional[str]) -> bool:
    s = (st or "").strip().upper()
    return s in ("READY", "READY(RECHECK)")


def _row_to_view(r: Any) -> Dict[str, Any]:
    rendered_state = r.rendered_state
    pre_gate = r.pre_gate_state
    rendered_ready = _is_ready_state(rendered_state)
    pre_ready = _is_ready_state(pre_gate)
    in_lock = bool(r.in_lock) if r.in_lock is not None else False
    lock_mismatch = bool(r.lock_mismatch) if r.lock_mismatch is not None else (
        rendered_ready and not in_lock
    )
    would_block = bool(r.vwap_would_block) if r.vwap_would_block is not None else False
    # Shadow exclude: gate would have blocked a pre-gate READY row
    shadow_would_exclude = bool(pre_ready and would_block)
    shadow_would_include = bool(pre_ready and not would_block)
    # Manual classification targets: rendered READY but shadow would exclude
    needs_classification = bool(rendered_ready and shadow_would_exclude)

    logged_at = r.logged_at
    if isinstance(logged_at, datetime):
        if logged_at.tzinfo is None:
            logged_at = IST.localize(logged_at)
        logged_at_s = logged_at.astimezone(IST).isoformat()
    else:
        logged_at_s = str(logged_at) if logged_at else None

    reviewed_at = getattr(r, "reviewed_at", None)
    if isinstance(reviewed_at, datetime):
        if reviewed_at.tzinfo is None:
            reviewed_at = IST.localize(reviewed_at)
        reviewed_at_s = reviewed_at.astimezone(IST).isoformat()
    else:
        reviewed_at_s = str(reviewed_at) if reviewed_at else None

    sd = r.session_date
    if isinstance(sd, date):
        sd_s = sd.isoformat()
    else:
        sd_s = str(sd)

    return {
        "log_id": int(r.id),
        "session_date": sd_s,
        "logged_at": logged_at_s,
        "symbol": (r.symbol or "").upper(),
        "direction": r.direction,
        "rendered_state": rendered_state,
        "rendered_ready": rendered_ready,
        "pre_gate_state": pre_gate,
        "symbol_in_lock": in_lock,
        "lock_rank": int(r.lock_rank) if r.lock_rank is not None else None,
        "lock_direction": r.lock_direction,
        "lock_mismatch": lock_mismatch,
        "vwap_slope_score": float(r.vwap_slope_score) if r.vwap_slope_score is not None else None,
        "steep_ok": bool(r.steep_ok) if r.steep_ok is not None else None,
        "flip_flop": bool(r.flip_flop) if r.flip_flop is not None else None,
        "whipsaw_crosses": int(r.whipsaw_crosses) if r.whipsaw_crosses is not None else None,
        "quality_pass": bool(r.quality_pass) if r.quality_pass is not None else None,
        "vwap_gate_enabled": bool(r.vwap_gate_enabled) if r.vwap_gate_enabled is not None else False,
        "shadow_would_exclude": shadow_would_exclude,
        "shadow_would_include": shadow_would_include,
        "vwap_gate_applied": bool(r.vwap_gate_applied) if r.vwap_gate_applied is not None else False,
        "needs_classification": needs_classification,
        "outcome_classification": getattr(r, "outcome_classification", None),
        "note": getattr(r, "note", None),
        "reviewed_at": reviewed_at_s,
    }


def _apply_filter(rows: List[Dict[str, Any]], filt: str) -> List[Dict[str, Any]]:
    f = (filt or "all").strip().lower()
    if f in ("mismatch", "mismatches"):
        return [r for r in rows if r.get("lock_mismatch")]
    if f in ("shadow_exclude", "shadow_excludes", "excludes"):
        return [r for r in rows if r.get("shadow_would_exclude")]
    return rows


def _rollup(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "total_rows": len(rows),
        "mismatch_count": sum(1 for r in rows if r.get("lock_mismatch")),
        "shadow_exclude_count": sum(1 for r in rows if r.get("shadow_would_exclude")),
        "shadow_include_count": sum(1 for r in rows if r.get("shadow_would_include")),
        "needs_classification_count": sum(1 for r in rows if r.get("needs_classification")),
        "classified_count": sum(
            1 for r in rows if r.get("needs_classification") and r.get("outcome_classification")
        ),
    }


def list_session_review(
    session_date: Optional[str] = None,
    *,
    filter: str = "all",
) -> Dict[str, Any]:
    """Full review payload for one session date."""
    ensure_review_table()
    sd = session_date or today_ist()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT
                    l.id, l.session_date, l.symbol, l.direction,
                    l.rendered_state, l.pre_gate_state,
                    l.in_lock, l.lock_rank, l.lock_direction, l.lock_mismatch,
                    l.vwap_slope_score, l.steep_ok, l.flip_flop, l.whipsaw_crosses,
                    l.quality_pass, l.vwap_gate_enabled, l.vwap_would_block,
                    l.vwap_gate_applied, l.logged_at,
                    r.outcome_classification, r.note, r.reviewed_at
                FROM kavach_ready_consistency_log l
                LEFT JOIN kavach_ready_consistency_review r ON r.log_id = l.id
                WHERE l.session_date = CAST(:d AS date)
                ORDER BY l.logged_at DESC, l.id DESC
                """
            ),
            {"d": sd},
        ).fetchall()
        all_rows = [_row_to_view(r) for r in rows]
        filtered = _apply_filter(all_rows, filter)
        return {
            "ok": True,
            "session_date": sd,
            "filter": filter or "all",
            "outcome_choices": list(OUTCOME_CHOICES),
            "rollup": _rollup(all_rows),
            "filtered_rollup": _rollup(filtered),
            "rows": filtered,
            "row_count": len(filtered),
        }
    except Exception as exc:
        logger.warning("ready shadow review list failed: %s", exc)
        return {
            "ok": False,
            "error": str(exc),
            "session_date": sd,
            "filter": filter or "all",
            "rollup": _rollup([]),
            "rows": [],
            "row_count": 0,
        }
    finally:
        db.close()


def upsert_review(
    log_id: int,
    *,
    outcome_classification: Optional[str] = None,
    note: Optional[str] = None,
    clear_note: bool = False,
) -> Dict[str, Any]:
    """Persist classification / note for one log row. Partial updates allowed."""
    ensure_review_table()
    db = SessionLocal()
    try:
        exists = db.execute(
            text("SELECT 1 FROM kavach_ready_consistency_log WHERE id = :id"),
            {"id": log_id},
        ).fetchone()
        if not exists:
            return {"ok": False, "error": "log row not found", "log_id": log_id}

        prev = db.execute(
            text(
                """
                SELECT outcome_classification, note
                FROM kavach_ready_consistency_review WHERE log_id = :id
                """
            ),
            {"id": log_id},
        ).fetchone()

        oc = prev.outcome_classification if prev else None
        nt = prev.note if prev else None
        if outcome_classification is not None:
            oc = (outcome_classification or "").strip() or None
        if note is not None:
            nt = str(note).strip() or None
        elif clear_note:
            nt = None

        db.execute(
            text(
                """
                INSERT INTO kavach_ready_consistency_review (
                    log_id, outcome_classification, note, reviewed_at, updated_at
                ) VALUES (:id, :oc, :note, NOW(), NOW())
                ON CONFLICT (log_id) DO UPDATE SET
                    outcome_classification = EXCLUDED.outcome_classification,
                    note = EXCLUDED.note,
                    reviewed_at = NOW(),
                    updated_at = NOW()
                """
            ),
            {"id": log_id, "oc": oc, "note": nt},
        )
        db.commit()
        row = db.execute(
            text(
                """
                SELECT outcome_classification, note, reviewed_at
                FROM kavach_ready_consistency_review WHERE log_id = :id
                """
            ),
            {"id": log_id},
        ).fetchone()
        return {
            "ok": True,
            "log_id": log_id,
            "outcome_classification": row.outcome_classification if row else oc,
            "note": row.note if row else nt,
            "reviewed_at": (
                row.reviewed_at.astimezone(IST).isoformat()
                if row and getattr(row, "reviewed_at", None) and getattr(row.reviewed_at, "tzinfo", None)
                else (row.reviewed_at.isoformat() if row and row.reviewed_at else None)
            ),
        }
    except Exception as exc:
        db.rollback()
        logger.warning("ready shadow review upsert failed: %s", exc)
        return {"ok": False, "error": str(exc), "log_id": log_id}
    finally:
        db.close()


def export_session(session_date: Optional[str] = None, *, fmt: str = "json") -> Dict[str, Any]:
    """Return export payload; CSV as text in ``content`` when fmt=csv."""
    data = list_session_review(session_date, filter="all")
    if not data.get("ok"):
        return data
    sd = data["session_date"]
    rows = data.get("rows") or []
    # Flatten for export — only EXPORT_FIELDS
    flat = [{k: r.get(k) for k in EXPORT_FIELDS} for r in rows]
    if (fmt or "json").lower() == "csv":
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=EXPORT_FIELDS, extrasaction="ignore")
        w.writeheader()
        for row in flat:
            # stringify bools/None cleanly
            out = {}
            for k, v in row.items():
                if v is None:
                    out[k] = ""
                elif isinstance(v, bool):
                    out[k] = "true" if v else "false"
                else:
                    out[k] = v
            w.writerow(out)
        return {
            "ok": True,
            "session_date": sd,
            "format": "csv",
            "filename": f"ready-shadow-review-{sd}.csv",
            "content": buf.getvalue(),
            "row_count": len(flat),
        }
    return {
        "ok": True,
        "session_date": sd,
        "format": "json",
        "filename": f"ready-shadow-review-{sd}.json",
        "rollup": data.get("rollup"),
        "rows": flat,
        "row_count": len(flat),
    }
