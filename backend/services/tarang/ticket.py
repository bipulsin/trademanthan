"""Trade Ticket actions — PAPER only in Phase 2 (no live order placement)."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.config import get_profiles
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.screener import get_candidate

logger = logging.getLogger(__name__)


def _settings_mode(db) -> str:
    row = db.execute(text("SELECT value FROM tarang_settings WHERE key = 'mode'")).mappings().first()
    if not row:
        return "PAPER"
    v = row["value"]
    if isinstance(v, str):
        return v.strip('"') or "PAPER"
    return str(v or "PAPER")


def build_ticket(candidate_id: int) -> Dict[str, Any]:
    cand = get_candidate(candidate_id)
    if not cand:
        return {"ok": False, "error": "candidate_not_found"}
    p = cand.get("payload") or {}
    legs = p.get("legs") or []
    return {
        "ok": True,
        "product": "Kosmic Tarang",
        "mode": "PAPER",
        "auto": False,
        "candidate_id": candidate_id,
        "profile_id": cand["profile_id"],
        "structure": cand.get("structure") or p.get("structure"),
        "status": p.get("status") or cand.get("status"),
        "legs": legs,
        "net_credit": p.get("net_credit"),
        "width": p.get("width"),
        "max_loss_per_unit_inr": p.get("max_loss_per_unit_inr"),
        "lots_or_contracts": p.get("lots_or_contracts"),
        "candidate_risk_inr": p.get("candidate_risk_inr"),
        "max_profit_approx_inr": p.get("max_profit_approx_inr"),
        "exit_levels": p.get("exit_levels"),
        "gates": p.get("gates"),
        "qualified_reasons": p.get("qualified_reasons"),
        "failed_gates": p.get("failed_gates"),
        "expiry": p.get("expiry"),
        "underlying": p.get("underlying"),
        "venue": p.get("venue"),
        "iv_percentile": p.get("iv_percentile"),
        "note": "Phase 2: Take trade records PAPER ticket only — no live broker orders.",
    }


def take_trade_paper(candidate_id: int, note: str = "") -> Dict[str, Any]:
    """Record a PAPER trade from a QUALIFIED candidate. Never places live orders."""
    ensure_tarang_tables()
    cand = get_candidate(candidate_id)
    if not cand:
        return {"ok": False, "error": "candidate_not_found"}
    p = cand.get("payload") or {}
    if (p.get("status") or "").upper() != "QUALIFIED":
        return {"ok": False, "error": "candidate_not_qualified", "status": p.get("status")}
    units = int(p.get("lots_or_contracts") or 0)
    if units < 1:
        return {"ok": False, "error": "sizing_zero"}

    profiles = get_profiles().get("profiles") or {}
    prof = profiles.get(cand["profile_id"]) or {}
    bucket = prof.get("risk_bucket") or "ENERGY"

    db = SessionLocal()
    try:
        mode = _settings_mode(db)
        if str(mode).upper() == "LIVE":
            # Phase 2 safety: still only PAPER ticket rows; refuse live placement path
            return {
                "ok": False,
                "error": "live_placement_disabled_phase2",
                "detail": "Phase 2 only supports PAPER ticket recording. LIVE order placement is Phase 4.",
            }
        row = db.execute(
            text(
                """
                INSERT INTO tarang_trades (
                    profile_id, risk_bucket, mode, holding_mode, status,
                    auto_managed, entry_credit, max_loss, lots_or_contracts, legs, meta
                ) VALUES (
                    :profile_id, :risk_bucket, 'PAPER', 'INTRADAY', 'ticket_taken',
                    FALSE, :entry_credit, :max_loss, :lots, CAST(:legs AS jsonb), CAST(:meta AS jsonb)
                )
                RETURNING id
                """
            ),
            {
                "profile_id": cand["profile_id"],
                "risk_bucket": bucket,
                "entry_credit": p.get("net_credit"),
                "max_loss": p.get("max_loss_per_unit_inr"),
                "lots": units,
                "legs": json.dumps(p.get("legs") or []),
                "meta": json.dumps(
                    {
                        "candidate_id": candidate_id,
                        "source": "take_trade_paper",
                        "note": note,
                        "exit_levels": p.get("exit_levels"),
                        "taken_at": datetime.now(timezone.utc).isoformat(),
                        "phase": 2,
                    }
                ),
            },
        ).mappings().first()
        db.execute(
            text("UPDATE tarang_candidates SET status = 'taken' WHERE id = :id"),
            {"id": candidate_id},
        )
        db.commit()
        return {
            "ok": True,
            "trade_id": int(row["id"]),
            "mode": "PAPER",
            "message": "PAPER trade recorded. No live orders placed.",
        }
    except Exception as e:
        db.rollback()
        logger.exception("take_trade_paper failed")
        return {"ok": False, "error": str(e)[:300]}
    finally:
        db.close()


def dismiss_candidate(candidate_id: int) -> Dict[str, Any]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        r = db.execute(
            text("UPDATE tarang_candidates SET status = 'dismissed' WHERE id = :id RETURNING id"),
            {"id": candidate_id},
        ).mappings().first()
        db.commit()
        if not r:
            return {"ok": False, "error": "candidate_not_found"}
        return {"ok": True, "candidate_id": candidate_id, "status": "dismissed"}
    finally:
        db.close()


def mark_taken_manual(
    candidate_id: int,
    fills: Optional[Dict[str, Any]] = None,
    note: str = "",
) -> Dict[str, Any]:
    """Operator entered fills manually — still PAPER audit row, no broker API."""
    ensure_tarang_tables()
    cand = get_candidate(candidate_id)
    if not cand:
        return {"ok": False, "error": "candidate_not_found"}
    p = dict(cand.get("payload") or {})
    profiles = get_profiles().get("profiles") or {}
    prof = profiles.get(cand["profile_id"]) or {}
    bucket = prof.get("risk_bucket") or "ENERGY"
    units = int(p.get("lots_or_contracts") or 1)
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                INSERT INTO tarang_trades (
                    profile_id, risk_bucket, mode, holding_mode, status,
                    auto_managed, entry_credit, max_loss, lots_or_contracts, legs, meta
                ) VALUES (
                    :profile_id, :risk_bucket, 'PAPER', 'INTRADAY', 'ticket_taken',
                    FALSE, :entry_credit, :max_loss, :lots, CAST(:legs AS jsonb), CAST(:meta AS jsonb)
                )
                RETURNING id
                """
            ),
            {
                "profile_id": cand["profile_id"],
                "risk_bucket": bucket,
                "entry_credit": (fills or {}).get("entry_credit", p.get("net_credit")),
                "max_loss": p.get("max_loss_per_unit_inr"),
                "lots": units,
                "legs": json.dumps(p.get("legs") or []),
                "meta": json.dumps(
                    {
                        "candidate_id": candidate_id,
                        "source": "mark_taken_manual",
                        "fills": fills or {},
                        "note": note,
                        "taken_at": datetime.now(timezone.utc).isoformat(),
                        "phase": 2,
                    }
                ),
            },
        ).mappings().first()
        db.execute(
            text("UPDATE tarang_candidates SET status = 'taken_manual' WHERE id = :id"),
            {"id": candidate_id},
        )
        db.commit()
        return {"ok": True, "trade_id": int(row["id"]), "mode": "PAPER"}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)[:300]}
    finally:
        db.close()
