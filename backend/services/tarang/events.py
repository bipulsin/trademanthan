"""Append-only trade events + in-app alerts for Kosmic Tarang."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.tarang.state_machine import ACTORS, assert_transition, normalize_status

logger = logging.getLogger(__name__)


def append_event(
    db: Session,
    *,
    trade_id: Optional[int],
    to_status: str,
    actor: str,
    event_type: str,
    from_status: Optional[str] = None,
    candidate_id: Optional[int] = None,
    payload: Optional[Dict[str, Any]] = None,
    enforce_transition: bool = True,
) -> int:
    actor_u = str(actor or "SYSTEM").upper()
    if actor_u not in ACTORS:
        actor_u = "SYSTEM"
    to_u = normalize_status(to_status)
    fr_u = normalize_status(from_status) if from_status else None
    if enforce_transition and fr_u:
        assert_transition(fr_u, to_u)
    row = db.execute(
        text(
            """
            INSERT INTO tarang_trade_events (
                trade_id, candidate_id, from_status, to_status, actor, event_type, payload
            ) VALUES (
                :trade_id, :candidate_id, :from_status, :to_status, :actor, :event_type,
                CAST(:payload AS jsonb)
            )
            RETURNING id
            """
        ),
        {
            "trade_id": trade_id,
            "candidate_id": candidate_id,
            "from_status": fr_u,
            "to_status": to_u,
            "actor": actor_u,
            "event_type": event_type,
            "payload": json.dumps(payload or {}),
        },
    ).mappings().first()
    return int(row["id"])


def create_alert(
    db: Session,
    message: str,
    *,
    level: str = "info",
    trade_id: Optional[int] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> int:
    row = db.execute(
        text(
            """
            INSERT INTO tarang_alerts (level, message, trade_id, meta)
            VALUES (:level, :message, :trade_id, CAST(:meta AS jsonb))
            RETURNING id
            """
        ),
        {
            "level": level,
            "message": message[:2000],
            "trade_id": trade_id,
            "meta": json.dumps(meta or {}),
        },
    ).mappings().first()
    return int(row["id"])


def list_alerts(db: Session, *, trade_id: Optional[int] = None, limit: int = 50) -> List[Dict[str, Any]]:
    if trade_id is not None:
        rows = db.execute(
            text(
                """
                SELECT id, created_at, level, message, trade_id, meta
                FROM tarang_alerts
                WHERE trade_id = :tid
                ORDER BY id DESC
                LIMIT :lim
                """
            ),
            {"tid": trade_id, "lim": limit},
        ).mappings().all()
    else:
        rows = db.execute(
            text(
                """
                SELECT id, created_at, level, message, trade_id, meta
                FROM tarang_alerts
                ORDER BY id DESC
                LIMIT :lim
                """
            ),
            {"lim": limit},
        ).mappings().all()
    out = []
    for r in rows:
        d = dict(r)
        if d.get("created_at"):
            d["created_at"] = d["created_at"].isoformat()
        out.append(d)
    return out


def list_events(db: Session, trade_id: int, limit: int = 100) -> List[Dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT id, created_at, trade_id, candidate_id, from_status, to_status,
                   actor, event_type, payload
            FROM tarang_trade_events
            WHERE trade_id = :tid
            ORDER BY id ASC
            LIMIT :lim
            """
        ),
        {"tid": trade_id, "lim": limit},
    ).mappings().all()
    out = []
    for r in rows:
        d = dict(r)
        if d.get("created_at"):
            d["created_at"] = d["created_at"].isoformat()
        out.append(d)
    return out
