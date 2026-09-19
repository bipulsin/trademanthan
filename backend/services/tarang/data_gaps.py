"""Record missed Tarang snapshot slots so backtests can exclude those periods."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)


def insert_data_gap(
    db,
    *,
    venue: str,
    reason: str,
    profile_id: Optional[str] = None,
    expected_at: Optional[datetime] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    db.execute(
        text(
            """
            INSERT INTO tarang_data_gaps (venue, profile_id, expected_at, reason, detail)
            VALUES (:venue, :profile_id, :expected_at, :reason, CAST(:detail AS jsonb))
            """
        ),
        {
            "venue": venue,
            "profile_id": profile_id,
            "expected_at": expected_at or datetime.now(timezone.utc),
            "reason": reason[:200],
            "detail": json.dumps(detail or {}),
        },
    )


def record_data_gap(
    *,
    venue: str,
    reason: str,
    profile_id: Optional[str] = None,
    expected_at: Optional[datetime] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        insert_data_gap(
            db,
            venue=venue,
            reason=reason,
            profile_id=profile_id,
            expected_at=expected_at,
            detail=detail,
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("tarang data_gap insert failed")
    finally:
        db.close()


def recent_gaps(limit: int = 40) -> List[Dict[str, Any]]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, recorded_at, venue, profile_id, expected_at, reason, detail
                FROM tarang_data_gaps
                ORDER BY COALESCE(expected_at, recorded_at) DESC
                LIMIT :lim
                """
            ),
            {"lim": limit},
        ).mappings().all()
        out = []
        for r in rows:
            d = dict(r)
            for k in ("recorded_at", "expected_at"):
                if d.get(k) is not None:
                    d[k] = d[k].isoformat()
            out.append(d)
        return out
    finally:
        db.close()
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        q = """
            SELECT id, recorded_at, venue, profile_id, expected_at, reason, detail
            FROM tarang_data_gaps
            WHERE COALESCE(expected_at, recorded_at) >= :start
              AND COALESCE(expected_at, recorded_at) <= :end
        """
        params: Dict[str, Any] = {"start": start, "end": end}
        if venue:
            q += " AND venue = :venue"
            params["venue"] = venue
        q += " ORDER BY COALESCE(expected_at, recorded_at)"
        rows = db.execute(text(q), params).mappings().all()
        out = []
        for r in rows:
            d = dict(r)
            for k in ("recorded_at", "expected_at"):
                if d.get(k) is not None:
                    d[k] = d[k].isoformat()
            out.append(d)
        return out
    finally:
        db.close()


def snapshot_in_gap(captured_at: datetime, gaps: List[Dict[str, Any]], slop_sec: int = 900) -> bool:
    ts = captured_at.timestamp() if captured_at.tzinfo else captured_at.replace(tzinfo=timezone.utc).timestamp()
    for g in gaps:
        raw = g.get("expected_at") or g.get("recorded_at")
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            continue
        if abs(dt.timestamp() - ts) <= slop_sec:
            return True
    return False
