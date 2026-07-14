"""Public READY shadow consistency review API (no auth)."""
from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

from backend.services import ready_shadow_review as svc

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ready-shadow-review"])


class ReviewBody(BaseModel):
    outcome_classification: Optional[str] = Field(
        None, description="correct exclusion | wrong exclusion | unclear"
    )
    note: Optional[str] = None


@router.get("/api/ready-shadow-review/export")
@router.get("/ready-shadow-review/export")
def export_review(
    session_date: Optional[str] = Query(None),
    format: str = Query("json", description="json | csv"),
):
    data = svc.export_session(session_date, fmt=format)
    if not data.get("ok"):
        return data
    filename = data.get("filename") or "ready-shadow-review.json"
    if (format or "").lower() == "csv":
        return Response(
            content=data.get("content") or "",
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    body = json.dumps(
        {
            "session_date": data.get("session_date"),
            "rollup": data.get("rollup"),
            "row_count": data.get("row_count"),
            "rows": data.get("rows"),
        },
        indent=2,
    )
    return Response(
        content=body,
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/api/ready-shadow-review")
@router.get("/ready-shadow-review")
def get_review(
    session_date: Optional[str] = Query(None, description="YYYY-MM-DD, default today IST"),
    filter: str = Query(
        "all",
        description="all | mismatches | shadow_excludes",
    ),
):
    return svc.list_session_review(session_date, filter=filter)


@router.put("/api/ready-shadow-review/{log_id}")
@router.put("/ready-shadow-review/{log_id}")
def put_review(log_id: int, body: ReviewBody):
    return svc.upsert_review(
        log_id,
        outcome_classification=body.outcome_classification,
        note=body.note,
    )
