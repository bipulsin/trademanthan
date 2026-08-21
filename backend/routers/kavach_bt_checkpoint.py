"""Kavach 22-Aug BT checkpoint dashboard API — research only."""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.kavach_bt_checkpoint.db import (
    ensure_bt_checkpoint_tables,
    latest_run_id,
    list_detail,
    list_summaries,
)
from backend.services.kavach_bt_checkpoint.export import detail_csv, summary_csv

router = APIRouter(prefix="/api/kavach-bt-checkpoint", tags=["kavach-bt-checkpoint"])


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


@router.get("/health")
def health(_: User = Depends(_auth_user)) -> Dict[str, Any]:
    ensure_bt_checkpoint_tables()
    return {"ok": True, "run_id": latest_run_id(), "research_only": True}


@router.get("/summary")
def summary(
    run_id: Optional[str] = None,
    _: User = Depends(_auth_user),
) -> Dict[str, Any]:
    ensure_bt_checkpoint_tables()
    rid = run_id or latest_run_id()
    rows = list_summaries(rid)
    recs = [r for r in rows if r.get("cohort_type") == "recommendation"]
    cohorts = [r for r in rows if r.get("cohort_type") != "recommendation"]
    return {
        "ok": True,
        "run_id": rid,
        "recommendations": recs,
        "cohorts": cohorts,
        "research_only": True,
        "notes": {
            "resistance": "warning_only",
            "garuda": "shadow_only",
            "pullback_5plus": "hard_block_display_only_live_deferred",
            "rule_15": "entry_only",
        },
    }


@router.get("/trades")
def trades(
    run_id: Optional[str] = None,
    symbol: Optional[str] = None,
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    pb_hard_blocked: Optional[bool] = None,
    res_confluence: Optional[bool] = None,
    garuda: Optional[str] = None,
    limit: int = Query(500, ge=1, le=2000),
    _: User = Depends(_auth_user),
) -> Dict[str, Any]:
    ensure_bt_checkpoint_tables()
    rid = run_id or latest_run_id()
    rows = list_detail(
        run_id=rid,
        symbol=symbol,
        date_from=date_from,
        date_to=date_to,
        pb_hard_blocked=pb_hard_blocked,
        res_confluence=res_confluence,
        garuda=garuda,
        limit=limit,
    )
    # Serialize dates
    out = []
    for r in rows:
        d = dict(r)
        for k, v in list(d.items()):
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
        out.append(d)
    return {"ok": True, "run_id": rid, "count": len(out), "trades": out}


@router.get("/export.csv")
def export_csv(
    kind: str = Query("detail", regex="^(detail|summary)$"),
    run_id: Optional[str] = None,
    symbol: Optional[str] = None,
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    _: User = Depends(_auth_user),
) -> PlainTextResponse:
    ensure_bt_checkpoint_tables()
    rid = run_id or latest_run_id()
    if kind == "summary":
        body = summary_csv(list_summaries(rid))
        fname = f"kavach_bt_summary_{rid or 'latest'}.csv"
    else:
        body = detail_csv(
            list_detail(
                run_id=rid,
                symbol=symbol,
                date_from=date_from,
                date_to=date_to,
                limit=2000,
            )
        )
        fname = f"kavach_bt_detail_{rid or 'latest'}.csv"
    return PlainTextResponse(
        body,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )
