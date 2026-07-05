"""Admin-only read-only diagnostics for Kavach Momentum Ignition validation."""
from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import SessionLocal, get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.kavach_momentum_ignition_validate import (
    format_backtest_plain_text,
    run_momentum_ignition_backtest,
)
from backend.services.upstox_market_feed import feed_status

logger = logging.getLogger(__name__)
router = APIRouter(tags=["kavach-ignition-diagnostics"])

IST = pytz.timezone("Asia/Kolkata")

_jobs_lock = threading.Lock()
_jobs: Dict[str, Dict[str, Any]] = {}


def _require_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _require_admin(user: User = Depends(_require_user)) -> User:
    if (getattr(user, "is_admin", None) or "").strip() != "Yes":
        raise HTTPException(status_code=403, detail="Administrator only")
    return user


class BacktestStartBody(BaseModel):
    days: int = Field(10, ge=1, le=31)
    symbols: int = Field(20, ge=1, le=50)
    side: str = Field("BULL", description="BULL or BEAR")


def _run_backtest_job(job_id: str, days: int, symbols: int, side: str) -> None:
    db = SessionLocal()
    try:
        result = run_momentum_ignition_backtest(
            db, days=days, symbols=symbols, side=side.upper()
        )
        with _jobs_lock:
            _jobs[job_id] = {
                "status": "done" if result.get("ok") else "error",
                "result": result,
                "error": result.get("error"),
                "finished_at": datetime.now(IST).isoformat(),
            }
    except Exception as exc:
        logger.exception("ignition backtest job %s failed", job_id)
        with _jobs_lock:
            _jobs[job_id] = {
                "status": "error",
                "error": str(exc),
                "finished_at": datetime.now(IST).isoformat(),
            }
    finally:
        db.close()


@router.post("/backtest/start")
def start_backtest(
    body: BacktestStartBody,
    admin: User = Depends(_require_admin),
):
    """
    Start background REST backtest (~7–15 min for 20 symbols; avoids nginx timeout).

    Read-only: no config writes, no ignition log writes, no RS job interaction.
    """
    job_id = str(uuid.uuid4())
    started_at = datetime.now(IST).isoformat()
    with _jobs_lock:
        _jobs[job_id] = {
            "status": "running",
            "started_at": started_at,
            "parameters": body.model_dump(),
        }
    t = threading.Thread(
        target=_run_backtest_job,
        args=(job_id, body.days, body.symbols, body.side),
        daemon=True,
    )
    t.start()
    return {
        "job_id": job_id,
        "status": "running",
        "started_at": started_at,
        "parameters": body.model_dump(),
        "note": (
            "Background job — poll GET /backtest/status/{job_id}. "
            "Expected runtime ~7–15 min for 20 symbols (Upstox rate limits)."
        ),
    }


@router.get("/backtest/status/{job_id}")
def backtest_status(job_id: str, admin: User = Depends(_require_admin)):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Unknown or expired job_id")
    out = {"job_id": job_id, **job}
    if job.get("status") == "done" and job.get("result"):
        out["plain_text"] = job["result"].get("plain_text") or format_backtest_plain_text(job["result"])
    return out


def _serialize_dt(v: Any) -> Optional[str]:
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def format_live_log_plain_text(payload: Dict[str, Any]) -> str:
    lines = [
        "=== Kavach Momentum Ignition — Live Log Snapshot ===",
        f"Fetched: {payload.get('fetched_at', '—')}",
        "",
        "--- WS feed status ---",
    ]
    feed = payload.get("feed") or {}
    lines.append(f"  thread_alive: {feed.get('thread_alive')}")
    lines.append(f"  cached_instruments: {feed.get('cached_instruments')}")
    lines.append(f"  enabled: {feed.get('enabled')}")
    if feed.get("last_error"):
        lines.append(f"  last_error: {feed.get('last_error')}")
    lines.append("")
    lines.append("--- upstox_ws_orderflow_latest ---")
    of = payload.get("orderflow_table") or {}
    lines.append(f"  row_count: {of.get('row_count', 0)}")
    lines.append(f"  updated_at_min: {of.get('updated_at_min') or '—'}")
    lines.append(f"  updated_at_max: {of.get('updated_at_max') or '—'}")
    if of.get("empty_message"):
        lines.append(f"  note: {of.get('empty_message')}")
    lines.append("")
    lines.append(f"--- rs_momentum_ignition_log (last {payload.get('limit', 50)} rows) ---")
    rows = payload.get("ignition_log") or []
    lines.append(f"  rows_returned: {len(rows)}")
    if payload.get("ignition_empty_message"):
        lines.append(f"  note: {payload.get('ignition_empty_message')}")
    for r in rows[:20]:
        lines.append(
            f"  {r.get('computed_at', '—')} | {r.get('symbol')} {r.get('side')} "
            f"score={r.get('ignition_score')} building={r.get('ignition_building')}"
        )
    if len(rows) > 20:
        lines.append(f"  ... and {len(rows) - 20} more rows (see JSON download)")
    return "\n".join(lines)


@router.get("/live-log")
def get_live_log(
    admin: User = Depends(_require_admin),
    db: Session = Depends(get_db),
    limit: int = Query(50, ge=1, le=500),
):
    """
    Read-only snapshot: ignition log, orderflow table stats, WS feed status.

    Does not write config or trigger ignition cycle.
    """
    fetched_at = datetime.now(IST).isoformat()
    feed = feed_status()

    try:
        of_row = db.execute(
            text(
                """
                SELECT COUNT(*) AS cnt,
                       MIN(updated_at) AS umin,
                       MAX(updated_at) AS umax
                FROM upstox_ws_orderflow_latest
                """
            )
        ).mappings().first()
    except Exception as exc:
        logger.warning("orderflow stats: %s", exc)
        of_row = {"cnt": 0, "umin": None, "umax": None}

    of_count = int(of_row.get("cnt") or 0)
    orderflow_table: Dict[str, Any] = {
        "row_count": of_count,
        "updated_at_min": _serialize_dt(of_row.get("umin")),
        "updated_at_max": _serialize_dt(of_row.get("umax")),
    }
    if of_count == 0:
        orderflow_table["empty_message"] = (
            "0 rows — WS order-flow table empty (feed inactive outside market hours or feed not started)."
        )

    ignition_rows: List[Dict[str, Any]] = []
    ignition_empty_message: Optional[str] = None
    try:
        raw = db.execute(
            text(
                """
                SELECT id, session_date, computed_at, symbol, side,
                       ignition_score, ignition_building, components_json, created_at
                FROM rs_momentum_ignition_log
                ORDER BY computed_at DESC NULLS LAST, id DESC
                LIMIT :lim
                """
            ),
            {"lim": limit},
        ).mappings().all()
        for r in raw:
            ignition_rows.append(
                {
                    "id": r["id"],
                    "session_date": _serialize_dt(r.get("session_date")),
                    "computed_at": _serialize_dt(r.get("computed_at")),
                    "symbol": r.get("symbol"),
                    "side": r.get("side"),
                    "ignition_score": float(r["ignition_score"]) if r.get("ignition_score") is not None else None,
                    "ignition_building": bool(r.get("ignition_building")),
                    "components_json": r.get("components_json"),
                    "created_at": _serialize_dt(r.get("created_at")),
                }
            )
    except Exception as exc:
        logger.warning("ignition log read: %s", exc)
        ignition_empty_message = f"Query failed: {exc}"

    if not ignition_rows and not ignition_empty_message:
        ignition_empty_message = (
            "0 rows — ignition log empty (5-min cycle runs during market hours only)."
        )

    payload = {
        "fetched_at": fetched_at,
        "limit": limit,
        "feed": feed,
        "orderflow_table": orderflow_table,
        "ignition_log": ignition_rows,
        "ignition_empty_message": ignition_empty_message,
    }
    payload["plain_text"] = format_live_log_plain_text(payload)
    return payload
