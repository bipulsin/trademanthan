"""Persist last success / failure for pipeline health."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.schema import ensure_tarang_tables


def record_job_run(
    job: str,
    *,
    ok: bool,
    detail: Optional[Dict[str, Any]] = None,
    next_run: Optional[str] = None,
) -> None:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                INSERT INTO tarang_job_runs (job, last_started_at, last_finished_at, last_ok, last_error, next_run_hint, detail)
                VALUES (:job, NOW(), NOW(), :ok, :err, :next, CAST(:d AS jsonb))
                ON CONFLICT (job) DO UPDATE SET
                    last_started_at = NOW(),
                    last_finished_at = NOW(),
                    last_ok = EXCLUDED.last_ok,
                    last_error = EXCLUDED.last_error,
                    next_run_hint = EXCLUDED.next_run_hint,
                    detail = EXCLUDED.detail,
                    fail_count = CASE WHEN EXCLUDED.last_ok THEN 0 ELSE tarang_job_runs.fail_count + 1 END
                """
            ),
            {
                "job": job,
                "ok": ok,
                "err": None if ok else str((detail or {}).get("error") or "failed")[:400],
                "next": next_run,
                "d": json.dumps(detail or {}, default=str),
            },
        )
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def list_job_runs() -> List[Dict[str, Any]]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT job, last_started_at, last_finished_at, last_ok, last_error, fail_count, next_run_hint, detail
                FROM tarang_job_runs
                ORDER BY job
                """
            )
        ).mappings().all()
        out = []
        for r in rows:
            d = dict(r)
            for k in ("last_started_at", "last_finished_at"):
                if d.get(k) is not None:
                    d[k] = d[k].isoformat()
            out.append(d)
        return out
    finally:
        db.close()
