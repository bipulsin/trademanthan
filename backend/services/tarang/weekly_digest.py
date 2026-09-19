"""Weekly Sunday-evening IST digest to private Telegram."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.alerts_telegram import notify_critical
from backend.services.tarang.schema import ensure_tarang_tables


def build_weekly_digest() -> Dict[str, Any]:
    ensure_tarang_tables()
    since = datetime.now(timezone.utc) - timedelta(days=7)
    db = SessionLocal()
    try:
        opened = db.execute(
            text("SELECT COUNT(*)::int AS n FROM tarang_trades WHERE created_at >= :s AND record_type = 'FORWARD_TEST'"),
            {"s": since},
        ).mappings().first()
        closed = db.execute(
            text(
                """
                SELECT COUNT(*)::int AS n,
                       COALESCE(SUM(net_pnl),0) AS pnl
                FROM tarang_trades
                WHERE COALESCE(exit_at, updated_at) >= :s
                  AND status IN ('CLOSED','REPORTED')
                  AND record_type = 'FORWARD_TEST'
                  AND COALESCE(excluded, false) = false
                """
            ),
            {"s": since},
        ).mappings().first()
        gaps = db.execute(
            text("SELECT COUNT(*)::int AS n FROM tarang_data_gaps WHERE recorded_at >= :s"),
            {"s": since},
        ).mappings().first()
        jobs = db.execute(
            text("SELECT job, last_ok, last_error, fail_count FROM tarang_job_runs ORDER BY job")
        ).mappings().all()
        failed = [dict(j) for j in jobs if not j.get("last_ok")]
        return {
            "opened": int((opened or {}).get("n") or 0),
            "closed": int((closed or {}).get("n") or 0),
            "net_pnl": float((closed or {}).get("pnl") or 0),
            "data_gaps": int((gaps or {}).get("n") or 0),
            "failed_jobs": failed,
            "coverage_jobs": [dict(j) for j in jobs],
        }
    finally:
        db.close()


def send_weekly_digest() -> Dict[str, Any]:
    d = build_weekly_digest()
    failed = ", ".join(j["job"] for j in d.get("failed_jobs") or []) or "none"
    msg = (
        "Weekly digest (Forward test)\n"
        f"Opened: {d['opened']}  Closed: {d['closed']}  Net: {d['net_pnl']:.0f}\n"
        f"Data gaps (7d): {d['data_gaps']}\n"
        f"Failed jobs: {failed}"
    )
    db = SessionLocal()
    try:
        out = notify_critical(db, kind="weekly_digest", message=msg, dedupe_key="weekly_digest", throttle_sec=0)
        db.commit()
        return {"ok": True, "digest": d, "telegram": out}
    finally:
        db.close()
