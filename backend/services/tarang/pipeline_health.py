"""Pipeline health aggregation + screener 24h stats."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.job_runs import list_job_runs
from backend.services.tarang.schema import ensure_tarang_tables

JOBS = [
    {"id": "delta_snapshots", "label": "Delta snapshots", "next": "every 15m (30m weekday :00/:30)"},
    {"id": "mcx_snapshots", "label": "MCX snapshots", "next": "in-session :00/:30 IST"},
    {"id": "screener", "label": "Screener / forward-test record", "next": "every 5m"},
    {"id": "exit_engine", "label": "Exit engine", "next": "every 1m (Delta 24x7; MCX in-session)"},
    {"id": "mcx_bhavcopy", "label": "MCX Bhavcopy download", "next": "00:30 IST then hourly to 12:00"},
    {"id": "delta_candles", "label": "Delta 1h underlying candles", "next": "daily 00:45 IST"},
    {"id": "expired_archiver", "label": "Expired option archiver", "next": "daily 01:15 IST"},
    {"id": "backups", "label": "tarang_* backups", "next": "daily 02:00 IST"},
]


def screener_stats_24h() -> Dict[str, Any]:
    ensure_tarang_tables()
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    db = SessionLocal()
    try:
        last_run = db.execute(
            text(
                """
                SELECT profile_id, MAX(created_at) AS last_at, COUNT(*)::int AS n
                FROM tarang_candidates
                WHERE created_at >= :s
                GROUP BY profile_id
                """
            ),
            {"s": since},
        ).mappings().all()
        evals = db.execute(
            text("SELECT COUNT(*)::int AS n FROM tarang_candidates WHERE created_at >= :s"),
            {"s": since},
        ).mappings().first()
        rej = db.execute(
            text(
                """
                SELECT gate_name, COUNT(*)::int AS n
                FROM tarang_rejections
                WHERE created_at >= :s
                GROUP BY gate_name
                ORDER BY n DESC
                LIMIT 12
                """
            ),
            {"s": since},
        ).mappings().all()
        statuses = db.execute(
            text(
                """
                SELECT status, COUNT(*)::int AS n
                FROM tarang_candidates
                WHERE created_at >= :s
                GROUP BY status
                """
            ),
            {"s": since},
        ).mappings().all()
        ft_days = db.execute(
            text(
                """
                SELECT created_at::date AS d, COUNT(*)::int AS n
                FROM tarang_trades
                WHERE record_type = 'FORWARD_TEST'
                GROUP BY created_at::date
                ORDER BY d DESC
                LIMIT 14
                """
            )
        ).mappings().all()
        ft_n = db.execute(
            text("SELECT COUNT(*)::int AS n FROM tarang_trades WHERE record_type = 'FORWARD_TEST'")
        ).mappings().first()
        skipped = db.execute(
            text(
                """
                SELECT reason, COUNT(*)::int AS n
                FROM tarang_skipped_signals
                WHERE created_at >= :s
                GROUP BY reason
                ORDER BY n DESC
                """
            ),
            {"s": since},
        ).mappings().all()
        why = []
        n_ft = int((ft_n or {}).get("n") or 0)
        if n_ft == 0:
            st = {str(r["status"]).lower(): r["n"] for r in statuses}
            if not last_run:
                why.append("no_screener_candidates_in_24h")
            if int(st.get("qualified", 0) or 0) == 0:
                why.append("no_QUALIFIED_candidates")
            if skipped:
                why.append("skipped_signals:" + ",".join(f"{r['reason']}={r['n']}" for r in skipped[:5]))
            if rej:
                why.append("top_gate:" + str(rej[0]["gate_name"]))
            from backend.services.tarang.calendar import mcx_session_open, mcx_is_holiday_or_weekend

            if mcx_is_holiday_or_weekend():
                why.append("mcx_weekend_or_holiday")
            elif not mcx_session_open():
                why.append("mcx_session_currently_closed")
        return {
            "evaluations_24h": int((evals or {}).get("n") or 0),
            "last_run_per_underlying": [
                {"profile_id": r["profile_id"], "last_at": r["last_at"].isoformat() if r["last_at"] else None, "n": r["n"]}
                for r in last_run
            ],
            "status_counts_24h": {r["status"]: r["n"] for r in statuses},
            "top_rejection_reasons": [{"gate": r["gate_name"], "n": r["n"]} for r in rej],
            "forward_test_total": n_ft,
            "forward_test_per_day": [{"date": str(r["d"]), "n": r["n"]} for r in ft_days],
            "skipped_24h": [{"reason": r["reason"], "n": r["n"]} for r in skipped],
            "why_zero_forward_tests": why,
        }
    finally:
        db.close()


def open_gaps_by_job() -> Dict[str, List[Dict[str, Any]]]:
    from backend.services.tarang.data_gaps import recent_gaps

    gaps = recent_gaps(40)
    by: Dict[str, List[Dict[str, Any]]] = {j["id"]: [] for j in JOBS}
    for g in gaps:
        reason = str(g.get("reason") or "")
        venue = str(g.get("venue") or "")
        if "bhavcopy" in reason:
            by["mcx_bhavcopy"].append(g)
        elif venue == "delta_india":
            by["delta_snapshots"].append(g)
        elif venue == "upstox_mcx":
            by["mcx_snapshots"].append(g)
        else:
            by["screener"].append(g)
    return by


def pipeline_health() -> Dict[str, Any]:
    runs = {r["job"]: r for r in list_job_runs()}
    gaps = open_gaps_by_job()
    jobs = []
    for spec in JOBS:
        r = runs.get(spec["id"]) or {}
        jobs.append(
            {
                **spec,
                "last_success": r.get("last_finished_at") if r.get("last_ok") else None,
                "last_finished_at": r.get("last_finished_at"),
                "last_ok": r.get("last_ok"),
                "last_error": r.get("last_error"),
                "fail_count": r.get("fail_count") or 0,
                "next_run": r.get("next_run_hint") or spec["next"],
                "open_data_gaps": len(gaps.get(spec["id"]) or []),
                "gap_sample": (gaps.get(spec["id"]) or [])[:3],
            }
        )
    return {"jobs": jobs, "screener_24h": screener_stats_24h()}
