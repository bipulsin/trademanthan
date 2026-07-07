#!/usr/bin/env python3
"""Shadow RS + universe archive checkpoint report (2-week and 4-week re-analysis).

Run: PYTHONPATH=. python3 scripts/analyze_rs_shadow_checkpoints.py [--checkpoint 2w|4w]

Shadow logging start date is inferred from first rs_shadow_selection_log row.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402

from backend.database import SessionLocal  # noqa: E402

SHADOW_START_FALLBACK = "2026-07-08"
CHECKPOINT_2W_TRADING_DAYS = 10
CHECKPOINT_4W_TRADING_DAYS = 20
CONTRACT_EXPIRY_HINT = "2026-07-26"


def _first_shadow_date(db) -> str:
    row = db.execute(
        text("SELECT MIN(session_date) AS d FROM rs_shadow_selection_log")
    ).fetchone()
    if row and row.d:
        return str(row.d)
    return SHADOW_START_FALLBACK


def _archive_summary(db) -> dict:
    run = db.execute(
        text(
            """
            SELECT COUNT(*) AS days,
                   MIN(session_date) AS first_d, MAX(session_date) AS last_d,
                   SUM(CASE WHEN rollover_detected THEN 1 ELSE 0 END) AS rollover_days,
                   AVG(universe_size)::int AS avg_universe,
                   AVG(symbols_archived)::int AS avg_archived
            FROM rs_universe_kavach_archive_run
            """
        )
    ).mappings().first()
    rows = db.execute(
        text(
            """
            SELECT session_date, rollover_detected, contract_month_hint,
                   instrument_key_sample, universe_size, symbols_archived
            FROM rs_universe_kavach_archive_run
            ORDER BY session_date DESC LIMIT 5
            """
        )
    ).mappings().all()
    return {
        "run_days": int(run["days"] or 0) if run else 0,
        "first_date": str(run["first_d"]) if run and run["first_d"] else None,
        "last_date": str(run["last_d"]) if run and run["last_d"] else None,
        "rollover_days_detected": int(run["rollover_days"] or 0) if run else 0,
        "avg_universe_size": run["avg_universe"] if run else None,
        "recent_runs": [dict(r) for r in rows],
    }


def _shadow_summary(db) -> dict:
    rows = db.execute(
        text(
            """
            SELECT session_date, checkpoint_label, selection_method, COUNT(*) AS n
            FROM rs_shadow_selection_log
            GROUP BY session_date, checkpoint_label, selection_method
            ORDER BY session_date DESC, checkpoint_label
            LIMIT 30
            """
        )
    ).mappings().all()
    tardy = db.execute(
        text(
            """
            SELECT COUNT(*) AS n,
                   COUNT(*) FILTER (WHERE NOT on_morning_lock) AS off_lock
            FROM rs_shadow_tardy_addendum
            """
        )
    ).mappings().first()
    return {
        "log_sample": [dict(r) for r in rows],
        "tardy_total": int(tardy["n"] or 0) if tardy else 0,
        "tardy_off_lock": int(tardy["off_lock"] or 0) if tardy else 0,
    }


def _checkpoint_dates(start: str) -> dict:
    d0 = date.fromisoformat(start[:10])
    return {
        "shadow_start": start,
        "checkpoint_2w_target": str(d0 + timedelta(days=14)),
        "checkpoint_4w_target": str(d0 + timedelta(days=28)),
        "contract_expiry_hint": CONTRACT_EXPIRY_HINT,
        "note": (
            "2-week checkpoint (~10 trading days) is an early read before Jul-26 "
            "contract rollover; 4-week uses fuller sample."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoint", choices=("2w", "4w", "status"), default="status")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        start = _first_shadow_date(db)
        dates = _checkpoint_dates(start)
        archive = _archive_summary(db)
        shadow = _shadow_summary(db)

        report = {
            "ok": True,
            "mode": args.checkpoint,
            "schedule": dates,
            "archive": archive,
            "shadow_logging": shadow,
        }

        trading_days = archive.get("run_days") or 0
        shadow_days = len({r["session_date"] for r in shadow.get("log_sample", [])})

        if args.checkpoint in ("2w", "4w"):
            need = CHECKPOINT_2W_TRADING_DAYS if args.checkpoint == "2w" else CHECKPOINT_4W_TRADING_DAYS
            report["trading_days_available"] = max(trading_days, shadow_days)
            report["trading_days_needed"] = need
            report["powered_enough"] = report["trading_days_available"] >= need
            if report["powered_enough"]:
                report["b1_volume_weighted"] = _volume_weighted_backtest(db)
                report["b2_relock_from_shadow"] = _relock_timing_backtest(db)
                report["a2_discard"] = _full_universe_discard_proxy(db)
                report["a4_maturity"] = _flip_rate_by_maturity(db)
            else:
                report["message"] = (
                    f"Checkpoint {args.checkpoint} not ready — "
                    f"have ~{report['trading_days_available']} days, need ~{need}."
                )

        print(json.dumps(report, indent=2, default=str))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
