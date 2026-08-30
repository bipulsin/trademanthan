#!/usr/bin/env python3
"""Read-only: compare breakfast history summary counts vs DB rows."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.breakfast_strategy.history import _history_path, load_history

PERIODS = [
    {
        "display": "Jul–Aug 2026 (primary)",
        "period_label": "2026-07-08",
        "mode": "backtest",
        "date_from": "2026-07-29",
        "date_to": "2026-08-28",
    },
    {
        "display": "Jun 2026 (spot-proxy)",
        "period_label": "2026-06",
        "mode": "backtest_oos_spot",
        "date_from": "2026-06-01",
        "date_to": "2026-06-30",
    },
    {
        "display": "Apr 2026 (spot-proxy)",
        "period_label": "2026-04",
        "mode": "backtest_oos_spot",
        "date_from": "2026-04-01",
        "date_to": "2026-04-30",
    },
]


def main() -> int:
    history = load_history()
    if not history:
        p = _history_path()
        print(f"ERROR: history artifact missing at {p}", file=sys.stderr)
        return 1

    db = SessionLocal()
    try:
        cols = db.execute(
            text(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'breakfast_strategy_trades'
                """
            )
        ).scalars().all()
        has_period = "period_label" in cols

        print(f"history artifact: {_history_path()}")
        print(f"period_label column: {has_period}")
        print()
        print("period | expected (summary) | DB (mode+dates) | DB (period_label) | artifact trades | match")

        for p in PERIODS:
            m = next(
                (x for x in history.get("months", []) if x.get("period_label") == p["period_label"]),
                None,
            )
            expected = int((m.get("summary") or {}).get("total_trades") or 0) if m else -1
            artifact_n = len((m or {}).get("trades") or [])

            by_range = int(
                db.execute(
                    text(
                        """
                        SELECT COUNT(*) FROM breakfast_strategy_trades
                        WHERE mode = :mode
                          AND session_date >= CAST(:df AS date)
                          AND session_date <= CAST(:dt AS date)
                        """
                    ),
                    {"mode": p["mode"], "df": p["date_from"], "dt": p["date_to"]},
                ).scalar()
                or 0
            )

            by_period = ""
            if has_period:
                by_period = str(
                    int(
                        db.execute(
                            text(
                                "SELECT COUNT(*) FROM breakfast_strategy_trades WHERE period_label = :pl"
                            ),
                            {"pl": p["period_label"]},
                        ).scalar()
                        or 0
                    )
                )
            else:
                by_period = "n/a"

            match = "yes" if expected >= 0 and by_range == expected else "no"
            print(
                f"{p['period_label']} | {expected} | {by_range} | {by_period} | {artifact_n} | {match}"
            )

        print()
        if has_period:
            null_rows = db.execute(
                text(
                    """
                    SELECT mode, COUNT(*) FROM breakfast_strategy_trades
                    WHERE period_label IS NULL
                    GROUP BY mode ORDER BY mode
                    """
                )
            ).all()
            print("NULL period_label by mode:", list(null_rows))

            spot_buckets = db.execute(
                text(
                    """
                    SELECT
                      CASE
                        WHEN session_date BETWEEN '2026-04-01' AND '2026-04-30' THEN '2026-04'
                        WHEN session_date BETWEEN '2026-06-01' AND '2026-06-30' THEN '2026-06'
                        ELSE 'other'
                      END AS bucket,
                      COALESCE(period_label, '(null)') AS pl,
                      COUNT(*)
                    FROM breakfast_strategy_trades
                    WHERE mode = 'backtest_oos_spot'
                    GROUP BY 1, 2
                    ORDER BY 1, 2
                    """
                )
            ).all()
            print("spot_proxy by calendar bucket + period_label:", list(spot_buckets))

        cons = db.execute(
            text(
                """
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conname = 'breakfast_strategy_trades_mode_check'
                """
            )
        ).scalar()
        print("mode_check:", cons)

        # Partial-write probe: unique (session_date, symbol, direction) vs summary for each period
        for p in PERIODS:
            m = next(
                (x for x in history.get("months", []) if x.get("period_label") == p["period_label"]),
                None,
            )
            trades = (m or {}).get("trades") or []
            if not trades:
                continue
            keys = {
                (
                    str(t.get("session_date") or "")[:10],
                    str(t.get("symbol") or "").upper(),
                    str(t.get("direction") or "").lower(),
                )
                for t in trades
            }
            db_keys = db.execute(
                text(
                    """
                    SELECT session_date::text, UPPER(symbol), direction
                    FROM breakfast_strategy_trades
                    WHERE mode = :mode
                      AND session_date >= CAST(:df AS date)
                      AND session_date <= CAST(:dt AS date)
                    """
                ),
                {"mode": p["mode"], "df": p["date_from"], "dt": p["date_to"]},
            ).all()
            db_set = {(r[0][:10], r[1], r[2]) for r in db_keys}
            missing_in_db = sorted(keys - db_set)
            extra_in_db = sorted(db_set - keys)
            if missing_in_db or extra_in_db:
                print()
                print(f"KEY DIFF {p['period_label']}: missing_in_db={len(missing_in_db)} extra_in_db={len(extra_in_db)}")
                if missing_in_db[:5]:
                    print("  sample missing:", missing_in_db[:5])
                if extra_in_db[:5]:
                    print("  sample extra:", extra_in_db[:5])

    finally:
        db.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
