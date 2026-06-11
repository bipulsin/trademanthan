#!/usr/bin/env python3
"""One-off: convert scan algo UTC-naive timestamps to IST-naive wall clock."""
from __future__ import annotations

from sqlalchemy import text

from backend.database import SessionLocal


def migrate(days_back: int = 14) -> int:
    db = SessionLocal()
    try:
        res = db.execute(
            text(
                """
                UPDATE intraday_stock_options
                SET
                  alert_time = (alert_time AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Kolkata',
                  trade_date = (trade_date AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Kolkata',
                  buy_time = CASE WHEN buy_time IS NOT NULL
                    THEN (buy_time AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Kolkata' ELSE NULL END,
                  sell_time = CASE WHEN sell_time IS NOT NULL
                    THEN (sell_time AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Kolkata' ELSE NULL END
                WHERE trade_date >= (CURRENT_DATE - CAST(:days AS integer))
                """
            ),
            {"days": days_back},
        )
        db.commit()
        return int(res.rowcount or 0)
    finally:
        db.close()


if __name__ == "__main__":
    n = migrate()
    print(f"migrated_rows={n}")
