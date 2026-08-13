#!/usr/bin/env python3
"""Retry failed Sambhav 10m import chunks (operational; no architecture change)."""
from __future__ import annotations

import json
import time
from datetime import date

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.sambhav.data_status import compute_data_status
from backend.services.sambhav.importer import import_historical_10m

FAILED_SUBCHUNKS = [
    (date(2022, 2, 1), date(2022, 2, 14)),
    (date(2022, 2, 15), date(2022, 3, 3)),
    (date(2023, 2, 8), date(2023, 2, 21)),
    (date(2023, 2, 22), date(2023, 3, 10)),
    (date(2024, 2, 15), date(2024, 2, 29)),
    (date(2024, 3, 1), date(2024, 3, 16)),
    (date(2025, 2, 21), date(2025, 3, 7)),
    (date(2025, 3, 8), date(2025, 3, 23)),
]


def main() -> None:
    t0 = time.time()
    db = SessionLocal()
    results = []
    try:
        for a, b in FAILED_SUBCHUNKS:
            print(json.dumps({"retry_chunk": [str(a), str(b)]}), flush=True)
            out = import_historical_10m(db, from_date=a, to_date=b, resume=False)
            row = {
                "from": str(a),
                "to": str(b),
                "ok": out.get("ok"),
                "upserted_10m": out.get("upserted_10m"),
                "received": out.get("received"),
                "errors": out.get("errors"),
            }
            results.append(row)
            print(json.dumps(row), flush=True)

        quality = compute_data_status(db)
        summary = dict(
            db.execute(
                text(
                    """
                    SELECT COUNT(*) AS n,
                           MIN(candle_start AT TIME ZONE 'Asia/Kolkata') AS mn,
                           MAX(candle_start AT TIME ZONE 'Asia/Kolkata') AS mx,
                           COUNT(DISTINCT (candle_start AT TIME ZONE 'Asia/Kolkata')::date) AS days
                    FROM sambhav_10m_candles
                    WHERE instrument_key = :ik
                    """
                ),
                {"ik": "NSE_INDEX|Nifty 50"},
            )
            .mappings()
            .first()
        )
        print(
            json.dumps(
                {
                    "phase": "retry_done",
                    "retry_duration_seconds": round(time.time() - t0, 1),
                    "retry_results": results,
                    "db_summary": {
                        k: (v.isoformat() if hasattr(v, "isoformat") else v)
                        for k, v in summary.items()
                    },
                    "quality": quality,
                },
                indent=2,
                default=str,
            ),
            flush=True,
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
