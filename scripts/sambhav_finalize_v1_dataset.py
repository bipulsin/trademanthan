#!/usr/bin/env python3
"""Finalize Sambhav V1 dataset: classify sessions, register version, print quality.

Does NOT re-download historical candles. Does NOT train models.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

# Allow running from repo root or container /app
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.database import SessionLocal  # noqa: E402
from backend.services.sambhav.config import DATASET_VERSION_V1, INSTRUMENT_KEY  # noqa: E402
from backend.services.sambhav.data_status import compute_data_status  # noqa: E402
from backend.services.sambhav.dataset import register_dataset_version  # noqa: E402
from backend.services.sambhav.sessions import classify_and_persist_sessions  # noqa: E402
from backend.services.sambhav.tables import ensure_sambhav_tables  # noqa: E402


def main() -> int:
    ensure_sambhav_tables()
    db = SessionLocal()
    try:
        classification = classify_and_persist_sessions(db, instrument_key=INSTRUMENT_KEY)
        quality = compute_data_status(db, refresh_sessions=False)
        meta = {
            "classification": classification,
            "quality_status": quality.get("status"),
            "note": quality.get("note"),
        }
        version = register_dataset_version(
            db,
            dataset_version=DATASET_VERSION_V1,
            start_date=date.fromisoformat(quality["start_date"]),
            end_date=date.fromisoformat(quality["end_date"]),
            regular_session_count=int(quality.get("regular_session_count") or 0),
            regular_candle_count=int(quality.get("regular_candle_count") or 0),
            total_candle_count=int(quality.get("total_candle_count") or 0),
            excluded_session_count=int(quality.get("excluded_session_count") or 0),
            excluded_holiday_count=int(quality.get("excluded_holiday_count") or 0),
            meta=meta,
            activate=True,
        )
        out = {
            "ok": quality.get("status") == "PASS",
            "classification": classification,
            "quality": {
                k: quality.get(k)
                for k in (
                    "status",
                    "data_integrity",
                    "dataset_version",
                    "period",
                    "regular_session_count",
                    "regular_candle_count",
                    "expected_regular_candles",
                    "regular_missing_candles",
                    "duplicates",
                    "invalid_ohlc",
                    "timestamp_anomalies",
                    "excluded_session_count",
                    "excluded_holiday_count",
                    "excluded_sessions",
                    "session_type_counts",
                    "note",
                    "model_status",
                )
            },
            "dataset_version": version,
        }
        print(json.dumps(out, indent=2, default=str))
        return 0 if out["ok"] else 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
