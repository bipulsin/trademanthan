#!/usr/bin/env python3
"""CLI: import NIFTY 10-minute history via Upstox V3 into sambhav_10m_candles.

V1 does not download 1-minute candles.
1-minute data may be added in a future Sambhav V2 feature-enhancement study.

Pilot:
  PYTHONPATH=. python scripts/sambhav_import.py --from-date 2025-01-01 --to-date 2025-01-31

Full historical (do not run until the pilot passes):
  PYTHONPATH=. python scripts/sambhav_import.py --from-date 2022-01-01
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.database import SessionLocal  # noqa: E402
from backend.services.sambhav.importer import import_historical_10m  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description="Sambhav NIFTY 10-minute historical import (Upstox V3)")
    p.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--to-date", default=None, help="YYYY-MM-DD (default: today IST)")
    p.add_argument("--no-resume", action="store_true", help="Re-fetch all chunks (upserts remain idempotent)")
    args = p.parse_args()
    from_d = date.fromisoformat(args.from_date)
    to_d = date.fromisoformat(args.to_date) if args.to_date else None
    db = SessionLocal()
    try:
        out = import_historical_10m(
            db,
            from_date=from_d,
            to_date=to_d,
            resume=not args.no_resume,
        )
        print(json.dumps(out, indent=2, default=str))
        return 0 if out.get("ok") else 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
