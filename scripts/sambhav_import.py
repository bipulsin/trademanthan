#!/usr/bin/env python3
"""CLI: import NIFTY 1m history into sambhav_raw_candles and rebuild 10m bars.

Usage:
  PYTHONPATH=. python scripts/sambhav_import.py --from-date 2025-01-01 --to-date 2025-03-31
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.database import SessionLocal  # noqa: E402
from backend.services.sambhav.importer import import_historical_1m  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description="Sambhav NIFTY 1m historical import")
    p.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--to-date", default=None, help="YYYY-MM-DD (default: today IST)")
    p.add_argument("--no-rebuild-10m", action="store_true")
    args = p.parse_args()
    from_d = date.fromisoformat(args.from_date)
    to_d = date.fromisoformat(args.to_date) if args.to_date else None
    db = SessionLocal()
    try:
        out = import_historical_1m(
            db,
            from_date=from_d,
            to_date=to_d,
            rebuild_10m=not args.no_rebuild_10m,
        )
        print(json.dumps(out, indent=2, default=str))
        return 0 if out.get("ok") else 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
