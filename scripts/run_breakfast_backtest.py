#!/usr/bin/env python3
"""Run Breakfast Strategy backtest (29-Jul–28-Aug-2026 by default)."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.breakfast_strategy.backtest import run_backtest
from backend.services.breakfast_strategy.config import DATE_FROM, DATE_TO

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("run_breakfast_backtest")


def main() -> int:
    ap = argparse.ArgumentParser(description="Breakfast Strategy backtest")
    ap.add_argument("--from", dest="date_from", default=DATE_FROM.isoformat())
    ap.add_argument("--to", dest="date_to", default=DATE_TO.isoformat())
    ap.add_argument("--force-fetch", action="store_true")
    ap.add_argument("--pnl-cap", action="store_true", help="Enable ₹5,000 profit exit cap")
    ap.add_argument("--no-db", action="store_true", help="Skip DB persist")
    ap.add_argument("--mode", default="backtest", choices=["backtest", "forward"])
    args = ap.parse_args()

    d0 = date.fromisoformat(args.date_from)
    d1 = date.fromisoformat(args.date_to)
    out = run_backtest(
        date_from=d0,
        date_to=d1,
        mode=args.mode,
        force_fetch=args.force_fetch,
        persist_db=not args.no_db,
        pnl_cap_enabled=args.pnl_cap,
    )
    logger.info("artifact: %s", out.get("artifact_path"))
    logger.info("summary: %s", json.dumps(out.get("summary") or {}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
