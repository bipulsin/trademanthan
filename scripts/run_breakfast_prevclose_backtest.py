#!/usr/bin/env python3
"""Run experimental Breakfast prev-close ranking backtest (May–Aug 2026).

Writes breakfast_strategy_prevclose.json only. Does not touch
breakfast_strategy_trades or breakfast_live_signals.
"""
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

from backend.services.breakfast_strategy.backtest_prevclose import run_prevclose_backtest
from backend.services.breakfast_strategy.config import PREVCLOSE_DATE_FROM, PREVCLOSE_DATE_TO

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("run_breakfast_prevclose_backtest")


def main() -> int:
    ap = argparse.ArgumentParser(description="Breakfast prev-close formula backtest")
    ap.add_argument("--from", dest="date_from", default=PREVCLOSE_DATE_FROM.isoformat())
    ap.add_argument("--to", dest="date_to", default=PREVCLOSE_DATE_TO.isoformat())
    ap.add_argument("--force-fetch", action="store_true")
    ap.add_argument("--skip-warm", action="store_true", help="Use existing candle cache only")
    args = ap.parse_args()

    out = run_prevclose_backtest(
        date_from=date.fromisoformat(args.date_from),
        date_to=date.fromisoformat(args.date_to),
        force_fetch=args.force_fetch,
        skip_warm=args.skip_warm,
    )
    logger.info("artifact: %s", out.get("artifact_path"))
    logger.info("summary: %s", json.dumps(out.get("summary") or {}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
