#!/usr/bin/env python3
"""Resume the MTF Swing backtest (Nov 2025–Oct 2026, 60m and 15m).

From the repo root:

    python3 scripts/run_mtf_swing_backtest.py
    python3 scripts/run_mtf_swing_backtest.py --timeframe 60m
    python3 scripts/run_mtf_swing_backtest.py --symbol RELIANCE --timeframe 15m

Finished month+timeframe pairs are recorded in data/mtf_swing/progress.json
and skipped on the next run. Trades append to data/mtf_swing/trades.csv.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.mtf_swing.runner import run_backtest  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Run or resume the MTF Swing backtest")
    parser.add_argument("--timeframe", choices=("60m", "15m", "both"), default="both")
    parser.add_argument("--symbol", action="append", default=[], help="Repeat to limit the universe")
    parser.add_argument("--no-resume", action="store_true", help="Recompute months even if marked done")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    tfs = ("60m", "15m") if args.timeframe == "both" else (args.timeframe,)
    summary = run_backtest(timeframes=tfs, symbols=args.symbol or None, resume=not args.no_resume)
    print(summary)


if __name__ == "__main__":
    main()
