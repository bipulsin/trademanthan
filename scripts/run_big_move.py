#!/usr/bin/env python3
"""Build the Big Move Study CSV, one month at a time.

    python scripts/run_big_move.py
    python scripts/run_big_move.py --months 2025-11 --symbols RELIANCE
    python scripts/run_big_move.py --retry-missing
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.big_move.job import run_study  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Big Move Study")
    parser.add_argument("--months", nargs="*", default=None, help="YYYY-MM, default every month in the study")
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--retry-missing", action="store_true")
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--train-end", default=None)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    params = {}
    if args.date_from:
        params["date_from"] = args.date_from
    if args.date_to:
        params["date_to"] = args.date_to
    if args.train_end:
        params["train_end"] = args.train_end
    result = run_study(
        params=params or None,
        symbols=args.symbols,
        months=args.months,
        retry_missing=args.retry_missing,
    )
    header = result.get("header") or {}
    print(
        f"stock_days={header.get('stock_days')} skipped={header.get('skipped')} "
        f"days={header.get('trading_days')} symbols={header.get('symbols')}"
    )


if __name__ == "__main__":
    main()
