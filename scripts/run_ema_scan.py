#!/usr/bin/env python3
"""Build the EMA 30 reclaim study (20 Oct 2026 – 13 Nov 2026)."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.ema_scan.job import run_scan  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the EMA scan study")
    parser.add_argument("--symbols", default="", help="Comma-separated subset, for a smoke run")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] or None
    report = run_scan(symbols=symbols)
    header = report.get("header") or {}
    print(
        f"signals={header.get('signals')} trading_days={header.get('trading_days')} "
        f"stocks_skipped={header.get('stocks_skipped')} universe={header.get('universe')}"
    )


if __name__ == "__main__":
    main()
