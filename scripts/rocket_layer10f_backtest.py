#!/usr/bin/env python3
"""Replay Pine Layer 10f on historical 10m NSE futures (research tables only)."""
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

from backend.services.rocket_layer10f_backtest import (  # noqa: E402
    DEFAULT_FROM,
    DEFAULT_TO,
    run_backtest,
)


def _d(s: str) -> date:
    return date.fromisoformat(s)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Layer 10f Rocket/Crash historical backtest")
    p.add_argument("--from", dest="date_from", type=_d, default=DEFAULT_FROM)
    p.add_argument("--to", dest="date_to", type=_d, default=DEFAULT_TO)
    p.add_argument("--limit", type=int, default=None, help="Max symbols (debug)")
    p.add_argument("--symbols", type=str, default=None, help="Comma-separated stocks")
    args = p.parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    result = run_backtest(
        date_from=args.date_from,
        date_to=args.date_to,
        symbol_limit=args.limit,
        symbols=symbols,
    )
    print(json.dumps(result, indent=2))
    return 0 if not result.get("errors") else 1


if __name__ == "__main__":
    raise SystemExit(main())
