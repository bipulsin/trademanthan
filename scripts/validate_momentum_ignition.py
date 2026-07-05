#!/usr/bin/env python3
"""Validate 5m-based momentum ignition signals against REST historical candles.

Usage (repo root):
  PYTHONPATH=. python3 scripts/validate_momentum_ignition.py --days 10 --symbols 20

Order-flow (WS depth) cannot be backtested — use the admin diagnostics page live log during market hours.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.database import SessionLocal  # noqa: E402
from backend.services.kavach_momentum_ignition_validate import (  # noqa: E402
    run_momentum_ignition_backtest,
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=10)
    ap.add_argument("--symbols", type=int, default=20)
    ap.add_argument("--side", default="BULL")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        result = run_momentum_ignition_backtest(
            db, days=args.days, symbols=args.symbols, side=args.side
        )
    finally:
        db.close()

    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
