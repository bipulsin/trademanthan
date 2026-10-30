#!/usr/bin/env python3
"""CLI / one-shot runner for MomentX Stage-1 backtest.

Examples:
  python3 scripts/run_momentx_backtest.py
  python3 scripts/run_momentx_backtest.py --from 2026-08-01 --to 2026-09-20
  python3 scripts/run_momentx_backtest.py --from 2026-07-01 --to 2026-07-31 --asof 14:45
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


def main() -> int:
    p = argparse.ArgumentParser(description="MomentX Stage-1 backtest")
    p.add_argument("--from", dest="date_from", default="2026-08-01")
    p.add_argument("--to", dest="date_to", default="2026-09-20")
    p.add_argument("--asof", dest="asof_hhmm", default="14:45")
    p.add_argument("--keep", action="store_true", help="Do not clear overlapping rows first")
    args = p.parse_args()

    d0 = date.fromisoformat(args.date_from[:10])
    d1 = date.fromisoformat(args.date_to[:10])

    from backend.services.momentx.backtest_engine import run_stage1_backtest

    result = run_stage1_backtest(
        date_from=d0,
        date_to=d1,
        asof_hhmm=args.asof_hhmm,
        replace_range=not args.keep,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
