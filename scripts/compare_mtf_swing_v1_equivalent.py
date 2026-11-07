#!/usr/bin/env python3
"""Compare v1 trades.csv vs v2 variant=v1_equivalent (like-for-like gate)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.mtf_swing.csv_store import read_rows
from backend.services.mtf_swing.data import trades_path, trades_path_v2
from backend.services.mtf_swing.metrics import compare_v1_equivalent


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--v1", type=Path, default=None)
    parser.add_argument("--v2", type=Path, default=None)
    parser.add_argument("--timeframe", default="all", choices=("60m", "15m", "all"))
    args = parser.parse_args()
    v1 = read_rows(args.v1 or trades_path())
    v2 = read_rows(args.v2 or trades_path_v2())
    if args.timeframe == "all":
        reports = {
            tf: compare_v1_equivalent(v1, v2, timeframe=tf) for tf in ("60m", "15m", "all")
        }
    else:
        reports = {args.timeframe: compare_v1_equivalent(v1, v2, timeframe=args.timeframe)}
    print(json.dumps(reports, indent=2, default=str))
    ok = all(r.get("ok") for r in reports.values())
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
