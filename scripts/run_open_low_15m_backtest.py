#!/usr/bin/env python3
"""Run Open-Low 15m futures backtest (Jul 22 – Aug 21 2026)."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.open_low_15m.backtest import run_open_low_15m_backtest
from backend.services.open_low_15m.config import DATE_FROM, DATE_TO


def main() -> int:
    p = argparse.ArgumentParser(description="Open-Low 15m futures backtest")
    p.add_argument("--from", dest="date_from", default=DATE_FROM.isoformat())
    p.add_argument("--to", dest="date_to", default=DATE_TO.isoformat())
    p.add_argument("--run-id", default=None)
    p.add_argument("--no-db", action="store_true", help="Skip DB write")
    p.add_argument("--merge", action="store_true", help="Merge chunk into existing artifact JSON")
    p.add_argument("--tp", choices=["TP1", "TP2", "TP3", "TP4"], default=None)
    p.add_argument("--day-pause", type=float, default=2.0, help="Seconds between session days")
    p.add_argument("--symbol-pause", type=float, default=0.12, help="Seconds before each M15 REST fetch")
    p.add_argument(
        "--out",
        default=None,
        help="Output JSON path (default: backend/data/open_low_15m_backtest.json)",
    )
    args = p.parse_args()

    out_path = (
        Path(args.out)
        if args.out
        else Path("/home/ubuntu/trademanthan/data/open_low_15m_backtest.json")
        if Path("/home/ubuntu/trademanthan/data").is_dir()
        else ROOT / "backend" / "data" / "open_low_15m_backtest.json"
    )
    result = run_open_low_15m_backtest(
        date.fromisoformat(args.date_from),
        date.fromisoformat(args.date_to),
        run_id=args.run_id,
        out_path=out_path,
        write_db=not args.no_db,
        tp_filter=args.tp,
        day_pause_sec=args.day_pause,
        symbol_pause_sec=args.symbol_pause,
        merge_into=args.merge,
    )
    slim = {k: v for k, v in result.items() if k != "rows"}
    slim["row_count"] = len(result.get("rows") or [])
    print(json.dumps(slim, indent=2, default=str))
    print(f"Wrote {out_path} ({slim['row_count']} trades)")
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
