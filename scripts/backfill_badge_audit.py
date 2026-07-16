#!/usr/bin/env python3
"""Backfill Whipsawed / DIR CONFLICT / REGIME / CHURN badge-input shadow log.

  python3 scripts/backfill_badge_audit.py --symbol UPL --date 2026-07-16 \\
      --direction LONG --start 09:45
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--direction", default="LONG", choices=("LONG", "SHORT"))
    ap.add_argument("--start", default="09:45")
    ap.add_argument("--end", default=None, help="HH:MM IST (default: now)")
    args = ap.parse_args()

    from backend.services.kavach_badge_audit import backfill_badge_audit_symbol

    print(
        f"Backfilling badge audit {args.symbol} {args.date} "
        f"{args.start}-{args.end or 'now'} {args.direction}…",
        flush=True,
    )
    out = backfill_badge_audit_symbol(
        args.symbol,
        args.date,
        direction=args.direction,
        start_hm=args.start,
        end_hm=args.end,
    )
    print(
        json.dumps(
            {k: v for k, v in out.items() if k != "samples"},
            indent=2,
            default=str,
        )
    )
    for s in out.get("samples") or []:
        print(
            f"  {s.get('at')} active={s.get('active')} whip={s.get('whip')} "
            f"dir={s.get('dir')} reason={s.get('dir_reason')} "
            f"sides={s.get('sides')} labels={s.get('labels')} "
            f"regime={s.get('regime')} churn={s.get('churn')} badges={s.get('badges')}",
            flush=True,
        )
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
