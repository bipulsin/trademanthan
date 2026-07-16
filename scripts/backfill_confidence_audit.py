#!/usr/bin/env python3
"""Backfill Confidence component + Structural Alignment shadow logs for a symbol/day.

  python3 scripts/backfill_confidence_audit.py --symbol UPL --date 2026-07-16 \\
      --direction LONG --start 09:45 --end 12:45
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
    ap.add_argument("--date", required=True, help="YYYY-MM-DD session date IST")
    ap.add_argument("--direction", default="LONG", choices=("LONG", "SHORT"))
    ap.add_argument("--start", default="09:45")
    ap.add_argument("--end", default="12:45")
    args = ap.parse_args()

    from backend.services.kavach_confidence_audit import backfill_symbol_session

    print(
        f"Backfilling {args.symbol} {args.date} {args.start}-{args.end} {args.direction}…",
        flush=True,
    )
    out = backfill_symbol_session(
        args.symbol,
        args.date,
        direction=args.direction,
        start_hm=args.start,
        end_hm=args.end,
    )
    print(json.dumps({k: v for k, v in out.items() if k != "samples"}, indent=2, default=str))
    for s in out.get("samples") or []:
        comps = s.get("components") or {}
        print(
            f"  {s.get('at')} score={s.get('trade_score')} grade={s.get('grade')} "
            f"rule={s.get('banding')} vol={s.get('vol')} "
            f"align={s.get('align')}/5 persist={s.get('persist')} "
            f"rs={comps.get('rs_pts')} kav={comps.get('kavach_pts')} "
            f"vol_pts={comps.get('volume_pts')} adx={comps.get('adx_pts')} "
            f"vwap={comps.get('vwap_side_pts')} state={comps.get('kavach_state')}",
            flush=True,
        )
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
