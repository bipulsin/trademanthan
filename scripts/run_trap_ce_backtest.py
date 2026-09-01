#!/usr/bin/env python3
"""Run Trap-CE CSV backtest (10m entry / trap SL)."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.config import settings
from backend.services.token_manager import load_upstox_token
from backend.services.trap_ce.backtest import run_trap_ce_backtest, write_artifact
from backend.services.trap_ce.config import DEFAULT_CSV
from backend.services.upstox_service import UpstoxService

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def main() -> int:
    p = argparse.ArgumentParser(description="Trap-CE 10m backtest from Chartink CSV")
    p.add_argument("--csv", default=str(ROOT / DEFAULT_CSV))
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--out", default=str(ROOT / "data" / "trap_ce" / "backtest_result.json"))
    p.add_argument("--symbol-pause", type=float, default=0.12)
    args = p.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.is_file():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 2
    token = (load_upstox_token() or "").strip()
    if not token:
        print("No Upstox token in file or UPSTOX_ACCESS_TOKEN — cannot fetch candles.", file=sys.stderr)
        return 3

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET, access_token=token)
    result = run_trap_ce_backtest(
        csv_path,
        upstox=upstox,
        limit=args.limit,
        symbol_pause_sec=args.symbol_pause,
    )
    out = Path(args.out)
    write_artifact(result, out)
    print(json.dumps(result["summary"], indent=2, default=str))
    print(f"Wrote {out} ({result['summary']['csv_rows']} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
