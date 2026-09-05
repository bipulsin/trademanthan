#!/usr/bin/env python3
"""Run HA-VWAP 10m backtest from CSV times on current-month stock futures."""
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

from backend.config import settings
from backend.services.ha_vwap.backtest import run_ha_vwap_backtest
from backend.services.ha_vwap.candles import default_out_dir
from backend.services.ha_vwap.signals import default_signals_path
from backend.services.token_manager import load_upstox_token
from backend.services.upstox_service import UpstoxService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main() -> int:
    p = argparse.ArgumentParser(description="HA-VWAP 10m CSV futures backtest")
    p.add_argument("--mode", choices=["futures"], default="futures")
    p.add_argument("--from", dest="date_from", default=None)
    p.add_argument("--to", dest="date_to", default=None)
    p.add_argument("--no-resume", action="store_true")
    p.add_argument("--drop-json", action="store_true", help="Delete existing combined/monthly JSON first")
    p.add_argument("--csv", dest="csv_path", default=None)
    p.add_argument("--symbol-pause", type=float, default=0.12)
    p.add_argument("--limit-symbols", type=int, default=None)
    p.add_argument("--out-dir", default=None)
    args = p.parse_args()

    token = (load_upstox_token() or "").strip()
    if not token:
        print("No Upstox token — cannot fetch candles.", file=sys.stderr)
        return 3
    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET, access_token=token)
    out_dir = Path(args.out_dir) if args.out_dir else default_out_dir()
    csv_path = Path(args.csv_path) if args.csv_path else default_signals_path()

    last = run_ha_vwap_backtest(
        upstox,
        mode="futures",
        date_from=date.fromisoformat(args.date_from) if args.date_from else None,
        date_to=date.fromisoformat(args.date_to) if args.date_to else None,
        out_dir=out_dir,
        resume=not args.no_resume and not args.drop_json,
        symbol_pause_sec=args.symbol_pause,
        limit_symbols=args.limit_symbols,
        csv_path=csv_path,
        drop_json=args.drop_json or args.no_resume,
    )
    slim = {k: v for k, v in last.items() if k != "trades"}
    slim["trade_count"] = len(last.get("trades") or [])
    print(json.dumps(slim.get("summary"), indent=2, default=str))
    print(f"mode=futures months={slim.get('months_status')} wrote {out_dir} csv={csv_path}")
    return 0 if last.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
