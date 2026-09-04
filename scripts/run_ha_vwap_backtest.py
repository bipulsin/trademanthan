#!/usr/bin/env python3
"""Run HA-VWAP 10m backtest (futures first, then cash months backward)."""
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
from backend.services.ha_vwap.config import CASH_FROM, CASH_TO, FUTURES_FROM, FUTURES_TO
from backend.services.token_manager import load_upstox_token
from backend.services.upstox_service import UpstoxService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main() -> int:
    p = argparse.ArgumentParser(description="HA-VWAP 10m backtest")
    p.add_argument("--mode", choices=["futures", "cash", "all"], default="futures")
    p.add_argument("--from", dest="date_from", default=None)
    p.add_argument("--to", dest="date_to", default=None)
    p.add_argument("--no-resume", action="store_true")
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

    modes = ["futures", "cash"] if args.mode == "all" else [args.mode]
    last = {}
    for mode in modes:
        df = date.fromisoformat(args.date_from) if args.date_from else None
        dt = date.fromisoformat(args.date_to) if args.date_to else None
        if mode == "futures" and df is None:
            df, dt = FUTURES_FROM, dt or FUTURES_TO
        if mode == "cash" and df is None:
            df, dt = CASH_FROM, dt or CASH_TO
        last = run_ha_vwap_backtest(
            upstox,
            mode=mode,
            date_from=df,
            date_to=dt,
            out_dir=out_dir,
            resume=not args.no_resume,
            symbol_pause_sec=args.symbol_pause,
            limit_symbols=args.limit_symbols,
        )
        slim = {k: v for k, v in last.items() if k != "trades"}
        slim["trade_count"] = len(last.get("trades") or [])
        print(json.dumps(slim.get("summary"), indent=2, default=str))
        print(f"mode={mode} months={slim.get('months_status')} wrote {out_dir}")
    return 0 if last.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
