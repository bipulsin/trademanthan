#!/usr/bin/env python3
"""Run OpenHighLow intraday stock-futures backtest."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Recommended Upstox pacing for candle-heavy backtests.
os.environ.setdefault("UPSTOX_CANDLE_RL_PER_MIN", "40")
os.environ.setdefault("UPSTOX_CANDLE_RL_PER_SEC", "1")

from backend.services.open_high_low.config import (  # noqa: E402
    BACKTEST_END,
    BACKTEST_START,
    BROKERAGE_PER_LOT,
    SLIPPAGE_POINTS,
    SQUARE_OFF_TIME,
    OHLConfig,
)
from backend.services.open_high_low.runner import run_backtest  # noqa: E402


def _parse_time(s: str) -> time:
    parts = s.strip().split(":")
    return time(int(parts[0]), int(parts[1]) if len(parts) > 1 else 0)


def main() -> int:
    p = argparse.ArgumentParser(description="OpenHighLow intraday backtest")
    p.add_argument("--from", dest="date_from", default=BACKTEST_START.isoformat())
    p.add_argument("--to", dest="date_to", default=BACKTEST_END.isoformat())
    p.add_argument("--square-off", default=SQUARE_OFF_TIME.strftime("%H:%M"))
    p.add_argument("--brokerage", type=float, default=BROKERAGE_PER_LOT)
    p.add_argument("--slippage", type=float, default=SLIPPAGE_POINTS)
    p.add_argument("--symbols", default="", help="Comma-separated subset")
    p.add_argument("--max-symbols", type=int, default=None)
    p.add_argument("--out", default=None, help="Output directory")
    p.add_argument("--no-public", action="store_true")
    p.add_argument("--no-downloads", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = OHLConfig(
        start=date.fromisoformat(args.date_from),
        end=date.fromisoformat(args.date_to),
        square_off=_parse_time(args.square_off),
        brokerage_per_lot=args.brokerage,
        slippage_points=args.slippage,
        symbols=[s.strip().upper() for s in args.symbols.split(",") if s.strip()] or None,
        max_symbols=args.max_symbols,
    )
    out_dir = Path(args.out) if args.out else None
    result = run_backtest(
        cfg,
        out_dir=out_dir,
        copy_public=not args.no_public,
        copy_downloads=not args.no_downloads,
    )
    slim = {
        "ok": result.get("ok"),
        "stocks_scanned": result.get("stocks_scanned"),
        "trading_days": result.get("trading_days"),
        "trades": result.get("trades"),
        "skips": result.get("skips"),
        "win_rate": result.get("win_rate"),
        "net_pnl": result.get("net_pnl"),
        "html": result.get("html"),
        "csv": result.get("csv"),
        "public": result.get("public"),
        "downloads": result.get("downloads"),
    }
    print(json.dumps(slim, indent=2, default=str))
    print(
        f"\nSummary: stocks={slim['stocks_scanned']} trades={slim['trades']} "
        f"win_rate={slim['win_rate']:.1f}% net_pnl={slim['net_pnl']:.0f}"
    )
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
