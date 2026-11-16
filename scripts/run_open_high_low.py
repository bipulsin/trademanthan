#!/usr/bin/env python3
"""Run OpenHighLow intraday stock-futures backtest (+ selection study)."""
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
    RISK_RUPEES_DEFAULT,
    SLIPPAGE_POINTS,
    SL_MODES,
    SQUARE_OFF_TIME,
    OHLConfig,
)
from backend.services.open_high_low.runner import run_backtest  # noqa: E402


def _parse_time(s: str) -> time:
    parts = s.strip().split(":")
    return time(int(parts[0]), int(parts[1]) if len(parts) > 1 else 0)


def main() -> int:
    p = argparse.ArgumentParser(description="OpenHighLow intraday backtest + selection study")
    p.add_argument("--from", dest="date_from", default=BACKTEST_START.isoformat())
    p.add_argument("--to", dest="date_to", default=BACKTEST_END.isoformat())
    p.add_argument("--square-off", default=SQUARE_OFF_TIME.strftime("%H:%M"))
    p.add_argument("--brokerage", type=float, default=BROKERAGE_PER_LOT)
    p.add_argument("--slippage", type=float, default=SLIPPAGE_POINTS)
    p.add_argument("--risk-rupees", type=float, default=RISK_RUPEES_DEFAULT)
    p.add_argument(
        "--sl-modes",
        default=",".join(SL_MODES),
        help="Comma-separated: fixed,candle_trail,atr_trail",
    )
    p.add_argument("--symbols", default="", help="Comma-separated subset")
    p.add_argument("--max-symbols", type=int, default=None)
    p.add_argument("--out", default=None, help="Output directory")
    p.add_argument("--no-resume", action="store_true", help="Ignore month_cache")
    p.add_argument("--no-public", action="store_true")
    p.add_argument("--no-downloads", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    modes = [s.strip() for s in args.sl_modes.split(",") if s.strip()]
    cfg = OHLConfig(
        start=date.fromisoformat(args.date_from),
        end=date.fromisoformat(args.date_to),
        square_off=_parse_time(args.square_off),
        brokerage_per_lot=args.brokerage,
        slippage_points=args.slippage,
        risk_rupees=args.risk_rupees,
        sl_modes=modes,
        symbols=[s.strip().upper() for s in args.symbols.split(",") if s.strip()] or None,
        max_symbols=args.max_symbols,
        resume_months=not args.no_resume,
    )
    out_dir = Path(args.out) if args.out else None
    result = run_backtest(
        cfg,
        out_dir=out_dir,
        copy_public=not args.no_public,
        copy_downloads=not args.no_downloads,
    )

    # Print study headline metrics
    summary = result.get("summary") or {}
    by_mode = summary.get("by_sl_mode") or {}
    print("\n=== SL path proof ===")
    for s in result.get("sl_path_proof") or []:
        print(
            f"  {s.get('date')} {s.get('symbol')} {s.get('side')} "
            f"initial={s.get('initial_sl')} final={s.get('final_sl')} "
            f"moved={s.get('moved')} path_len={s.get('path_len')}"
        )

    print("\n=== Metrics by SL mode / rule (one lot) ===")
    for mode, block in sorted(by_mode.items()):
        base = block.get("baseline_one_lot") or {}
        print(
            f"  [{mode}] baseline trades={base.get('trades')} "
            f"WR={base.get('win_rate'):.1f}% net={base.get('net_pnl'):.0f} "
            f"PF={base.get('profit_factor')} avgR={base.get('avg_r'):.2f} "
            f"maxDD={base.get('max_drawdown'):.0f} t/day={base.get('trades_per_day'):.2f}"
            if base.get("trades") is not None
            else f"  [{mode}] (empty)"
        )
        for rule, rb in (block.get("rules") or {}).items():
            m = rb.get("one_lot") or {}
            if not m:
                continue
            print(
                f"    rule={rule}: trades={m.get('trades')} WR={m.get('win_rate'):.1f}% "
                f"net={m.get('net_pnl'):.0f} avgR={m.get('avg_r'):.2f}"
            )

    pct = (summary.get("selection") or {}).get("random_percentiles") or {}
    if pct:
        print("\n=== Random baseline mean percentiles ===")
        for mode, rules in sorted(pct.items()):
            for rule, info in sorted(rules.items()):
                print(
                    f"  {mode}/{rule}: pctl={info.get('mean_percentile')} "
                    f"days={info.get('days_scored')}"
                )

    slim = {
        "ok": result.get("ok"),
        "actual_start": result.get("actual_start"),
        "end": result.get("end"),
        "stocks_scanned": result.get("stocks_scanned"),
        "trading_days": result.get("trading_days"),
        "signals": result.get("signals"),
        "trades": result.get("trades"),
        "skips": result.get("skips"),
        "html": result.get("html"),
        "csv": result.get("csv"),
        "selection_summary": result.get("selection_summary"),
        "sl_path_samples": result.get("sl_path_samples"),
        "public": result.get("public"),
        "downloads": result.get("downloads"),
    }
    print("\n" + json.dumps(slim, indent=2, default=str))
    print(
        f"\nDate range: {slim.get('actual_start')} → {slim.get('end')} | "
        f"signals={slim.get('signals')} trade-rows={slim.get('trades')}"
    )
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
