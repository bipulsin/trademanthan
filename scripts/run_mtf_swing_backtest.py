#!/usr/bin/env python3
"""Resume the MTF Swing backtest (v1 full year, or v2 continuous experiment).

From the repo root:

    # v1 (Nov 2025–Oct 2026)
    python3 scripts/run_mtf_swing_backtest.py
    python3 scripts/run_mtf_swing_backtest.py --timeframe 60m
    python3 scripts/run_mtf_swing_backtest.py --symbol RELIANCE --timeframe 15m

    # v2 continuous full period (state carries across months)
    python3 scripts/run_mtf_swing_backtest.py --v2 --start 2025-11 --end 2026-10 \\
        --timeframes 60m,15m --variants v1_equivalent
    python3 scripts/run_mtf_swing_backtest.py --v2 --start 2025-11 --end 2026-10 \\
        --timeframes 60m,15m --variants all
    python3 scripts/run_mtf_swing_backtest.py --v2 --ablation --leave-one-out

v1 writes data/mtf_swing/; v2 writes data/mtf_swing_v2/ and never rewrites v1 CSVs.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run or resume the MTF Swing backtest")
    parser.add_argument("--v2", action="store_true", help="Run the v2 experiment (separate data dir)")
    parser.add_argument("--timeframe", choices=("60m", "15m", "both"), default="both", help="v1 timeframe selector")
    parser.add_argument(
        "--timeframes",
        default="",
        help="v2 comma-separated timeframes (default 60m,15m)",
    )
    parser.add_argument(
        "--months",
        default="",
        help="v2 comma-separated YYYY-MM months (overrides --start/--end when set)",
    )
    parser.add_argument("--start", default="", help="v2 first month YYYY-MM (default 2025-11)")
    parser.add_argument("--end", default="", help="v2 last month YYYY-MM (default 2026-10)")
    parser.add_argument(
        "--variants",
        default="",
        help="v2 variants: bare 'all' = full matrix; or comma list e.g. v1_equivalent,regime_only",
    )
    parser.add_argument("--ablation", action="store_true", help="v2: run the full primary ablation matrix")
    parser.add_argument(
        "--leave-one-out",
        action="store_true",
        help="v2: also run all_except_* leave-one-out variants",
    )
    parser.add_argument("--symbol", action="append", default=[], help="Repeat to limit the universe")
    parser.add_argument("--no-resume", action="store_true", help="Recompute months even if marked done")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.v2:
        from backend.services.mtf_swing.runner_v2 import run_backtest_v2  # noqa: E402

        months = [m for m in (args.months or "").split(",") if m.strip()] or None
        tfs = [t for t in (args.timeframes or "").split(",") if t.strip()] or None
        variants = [v for v in (args.variants or "").split(",") if v.strip()] or None
        summary = run_backtest_v2(
            timeframes=tfs,
            months=months,
            start=(args.start or None),
            end=(args.end or None),
            symbols=args.symbol or None,
            resume=not args.no_resume,
            ablation=bool(args.ablation),
            variants=variants,
            leave_one_out=bool(args.leave_one_out),
        )
    else:
        from backend.services.mtf_swing.runner import run_backtest  # noqa: E402

        tfs = ("60m", "15m") if args.timeframe == "both" else (args.timeframe,)
        summary = run_backtest(timeframes=tfs, symbols=args.symbol or None, resume=not args.no_resume)
    print(summary)


if __name__ == "__main__":
    main()
