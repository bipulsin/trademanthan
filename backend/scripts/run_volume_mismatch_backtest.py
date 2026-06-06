#!/usr/bin/env python3
"""
Run Gap + Bollinger Band Futures backtest and write public JSON artifact.

Usage::

    PYTHONPATH=. python backend/scripts/run_volume_mismatch_backtest.py
    PYTHONPATH=. python backend/scripts/run_volume_mismatch_backtest.py \\
        --from-date 2026-05-01 --to-date 2026-06-06

Artifact: ``volume_mismatch_backtest.json`` under ``data/`` (served at
``/volume-mismatch-backtest/data``). Live Volume Mismatch scanner is unchanged.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
os.environ.setdefault("PYTHONPATH", str(_ROOT))


def main() -> int:
    import backend.env_bootstrap  # noqa: F401

    from backend.services.volume_mismatch.backtest import (
        BACKTEST_DEFAULT_FROM,
        build_output_document,
        default_out_path,
        run_volume_mismatch_backtest,
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from-date",
        default=BACKTEST_DEFAULT_FROM.isoformat(),
        help="YYYY-MM-DD (default 2026-05-01)",
    )
    parser.add_argument(
        "--to-date",
        default=date.today().isoformat(),
        help="YYYY-MM-DD (default today IST calendar)",
    )
    parser.add_argument("--out", type=Path, default=None, help="Output JSON path")
    parser.add_argument("--day-pause", type=float, default=1.0, help="Seconds between session days")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="Parallel Upstox candle fetches per day (use 1-2 to avoid 429)",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)
    out_path = args.out or default_out_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    logging.info("Gap+BB backtest %s .. %s -> %s", from_date, to_date, out_path)
    raw = run_volume_mismatch_backtest(
        from_date,
        to_date,
        day_pause_sec=args.day_pause,
        max_workers=args.max_workers,
        out_path=out_path,
    )
    doc = build_output_document(raw)
    logging.info(
        "Wrote %s signals across %s days to %s",
        doc.get("summary", {}).get("total_signals"),
        doc.get("summary", {}).get("trading_days_scanned"),
        out_path,
    )
    if doc.get("error"):
        print(doc["error"], file=sys.stderr)
        return 1
    print(json.dumps(doc.get("summary") or {}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
