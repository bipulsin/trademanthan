#!/usr/bin/env python3
"""CLI: run Sambhav walk-forward backtest on DB 10m candles.

Usage:
  PYTHONPATH=. python scripts/sambhav_backtest.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.database import SessionLocal  # noqa: E402
from backend.services.sambhav.candles import load_10m_df_rows  # noqa: E402
from backend.services.sambhav.walk_forward import WalkForwardConfig, run_walk_forward  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description="Sambhav walk-forward backtest")
    p.add_argument("--train-bars", type=int, default=1500)
    p.add_argument("--test-bars", type=int, default=300)
    p.add_argument("--step-bars", type=int, default=300)
    p.add_argument("--model", default="xgboost")
    p.add_argument("--calibration", default="isotonic")
    args = p.parse_args()
    db = SessionLocal()
    try:
        bars = load_10m_df_rows(db, complete_only=True)
        out = run_walk_forward(
            bars,
            WalkForwardConfig(
                train_bars=args.train_bars,
                test_bars=args.test_bars,
                step_bars=args.step_bars,
                model=args.model,
                calibration=args.calibration,  # type: ignore[arg-type]
            ),
        )
        print(json.dumps(out, indent=2, default=str))
        return 0 if out.get("status") != "INSUFFICIENT DATA" or out.get("n_folds") else 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
