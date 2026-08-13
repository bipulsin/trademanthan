#!/usr/bin/env python3
"""CLI: train Sambhav model + optional walk-forward validation.

Usage:
  PYTHONPATH=. python scripts/sambhav_train.py
  PYTHONPATH=. python scripts/sambhav_train.py --model logistic --calibration platt
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
from backend.services.sambhav.train import train_and_save  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description="Train Sambhav RESEARCH model")
    p.add_argument("--name", default="sambhav_xgb_v1")
    p.add_argument("--model", default="xgboost", choices=["xgboost", "logistic"])
    p.add_argument("--calibration", default="isotonic", choices=["isotonic", "platt", "none"])
    p.add_argument("--skip-validation", action="store_true")
    args = p.parse_args()
    db = SessionLocal()
    try:
        out = train_and_save(
            db,
            model_name=args.name,
            model_kind=args.model,
            calibration=args.calibration,
            run_validation=not args.skip_validation,
        )
        print(json.dumps(out, indent=2, default=str))
        return 0 if out.get("ok") else 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
