#!/usr/bin/env python3
"""One-shot Analysis snapshot (equity OHLC from Upstox). Rate-limited; skips Breakfast exclusivity unless --force.

  PYTHONPATH=. python scripts/run_analysis_snapshot.py
  PYTHONPATH=. python scripts/run_analysis_snapshot.py --force
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--symbols", default="", help="Comma-separated subset")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from backend.services.analysis_page.job import run_analysis_snapshot_job

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] or None
    out = run_analysis_snapshot_job(trigger="cli", symbols=symbols, force=args.force)
    print(json.dumps(out, default=str))
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
