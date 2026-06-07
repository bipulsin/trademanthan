#!/usr/bin/env python3
"""
Run NK VM Bull backtest and write public JSON artifact.

Usage::

    PYTHONPATH=. python backend/scripts/run_nk_vm_bull_backtest.py
    PYTHONPATH=. python backend/scripts/run_nk_vm_bull_backtest.py \\
        --source data/nk_vm_bull_backtest_source.csv

Artifact: ``nk_vm_bull_backtest.json`` under ``data/`` (served at
``/nk-vm-bull-backtest/data``).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
os.environ.setdefault("PYTHONPATH", str(_ROOT))


def main() -> int:
    import backend.env_bootstrap  # noqa: F401

    from backend.services.nk_vm_bull.backtest import (
        default_out_path,
        default_source_path,
        run_nk_vm_bull_backtest,
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=None, help="Input CSV path")
    parser.add_argument("--out", type=Path, default=None, help="Output JSON path")
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.15,
        help="Seconds between Upstox candle fetches (use 0.15-0.3 to avoid 429)",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    source_path = args.source or default_source_path()
    out_path = args.out or default_out_path()
    if not source_path.is_file():
        print(f"Source CSV not found: {source_path}", file=sys.stderr)
        return 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    logging.info("NK VM Bull backtest %s -> %s", source_path, out_path)
    doc = run_nk_vm_bull_backtest(source_path, out_path=out_path, throttle_sec=args.throttle)
    if doc.get("error"):
        print(doc["error"], file=sys.stderr)
        return 1
    logging.info(
        "Wrote %s trades (%s errors) to %s",
        doc.get("summary", {}).get("total_trades"),
        doc.get("summary", {}).get("errors"),
        out_path,
    )
    print(json.dumps(doc.get("summary") or {}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
