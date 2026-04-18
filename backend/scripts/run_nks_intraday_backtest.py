"""
Run the NKS intraday backtest over the committed CSV and emit JSON artifacts.

Usage::

    PYTHONPATH=/home/ubuntu/trademanthan \\
      python3 backend/scripts/run_nks_intraday_backtest.py [--mode same|next|both]

By default both same-day and next-trading-day artifacts are produced:

- ``/home/ubuntu/trademanthan/data/nks_intraday_backtest.json``      (same day)
- ``/home/ubuntu/trademanthan/data/nks_intraday_backtest_nextday.json`` (next trading day)

Both paths survive ``git pull`` and are served by the public
``/api/nks-intraday/data`` endpoint via the ``day=same|next`` query parameter.
Use ``--out-same`` / ``--out-next`` to override the output paths. Use
``--csv <path>`` to override the input CSV (defaults to
``backend/data/nks_intraday_stocks.csv``).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.nks_intraday_backtest import (  # noqa: E402
    build_output_document,
    load_stocks_csv,
    run_backtest,
)


def _default_out_path(mode: str) -> Path:
    suffix = "_nextday" if mode == "next" else ""
    ec2 = Path(f"/home/ubuntu/trademanthan/data/nks_intraday_backtest{suffix}.json")
    if ec2.parent.is_dir():
        return ec2
    return REPO_ROOT / "backend" / "data" / f"nks_intraday_backtest{suffix}.json"


def _emit(rows, out_path: Path, mode: str, throttle: float, log) -> int:
    log.info("=== Running backtest in mode=%s ===", mode)
    results = run_backtest(rows, throttle_sec=throttle, logger_fn=log.info, day_mode=mode)
    doc = build_output_document(results, day_mode=mode)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)
    tmp.replace(out_path)
    s = doc["summary"]
    log.info(
        "[%s] wrote %s rows to %s (fut=%s eq=%s with_prices=%s "
        "Σpts=%s Σ₹=%s Σdd₹=%s worst_dd₹=%s | taken=%s skipped=%s "
        "skip_reasons=%s taken_Σ₹=%s taken_Σdd₹=%s)",
        mode,
        s["total_rows"],
        out_path,
        s["rows_fut_source"],
        s["rows_eq_source"],
        s["rows_with_prices"],
        s["sum_pnl_points"],
        s["sum_pnl_rupees"],
        s.get("sum_drawdown_rupees"),
        s.get("worst_drawdown_rupees"),
        s.get("taken_rows"),
        s.get("skipped_rows"),
        s.get("skipped_by_reason"),
        s.get("taken_sum_pnl_rupees"),
        s.get("taken_sum_drawdown_rupees"),
    )
    return s["total_rows"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=None, help="Input CSV path")
    parser.add_argument(
        "--mode",
        choices=["same", "next", "both"],
        default="both",
        help="Which artifact(s) to generate",
    )
    parser.add_argument("--out-same", type=Path, default=None, help="Same-day output path")
    parser.add_argument("--out-next", type=Path, default=None, help="Next-day output path")
    parser.add_argument("--throttle", type=float, default=0.08, help="Seconds between Upstox calls")
    parser.add_argument("--verbose", action="store_true", help="DEBUG logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("nks_intraday_backtest_cli")

    csv_path: Path = args.csv or (REPO_ROOT / "backend" / "data" / "nks_intraday_stocks.csv")
    if not csv_path.is_file():
        log.error("Input CSV not found: %s", csv_path)
        return 2

    rows = load_stocks_csv(csv_path)
    log.info("Loaded %s CSV rows from %s", len(rows), csv_path)
    if not rows:
        log.error("No rows to process")
        return 3

    modes = ["same", "next"] if args.mode == "both" else [args.mode]
    for m in modes:
        out_path = args.out_same if m == "same" else args.out_next
        out_path = out_path or _default_out_path(m)
        _emit(rows, out_path, m, args.throttle, log)
    return 0


if __name__ == "__main__":
    sys.exit(main())
