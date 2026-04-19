"""
Run the FNO Bullish Trend scanner backtest and emit a JSON artifact.

Usage::

    PYTHONPATH=/home/ubuntu/trademanthan \\
      python3 backend/scripts/run_fno_bullish_backtest.py [--csv path] [--out path]

Default input : ``backend/data/fno_bullish_scanner.csv``
Default output: ``/home/ubuntu/trademanthan/data/fno_bullish_backtest.json`` if
that directory exists, else ``backend/data/fno_bullish_backtest.json`` in the
repo. The artifact is served by ``/api/fno-bullish/data``.

Use ``--1515-second-scan`` to require 2+ 15-min hits per run, anchor entry and
conviction at the **second** scan (+5 min for entry), and write
``fno_bullish_backtest_1515.json`` (``/api/fno-bullish/data-1515``).
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

from backend.services.fno_bullish_backtest import (  # noqa: E402
    build_output_document,
    load_scanner_csv,
    run_backtest,
)


def _default_out_path(s1515: bool = False) -> Path:
    name = "fno_bullish_backtest_1515.json" if s1515 else "fno_bullish_backtest.json"
    ec2 = Path("/home/ubuntu/trademanthan/data") / name
    if ec2.parent.is_dir():
        return ec2
    return REPO_ROOT / "backend" / "data" / name


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=None, help="Input CSV path")
    parser.add_argument("--out", type=Path, default=None, help="Output JSON path")
    parser.add_argument("--throttle", type=float, default=0.05,
                        help="Seconds to sleep between trade rows (Upstox calls are cached per day)")
    parser.add_argument("--verbose", action="store_true", help="DEBUG logging")
    parser.add_argument(
        "--1515-second-scan",
        dest="second_scan_1515",
        action="store_true",
        help="min 2 scans per streak, entry+conviction anchored at 2nd 15-min bar; default out 1515 JSON",
    )
    parser.add_argument("--min-scans", type=int, default=1,
                        help="Drop scanner runs shorter than this (count of 15-min hits in a row)")
    parser.add_argument("--entry-scan-index", type=int, default=0,
                        help="0 = first scan, 1 = second scan — LTP entry at this slot + entry offset")
    parser.add_argument("--conviction-scan-index", type=int, default=None,
                        help="Conviction VWAP/OI reference; defaults to entry-scan-index")
    args = parser.parse_args()

    min_scans = 2 if args.second_scan_1515 else args.min_scans
    entry_idx = 1 if args.second_scan_1515 else args.entry_scan_index
    conv_idx = (
        entry_idx
        if args.conviction_scan_index is None
        else args.conviction_scan_index
    )
    if args.second_scan_1515:
        conv_idx = entry_idx

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("fno_bullish_backtest_cli")

    csv_path: Path = args.csv or (REPO_ROOT / "backend" / "data" / "fno_bullish_scanner.csv")
    if not csv_path.is_file():
        log.error("Input CSV not found: %s", csv_path)
        return 2

    rows = load_scanner_csv(csv_path)
    log.info(
        "Loaded %s scanner rows from %s | min_scans=%s entry_scan_idx=%s conviction_scan_idx=%s",
        len(rows),
        csv_path,
        min_scans,
        entry_idx,
        conv_idx,
    )
    if not rows:
        log.error("No rows to process")
        return 3

    results = run_backtest(
        rows,
        throttle_sec=args.throttle,
        logger_fn=log.info,
        min_scan_count=min_scans,
        entry_scan_index=entry_idx,
        conviction_scan_index=conv_idx,
    )
    doc = build_output_document(
        results,
        min_scan_count=min_scans,
        entry_scan_index=entry_idx,
        conviction_scan_index=conv_idx,
    )

    out_path = args.out or _default_out_path(args.second_scan_1515)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)
    tmp.replace(out_path)

    s = doc["summary"]
    log.info(
        "wrote %s trades to %s | reentries=%s fut=%s eq=%s never_disapp=%s "
        "exit1_capped=%s | Σ₹(Exit1)=%s pos/neg=%s/%s best=%s worst=%s | "
        "Σ₹(Exit2)=%s pos/neg=%s/%s best=%s worst=%s",
        s["total_trades"], out_path,
        s["reentry_trades"], s["fut_rows"], s["eq_rows"],
        s["never_disappeared_rows"], s["exit1_capped_to_1515"],
        s["exit1"]["sum_pnl_rupees"], s["exit1"]["positive_rows"], s["exit1"]["negative_rows"],
        s["exit1"]["best_pnl_rupees"], s["exit1"]["worst_pnl_rupees"],
        s["exit2"]["sum_pnl_rupees"], s["exit2"]["positive_rows"], s["exit2"]["negative_rows"],
        s["exit2"]["best_pnl_rupees"], s["exit2"]["worst_pnl_rupees"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
