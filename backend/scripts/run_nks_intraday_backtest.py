"""
Run the NKS intraday backtest over the committed CSV and emit a JSON artifact.

Usage::

    PYTHONPATH=/home/ubuntu/trademanthan python3 backend/scripts/run_nks_intraday_backtest.py

By default the output JSON is written to
``/home/ubuntu/trademanthan/data/nks_intraday_backtest.json`` so it survives
``git pull`` and can be served by the public ``/api/nks-intraday/data``
endpoint. Pass ``--out <path>`` to override. Pass ``--csv <path>`` to override
the input CSV (defaults to ``backend/data/nks_intraday_stocks.csv``).
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


def _default_out_path() -> Path:
    ec2 = Path("/home/ubuntu/trademanthan/data/nks_intraday_backtest.json")
    if ec2.parent.is_dir():
        return ec2
    local = REPO_ROOT / "backend" / "data" / "nks_intraday_backtest.json"
    return local


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=None, help="Input CSV path")
    parser.add_argument("--out", type=Path, default=None, help="Output JSON path")
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
    out_path: Path = args.out or _default_out_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = load_stocks_csv(csv_path)
    log.info("Loaded %s CSV rows from %s", len(rows), csv_path)
    if not rows:
        log.error("No rows to process")
        return 3

    results = run_backtest(rows, throttle_sec=args.throttle, logger_fn=log.info)
    doc = build_output_document(results)

    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)
    tmp.replace(out_path)
    log.info(
        "Wrote %s rows to %s (fut=%s eq=%s with_prices=%s)",
        doc["summary"]["total_rows"],
        out_path,
        doc["summary"]["rows_fut_source"],
        doc["summary"]["rows_eq_source"],
        doc["summary"]["rows_with_prices"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
