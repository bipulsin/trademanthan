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
    V2_MIN_DATE,
    build_output_document,
    load_stocks_csv,
    run_backtest,
)


def _default_out_path(mode: str) -> Path:
    if mode == "next":
        suffix = "_nextday"
    elif mode == "v2":
        suffix = "_v2"
    else:
        suffix = ""
    ec2 = Path(f"/home/ubuntu/trademanthan/data/nks_intraday_backtest{suffix}.json")
    if ec2.parent.is_dir():
        return ec2
    return REPO_ROOT / "backend" / "data" / f"nks_intraday_backtest{suffix}.json"


def _filter_rows_v2(rows):
    """V2 runs over FUTURES-tradable symbols only, from V2_MIN_DATE onwards.

    At CSV-load time we don't yet know which symbols resolve to a FUT contract
    (the resolver decides this per (symbol, date)), so we apply the date cut
    here and let the source/FUT filter happen post-compute in _emit.
    """
    kept = []
    for r in rows:
        d = r.get("session_date")
        if d is None:
            continue
        if d < V2_MIN_DATE:
            continue
        kept.append(r)
    return kept


def _emit(rows, out_path: Path, mode: str, throttle: float, log) -> int:
    log.info("=== Running backtest in mode=%s ===", mode)
    # V2 uses the "same-day" computation but filters the CSV to the post-
    # 20-Mar window, and further drops any row whose resolver fell back to
    # the EQ underlying (we want FUT-only).
    compute_mode = "same" if mode == "v2" else mode
    src_rows = _filter_rows_v2(rows) if mode == "v2" else rows
    log.info("[%s] input rows after filter: %s", mode, len(src_rows))

    results = run_backtest(
        src_rows, throttle_sec=throttle, logger_fn=log.info, day_mode=compute_mode
    )
    if mode == "v2":
        before = len(results)
        results = [r for r in results if r.get("source") == "FUT"]
        log.info("[v2] kept %s/%s FUT rows (dropped EQ fallbacks)", len(results), before)

    doc = build_output_document(results, day_mode=mode)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)
    tmp.replace(out_path)
    s = doc["summary"]
    if mode == "v2":
        log.info(
            "[v2] wrote %s FUT rows to %s | TAKE=%s SKIP=%s stopped=%s held=%s "
            "Σ₹=%s worst=%s best=%s Σsus_dd₹=%s worst_sus_dd₹=%s "
            "skip_reasons=%s score_dist=%s",
            s["total_rows"],
            out_path,
            s.get("v2_take_rows"),
            s.get("v2_skip_rows"),
            s.get("v2_stopped_out"),
            s.get("v2_held_to_close"),
            s.get("v2_sum_pnl_rupees"),
            s.get("v2_worst_pnl_rupees"),
            s.get("v2_best_pnl_rupees"),
            s.get("v2_sum_sustained_dd_rupees"),
            s.get("v2_worst_sustained_dd_rupees"),
            s.get("v2_skip_by_reason"),
            s.get("v2_score_distribution"),
        )
    else:
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
        choices=["same", "next", "both", "v2", "all"],
        default="both",
        help="Which artifact(s) to generate ('all' = same+next+v2)",
    )
    parser.add_argument("--out-same", type=Path, default=None, help="Same-day output path")
    parser.add_argument("--out-next", type=Path, default=None, help="Next-day output path")
    parser.add_argument("--out-v2", type=Path, default=None, help="V2 (expert, FUT-only) output path")
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

    if args.mode == "both":
        modes = ["same", "next"]
    elif args.mode == "all":
        modes = ["same", "next", "v2"]
    else:
        modes = [args.mode]
    out_overrides = {"same": args.out_same, "next": args.out_next, "v2": args.out_v2}
    for m in modes:
        out_path = out_overrides.get(m) or _default_out_path(m)
        _emit(rows, out_path, m, args.throttle, log)
    return 0


if __name__ == "__main__":
    sys.exit(main())
