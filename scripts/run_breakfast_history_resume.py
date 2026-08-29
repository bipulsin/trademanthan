#!/usr/bin/env python3
"""Resume spot-proxy rolling months — skip complete, re-run failed/running/pending."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.breakfast_strategy.history import (
    ROLLING_MONTHS_BACKWARD,
    load_history,
    run_spot_proxy_month,
    seed_history_from_existing,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("run_breakfast_history_resume")


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Resume spot-proxy rolling months (skip complete).")
    parser.add_argument("--from", dest="from_period", metavar="YYYY-MM", help="First period (inclusive), e.g. 2025-11")
    parser.add_argument("--to", dest="to_period", metavar="YYYY-MM", help="Last period (inclusive), e.g. 2025-06")
    parser.add_argument("--force", action="store_true", help="Re-run even if status is complete")
    args = parser.parse_args()

    months = list(ROLLING_MONTHS_BACKWARD)
    if args.from_period:
        if args.from_period not in months:
            raise SystemExit(f"Unknown period {args.from_period}")
        i0 = months.index(args.from_period)
        i1 = months.index(args.to_period) if args.to_period else len(months) - 1
        if args.to_period and args.to_period not in months:
            raise SystemExit(f"Unknown period {args.to_period}")
        if i0 > i1:
            raise SystemExit("--from must be >= --to in backward list (e.g. --from 2025-11 --to 2025-06)")
        months = months[i0 : i1 + 1]

    logger.info("Seeding history (preserve completed months)…")
    seed_history_from_existing()
    doc = load_history()
    by_pl = {str(m.get("period_label") or ""): m for m in doc.get("months") or []}

    for period_label in months:
        existing = by_pl.get(period_label)
        if not args.force and existing and existing.get("status") == "complete":
            logger.info("Skip complete month %s (%s trades)", period_label, (existing.get("summary") or {}).get("total_trades"))
            continue
        logger.info("=== Running spot-proxy month %s (was %s) ===", period_label, (existing or {}).get("status", "pending"))
        entry = run_spot_proxy_month(period_label, persist_db=True, force_fetch=False)
        logger.info(
            "Month %s status=%s trades=%s pnl=%s",
            period_label,
            entry.get("status"),
            (entry.get("summary") or {}).get("total_trades"),
            (entry.get("summary") or {}).get("total_pnl_inr"),
        )
        if entry.get("status") == "failed":
            logger.error("Failed month %s: %s", period_label, entry.get("error"))

    logger.info("Resume run finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
