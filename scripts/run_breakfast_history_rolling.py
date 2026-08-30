#!/usr/bin/env python3
"""Run 12-month backward spot-proxy history (May-2026 → Jun-2025), incremental JSON updates."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.breakfast_strategy.history import (
    ROLLING_MONTHS_BACKWARD,
    run_spot_proxy_month,
    seed_history_from_existing,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("run_breakfast_history_rolling")


def main() -> int:
    logger.info("Seeding history from Jul–Aug futures + Jun-2026 spot-proxy…")
    seed_history_from_existing()

    for period_label in ROLLING_MONTHS_BACKWARD:
        logger.info("=== Running spot-proxy month %s ===", period_label)
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

    logger.info("12-month rolling history run finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
