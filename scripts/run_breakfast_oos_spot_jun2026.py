#!/usr/bin/env python3
"""June 2026 OOS backtest with spot-proxy fallback (mode=backtest_oos_spot)."""
from __future__ import annotations

import json
import logging
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.breakfast_strategy.backtest import run_backtest
from backend.services.breakfast_strategy.config import OOS_SPOT_ARTIFACT_NAME

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("run_breakfast_oos_spot_jun2026")

OOS_FROM = date(2026, 6, 1)
OOS_TO = date(2026, 6, 30)


def main() -> int:
    out = run_backtest(
        date_from=OOS_FROM,
        date_to=OOS_TO,
        mode="backtest_oos_spot",
        force_fetch=False,
        persist_db=True,
        pnl_cap_enabled=False,
        artifact_basename=OOS_SPOT_ARTIFACT_NAME,
        spot_proxy_fallback=True,
    )
    logger.info("artifact: %s", out.get("artifact_path"))
    logger.info("price_source_counts: %s", out.get("price_source_counts"))
    logger.info("summary: %s", json.dumps(out.get("summary") or {}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
