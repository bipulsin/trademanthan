#!/usr/bin/env python3
"""Re-run Primary + 12-month rolling on the corrected 9:15–9:20 bar.

Writes NEW artifacts only. Does not overwrite breakfast_strategy_backtest.json
or breakfast_strategy_history.json. Does not write breakfast_strategy_trades.
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.breakfast_strategy.backtest import _summary, run_backtest, write_artifact
from backend.services.breakfast_strategy.config import (
    CORRECTED_915_ARTIFACT_NAME,
    CORRECTED_915_HISTORY_ARTIFACT_NAME,
    DATE_FROM,
    DATE_TO,
)
from backend.services.breakfast_strategy.history import (
    ROLLING_MONTHS_BACKWARD,
    _month_entry,
    month_calendar_bounds,
    rebuild_history_doc,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("run_breakfast_corrected_915_bar")


def _trades_and_summary(out: dict) -> tuple[list, dict]:
    trades = (out.get("variants") or {}).get("false", {}).get("trades") or out.get("trades") or []
    summary = (out.get("variants") or {}).get("false", {}).get("summary") or out.get("summary") or _summary(trades)
    return trades, summary


def main() -> int:
    logger.info("Primary Jul–Aug on corrected 9:15 stamp → %s", CORRECTED_915_ARTIFACT_NAME)
    primary = run_backtest(
        date_from=DATE_FROM,
        date_to=DATE_TO,
        persist_db=False,
        artifact_basename=CORRECTED_915_ARTIFACT_NAME,
        write_artifact_file=True,
        spot_proxy_fallback=False,
    )
    p_trades, p_summary = _trades_and_summary(primary)
    logger.info("Primary summary: %s", json.dumps(p_summary, indent=2))

    months = [
        _month_entry(
            period_label="2026-07-08",
            date_from=DATE_FROM,
            date_to=DATE_TO,
            price_source="futures",
            mode="backtest",
            status="complete",
            summary=p_summary,
            trades=p_trades,
            coverage={"note": "corrected_915_bar Primary Jul 29 – Aug 28 2026"},
        )
    ]

    for period_label in ["2026-06"] + list(ROLLING_MONTHS_BACKWARD):
        d0, d1 = month_calendar_bounds(period_label)
        logger.info("=== corrected_915_bar spot-proxy month %s ===", period_label)
        out = run_backtest(
            date_from=d0,
            date_to=d1,
            mode="backtest_oos_spot",
            persist_db=False,
            write_artifact_file=False,
            spot_proxy_fallback=True,
        )
        trades, summary = _trades_and_summary(out)
        for t in trades:
            t["period_label"] = period_label
        months.append(
            _month_entry(
                period_label=period_label,
                date_from=d0,
                date_to=d1,
                price_source="spot_proxy",
                mode="backtest_oos_spot",
                status="complete",
                summary=summary,
                trades=trades,
            )
        )
        logger.info(
            "Month %s trades=%s wr=%s avg_r=%s",
            period_label,
            summary.get("total_trades"),
            summary.get("win_rate_pct"),
            summary.get("avg_r"),
        )

    doc = rebuild_history_doc(months)
    doc["bar_label"] = "corrected_915_bar"
    path = write_artifact(doc, basename=CORRECTED_915_HISTORY_ARTIFACT_NAME)
    logger.info("History artifact: %s", path)
    logger.info("spot_proxy_rollup: %s", json.dumps(doc.get("spot_proxy_rollup") or {}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
