"""CSV-fed BTST backtest orchestration."""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from backend.services.btst_backtest import progress as btst_progress
from backend.services.btst_backtest.config import get_config
from backend.services.btst_backtest.data_access import BtstDataAccess
from backend.services.btst_backtest.repository import create_run, upsert_result
from backend.services.btst_backtest.row_processor import process_csv_row

logger = logging.getLogger(__name__)


def run_csv_backtest(
    csv_rows: List[Dict[str, Any]],
    *,
    csv_filename: str = "",
    notes: str = "",
) -> Dict[str, Any]:
    if not csv_rows:
        return {"error": "no CSV rows to process"}
    cfg = get_config()
    run_id = create_run(csv_filename=csv_filename, notes=notes or "csv")
    btst_progress.set_run_created(run_id)
    data = BtstDataAccess()
    row_ids: List[int] = []
    total = len(csv_rows)
    for i, csv_row in enumerate(csv_rows, start=1):
        btst_progress.set_row(i - 1, total, symbol=csv_row.get("stock_symbol"))
        logger.info(
            "BTST CSV row %s/%s: %s %s",
            i,
            total,
            csv_row.get("trade_date"),
            csv_row.get("stock_symbol"),
        )
        result = process_csv_row(data, csv_row, cfg)
        row_ids.append(upsert_result(run_id, result))
        btst_progress.set_row(i, total, symbol=csv_row.get("stock_symbol"))
    return {"run_id": run_id, "rows_processed": len(row_ids), "result_ids": row_ids}
