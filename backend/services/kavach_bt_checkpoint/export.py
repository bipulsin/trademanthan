"""CSV export helpers for Kavach BT checkpoint."""
from __future__ import annotations

import csv
import io
from typing import Any, Dict, List


DETAIL_COLUMNS = [
    "trade_log_id",
    "session_date",
    "symbol",
    "direction",
    "entry_time",
    "entry_price",
    "exit_time",
    "exit_price",
    "grade",
    "r_realized",
    "mfe_r",
    "mae_r",
    "pnl",
    "pb_legacy",
    "pb_v2",
    "pb_hard_blocked",
    "res_confluence",
    "nearest_pivot",
    "pivot_kind",
    "cluster_n",
    "exit_a_r",
    "exit_a_reason",
    "exit_b_r",
    "exit_b_reason",
    "exit_c_r",
    "exit_c_reason",
    "best_exit_method",
    "garuda_confluence",
    "garuda_rank",
    "garuda_direction",
]

SUMMARY_COLUMNS = [
    "cohort_type",
    "cohort_key",
    "n",
    "win_rate",
    "avg_r",
    "total_pnl",
    "avg_mfe",
    "avg_mae",
    "recommendation_text",
]


def _cell(v: Any) -> Any:
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


def rows_to_csv(rows: List[Dict[str, Any]], columns: List[str]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow({c: _cell(r.get(c)) for c in columns})
    return buf.getvalue()


def detail_csv(rows: List[Dict[str, Any]]) -> str:
    return rows_to_csv(rows, DETAIL_COLUMNS)


def summary_csv(rows: List[Dict[str, Any]]) -> str:
    return rows_to_csv(rows, SUMMARY_COLUMNS)
