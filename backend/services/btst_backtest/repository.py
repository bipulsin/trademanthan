"""Persist CSV-fed BTST backtest runs and per-stock rows."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal

RESULT_COLS = [
    "run_id",
    "trade_date",
    "stock_symbol",
    "sector",
    "change_pct",
    "reference_price",
    "atm_strike",
    "direction",
    "option_symbol",
    "numeric_instrument_key",
    "data_mode",
    "supertrend_pass",
    "hull_pass",
    "eligible_final",
    "entry_premium",
    "exit_a_premium",
    "exit_b_premium",
    "lot_size",
    "buy_cost",
    "exit_a_pnl",
    "exit_b_pnl",
    "no_data_reason",
]


def create_run(*, csv_filename: str = "", notes: str = "") -> int:
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                INSERT INTO btst_backtest_runs (csv_filename, notes)
                VALUES (:f, :n)
                RETURNING id
                """
            ),
            {"f": csv_filename or None, "n": notes or None},
        ).fetchone()
        db.commit()
        return int(row.id)
    finally:
        db.close()


def upsert_result(run_id: int, row: Dict[str, Any]) -> int:
    params = {"run_id": run_id}
    for c in RESULT_COLS:
        if c == "run_id":
            continue
        params[c] = row.get(c)
    sets = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in RESULT_COLS if c not in ("trade_date", "stock_symbol")
    )
    conflict_where = (
        "WHERE stock_symbol IS NOT NULL AND TRIM(stock_symbol) <> ''"
    )
    db = SessionLocal()
    try:
        r = db.execute(
            text(
                f"""
                INSERT INTO btst_backtest_results ({", ".join(RESULT_COLS)})
                VALUES ({", ".join(":" + c for c in RESULT_COLS)})
                ON CONFLICT (trade_date, stock_symbol) {conflict_where}
                DO UPDATE SET {sets}
                RETURNING id
                """
            ),
            params,
        ).fetchone()
        db.commit()
        return int(r.id)
    finally:
        db.close()


def update_manual_fill(result_id: int, fields: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {
        "entry_premium",
        "exit_a_premium",
        "exit_b_premium",
        "exit_a_pnl",
        "exit_b_pnl",
        "buy_cost",
    }
    sets = []
    params: Dict[str, Any] = {"id": result_id}
    for k, v in fields.items():
        if k not in allowed:
            continue
        sets.append(f"{k} = :{k}")
        params[k] = v
    if not sets:
        return fetch_result(result_id) or {}
    db = SessionLocal()
    try:
        db.execute(
            text(f"UPDATE btst_backtest_results SET {', '.join(sets)} WHERE id = :id"),
            params,
        )
        db.commit()
    finally:
        db.close()
    return fetch_result(result_id) or {}


def fetch_result(result_id: int) -> Optional[Dict[str, Any]]:
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT * FROM btst_backtest_results WHERE id = :id"),
            {"id": result_id},
        ).mappings().fetchone()
        return dict(row) if row else None
    finally:
        db.close()


def fetch_all_results() -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                "SELECT * FROM btst_backtest_results ORDER BY trade_date DESC, stock_symbol ASC"
            )
        ).mappings().fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()


def fetch_latest_run_meta() -> Optional[Dict[str, Any]]:
    db = SessionLocal()
    try:
        run = db.execute(
            text("SELECT * FROM btst_backtest_runs ORDER BY id DESC LIMIT 1")
        ).mappings().fetchone()
        return dict(run) if run else None
    finally:
        db.close()


def count_results_for_run(run_id: int) -> int:
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT COUNT(*) AS n FROM btst_backtest_results WHERE run_id = :rid"),
            {"rid": int(run_id)},
        ).fetchone()
        return int(row.n) if row else 0
    finally:
        db.close()


def compute_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    def _has_pnl(r: Dict[str, Any]) -> bool:
        return r.get("entry_premium") is not None

    def _sum(filter_fn):
        a = b = 0.0
        for r in rows:
            if not filter_fn(r) or not _has_pnl(r):
                continue
            if r.get("exit_a_pnl") is not None:
                a += float(r["exit_a_pnl"])
            if r.get("exit_b_pnl") is not None:
                b += float(r["exit_b_pnl"])
        return a, b

    ce_a, ce_b = _sum(lambda r: r.get("direction") == "CE")
    pe_a, pe_b = _sum(lambda r: r.get("direction") == "PE")
    manual_needs = sum(
        1
        for r in rows
        if r.get("data_mode") == "manual_fill"
        and (
            r.get("entry_premium") is None
            or r.get("exit_a_premium") is None
            or r.get("exit_b_premium") is None
        )
    )
    manual_total = sum(1 for r in rows if r.get("data_mode") == "manual_fill")
    api_failed = sum(1 for r in rows if r.get("no_data_reason") == "option_premium_fetch_failed")
    return {
        "ce_scenario_a_total": ce_a,
        "ce_scenario_b_total": ce_b,
        "pe_scenario_a_total": pe_a,
        "pe_scenario_b_total": pe_b,
        "final_scenario_a_total": ce_a + pe_a,
        "final_scenario_b_total": ce_b + pe_b,
        "manual_fill_needs_data": manual_needs,
        "manual_fill_total": manual_total,
        "api_fetch_failed_count": api_failed,
        "row_count": len(rows),
    }
