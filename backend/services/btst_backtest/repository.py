"""Persist BTST backtest runs and per-day per-side rows."""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from backend.database import SessionLocal

RESULT_COLS = [
    "run_id",
    "trade_date",
    "side",
    "stock_symbol",
    "change_pct_at_1445",
    "rank_type",
    "scan_rank",
    "spot_price_1445",
    "cpr_pivot",
    "cpr_tc",
    "cpr_bc",
    "cpr_gate_pass",
    "rsi_14_5min",
    "rsi_gate_pass",
    "liquidity_gate_pass",
    "direction",
    "atm_strike",
    "option_symbol",
    "data_mode",
    "premium_at_1500",
    "supertrend_pass",
    "hull_pass",
    "entry_time",
    "entry_premium",
    "lot_size",
    "buy_cost",
    "exit_a_time",
    "exit_a_premium",
    "exit_a_pnl",
    "exit_b_time",
    "exit_b_premium",
    "exit_b_pnl",
    "eligible_final",
    "no_eligible_reason",
]


def create_run(start_date: date, end_date: date, notes: str = "") -> int:
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                INSERT INTO btst_backtest_runs (start_date, end_date, notes)
                VALUES (:s, :e, :n)
                RETURNING id
                """
            ),
            {"s": start_date, "e": end_date, "n": notes},
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
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in RESULT_COLS if c not in ("trade_date", "side"))
    db = SessionLocal()
    try:
        r = db.execute(
            text(
                f"""
                INSERT INTO btst_backtest_results ({", ".join(RESULT_COLS)})
                VALUES ({", ".join(":" + c for c in RESULT_COLS)})
                ON CONFLICT (trade_date, side) DO UPDATE SET {sets}
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
                "SELECT * FROM btst_backtest_results ORDER BY trade_date DESC, side ASC"
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


def fetch_earliest_trade_date() -> Optional[date]:
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT MIN(trade_date) AS d FROM btst_backtest_results")
        ).fetchone()
        return row.d if row and row.d else None
    finally:
        db.close()


def fetch_failed_row_keys() -> List[Tuple[date, str]]:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT trade_date, side FROM btst_backtest_results
                WHERE no_eligible_reason = 'api_fetch_failed'
                ORDER BY trade_date DESC, side
                """
            )
        ).fetchall()
        return [(r.trade_date, r.side) for r in rows if r.trade_date and r.side]
    finally:
        db.close()


def compute_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    def _counts_for_pnl(r: Dict[str, Any]) -> bool:
        if r.get("direction") not in ("bullish", "bearish"):
            return False
        if r.get("data_mode") == "manual_fill":
            return r.get("entry_premium") is not None
        return bool(r.get("eligible_final"))

    def _sum(filter_fn):
        a = b = 0.0
        for r in rows:
            if not filter_fn(r):
                continue
            if r.get("exit_a_pnl") is not None:
                a += float(r["exit_a_pnl"])
            if r.get("exit_b_pnl") is not None:
                b += float(r["exit_b_pnl"])
        return a, b

    ce_a, ce_b = _sum(lambda r: r.get("direction") == "bullish" and _counts_for_pnl(r))
    pe_a, pe_b = _sum(lambda r: r.get("direction") == "bearish" and _counts_for_pnl(r))
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
    api_failed = sum(1 for r in rows if r.get("no_eligible_reason") == "api_fetch_failed")
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
