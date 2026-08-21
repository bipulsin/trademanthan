"""Ensure + upsert bt_checkpoint_* research tables."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import engine

_ENSURED = False


def ensure_bt_checkpoint_tables() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS bt_checkpoint_trade_detail (
                    id BIGSERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    trade_log_id BIGINT NOT NULL,
                    session_date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    direction TEXT,
                    entry_time TIMESTAMPTZ,
                    entry_price DOUBLE PRECISION,
                    exit_time TIMESTAMPTZ,
                    exit_price DOUBLE PRECISION,
                    grade TEXT,
                    r_realized DOUBLE PRECISION,
                    mfe_r DOUBLE PRECISION,
                    mae_r DOUBLE PRECISION,
                    pnl DOUBLE PRECISION,
                    pb_legacy INTEGER,
                    pb_v2 INTEGER,
                    pb_hard_blocked BOOLEAN NOT NULL DEFAULT FALSE,
                    res_confluence BOOLEAN NOT NULL DEFAULT FALSE,
                    nearest_pivot DOUBLE PRECISION,
                    pivot_kind TEXT,
                    pivot_zone_pct DOUBLE PRECISION,
                    cluster_n INTEGER,
                    exit_a_price DOUBLE PRECISION,
                    exit_a_time TEXT,
                    exit_a_r DOUBLE PRECISION,
                    exit_a_reason TEXT,
                    exit_b_price DOUBLE PRECISION,
                    exit_b_time TEXT,
                    exit_b_r DOUBLE PRECISION,
                    exit_b_reason TEXT,
                    exit_c_price DOUBLE PRECISION,
                    exit_c_time TEXT,
                    exit_c_r DOUBLE PRECISION,
                    exit_c_reason TEXT,
                    exit_c_trigger_type TEXT,
                    best_exit_method TEXT,
                    garuda_confluence TEXT,
                    garuda_rank INTEGER,
                    garuda_direction TEXT,
                    components JSONB,
                    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (run_id, trade_log_id)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_bt_cp_detail_run
                ON bt_checkpoint_trade_detail (run_id, session_date, symbol)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS bt_checkpoint_pullback_bars (
                    id BIGSERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    session_date DATE NOT NULL,
                    bar_end TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    pb_legacy INTEGER,
                    pb_v2 INTEGER,
                    touched_ema5 BOOLEAN,
                    touched_ema10 BOOLEAN,
                    touched_vwap BOOLEAN,
                    dual_reset BOOLEAN,
                    UNIQUE (run_id, session_date, bar_end, symbol)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS bt_checkpoint_summary (
                    id BIGSERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    cohort_type TEXT NOT NULL,
                    cohort_key TEXT NOT NULL,
                    n INTEGER NOT NULL DEFAULT 0,
                    win_rate DOUBLE PRECISION,
                    avg_r DOUBLE PRECISION,
                    total_pnl DOUBLE PRECISION,
                    avg_mfe DOUBLE PRECISION,
                    avg_mae DOUBLE PRECISION,
                    recommendation_text TEXT,
                    extras JSONB,
                    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (run_id, cohort_type, cohort_key)
                )
                """
            )
        )
    _ENSURED = True


def upsert_trade_detail(row: Dict[str, Any]) -> None:
    ensure_bt_checkpoint_tables()
    comps = row.get("components")
    if comps is not None and not isinstance(comps, str):
        comps = json.dumps(comps)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO bt_checkpoint_trade_detail (
                    run_id, trade_log_id, session_date, symbol, direction,
                    entry_time, entry_price, exit_time, exit_price, grade,
                    r_realized, mfe_r, mae_r, pnl,
                    pb_legacy, pb_v2, pb_hard_blocked,
                    res_confluence, nearest_pivot, pivot_kind, pivot_zone_pct, cluster_n,
                    exit_a_price, exit_a_time, exit_a_r, exit_a_reason,
                    exit_b_price, exit_b_time, exit_b_r, exit_b_reason,
                    exit_c_price, exit_c_time, exit_c_r, exit_c_reason, exit_c_trigger_type,
                    best_exit_method, garuda_confluence, garuda_rank, garuda_direction,
                    components, computed_at
                ) VALUES (
                    :run_id, :trade_log_id, CAST(:session_date AS date), :symbol, :direction,
                    :entry_time, :entry_price, :exit_time, :exit_price, :grade,
                    :r_realized, :mfe_r, :mae_r, :pnl,
                    :pb_legacy, :pb_v2, :pb_hard_blocked,
                    :res_confluence, :nearest_pivot, :pivot_kind, :pivot_zone_pct, :cluster_n,
                    :exit_a_price, :exit_a_time, :exit_a_r, :exit_a_reason,
                    :exit_b_price, :exit_b_time, :exit_b_r, :exit_b_reason,
                    :exit_c_price, :exit_c_time, :exit_c_r, :exit_c_reason, :exit_c_trigger_type,
                    :best_exit_method, :garuda_confluence, :garuda_rank, :garuda_direction,
                    CAST(:components AS jsonb), NOW()
                )
                ON CONFLICT (run_id, trade_log_id) DO UPDATE SET
                    session_date = EXCLUDED.session_date,
                    symbol = EXCLUDED.symbol,
                    direction = EXCLUDED.direction,
                    entry_time = EXCLUDED.entry_time,
                    entry_price = EXCLUDED.entry_price,
                    exit_time = EXCLUDED.exit_time,
                    exit_price = EXCLUDED.exit_price,
                    grade = EXCLUDED.grade,
                    r_realized = EXCLUDED.r_realized,
                    mfe_r = EXCLUDED.mfe_r,
                    mae_r = EXCLUDED.mae_r,
                    pnl = EXCLUDED.pnl,
                    pb_legacy = EXCLUDED.pb_legacy,
                    pb_v2 = EXCLUDED.pb_v2,
                    pb_hard_blocked = EXCLUDED.pb_hard_blocked,
                    res_confluence = EXCLUDED.res_confluence,
                    nearest_pivot = EXCLUDED.nearest_pivot,
                    pivot_kind = EXCLUDED.pivot_kind,
                    pivot_zone_pct = EXCLUDED.pivot_zone_pct,
                    cluster_n = EXCLUDED.cluster_n,
                    exit_a_price = EXCLUDED.exit_a_price,
                    exit_a_time = EXCLUDED.exit_a_time,
                    exit_a_r = EXCLUDED.exit_a_r,
                    exit_a_reason = EXCLUDED.exit_a_reason,
                    exit_b_price = EXCLUDED.exit_b_price,
                    exit_b_time = EXCLUDED.exit_b_time,
                    exit_b_r = EXCLUDED.exit_b_r,
                    exit_b_reason = EXCLUDED.exit_b_reason,
                    exit_c_price = EXCLUDED.exit_c_price,
                    exit_c_time = EXCLUDED.exit_c_time,
                    exit_c_r = EXCLUDED.exit_c_r,
                    exit_c_reason = EXCLUDED.exit_c_reason,
                    exit_c_trigger_type = EXCLUDED.exit_c_trigger_type,
                    best_exit_method = EXCLUDED.best_exit_method,
                    garuda_confluence = EXCLUDED.garuda_confluence,
                    garuda_rank = EXCLUDED.garuda_rank,
                    garuda_direction = EXCLUDED.garuda_direction,
                    components = EXCLUDED.components,
                    computed_at = NOW()
                """
            ),
            {
                **{k: row.get(k) for k in (
                    "run_id", "trade_log_id", "session_date", "symbol", "direction",
                    "entry_time", "entry_price", "exit_time", "exit_price", "grade",
                    "r_realized", "mfe_r", "mae_r", "pnl",
                    "pb_legacy", "pb_v2", "pb_hard_blocked",
                    "res_confluence", "nearest_pivot", "pivot_kind", "pivot_zone_pct", "cluster_n",
                    "exit_a_price", "exit_a_time", "exit_a_r", "exit_a_reason",
                    "exit_b_price", "exit_b_time", "exit_b_r", "exit_b_reason",
                    "exit_c_price", "exit_c_time", "exit_c_r", "exit_c_reason", "exit_c_trigger_type",
                    "best_exit_method", "garuda_confluence", "garuda_rank", "garuda_direction",
                )},
                "components": comps,
            },
        )


def replace_summaries(run_id: str, rows: List[Dict[str, Any]]) -> None:
    ensure_bt_checkpoint_tables()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM bt_checkpoint_summary WHERE run_id = :r"), {"r": run_id})
        for row in rows:
            extras = row.get("extras")
            if extras is not None and not isinstance(extras, str):
                extras = json.dumps(extras)
            conn.execute(
                text(
                    """
                    INSERT INTO bt_checkpoint_summary (
                        run_id, cohort_type, cohort_key, n, win_rate, avg_r,
                        total_pnl, avg_mfe, avg_mae, recommendation_text, extras
                    ) VALUES (
                        :run_id, :cohort_type, :cohort_key, :n, :win_rate, :avg_r,
                        :total_pnl, :avg_mfe, :avg_mae, :recommendation_text, CAST(:extras AS jsonb)
                    )
                    """
                ),
                {
                    "run_id": run_id,
                    "cohort_type": row["cohort_type"],
                    "cohort_key": row["cohort_key"],
                    "n": int(row.get("n") or 0),
                    "win_rate": row.get("win_rate"),
                    "avg_r": row.get("avg_r"),
                    "total_pnl": row.get("total_pnl"),
                    "avg_mfe": row.get("avg_mfe"),
                    "avg_mae": row.get("avg_mae"),
                    "recommendation_text": row.get("recommendation_text"),
                    "extras": extras,
                },
            )


def upsert_pullback_bar(row: Dict[str, Any]) -> None:
    ensure_bt_checkpoint_tables()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO bt_checkpoint_pullback_bars (
                    run_id, session_date, bar_end, symbol,
                    pb_legacy, pb_v2, touched_ema5, touched_ema10, touched_vwap, dual_reset
                ) VALUES (
                    :run_id, CAST(:session_date AS date), :bar_end, :symbol,
                    :pb_legacy, :pb_v2, :touched_ema5, :touched_ema10, :touched_vwap, :dual_reset
                )
                ON CONFLICT (run_id, session_date, bar_end, symbol) DO UPDATE SET
                    pb_legacy = EXCLUDED.pb_legacy,
                    pb_v2 = EXCLUDED.pb_v2,
                    touched_ema5 = EXCLUDED.touched_ema5,
                    touched_ema10 = EXCLUDED.touched_ema10,
                    touched_vwap = EXCLUDED.touched_vwap,
                    dual_reset = EXCLUDED.dual_reset
                """
            ),
            row,
        )


def latest_run_id() -> Optional[str]:
    ensure_bt_checkpoint_tables()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT run_id FROM bt_checkpoint_trade_detail
                ORDER BY computed_at DESC NULLS LAST LIMIT 1
                """
            )
        ).fetchone()
        return row[0] if row else None


def list_detail(
    *,
    run_id: Optional[str] = None,
    symbol: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    pb_hard_blocked: Optional[bool] = None,
    res_confluence: Optional[bool] = None,
    garuda: Optional[str] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    ensure_bt_checkpoint_tables()
    rid = run_id or latest_run_id()
    if not rid:
        return []
    clauses = ["run_id = :run_id"]
    params: Dict[str, Any] = {"run_id": rid, "lim": limit}
    if symbol:
        clauses.append("UPPER(symbol) = UPPER(:symbol)")
        params["symbol"] = symbol
    if date_from:
        clauses.append("session_date >= CAST(:df AS date)")
        params["df"] = date_from
    if date_to:
        clauses.append("session_date <= CAST(:dt AS date)")
        params["dt"] = date_to
    if pb_hard_blocked is not None:
        clauses.append("pb_hard_blocked = :pbh")
        params["pbh"] = pb_hard_blocked
    if res_confluence is not None:
        clauses.append("res_confluence = :rc")
        params["rc"] = res_confluence
    if garuda:
        clauses.append("garuda_confluence = :gc")
        params["gc"] = garuda
    sql = f"""
        SELECT * FROM bt_checkpoint_trade_detail
        WHERE {' AND '.join(clauses)}
        ORDER BY session_date, entry_time
        LIMIT :lim
    """
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(sql), params).mappings().all()]


def list_summaries(run_id: Optional[str] = None) -> List[Dict[str, Any]]:
    ensure_bt_checkpoint_tables()
    rid = run_id or latest_run_id()
    if not rid:
        return []
    with engine.connect() as conn:
        return [
            dict(r)
            for r in conn.execute(
                text(
                    """
                    SELECT * FROM bt_checkpoint_summary
                    WHERE run_id = :r
                    ORDER BY cohort_type, cohort_key
                    """
                ),
                {"r": rid},
            )
            .mappings()
            .all()
        ]
