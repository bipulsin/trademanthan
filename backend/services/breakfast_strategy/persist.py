"""Persist Breakfast Strategy trades to PostgreSQL."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import SessionLocal, engine

logger = logging.getLogger(__name__)

_MIGRATION = Path(__file__).resolve().parents[2] / "migrations" / "add_breakfast_strategy_trades.sql"
_MIGRATION_ANCHOR = Path(__file__).resolve().parents[2] / "migrations" / "add_breakfast_strategy_anchor_extreme.sql"
_MIGRATION_PNL_CAP = Path(__file__).resolve().parents[2] / "migrations" / "add_breakfast_strategy_pnl_cap_exit.sql"
_MIGRATION_INSTRUMENT = (
    Path(__file__).resolve().parents[2] / "migrations" / "add_breakfast_strategy_instrument_label.sql"
)
_MIGRATION_OOS_MODE = (
    Path(__file__).resolve().parents[2] / "migrations" / "add_breakfast_strategy_oos_mode.sql"
)
_MIGRATION_SPOT_PROXY = (
    Path(__file__).resolve().parents[2] / "migrations" / "add_breakfast_strategy_spot_proxy.sql"
)
_MIGRATION_PERIOD = (
    Path(__file__).resolve().parents[2] / "migrations" / "add_breakfast_strategy_period_label.sql"
)
_ENSURED = False


def ensure_breakfast_strategy_table() -> None:
    global _ENSURED
    if _ENSURED:
        return
    for mig in (
        _MIGRATION,
        _MIGRATION_ANCHOR,
        _MIGRATION_PNL_CAP,
        _MIGRATION_INSTRUMENT,
        _MIGRATION_OOS_MODE,
        _MIGRATION_SPOT_PROXY,
        _MIGRATION_PERIOD,
    ):
        if not mig.is_file():
            logger.warning("breakfast migration file missing: %s", mig)
            continue
        sql = mig.read_text(encoding="utf-8")
        with engine.begin() as conn:
            conn.execute(text(sql))
    _ENSURED = True
    logger.info("breakfast_strategy_trades table ensured")


def clear_backtest_trades() -> int:
    """Delete all backtest-mode rows before a fresh full backtest."""
    ensure_breakfast_strategy_table()
    db = SessionLocal()
    try:
        n = db.execute(
            text("DELETE FROM breakfast_strategy_trades WHERE mode = 'backtest'")
        ).rowcount
        db.commit()
        return int(n or 0)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def trade_exists(db: Session, session_date: str, symbol: str, direction: str) -> bool:
    n = db.execute(
        text(
            """
            SELECT COUNT(*) FROM breakfast_strategy_trades
            WHERE session_date = CAST(:d AS date)
              AND UPPER(symbol) = UPPER(:sym)
              AND direction = :dir
            """
        ),
        {"d": session_date, "sym": symbol, "dir": direction},
    ).scalar()
    return int(n or 0) > 0


def insert_trade(db: Session, row: Dict[str, Any]) -> bool:
    sd = str(row.get("session_date") or "")[:10]
    sym = str(row.get("symbol") or "").strip().upper()
    direction = str(row.get("direction") or "").strip().lower()
    if trade_exists(db, sd, sym, direction):
        return False
    db.execute(
        text(
            """
            INSERT INTO breakfast_strategy_trades (
                session_date, symbol, underlying_symbol, instrument_label, direction, mode, strategy_status,
                sector, sector_index, sector_rank, stock_rank,
                nifty_bias, nifty_bias_pct, nifty_open_5m, nifty_close_5m,
                stock_move_pct_at_entry,
                setup_open_5m, setup_high_5m, setup_low_5m, setup_close_5m, setup_volume_5m,
                instrument_key, lot_size, price_source, period_label,
                entry_time, entry_price, anchor_price, sl_price, tp_price, pre_exit_extreme,
                exit_time, exit_price, exit_trigger_type,
                pnl_inr, pnl_points, notes
            ) VALUES (
                CAST(:session_date AS date), :symbol, :underlying_symbol, :instrument_label, :direction, :mode, :strategy_status,
                :sector, :sector_index, :sector_rank, :stock_rank,
                :nifty_bias, :nifty_bias_pct, :nifty_open_5m, :nifty_close_5m,
                :stock_move_pct_at_entry,
                :setup_open_5m, :setup_high_5m, :setup_low_5m, :setup_close_5m, :setup_volume_5m,
                :instrument_key, :lot_size, :price_source, :period_label,
                CAST(:entry_time AS timestamptz), :entry_price, :anchor_price, :sl_price, :tp_price,
                :pre_exit_extreme,
                CAST(:exit_time AS timestamptz), :exit_price, :exit_trigger_type,
                :pnl_inr, :pnl_points, :notes
            )
            """
        ),
        row,
    )
    return True


def persist_trades(rows: List[Dict[str, Any]], *, mode: str = "backtest") -> Dict[str, int]:
    ensure_breakfast_strategy_table()
    db = SessionLocal()
    inserted = skipped = 0
    try:
        for r in rows:
            payload = dict(r)
            payload["mode"] = mode
            payload.setdefault("strategy_status", "shadow")
            payload.setdefault("price_source", "futures")
            payload.setdefault("period_label", None)
            if insert_trade(db, payload):
                inserted += 1
            else:
                skipped += 1
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return {"inserted": inserted, "skipped": skipped}


def fetch_trades(
    *,
    mode: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ensure_breakfast_strategy_table()
    db = SessionLocal()
    try:
        clauses = ["1=1"]
        params: Dict[str, Any] = {}
        if mode:
            clauses.append("mode = :mode")
            params["mode"] = mode
        if start_date:
            clauses.append("session_date >= CAST(:sd AS date)")
            params["sd"] = start_date
        if end_date:
            clauses.append("session_date <= CAST(:ed AS date)")
            params["ed"] = end_date
        where = " AND ".join(clauses)
        rows = db.execute(
            text(
                f"""
                SELECT *
                FROM breakfast_strategy_trades
                WHERE {where}
                ORDER BY session_date DESC, sector_rank, stock_rank, symbol
                """
            ),
            params,
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()
