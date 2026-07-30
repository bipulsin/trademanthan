#!/usr/bin/env python3
"""Log 30-Jul-2026 PAGEIND trade + session wrap-up note.

Logging only — no FSM/gating/live scoring changes.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.rule27_session_log import (
    ensure_trade_session_log_table,
    minutes_remaining_in_entry_window,
    net_pnl_from_trade_log,
    upsert_session,
)
from backend.services.rule27_trade_log import ensure_trade_log_table, upsert_trade

TRADE: Dict[str, Any] = {
    "session_date": "2026-07-30",
    "symbol": "PAGEIND",
    "contract": "PAGEIND FUT Aug 2026 (PAGEIND 25 AUG 26)",
    "direction": "LONG",
    "qty": 20,
    "entry_time": "12:38:00",
    "entry_price": 41380.0,
    "exit_time": "13:05:00",
    "exit_price": 41260.0,
    "points_captured": -120.0,
    # Nearest chart audit around entry/exit windows.
    "ema10_at_entry": 41293.195,
    "ema10_at_exit": 41293.195,
    "confidence_at_entry": "A",
    "trade_score_at_entry": 85.0,
    "confidence_at_exit": "B",
    "trade_score_at_exit": 77.0,
    "entry_trigger_type": "pullback_entry",
    "pullback_number_at_entry": 0,
    "exit_trigger_type": "rule_compliant",
    "exit_trigger": (
        "Rule 15 two-candle validation failure: entry candle 41340–41490, "
        "price did not exceed 41490 in the next two validation candles; "
        "exit independent of EMA10 trail level"
    ),
    "notes": (
        "LONG PAGEIND FUT Aug-2026. Fill 41380→41260 (−120pts × 20 = −₹2,400). "
        "exit_trigger_type=rule_compliant per Rule 15 two-candle validation failure. "
        "Entry candle range 41340–41490; no validation-candle breakout above 41490. "
        "Entry confidence/score A/85 (READY TO LONG, pullback #0 fresh leg); exit B/77. "
        "FLAG slippage_review (8-Aug): decision point was in 41360–41390 zone but exit fill "
        "was 41260 (gap ~100–130 pts). Check if typical for PAGEIND liquidity profile or "
        "broader execution-timing issue on Rule 15 exits."
    ),
    "source": "manual_20260730",
}

SESSION_NOTE = (
    "Session case-study flag for 8-Aug checkpoint (read-only): 3 trades today all "
    "rule-compliant exits — INOXWIND −₹1,920, HEROMOTOCO −₹555, PAGEIND −₹2,400; "
    "net −₹4,875. Separate from trade P&L review, session surfaced scoring/promotion "
    "reliability concerns: 360ONE FUT dashboard READY NOW/TS85 vs TV NOT READY/D!/TS43; "
    "dashboard lock-churn cycles>1 on HEROMOTOCO, KPITTECH, ADANIPORTS, 360ONE; "
    "TVSMOTOR and PAGEIND showed dashboard READY NOW while chart still WATCHING with "
    "price already past suggested entry."
)


def main() -> None:
    ensure_trade_log_table()
    ensure_trade_session_log_table()
    db = SessionLocal()
    try:
        trade_id = upsert_trade(db, TRADE)
        pnl = net_pnl_from_trade_log(db, "2026-07-30")
        upsert_session(
            db,
            {
                "session_date": "2026-07-30",
                "trades_taken_count": 3,
                "last_exit_time": "13:05:00",
                "entry_window_remaining_at_last_exit": True,
                "entry_window_remaining_minutes": minutes_remaining_in_entry_window(
                    datetime.strptime("13:05:00", "%H:%M:%S").time()
                ),
                "session_end_reason": None,
                "net_pnl_at_session_end": pnl,
                "notes": SESSION_NOTE,
                "source": "manual_20260730",
            },
        )
        db.commit()

        trade_row = dict(
            db.execute(
                text(
                    """
                    SELECT id, session_date, symbol, contract, direction, qty,
                           entry_time, entry_price, exit_time, exit_price,
                           points_captured, ema10_at_entry, ema10_at_exit,
                           confidence_at_entry, trade_score_at_entry,
                           confidence_at_exit, trade_score_at_exit,
                           entry_trigger_type, pullback_number_at_entry,
                           exit_trigger, exit_trigger_type, notes, source,
                           created_at, updated_at
                    FROM trade_log
                    WHERE id = :id
                    """
                ),
                {"id": trade_id},
            ).mappings().first()
        )
        session_row = dict(
            db.execute(
                text("SELECT * FROM trade_session_log WHERE session_date = CAST(:d AS date)"),
                {"d": "2026-07-30"},
            ).mappings().first()
        )
    finally:
        db.close()

    trade_row["session_date"] = str(trade_row["session_date"])
    trade_row["entry_time"] = str(trade_row["entry_time"])
    trade_row["exit_time"] = str(trade_row["exit_time"]) if trade_row.get("exit_time") else None
    trade_row["created_at"] = str(trade_row["created_at"])
    trade_row["updated_at"] = str(trade_row["updated_at"])
    trade_row["pnl_inr"] = round(float(trade_row["points_captured"]) * int(trade_row["qty"]), 2)

    session_row["session_date"] = str(session_row["session_date"])
    if session_row.get("last_exit_time"):
        session_row["last_exit_time"] = str(session_row["last_exit_time"])
    session_row["created_at"] = str(session_row["created_at"])
    session_row["updated_at"] = str(session_row["updated_at"])

    print(
        json.dumps(
            {
                "trade_log": trade_row,
                "trade_session_log": session_row,
            },
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
