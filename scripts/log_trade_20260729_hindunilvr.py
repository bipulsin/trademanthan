#!/usr/bin/env python3
"""Log 29-Jul-2026 HINDUNILVR Aug-2026 futures trade to trade_log (Rule 27).

Journal/logging only — no live gate / FSM changes.
Uses existing exit_price_intended + slippage_pts; slippage_inr is new.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.rule27_trade_log import ensure_trade_log_table, upsert_trade

# NSE underlying HINDUNILVR; contract Aug 2026 FUT.
# Risk: 2105 − 2099.4 = 5.6 pts × 300 = ₹1,680; R = 13.4/5.6 ≈ 2.39
TRADES: List[Dict[str, Any]] = [
    {
        "session_date": "2026-07-29",
        "symbol": "HINDUNILVR",
        "contract": "HUL FUT Aug 2026 (HINDUNILVR 25 AUG 26)",
        "direction": "LONG",
        "qty": 300,
        "entry_time": "11:00:00",
        "entry_price": 2105.0,
        "exit_time": "11:25:00",
        "exit_price": 2118.4,
        "exit_price_intended": 2120.0,
        "slippage_pts": 1.6,
        "slippage_inr": 480.0,
        "points_captured": 13.4,
        "ema10_at_entry": 2099.4,
        "planned_risk_pts": 5.6,
        "planned_risk_inr": 1680.0,
        "r_realized": 2.39,
        "bars_held_10m": 3,
        "exit_trigger": (
            "Discretionary exit — order placed at 2120; fill 2118.4 "
            "(1.6 pt adverse slippage)"
        ),
        "exit_trigger_type": "discretionary",
        "notes": (
            "Stop reference at entry: EMA10 = 2099.4 (initial risk 5.6 pts / ₹1,680). "
            "Gross P&L ₹4,020 (+13.4pts × 300). R ≈ +2.39R. "
            "Execution quality: intended_exit=2120, fill=2118.4, "
            "slippage_pts=1.6, slippage_inr=480. "
            "exit_trigger_type=discretionary — rule-discipline separate from slippage."
        ),
        "source": "manual_20260729",
    },
]


def main() -> None:
    ensure_trade_log_table()
    db = SessionLocal()
    ids = []
    try:
        for t in TRADES:
            rid = upsert_trade(db, t)
            ids.append({"id": rid, "symbol": t["symbol"]})
        db.commit()
        rows = [
            dict(r)
            for r in db.execute(
                text(
                    """
                    SELECT id, session_date, symbol, contract, direction, qty,
                           entry_time, entry_price, exit_time, exit_price,
                           exit_price_intended, slippage_pts, slippage_inr,
                           points_captured, ema10_at_entry, entry_to_ema10_buffer_pct,
                           planned_risk_pts, planned_risk_inr, r_realized,
                           bars_held_10m, exit_trigger, exit_trigger_type,
                           garuda_confluence, garuda_rank, garuda_direction,
                           notes, source, created_at, updated_at
                    FROM trade_log
                    WHERE session_date = CAST(:d AS date)
                      AND symbol = 'HINDUNILVR'
                    ORDER BY id
                    """
                ),
                {"d": "2026-07-29"},
            ).mappings()
        ]
    finally:
        db.close()
    out = {
        "table": "trade_log",
        "schema_mapping": {
            "intended_exit_price": "exit_price_intended (pre-existing)",
            "slippage_points": "slippage_pts (pre-existing)",
            "slippage_inr": "slippage_inr (NEW)",
        },
        "upserted_ids": ids,
        "rows": [
            {
                **r,
                "session_date": str(r["session_date"]),
                "entry_time": str(r["entry_time"]),
                "exit_time": str(r["exit_time"]),
                "created_at": str(r["created_at"]),
                "updated_at": str(r["updated_at"]),
                "pnl_inr": round(float(r["points_captured"]) * int(r["qty"]), 2)
                if r.get("points_captured") is not None and r.get("qty")
                else None,
            }
            for r in rows
        ],
    }
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
