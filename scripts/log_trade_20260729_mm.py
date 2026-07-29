#!/usr/bin/env python3
"""Log 29-Jul-2026 M&M Aug-2026 futures SHORT trade to trade_log (Rule 27).

Journal/logging only — no live gate / FSM changes.
Risk: 3244.5 − 3240.7 = 3.8 pts × 200 = ₹760; R = 15/3.8 ≈ 3.95
Pullback #2 entry on EMA5. Discretionary exit at market (no pre-set limit).
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.rule27_trade_log import ensure_trade_log_table, upsert_trade

TRADES: List[Dict[str, Any]] = [
    {
        "session_date": "2026-07-29",
        "symbol": "M&M",
        "contract": "M&M FUT Aug 2026 (M&M 25 AUG 26)",
        "direction": "SHORT",
        "qty": 200,
        "entry_time": "13:12:00",
        "entry_price": 3240.7,
        "exit_price": 3225.70,
        # Discretionary market close — no pre-set limit, so intended = null
        "exit_price_intended": None,
        # Fill 3240.7 vs intended limit 3240.5 → 0.2 pt adverse, negligible
        "slippage_pts": 0.2,
        "slippage_inr": 40.0,   # 0.2 × 200
        "points_captured": 15.0,
        "ema10_at_entry": 3244.5,
        "planned_risk_pts": 3.8,   # 3244.5 − 3240.7
        "planned_risk_inr": 760.0,
        "r_realized": 3.95,
        "confidence_at_entry": "B",
        "exit_trigger_type": "discretionary",
        "entry_trigger_type": "pullback_entry",
        "pullback_number_at_entry": 2,
        "exit_trigger": "Discretionary close at market",
        "notes": (
            "SHORT M&M FUT Aug-2026. Stop ref at entry: EMA10 = 3244.5 "
            "(initial risk 3.8 pts / ₹760). Gross P&L ₹3,000 (+15pts × 200). "
            "R ≈ +3.95R. Pullback #2 entry (EMA5 standard pullback). "
            "confidence_at_entry=B. Discretionary exit at market — no pre-set limit. "
            "Entry slippage: intended 3240.5, fill 3240.7 (0.2 pt, ₹40, negligible)."
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
                           confidence_at_entry, bars_held_10m,
                           exit_trigger, exit_trigger_type,
                           entry_trigger_type, pullback_number_at_entry,
                           garuda_confluence, garuda_rank, garuda_direction,
                           notes, source, created_at, updated_at
                    FROM trade_log
                    WHERE session_date = CAST(:d AS date)
                      AND symbol = 'M&M'
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
        "upserted_ids": ids,
        "rows": [
            {
                **r,
                "session_date": str(r["session_date"]),
                "entry_time": str(r["entry_time"]),
                "exit_time": str(r["exit_time"]) if r.get("exit_time") else None,
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
