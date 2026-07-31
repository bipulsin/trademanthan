#!/usr/bin/env python3
"""Log 31-Jul-2026 TVSMOTOR Aug-2026 LONG + peak/giveback journal fields.

Journal/logging only — no live gate / FSM changes.

Fill: 4296 → 4311.70 (+15.7 pts × 175 = ₹2,747.50).
1R = 9.6 pts (entry−EMA10) → planned_risk_inr = ₹1,680; R ≈ 1.63R.
Peak unrealized ₹4,672.50 (~2.78R) ~12:05; peak→exit giveback ~₹1,925 (~1.15R).
"""
from __future__ import annotations

import json
from typing import Any, Dict

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.rule27_trade_log import ensure_trade_log_table, upsert_trade

TRADE: Dict[str, Any] = {
    "session_date": "2026-07-31",
    "symbol": "TVSMOTOR",
    "contract": "TVSMOTOR FUT Aug 2026 (TVSMOTOR 25 AUG 26)",
    "direction": "LONG",
    "qty": 175,
    # Candle 11:35 IST aligns with fill ~4296; entry candle high (Dhan) 4312.1.
    "entry_time": "11:35:00",
    "entry_price": 4296.0,
    # Exit fill 4311.70 matches 12:55 IST 5m candle open (O 4311.7).
    "exit_time": "12:55:00",
    "exit_price": 4311.70,
    "points_captured": 15.7,
    # 1R = 9.6 pts entry-to-EMA10 → ema10_at_entry = 4296 − 9.6
    "ema10_at_entry": 4286.4,
    "planned_risk_pts": 9.6,
    "planned_risk_inr": 1680.0,
    "r_realized": 1.63,
    "mfe_r": 2.78,
    "bars_held_10m": 8,
    "confidence_at_entry": "A",
    "peak_unrealized_pnl": 4672.50,
    "peak_to_exit_giveback_r": 1.15,
    "entry_trigger_type": "pullback_entry",
    "exit_trigger_type": "rule_compliant",
    "exit_trigger": (
        "Rule 25 (EMA5 confirmed close post-2R ratchet); "
        "2R=4315.2 touched intrabar high 4316.8 ~11:40–11:45 IST "
        "(spike-and-reverse; closed back below) → Rule 25 activated"
    ),
    "notes": (
        "LONG TVSMOTOR FUT Aug-2026. Fill 4296→4311.70 (+15.7pts × 175 = ₹2,747.50). "
        "exit_trigger_type=rule_compliant — Rule 25 EMA5 confirmed close after 2R ratchet. "
        "Entry ~11:35 IST (candle high Dhan ref 4312.1); exit ~12:55 IST (5m open 4311.7). "
        "1R=9.6pts → ema10_at_entry=4286.4; planned_risk ₹1,680; R≈1.63R. "
        "Peak unrealized ₹4,672.50 (~2.78R) observed ~12:05 IST; "
        "peak_to_exit_giveback ≈₹1,925 (~1.15R). "
        "2R touch: yes — intrabar H 4316.8 vs 2R 4315.2 around 11:40–11:45 candle, "
        "closed back below (spike-and-reverse) then Rule 25 armed. "
        "Two consolidation pullbacks (~12:15, ~12:25) tested EMA5 within ~0.4–1.5 pts "
        "without exit; final breakdown after sustained EMA5 breach. "
        "Research flags: EMA5 trail giveback pattern (with ADANIGREEN / POLICYBZR / "
        "FEDERALBANK profit-protection shadow thread)."
    ),
    "source": "manual_20260731",
}


def main() -> None:
    ensure_trade_log_table()
    db = SessionLocal()
    try:
        trade_id = upsert_trade(db, TRADE)
        db.commit()
        row = dict(
            db.execute(
                text(
                    """
                    SELECT id, session_date, symbol, contract, direction, qty,
                           entry_time, entry_price, exit_time, exit_price,
                           points_captured, ema10_at_entry, planned_risk_pts,
                           planned_risk_inr, r_realized, mfe_r, bars_held_10m,
                           confidence_at_entry, peak_unrealized_pnl,
                           peak_to_exit_giveback_r, exit_trigger, exit_trigger_type,
                           notes, source, created_at, updated_at
                    FROM trade_log
                    WHERE id = :id
                    """
                ),
                {"id": trade_id},
            ).mappings().first()
        )
    finally:
        db.close()

    row["session_date"] = str(row["session_date"])
    row["entry_time"] = str(row["entry_time"])
    row["exit_time"] = str(row["exit_time"]) if row.get("exit_time") else None
    row["created_at"] = str(row["created_at"])
    row["updated_at"] = str(row["updated_at"])
    row["pnl_inr"] = round(float(row["points_captured"]) * int(row["qty"]), 2)
    print(json.dumps({"trade_log": row}, indent=2, default=str))


if __name__ == "__main__":
    main()
