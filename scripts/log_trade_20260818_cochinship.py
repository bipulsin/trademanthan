#!/usr/bin/env python3
"""Log 18-Aug-2026 COCHINSHIP Aug-FUT LONG round-trip to trade_log (Rule 27).

Journal/logging only — no live gate / FSM / scoring changes.

Fill math:
  qty=400 → (1496.00 − 1488.50) × 400 = +₹3,000.00 (+7.50 pts)
  planned_risk_pts = entry_candle_high − entry = 1492.50 − 1488.50 = 4.00 pts
  planned_risk_inr = 4.00 × 400 = ₹1,400
  r_realized = 3000 / 1400 ≈ +2.14R  ✓
"""
from __future__ import annotations

import json
from typing import Any, Dict

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.rule27_trade_log import ensure_trade_log_table, upsert_trade

SESSION_DATE = "2026-08-18"

TRADE: Dict[str, Any] = {
    "session_date": SESSION_DATE,
    "symbol": "COCHINSHIP",
    "contract": "COCHINSHIP FUT Aug 2026",
    "direction": "LONG",
    "qty": 400,
    "entry_time": "10:20:40",
    "entry_price": 1488.50,
    "exit_time": "11:08:06",
    "exit_price": 1496.00,
    "points_captured": 7.50,
    # Risk defined to entry candle high (1492.50 − 1488.50 = 4.00 pts)
    "planned_risk_pts": 4.00,
    "planned_risk_inr": 1400.0,
    "r_realized": round(3000.0 / 1400.0, 4),   # 2.1429 ≈ +2.14R
    "bars_held_10m": 5,
    # Grades
    "confidence_at_entry": "A",
    "entry_grade": "A",          # Rule 15 passed
    # Entry context
    "entry_trigger_type": "rule15_pullback",
    "pullback_number_at_entry": 1,
    # Exit context
    "exit_trigger_type": "resistance_zone_breakout_extension",
    "exit_trigger": (
        "Resistance zone breakout extension — price pushed to 1496 on final candle "
        "after re-ignition above VWAP/EMA10 support at 1486.51."
    ),
    "notes": (
        "LONG COCHINSHIP Aug-FUT. "
        "Entry 1488.50 @ 10:20:40 IST (10-min, Rule 15 pullback to EMA5). "
        "Entry_candle_high: 1492.50. "
        "Entry_grade: A (Rule 15 passed). "
        "Confidence_grade: A (after breakout candle 2 confirmed). "
        "Exit 1496.00 @ 11:08:06 IST. P&L: +₹3,000 (+7.50pts × 400 lots). "
        "R_multiple: +2.14R. Duration: 5 candles (~17 min). "
        "Structural notes — "
        "Resistance_confluence_at_entry: YES (1492–1500 supply zone). "
        "Initial_reaction: Rejection (1494 high → 1489.90). "
        "Support_hold: YES (VWAP/EMA10 at 1486.51, held on candle 5). "
        "Re_ignition: YES (pushed to 1496 on final candle). "
        "Pattern_type: retest_and_continuation (NOT whipsaw). "
        "Research_tag: rule15_resistance_confluence_retest_continuation. "
        "Outcome: PROFITABLE | Pattern_valid_with_support_hold."
    ),
    "source": "manual_enriched",
}


def main() -> None:
    ensure_trade_log_table()
    db = SessionLocal()
    try:
        trade_id = upsert_trade(db, TRADE)
        db.commit()
        row = db.execute(
            text(
                "SELECT id, session_date, symbol, direction, entry_time, entry_price, "
                "exit_time, exit_price, points_captured, r_realized, bars_held_10m, "
                "entry_trigger_type, exit_trigger_type, notes "
                "FROM trade_log WHERE id = :id"
            ),
            {"id": trade_id},
        ).mappings().one()
        print(json.dumps({k: str(v) for k, v in row.items()}, indent=2, ensure_ascii=False))
        print(f"\n✅  Upserted trade_log row id={trade_id} for {SESSION_DATE} COCHINSHIP LONG")
    finally:
        db.close()


if __name__ == "__main__":
    main()
