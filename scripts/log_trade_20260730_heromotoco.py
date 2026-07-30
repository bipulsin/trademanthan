#!/usr/bin/env python3
"""Log 30-Jul-2026 HEROMOTOCO Aug-2026 futures LONG trade to trade_log.

Journal/logging only — no live gate / FSM changes.

P&L: (5346.30 − 5350) × 150 = −₹555 (−3.7 pts).
1R = 7.2 pts (entry−EMA10 at fill) → ema10_at_entry = 5342.8;
planned_risk_inr = 7.2 × 150 = ₹1,080; R ≈ −3.7/7.2 = −0.51R.
2R ≈ 5364.40; intraday high 5362.10 approached but did not confirm.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.rule27_trade_log import ensure_trade_log_table, upsert_trade

TRADES: List[Dict[str, Any]] = [
    {
        "session_date": "2026-07-30",
        "symbol": "HEROMOTOCO",
        "contract": "HEROMOTOCO FUT Aug 2026 (HEROMOTOCO 25 AUG 26)",
        "direction": "LONG",
        "qty": 150,
        "entry_time": "11:59:00",
        "entry_price": 5350.0,
        "exit_time": "12:15:00",
        "exit_price": 5346.30,
        "exit_price_intended": None,
        "slippage_pts": None,
        "slippage_inr": None,
        "points_captured": -3.7,
        # Trader-stated: 1R = 7.2 pts entry-to-EMA10 at fill
        "ema10_at_entry": 5342.8,
        # Audit 12:15 bar (computed ~12:20): ema10 ≈ 5348.40
        "ema10_at_exit": 5348.403,
        "planned_risk_pts": 7.2,
        "planned_risk_inr": 1080.0,
        "r_realized": -0.51,
        "bars_held_10m": 2,
        "confidence_at_entry": "A",
        "trade_score_at_entry": 85.0,
        "confidence_at_exit": "C",
        "trade_score_at_exit": 77.0,
        "entry_trigger_type": "pullback_entry",
        "pullback_number_at_entry": 1,
        "exit_trigger_type": "rule_compliant",
        "exit_trigger": (
            "EMA10 confirmed close below (pre-1R trail rule); "
            "2R not confirmed so EMA5 ratchet never activated"
        ),
        "notes": (
            "LONG HEROMOTOCO FUT Aug-2026. Fill 5350→5346.30 (−3.7pts × 150 = −₹555). "
            "exit_trigger_type=rule_compliant (EMA10 confirmed close below, pre-1R trail). "
            "Pullback #1 structure; chart-confirmed READY TO LONG. "
            "confidence/score entry A/85; exit C/77. "
            "1R=7.2pts → ema10_at_entry=5342.8 (entry-to-EMA10 at fill); "
            "planned_risk ₹1,080; R≈−0.51R. "
            "ema10_at_exit=5348.403 from rs_live_kavach_audit 12:15 bar. "
            "FLAG near_miss_2R_ratchet_rule25 (8-Aug checkpoint): intraday high 5362.10 "
            "touched close to but not confirmed through 2R≈5364.40; no closed candle above 2R "
            "→ EMA5 ratchet never activated; rolled over on wider EMA10 trail and gave back. "
            "Candidate pattern: approached-but-did-not-confirm-2R-then-reversed-to-loss — "
            "assess frequency and economic significance. "
            "FLAG scoring_stability_review (8-Aug): grade decay A/85 → B/77-ish → C/77 "
            "across ~16m hold — third same-session in-trade grade decay after 360ONE and "
            "INOXWIND. "
            "FLAG promotion_logic / lock_churn (8-Aug): dashboard mid-session flagged "
            "HEROMOTOCO among 4 elevated lock-churn symbols (HEROMOTOCO, KPITTECH, "
            "ADANIPORTS, 360ONE) with cycles>1; second READY NOW promotion (entry 5347.59) "
            "fired while this position was already open."
        ),
        "source": "manual_20260730",
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
                           points_captured, ema10_at_entry, ema10_at_exit,
                           entry_to_ema10_buffer_pct,
                           planned_risk_pts, planned_risk_inr, r_realized,
                           confidence_at_entry, trade_score_at_entry,
                           confidence_at_exit, trade_score_at_exit,
                           entry_trigger_type, pullback_number_at_entry,
                           bars_held_10m, exit_trigger, exit_trigger_type,
                           notes, source, created_at, updated_at
                    FROM trade_log
                    WHERE session_date = CAST(:d AS date)
                      AND symbol = 'HEROMOTOCO'
                    ORDER BY id
                    """
                ),
                {"d": "2026-07-30"},
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
