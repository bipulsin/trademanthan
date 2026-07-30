#!/usr/bin/env python3
"""Log 30-Jul-2026 INOXWIND Aug-2026 futures SHORT trade to trade_log.

Journal/logging only — no live gate / FSM changes.

P&L: (74.60 − 74.90) × 6400 = −₹1,920 (−0.30 pts).
Planned risk (trader-stated initial EMA10 buffer): ₹1,152–1,600 → use lower
bound 0.18 pts / ₹1,152 for R; R ≈ −0.30/0.18 = −1.67R.
ema10_at_entry from rs_live_kavach_audit (11:05 bar, nearest); ema10_at_exit
not in audit around exit — left NULL (fill 74.90 was EMA10 confirmed close above).
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
        "symbol": "INOXWIND",
        "contract": "INOXWIND FUT Aug 2026 (INOXWIND 25 AUG 26)",
        "direction": "SHORT",
        "qty": 6400,
        "entry_time": "11:04:00",
        "entry_price": 74.60,
        "exit_time": "11:25:00",
        "exit_price": 74.90,
        "exit_price_intended": None,
        "slippage_pts": None,
        "slippage_inr": None,
        "points_captured": -0.30,
        # System audit nearest entry (bar_evaluated 11:05, computed ~11:08)
        "ema10_at_entry": 74.555,
        "ema10_at_exit": None,  # not recovered from sparse audit; see notes
        "ema5_at_entry": 74.506,
        "planned_risk_pts": 0.18,  # lower bound of stated ₹1,152–1,600 / 6400
        "planned_risk_inr": 1152.0,
        "r_realized": -1.67,
        "bars_held_10m": 2,
        "confidence_at_entry": "A",
        "trade_score_at_entry": 85.0,
        "confidence_at_exit": "D",
        "trade_score_at_exit": 45.0,
        "exit_trigger_type": "rule_compliant",
        "exit_trigger": (
            "EMA10 confirmed close above (pre-1R trail rule); "
            "state flipped SELL→BUY on exit candle"
        ),
        "notes": (
            "SHORT INOXWIND FUT Aug-2026. Fill 74.60→74.90 (−0.30pts × 6400 = −₹1,920). "
            "exit_trigger_type=rule_compliant (EMA10 confirmed close above, pre-1R trail). "
            "confidence/score entry A/85 (READY TO SHORT promotion); exit D/45 "
            "(State SELL→BUY on exit candle). "
            "ema10_at_entry=74.555 from rs_live_kavach_audit 11:05 bar (nearest); "
            "ema10_at_exit=NULL — sparse audit had no ~11:25 sample; exit fill 74.90 was "
            "EMA10 confirmed close above (implies EMA10 ≤ 74.90 at trigger). "
            "RISK_DRIFT: trader-observed entry-to-EMA10 buffer ~₹1,152–1,600 at fill; "
            "realized −₹1,920 — EMA10 drift between entry and confirmed-close exit. "
            "planned_risk uses lower bound ₹1,152 (0.18pt); R≈−1.67R. "
            "System |entry−ema10_audit|≈₹288 only — FLAG vs trader-implied stop distance "
            "(scoring/sync integrity). "
            "FLAG scoring_stability_review (8-Aug checkpoint): grade/score oscillated "
            "A/85 → D/43 → B/72 → D/45 across ~21m hold, independent of dashboard/chart "
            "mismatch. "
            "FLAG scoring_sync_integrity (8-Aug checkpoint, not one-off): pre-entry ~11:01 "
            "dashboard WATCHING while TV Kavach v3.0 READY TO SHORT; entry after dashboard "
            "promoted READY TO SHORT minutes later. Same-session related evidence: 360ONE FUT "
            "~11:08 dashboard READY NOW/TS85 vs TV NOT READY/D!/TS43."
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
                           ema5_at_entry, entry_to_ema10_buffer_pct,
                           planned_risk_pts, planned_risk_inr, r_realized,
                           confidence_at_entry, trade_score_at_entry,
                           confidence_at_exit, trade_score_at_exit,
                           bars_held_10m, exit_trigger, exit_trigger_type,
                           notes, source, created_at, updated_at
                    FROM trade_log
                    WHERE session_date = CAST(:d AS date)
                      AND symbol = 'INOXWIND'
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
        "schema_note": "Added ema10_at_exit (nullable) for stop-ref drift queries",
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
