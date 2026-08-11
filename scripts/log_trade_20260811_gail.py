#!/usr/bin/env python3
"""Log 11-Aug-2026 GAIL Aug-FUT LONG round-trip to trade_log (Rule 27).

Journal/logging only — no live gate / FSM / scoring changes.

Fill math:
  qty=3550 → (174.75−175.08)×3550 = −₹1,171.50 (−0.33 pts)
  planned_risk_inr = ₹1,142 (dashboard) → R ≈ −1.03
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.rule27_trade_log import ensure_trade_log_table, upsert_trade

SESSION_DATE = "2026-08-11"

TRADE: Dict[str, Any] = {
    "session_date": SESSION_DATE,
    "symbol": "GAIL",
    "contract": "GAIL FUT Aug 2026 (GAILQ2026 / GAIL 25 AUG 26)",
    "direction": "LONG",
    "qty": 3550,
    "entry_time": "13:37:00",
    "entry_price": 175.08,
    "exit_time": "14:05:00",
    "exit_price": 174.75,
    "exit_price_intended": 174.88,
    "slippage_pts": 0.13,  # punched 174.88 → filled 174.75
    "slippage_inr": 461.5,  # 0.13 × 3550
    "points_captured": -0.33,
    "planned_risk_pts": round(1142.0 / 3550.0, 4),  # ≈0.3217
    "planned_risk_inr": 1142.0,
    "r_realized": round(-1171.5 / 1142.0, 4),  # ≈−1.0258
    "bars_held_10m": 3,
    "confidence_at_entry": "A",
    "trade_score_at_entry": 85.0,
    "adx_at_entry": 47.69,
    "entry_trigger_type": "pullback_entry",
    "pullback_number_at_entry": 1,
    "exit_trigger_type": "rule_compliant",
    "exit_trigger": (
        "Rule 15 (two-candle high-breach fail, long-side) — "
        "entry candle high 175.35 not breached; subsequent highs 175.26, 174.99"
    ),
    "notes": (
        "LONG GAIL Aug-FUT (GAILQ2026). "
        "Fill 175.08→174.75 (−0.33pts × 3550 = −₹1,171.50 before brokerage). "
        "entry_time≈13:37 IST: limit placed 175.00, one deliberate re-price to 175.10, "
        "filled 175.08 (no chasing). "
        "exit_time≈14:05 IST: punched 174.88, executed 174.75 (0.13pt / ₹461.50 slippage). "
        "confidence_at_entry=A (upgraded from B shortly before fill); trade_score=85; "
        "ADX=47.69; volume High (1.21x); RSI(14)=79.41 extended/overbought. "
        "entry_trigger_type=pullback_entry; pullback_number_at_entry=1 (confirmed pullback, "
        "not ignition-leg). "
        "exit_trigger_type=rule_compliant — Rule 15 two-candle high-breach fail: "
        "entry high 175.35 not reclaimed (candle highs 175.26 then 174.99). "
        "planned_risk ₹1,142 → R≈−1.03. "
        "RESEARCH FLAG: high RSI at entry (79.41) — sequential lower highs post-entry "
        "(175.35 → 175.26 → 174.99), no continuation — candidate for RSI-at-entry vs "
        "Rule 15 outcomes pattern research."
    ),
    "source": "manual_20260811",
}


def _nearest_rs_row(
    db,
    *,
    symbol: str,
    session_date: str,
    before_time: str,
) -> Optional[Dict[str, Any]]:
    row = db.execute(
        text(
            """
            SELECT scan_time, ema10, ema5, vwap, confidence_grade, trade_score, adx,
                   current_price
            FROM rs_universe_score_snapshot
            WHERE session_date = CAST(:d AS date)
              AND UPPER(symbol) = UPPER(:sym)
              AND scan_time AT TIME ZONE 'Asia/Kolkata'
                  <= CAST(:d AS date) + CAST(:t AS time)
            ORDER BY scan_time DESC
            LIMIT 1
            """
        ),
        {"d": session_date, "sym": symbol, "t": before_time},
    ).mappings().first()
    return dict(row) if row else None


def enrich_from_rs(db, trade: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(trade)
    entry_rs = _nearest_rs_row(
        db, symbol=out["symbol"], session_date=SESSION_DATE, before_time=str(out["entry_time"])
    )
    if entry_rs:
        if out.get("ema10_at_entry") is None and entry_rs.get("ema10") is not None:
            out["ema10_at_entry"] = float(entry_rs["ema10"])
        if out.get("ema5_at_entry") is None and entry_rs.get("ema5") is not None:
            out["ema5_at_entry"] = float(entry_rs["ema5"])
        if out.get("vwap_at_entry") is None and entry_rs.get("vwap") is not None:
            out["vwap_at_entry"] = float(entry_rs["vwap"])
    xt = out.get("exit_time")
    if xt:
        exit_rs = _nearest_rs_row(
            db, symbol=out["symbol"], session_date=SESSION_DATE, before_time=str(xt)
        )
        if exit_rs and out.get("ema10_at_exit") is None and exit_rs.get("ema10") is not None:
            out["ema10_at_exit"] = float(exit_rs["ema10"])
        if exit_rs and out.get("confidence_at_exit") is None and exit_rs.get("confidence_grade"):
            out["confidence_at_exit"] = str(exit_rs["confidence_grade"])
        if exit_rs and out.get("trade_score_at_exit") is None and exit_rs.get("trade_score") is not None:
            out["trade_score_at_exit"] = float(exit_rs["trade_score"])
    return out


def main() -> None:
    ensure_trade_log_table()
    db = SessionLocal()
    try:
        trade = enrich_from_rs(db, TRADE)
        trade_id = upsert_trade(db, trade)
        db.commit()
        row = dict(
            db.execute(
                text(
                    """
                    SELECT id, session_date, symbol, contract, direction, qty,
                           entry_time, entry_price, exit_time, exit_price,
                           exit_price_intended, slippage_pts, slippage_inr,
                           points_captured, ema10_at_entry, ema10_at_exit,
                           ema5_at_entry, vwap_at_entry, entry_to_ema10_buffer_pct,
                           planned_risk_pts, planned_risk_inr, r_realized,
                           confidence_at_entry, trade_score_at_entry, adx_at_entry,
                           confidence_at_exit, trade_score_at_exit,
                           entry_trigger_type, pullback_number_at_entry,
                           bars_held_10m, exit_trigger, exit_trigger_type,
                           garuda_confluence, garuda_rank, garuda_direction,
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

    out = {
        "table": "trade_log",
        "upserted_id": trade_id,
        "pnl_inr": round(float(row["points_captured"]) * int(row["qty"]), 2),
        "row": {
            **row,
            "session_date": str(row["session_date"]),
            "entry_time": str(row["entry_time"]),
            "exit_time": str(row["exit_time"]) if row.get("exit_time") else None,
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        },
    }
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
