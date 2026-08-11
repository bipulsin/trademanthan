#!/usr/bin/env python3
"""Log 11-Aug-2026 EICHERMOT + SRF Aug-FUT round-trips to trade_log (Rule 27).

Journal/logging only — no live gate / FSM / scoring changes.
Excludes unfilled WIPRO and unfilled SRF Limit@2572 (0/200).

Fill math (Dhan order book confirmed):
  EICHERMOT lot=100 → (8055−8095)×100 = −₹4,000 (−40 pts)
  SRF         lot=200 → (2570.50−2569.00)×200 = +₹300 (+1.50 pts)

Context: RS universe + Garuda confluence auto-filled at upsert when DB available.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.rule27_trade_log import (
    compute_peak_giveback_metrics,
    ensure_trade_log_table,
    upsert_trade,
)

SESSION_DATE = "2026-08-11"

TRADES: List[Dict[str, Any]] = [
    {
        "session_date": SESSION_DATE,
        "symbol": "EICHERMOT",
        "contract": "EICHERMOT FUT Aug 2026 (EICHERMOT 25 AUG 26)",
        "direction": "LONG",
        "qty": 100,
        "entry_time": "09:52:13",
        "entry_price": 8095.0,
        "exit_time": "10:25:15",
        "exit_price": 8055.0,
        "exit_price_intended": None,
        "slippage_pts": None,
        "points_captured": -40.0,
        # Entry-time 1R ≈ entry−EMA10 → planned_risk ~₹2,400 (24 pts × 100)
        "ema10_at_entry": 8071.0,
        "planned_risk_pts": 24.0,
        "planned_risk_inr": 2400.0,
        "r_realized": round(-4000.0 / 2400.0, 2),
        "bars_held_10m": 3,
        "confidence_at_entry": "A+",
        "entry_trigger_type": "ignition_leg",
        "pullback_number_at_entry": 0,
        "exit_trigger_type": "rule_compliant",
        "exit_trigger": (
            "Rule 15 (entry high 8105 not breached in 2-candle window) + "
            "EMA10 pre-1R close breach + VWAP invalidation "
            "(stacked triggers, same confirmed close)"
        ),
        "notes": (
            "LONG EICHERMOT Aug-FUT. Fill 8095→8055 (−40pts × 100 = −₹4,000). "
            "exit_order_type=Market. exit_trigger_type=rule_compliant — "
            "stacked exit signal (Rule 15 fail + EMA10 + VWAP) confirmed at 8055 close. "
            "Ignition-leg entry; pullback_number_at_entry=0 (leg 0). "
            "Entry candle high ref 8105 (Rule 15 2-candle window). "
            "Realized loss (₹4,000) exceeded computed entry-time risk (~₹2,400) — "
            "flag for Rule 15 retroactive-validation dataset; gap between planned vs "
            "realized risk."
        ),
        "source": "manual_20260811",
    },
    {
        "session_date": SESSION_DATE,
        "symbol": "SRF",
        "contract": "SRF FUT Aug 2026 (SRF 25 AUG 26)",
        "direction": "SHORT",
        "qty": 200,
        "entry_time": "11:52:10",
        "entry_price": 2570.50,
        "exit_time": "12:45:30",
        "exit_price": 2569.0,
        "exit_price_intended": None,
        "slippage_pts": None,
        "points_captured": 1.50,
        "bars_held_10m": 5,
        "confidence_at_entry": "A",
        "entry_trigger_type": "ignition_leg",
        "pullback_number_at_entry": 0,
        "exit_trigger_type": "rule_compliant",
        "exit_trigger": "Rule 25/17 (EMA5 post-1R ratchet close breach)",
        "peak_unrealized_pnl": 1200.0,
        "notes": (
            "SHORT SRF Aug-FUT. Fill 2570.50→2569.00 (+1.50pts × 200 = +₹300). "
            "exit_order_type=Market. exit_trigger_type=rule_compliant — "
            "Rule 25/17 EMA5 post-1R ratchet close breach. "
            "Ignition-leg entry; pullback_number_at_entry=0 (leg 0). "
            "Reached >1R before stalling. "
            "peak_unrealized_pnl≈₹1,200 (live tracking; confirm vs tick/OHLC backfill). "
            "EMA5 ratchet gave back majority of peak unrealized profit before triggering "
            "exit — data point for profit-protection thread "
            "(ADANIGREEN/POLICYBZR/FEDERALBANK/TATAELXSI pattern). "
            "Separate unfilled SRF Limit@2572 (0/200) not logged."
        ),
        "source": "manual_20260811",
    },
]


def _nearest_rs_row(
    db,
    *,
    symbol: str,
    session_date: str,
    before_time: str,
) -> Optional[Dict[str, Any]]:
    """Nearest pre-event RS universe scan (read-only)."""
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
    """Fill EMA/score context from RS when caller did not supply (read-only)."""
    out = dict(trade)
    sym = str(out["symbol"])
    et = str(out["entry_time"])
    xt = out.get("exit_time")
    entry_rs = _nearest_rs_row(db, symbol=sym, session_date=SESSION_DATE, before_time=et)
    if entry_rs:
        if out.get("ema10_at_entry") is None and entry_rs.get("ema10") is not None:
            out["ema10_at_entry"] = float(entry_rs["ema10"])
        if out.get("ema5_at_entry") is None and entry_rs.get("ema5") is not None:
            out["ema5_at_entry"] = float(entry_rs["ema5"])
        if out.get("vwap_at_entry") is None and entry_rs.get("vwap") is not None:
            out["vwap_at_entry"] = float(entry_rs["vwap"])
        if out.get("trade_score_at_entry") is None and entry_rs.get("trade_score") is not None:
            out["trade_score_at_entry"] = float(entry_rs["trade_score"])
        if out.get("adx_at_entry") is None and entry_rs.get("adx") is not None:
            out["adx_at_entry"] = float(entry_rs["adx"])
    if xt:
        exit_rs = _nearest_rs_row(db, symbol=sym, session_date=SESSION_DATE, before_time=str(xt))
        if exit_rs and out.get("ema10_at_exit") is None and exit_rs.get("ema10") is not None:
            out["ema10_at_exit"] = float(exit_rs["ema10"])
        if exit_rs and out.get("confidence_at_exit") is None and exit_rs.get("confidence_grade"):
            out["confidence_at_exit"] = str(exit_rs["confidence_grade"])
        if exit_rs and out.get("trade_score_at_exit") is None and exit_rs.get("trade_score") is not None:
            out["trade_score_at_exit"] = float(exit_rs["trade_score"])

    direction = str(out.get("direction") or "").upper()
    entry = float(out["entry_price"])
    qty = int(out["qty"])
    if out.get("planned_risk_pts") is None and out.get("ema10_at_entry") is not None:
        e10 = float(out["ema10_at_entry"])
        risk_pts = abs(entry - e10)
        out["planned_risk_pts"] = round(risk_pts, 4)
        out["planned_risk_inr"] = round(risk_pts * qty, 2)
    if out.get("r_realized") is None and out.get("planned_risk_inr") and out.get("points_captured") is not None:
        risk = float(out["planned_risk_inr"])
        pts = float(out["points_captured"])
        out["r_realized"] = round((pts * qty) / risk, 2) if risk > 0 else None

    peak_pnl = out.get("peak_unrealized_pnl")
    if (
        peak_pnl is not None
        and out.get("planned_risk_inr")
        and out.get("peak_to_exit_giveback_r") is None
    ):
        risk_inr = float(out["planned_risk_inr"])
        exit_px = float(out["exit_price"])
        peak_pts = float(peak_pnl) / qty
        if direction == "SHORT":
            peak_price = entry - peak_pts
        else:
            peak_price = entry + peak_pts
        metrics = compute_peak_giveback_metrics(
            direction=direction,
            entry_price=entry,
            exit_price=exit_px,
            qty=qty,
            planned_risk_inr=risk_inr,
            peak_favorable_price=peak_price,
        )
        out["peak_to_exit_giveback_r"] = metrics.get("peak_to_exit_giveback_r")
        if out.get("mfe_r") is None:
            out["mfe_r"] = metrics.get("mfe_r")
    return out


def main() -> None:
    ensure_trade_log_table()
    db = SessionLocal()
    ids: List[Dict[str, Any]] = []
    try:
        for raw in TRADES:
            trade = enrich_from_rs(db, raw)
            rid = upsert_trade(db, trade)
            ids.append({"id": rid, "symbol": trade["symbol"]})
        db.commit()
        rows = [
            dict(r)
            for r in db.execute(
                text(
                    """
                    SELECT id, session_date, symbol, contract, direction, qty,
                           entry_time, entry_price, exit_time, exit_price,
                           points_captured, ema10_at_entry, ema10_at_exit,
                           ema5_at_entry, vwap_at_entry, entry_to_ema10_buffer_pct,
                           planned_risk_pts, planned_risk_inr, r_realized,
                           confidence_at_entry, trade_score_at_entry, adx_at_entry,
                           confidence_at_exit, trade_score_at_exit,
                           entry_trigger_type, pullback_number_at_entry,
                           bars_held_10m, peak_unrealized_pnl, peak_to_exit_giveback_r,
                           mfe_r, exit_trigger, exit_trigger_type,
                           garuda_confluence, garuda_rank, garuda_direction,
                           notes, source, created_at, updated_at
                    FROM trade_log
                    WHERE session_date = CAST(:d AS date)
                      AND symbol IN ('EICHERMOT', 'SRF')
                    ORDER BY entry_time, id
                    """
                ),
                {"d": SESSION_DATE},
            ).mappings()
        ]
    finally:
        db.close()

    out = {
        "table": "trade_log",
        "schema_service": "backend.services.rule27_trade_log",
        "unique_key": "(session_date, symbol, direction, entry_time)",
        "confirmations": {
            "eichermot_qty_lot": 100,
            "eichermot_pnl_inr": -4000.0,
            "srf_qty_lot": 200,
            "srf_pnl_inr": 300.0,
            "excluded_unfilled": ["WIPRO", "SRF Limit@2572 (0/200)"],
        },
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
