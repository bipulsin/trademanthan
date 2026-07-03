"""CSV export helpers for RS conviction board logs."""
from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal

IST = pytz.timezone("Asia/Kolkata")


def _today() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _csv(rows: list, headers: list) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    for row in rows:
        w.writerow(row)
    return buf.getvalue()


def export_scoring_csv(session_date: Optional[str] = None) -> str:
    sd = session_date or _today()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT cycle_time, symbol, side, rs_component, opening_anchor,
                       persistence_credit, slope_component, accum_component,
                       whip_penalty, conviction_score, in_raw_top5
                FROM rs_conviction_scoring_log
                WHERE session_date = :d
                ORDER BY cycle_time, side, conviction_score DESC
                """
            ),
            {"d": sd},
        ).fetchall()
    finally:
        db.close()
    return _csv(
        [
            [
                r.cycle_time.isoformat() if r.cycle_time else "",
                r.symbol, r.side, r.rs_component, r.opening_anchor,
                r.persistence_credit, r.slope_component, r.accum_component,
                r.whip_penalty, r.conviction_score, r.in_raw_top5,
            ]
            for r in rows
        ],
        [
            "cycle_time", "symbol", "side", "rs_component", "opening_anchor",
            "persistence_credit", "slope_component", "accum_component",
            "whip_penalty", "conviction_score", "in_raw_top5",
        ],
    )


def export_promotions_csv(session_date: Optional[str] = None) -> str:
    sd = session_date or _today()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT event_time, side, event_type, symbol, replaced_symbol, detail_json
                FROM rs_conviction_promotion_log
                WHERE session_date = :d
                ORDER BY event_time
                """
            ),
            {"d": sd},
        ).fetchall()
    finally:
        db.close()
    return _csv(
        [
            [
                r.event_time.isoformat() if r.event_time else "",
                r.side, r.event_type, r.symbol, r.replaced_symbol or "",
                r.detail_json or "",
            ]
            for r in rows
        ],
        ["event_time", "side", "event_type", "symbol", "replaced_symbol", "detail_json"],
    )


def export_radar_log_csv(session_date: Optional[str] = None) -> str:
    sd = session_date or _today()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT event_time, symbol, side, state_from, state_to,
                       gap_atr, sl_pct, whipsaw_count
                FROM rs_setup_radar_log
                WHERE session_date = :d
                ORDER BY event_time
                """
            ),
            {"d": sd},
        ).fetchall()
    finally:
        db.close()
    return _csv(
        [
            [
                r.event_time.isoformat() if r.event_time else "",
                r.symbol, r.side, r.state_from or "", r.state_to or "",
                r.gap_atr, r.sl_pct, r.whipsaw_count,
            ]
            for r in rows
        ],
        ["event_time", "symbol", "side", "state_from", "state_to", "gap_atr", "sl_pct", "whipsaw_count"],
    )
