"""
Read-only sentiment map for backtests.

Uses the same COALESCE expression as the live picker (no changes to fin_sentiment jobs).
Annotates how many rows had ``current_run_at`` on the session date in IST (informational only).
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Dict, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

IST = pytz.timezone("Asia/Kolkata")


def load_sentiment_map_for_session_date(db: Session, session_date: date) -> Tuple[Dict[str, float], str, int]:
    """
    Returns (sentiment_map, source_note, run_at_match_count).

    ``run_at_match_count`` counts stocks where current_run_at (in IST) falls on session_date.
    Values still use the same COALESCE as production picker when a row exists.
    """
    try:
        rows = db.execute(
            text(
                """
                SELECT stock,
                       COALESCE(current_combined_sentiment, combined_sentiment_avg,
                                last_combined_sentiment, api_sentiment_avg, 0.0) AS sc,
                       current_run_at
                FROM stock_fin_sentiment
                """
            )
        ).fetchall()
    except Exception:
        return {}, "sentiment_table_unavailable", 0

    out: Dict[str, float] = {}
    match_count = 0
    for row in rows:
        stock = row[0]
        sc = row[1]
        cra = row[2]
        if not stock:
            continue
        try:
            out[str(stock).strip().upper()] = float(sc or 0.0)
        except (TypeError, ValueError):
            out[str(stock).strip().upper()] = 0.0
        if cra is not None and hasattr(cra, "astimezone"):
            try:
                d_ist = cra.astimezone(IST).date()
                if d_ist == session_date:
                    match_count += 1
            except Exception:
                pass
        elif cra is not None:
            try:
                if isinstance(cra, datetime) and cra.tzinfo is None:
                    d_ist = IST.localize(cra).date()
                else:
                    d_ist = None
                if d_ist == session_date:
                    match_count += 1
            except Exception:
                pass

    note = (
        f"picker_equivalent_coalesce; current_run_at_on_session_ist_count={match_count}/"
        f"{len(rows)} (informational; DB has no per-day sentiment history)"
    )
    return out, note, match_count
