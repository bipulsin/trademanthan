#!/usr/bin/env python3
"""One-off retrospective backfill for breakfast_live_signals (display/research only).

Reconstructs the 9:15–9:20 IST lock snapshot from Upstox historical 5m candles.
Tags rows via manual_note = backfilled_retrospective (no schema change).

NOT for trade execution — trade_taken stays false.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, time as dt_time
from typing import Any, Dict, List

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.breakfast_strategy.live import build_off_cycle_preview_state
from backend.services.breakfast_strategy.live_persist import (
    ensure_breakfast_live_signals_table,
    rows_from_live_state,
)

IST = pytz.timezone("Asia/Kolkata")
BACKFILL_NOTE = "backfilled_retrospective"
LOCK_TIME = dt_time(9, 20, 8)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("backfill_breakfast_live_snapshot")


def _parse_session_date(raw: str) -> date:
    return datetime.strptime(str(raw).strip()[:10], "%Y-%m-%d").date()


def _locked_at_iso(session_date: date) -> str:
    return IST.localize(datetime.combine(session_date, LOCK_TIME)).isoformat()


def build_backfill_state(session_date: date) -> Dict[str, Any]:
    """Reconstruct locked breakfast state from Upstox REST (off-cycle path)."""
    now = IST.localize(datetime.combine(session_date, LOCK_TIME))
    state = build_off_cycle_preview_state(now)
    state["server_time"] = _locked_at_iso(session_date)
    state["state"] = "locked"
    state["phase"] = "frozen"
    state["banner"] = f"BACKFILLED — 9:20 IST snapshot ({session_date.isoformat()})"
    state["backfill"] = True
    state["backfill_source"] = BACKFILL_NOTE
    return state


def _row_exists(db, row: Dict[str, Any]) -> bool:
    n = db.execute(
        text(
            """
            SELECT COUNT(*) FROM breakfast_live_signals
            WHERE session_date = CAST(:session_date AS date)
              AND UPPER(symbol) = UPPER(:symbol)
              AND direction = :direction
            """
        ),
        row,
    ).scalar()
    return int(n or 0) > 0


def _insert_backfill_row(db, row: Dict[str, Any]) -> bool:
    if _row_exists(db, row):
        return False
    db.execute(
        text(
            """
            INSERT INTO breakfast_live_signals (
                session_date, symbol, direction, sector, sector_rank, rank_at_lock,
                nifty_bias_pct, sector_move_pct, stock_move_pct_at_lock, ltp_at_lock,
                anchor_price, tp_price, sl_price, lot_size, locked_at_timestamp,
                websocket_rest_cross_check_status, instrument_key, manual_note, trade_taken
            ) VALUES (
                CAST(:session_date AS date), :symbol, :direction, :sector, :sector_rank, :rank_at_lock,
                :nifty_bias_pct, :sector_move_pct, :stock_move_pct_at_lock, :ltp_at_lock,
                :anchor_price, :tp_price, :sl_price, :lot_size,
                CAST(:locked_at_timestamp AS timestamptz),
                :websocket_rest_cross_check_status, :instrument_key, :manual_note, FALSE
            )
            """
        ),
        {**row, "manual_note": BACKFILL_NOTE},
    )
    return True


def persist_backfill(state: Dict[str, Any], *, apply: bool) -> Dict[str, Any]:
    rows = rows_from_live_state(state, "matched")
    if not rows:
        return {"ok": False, "reason": "no_rows_built", "inserted": 0, "skipped": 0, "rows": []}

    ensure_breakfast_live_signals_table()
    inserted = skipped = 0
    if apply:
        db = SessionLocal()
        try:
            for row in rows:
                if _insert_backfill_row(db, row):
                    inserted += 1
                else:
                    skipped += 1
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    summary = {
        "ok": True,
        "session_date": state.get("session_date"),
        "nifty": state.get("nifty"),
        "sectors": [
            {
                "rank": s.get("sector_rank"),
                "label": s.get("sector_label"),
                "move_pct": s.get("move_pct"),
                "stocks": [st.get("symbol") for st in (s.get("stocks") or [])],
            }
            for s in (state.get("sectors") or [])
        ],
        "row_count": len(rows),
        "inserted": inserted,
        "skipped": skipped,
        "applied": apply,
        "provenance": BACKFILL_NOTE,
    }
    return summary


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Backfill breakfast_live_signals from Upstox historical candles")
    p.add_argument("--session-date", required=True, help="YYYY-MM-DD (IST session)")
    p.add_argument("--apply", action="store_true", help="Write rows (default: dry-run preview)")
    p.add_argument("--json", action="store_true", help="Emit JSON summary")
    args = p.parse_args(argv)

    session_date = _parse_session_date(args.session_date)
    logger.info("Building backfill state for %s (locked_at %s IST)", session_date, LOCK_TIME)
    state = build_backfill_state(session_date)
    result = persist_backfill(state, apply=args.apply)

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        nifty = result.get("nifty") or {}
        print(f"session_date={result.get('session_date')}")
        print(f"nifty_bias={nifty.get('bias')} bias_pct={nifty.get('bias_pct')} direction={nifty.get('direction')}")
        for sec in result.get("sectors") or []:
            stocks = ", ".join(sec.get("stocks") or [])
            print(f"  sector #{sec.get('rank')} {sec.get('label')} ({sec.get('move_pct')}%): {stocks}")
        print(f"rows={result.get('row_count')} inserted={result.get('inserted')} skipped={result.get('skipped')} applied={result.get('applied')}")
        print(f"provenance={result.get('provenance')}")

    if not result.get("ok"):
        return 1
    if result.get("row_count", 0) == 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
