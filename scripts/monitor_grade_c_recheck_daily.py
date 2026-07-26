#!/usr/bin/env python3
"""Daily monitor for Variant A plain-C → READY(RECHECK) live path.

Counts consistency-log rows where grade-C RECHECK fired
(``inputs.grade_c_recheck_path``), plus take_enabled and optional outcomes.

Usage (paperclip app container)::

    PYTHONPATH=/app /opt/venv/bin/python /app/scripts/monitor_grade_c_recheck_daily.py

    # specific session date:
    SESSION_DATE=2026-07-26 \\
      PYTHONPATH=/app /opt/venv/bin/python /app/scripts/monitor_grade_c_recheck_daily.py

    # date range:
    GRADE_C_START=2026-07-20 GRADE_C_END=2026-07-26 \\
      PYTHONPATH=/app /opt/venv/bin/python /app/scripts/monitor_grade_c_recheck_daily.py

SQL (ad-hoc)::

    SELECT session_date, symbol, rendered_state,
           inputs->>'grade_c_recheck_grade' AS grade,
           inputs->>'grade_c_recheck_path' AS path,
           inputs->>'trade_take_enabled' AS take_enabled,
           inputs->>'atr_ready_suppress_fired' AS atr_suppress
    FROM kavach_ready_consistency_log
    WHERE session_date = CURRENT_DATE
      AND (inputs->>'grade_c_recheck_path') = 'true';
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pytz  # noqa: E402

IST = pytz.timezone("Asia/Kolkata")


def _q(sql: str, **params):
    from backend.database import SessionLocal
    from sqlalchemy import text

    db = SessionLocal()
    try:
        rows = db.execute(text(sql), params).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


def main():
    start = os.environ.get("GRADE_C_START") or os.environ.get("SESSION_DATE")
    end = os.environ.get("GRADE_C_END") or os.environ.get("SESSION_DATE")
    if not start:
        start = end = datetime.now(IST).date().isoformat()
    if not end:
        end = start

    print(f"[monitor] grade-C READY(RECHECK)  {start} → {end}")
    rows = _q(
        """
        SELECT session_date::text AS session_date, symbol, direction,
               rendered_state, logged_at,
               inputs->>'grade_c_recheck_path' AS grade_c_recheck_path,
               inputs->>'grade_c_recheck_would_apply' AS grade_c_recheck_would_apply,
               inputs->>'grade_c_recheck_live_enabled' AS grade_c_recheck_live_enabled,
               inputs->>'grade_c_recheck_grade' AS grade_c_recheck_grade,
               inputs->>'confidence' AS confidence,
               inputs->>'trade_take_enabled' AS trade_take_enabled,
               inputs->>'atr_ready_suppress_fired' AS atr_ready_suppress_fired,
               inputs->>'trade_entry' AS trade_entry,
               inputs->>'trade_sl' AS trade_sl
        FROM kavach_ready_consistency_log
        WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
          AND (
            (inputs->>'grade_c_recheck_would_apply') = 'true'
            OR (inputs->>'grade_c_recheck_path') = 'true'
            OR UPPER(COALESCE(inputs->>'grade_c_recheck_grade', inputs->>'confidence', '')) = 'C'
          )
        ORDER BY session_date, symbol, logged_at
        """,
        a=start,
        b=end,
    )

    path_rows = [
        r for r in rows if str(r.get("grade_c_recheck_path") or "").lower() == "true"
    ]
    would_rows = [
        r
        for r in rows
        if str(r.get("grade_c_recheck_would_apply") or "").lower() == "true"
    ]
    recheck_renders = [
        r
        for r in path_rows
        if (r.get("rendered_state") or "").upper() in ("READY(RECHECK)", "READY")
    ]
    take_on = [
        r
        for r in path_rows
        if str(r.get("trade_take_enabled") or "").lower() == "true"
    ]
    atr_suppressed = [
        r
        for r in path_rows
        if str(r.get("atr_ready_suppress_fired") or "").lower() == "true"
    ]

    by_day_path = defaultdict(int)
    by_sym_path = defaultdict(int)
    for r in path_rows:
        by_day_path[str(r["session_date"])] += 1
        by_sym_path[(r.get("symbol") or "").upper()] += 1

    print(f"  candidate rows (C / would_apply / path): {len(rows)}")
    print(f"  grade_c_recheck_would_apply:            {len(would_rows)}")
    print(f"  grade_c_recheck_path (live path):       {len(path_rows)}")
    print(f"  path + READY-family rendered:           {len(recheck_renders)}")
    print(f"  path + take_enabled:                    {len(take_on)}")
    print(f"  path + atr_ready_suppress_fired:        {len(atr_suppressed)}")
    print("  by session (path):")
    for d in sorted(by_day_path):
        print(f"    {d}: {by_day_path[d]}")
    print("  top symbols (path):")
    for sym, n in sorted(by_sym_path.items(), key=lambda x: -x[1])[:15]:
        print(f"    {sym}: {n}")

    # Optional: open-trade outcomes when overlapping symbols exist.
    outcomes = []
    if by_sym_path:
        try:
            sym_list = sorted(by_sym_path.keys())
            outcomes = _q(
                """
                SELECT symbol, direction, session_date::text AS session_date,
                       status, pnl_r, exit_reason
                FROM kavach_open_trades
                WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
                  AND UPPER(symbol) IN :syms
                ORDER BY session_date, symbol
                """.replace(
                    "IN :syms",
                    "IN (" + ", ".join(f"'{s}'" for s in sym_list) + ")",
                ),
                a=start,
                b=end,
            )
        except Exception as exc:
            print(f"  outcomes: skipped ({exc})")
            outcomes = []
    else:
        print("  outcomes: none (no path symbols)")

    if outcomes:
        print(f"  open-trade rows overlapping path symbols: {len(outcomes)}")
        for o in outcomes[:20]:
            print(
                f"    {o.get('session_date')} {o.get('symbol')} "
                f"{o.get('status')} pnl_r={o.get('pnl_r')} "
                f"exit={o.get('exit_reason')}"
            )

    out_dir = os.environ.get("GRADE_C_OUT")
    if out_dir:
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        artifact = {
            "start": start,
            "end": end,
            "n_candidate_rows": len(rows),
            "n_would_apply": len(would_rows),
            "n_path": len(path_rows),
            "n_path_ready_family": len(recheck_renders),
            "n_path_take_enabled": len(take_on),
            "n_path_atr_suppress": len(atr_suppressed),
            "by_day_path": dict(by_day_path),
            "by_sym_path": dict(by_sym_path),
            "outcomes": outcomes,
        }
        path = Path(out_dir) / f"grade_c_recheck_{start}_{end}.json"
        path.write_text(json.dumps(artifact, indent=2, default=str))
        print(f"  wrote {path}")


if __name__ == "__main__":
    main()
