#!/usr/bin/env python3
"""Summarize READY dwell + entry-distance shadow for a session (read-only).

Usage:
  PYTHONPATH=. python3 scripts/analyze_ready_dwell_entry_shadow.py
  PYTHONPATH=. python3 scripts/analyze_ready_dwell_entry_shadow.py --date 2026-07-17

Reads ``kavach_ready_consistency_log.inputs.dwell_entry_shadow`` and
``kavach_ready_dwell_entry_shadow``. Breaks out check2 vs check3 blocks and
flags check3-only (possible thin-but-trending pullbacks — research, not a patch).
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz

from backend.database import SessionLocal
from sqlalchemy import text

IST = pytz.timezone("Asia/Kolkata")

# From 17-Jul investigation: short READY flashes killed by warning_stack.
FLASH_JUNK_HINTS = {"BSE", "CUMMINSIND", "ALKEM"}


def _session_date(arg: Optional[str]) -> str:
    if arg:
        return arg
    return datetime.now(IST).strftime("%Y-%m-%d")


def _shadow(row_inputs: Any) -> Dict[str, Any]:
    if isinstance(row_inputs, str):
        try:
            row_inputs = json.loads(row_inputs)
        except Exception:
            return {}
    if not isinstance(row_inputs, dict):
        return {}
    s = row_inputs.get("dwell_entry_shadow") or {}
    return s if isinstance(s, dict) else {}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="IST session date YYYY-MM-DD (default: today)")
    args = ap.parse_args()
    d = _session_date(args.date)

    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT symbol, direction, rendered_state, pre_gate_state,
                       inputs, logged_at
                FROM kavach_ready_consistency_log
                WHERE session_date = CAST(:d AS date)
                ORDER BY logged_at ASC, id ASC
                """
            ),
            {"d": d},
        ).fetchall()

        state_rows = db.execute(
            text(
                """
                SELECT symbol, shadow_ready_since, last_outcome,
                       distance_blocked, check3_only, inputs, updated_at
                FROM kavach_ready_dwell_entry_shadow
                WHERE session_date = CAST(:d AS date)
                ORDER BY symbol
                """
            ),
            {"d": d},
        ).fetchall()
    finally:
        db.close()

    outcomes = Counter()
    block_check2_syms = set()
    block_check3_syms = set()
    check3_only_syms = set()
    would_dwell_syms = set()
    would_extend_syms = set()
    warn_syms = set()
    by_sym_outcomes: Dict[str, Counter] = defaultdict(Counter)
    check3_only_events: List[Dict[str, Any]] = []

    for r in rows:
        sh = _shadow(r.inputs)
        if not sh:
            continue
        sym = str(r.symbol).upper()
        out = str(sh.get("outcome") or "unknown")
        outcomes[out] += 1
        by_sym_outcomes[sym][out] += 1

        if sh.get("block_check2") or (sh.get("distance") or {}).get("check2_entry_thin"):
            block_check2_syms.add(sym)
        if sh.get("block_check3") or (sh.get("distance") or {}).get("check3_stack_thin"):
            block_check3_syms.add(sym)
        if sh.get("check3_only") or (sh.get("distance") or {}).get("check3_only"):
            check3_only_syms.add(sym)
            check3_only_events.append(
                {
                    "symbol": sym,
                    "logged_at": str(r.logged_at),
                    "outcome": out,
                    "ema5_to_ema10": (sh.get("distance") or {}).get("ema5_to_ema10"),
                    "entry_to_ema10": (sh.get("distance") or {}).get("entry_to_ema10"),
                    "min_gap_pts": (sh.get("distance") or {}).get("min_gap_pts"),
                    "flash_junk_hint": sym in FLASH_JUNK_HINTS,
                }
            )
        if sh.get("would_enter_dwell") or str(out).startswith("would_start_dwell") or str(
            out
        ).startswith("would_continue_dwell"):
            would_dwell_syms.add(sym)
        if sh.get("would_extend_dwell"):
            would_extend_syms.add(sym)
        if (sh.get("distance") or {}).get("warn_entry_off_ema5"):
            warn_syms.add(sym)

    print(f"=== READY dwell/entry shadow — {d} ===")
    print(f"consistency rows with shadow payload: {sum(1 for r in rows if _shadow(r.inputs))}")
    print(f"state-table symbols: {len(state_rows)}")
    print()
    print("Outcome counts (row-level):")
    for k, v in outcomes.most_common():
        print(f"  {k}: {v}")
    print()
    print("Symbol sets:")
    print(f"  would get dwell (start/continue): {sorted(would_dwell_syms)}")
    print(f"  would extend dwell on soft hide:  {sorted(would_extend_syms)}")
    print(f"  blocked by check2 (entry→EMA10):  {sorted(block_check2_syms)}")
    print(f"  blocked by check3 (EMA5→EMA10):   {sorted(block_check3_syms)}")
    print(f"  check3-ONLY (research flag):      {sorted(check3_only_syms)}")
    print(f"  entry-vs-EMA5 WARN (no block):    {sorted(warn_syms)}")
    print()

    both = block_check2_syms & block_check3_syms
    c2_only = block_check2_syms - block_check3_syms
    c3_only = block_check3_syms - block_check2_syms
    print("Block breakout (unique symbols ever blocked that day):")
    print(f"  check2 AND check3: {len(both)}  {sorted(both)}")
    print(f"  check2 only:       {len(c2_only)}  {sorted(c2_only)}")
    print(f"  check3 only:       {len(c3_only)}  {sorted(c3_only)}")
    print()

    if check3_only_events:
        print("CHECK3-ONLY events (flag if these look like real trends, not flash junk):")
        for e in check3_only_events[:40]:
            hint = " [17-Jul flash-junk cohort]" if e.get("flash_junk_hint") else ""
            print(
                f"  {e['symbol']} @ {e['logged_at']}  gap={e.get('ema5_to_ema10')} "
                f"floor={e.get('min_gap_pts')}  {e.get('outcome')}{hint}"
            )
        if len(check3_only_events) > 40:
            print(f"  ... +{len(check3_only_events) - 40} more")
        print(
            "  NOTE: check3-only on otherwise healthy trends may mean EMA5 pullback "
            "entries are inherently thin on that symbol — separate research question."
        )
    print()

    flash_hit = FLASH_JUNK_HINTS & (block_check2_syms | block_check3_syms | would_extend_syms)
    print(f"17-Jul flash-junk cohort cross-ref ({sorted(FLASH_JUNK_HINTS)}):")
    for sym in sorted(FLASH_JUNK_HINTS):
        tags = []
        if sym in block_check2_syms:
            tags.append("check2")
        if sym in block_check3_syms:
            tags.append("check3")
        if sym in check3_only_syms:
            tags.append("check3_only")
        if sym in would_extend_syms:
            tags.append("dwell_extend")
        if sym in would_dwell_syms:
            tags.append("dwell_ok")
        print(f"  {sym}: {', '.join(tags) if tags else 'no shadow hit yet'}")
    if not flash_hit and not rows:
        print("  (no shadow rows yet — deploy must be live during session)")
    print()
    print("State table latest outcomes:")
    for r in state_rows:
        print(
            f"  {r.symbol}: outcome={r.last_outcome} "
            f"blocked={r.distance_blocked} check3_only={r.check3_only} "
            f"since={r.shadow_ready_since}"
        )


if __name__ == "__main__":
    main()
