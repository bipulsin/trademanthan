#!/usr/bin/env python3
"""Dry-run: would intraday promotion have locked the known RSCD repro stock-days?

Read-only against relative_strength_snapshot + daily_snapshot. Does not write.
"""
from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.daily_checklist_snapshot import (
    PROMOTION_CUTOFF_MIN,
    _eligible_consecutive_top5,
    _snap_side_from_ranking,
)

IST = pytz.timezone("Asia/Kolkata")

# Known "RS confirmed but not promoted" / direction-flip cases from RSCD review.
DEFAULT_CASES = [
    ("GODREJPROP", "2026-07-10"),
    ("SOLARINDS", None),
    ("MANAPPURAM", None),
    ("PAYTM", None),
    ("RBLBANK", None),
    ("INDIANB", None),
    ("BSE", None),
    ("KALYANKJIL", None),
    ("MCX", None),
    ("NAUKRI", None),
]


def _resolve_dates(db, symbol: str, date: Optional[str]) -> List[str]:
    if date:
        return [date]
    rows = db.execute(
        text(
            """
            SELECT DISTINCT scan_time::date::text AS d
            FROM relative_strength_snapshot
            WHERE UPPER(symbol) = :s
              AND rank_position <= 5
              AND scan_time >= NOW() - INTERVAL '30 days'
            ORDER BY d DESC
            LIMIT 5
            """
        ),
        {"s": symbol.upper()},
    ).fetchall()
    return [r.d for r in rows]


def _morning_lock(db, session_date: str, symbol: str) -> Optional[Dict[str, Any]]:
    r = db.execute(
        text(
            """
            SELECT direction, rank, rs_score
            FROM daily_snapshot
            WHERE snapshot_date = CAST(:d AS date) AND UPPER(symbol) = :s
            ORDER BY rank
            LIMIT 1
            """
        ),
        {"d": session_date, "s": symbol.upper()},
    ).fetchone()
    if not r:
        return None
    return {"direction": r.direction, "rank": r.rank, "rs_score": r.rs_score}


def _first_qualify_time(
    db, session_date: str, symbol: str, side: str
) -> Optional[datetime]:
    """Earliest scan_time at which (symbol, side) completed 2 consecutive Top-5 scans."""
    end = IST.localize(datetime.strptime(session_date, "%Y-%m-%d").replace(hour=14, minute=30))
    # Walk chronologically: after each scan, recompute eligibility up to that time
    times = db.execute(
        text(
            """
            SELECT DISTINCT scan_time
            FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date)
            ORDER BY scan_time
            """
        ),
        {"d": session_date},
    ).fetchall()
    key = (symbol.upper(), side)
    for r in times:
        st = r.scan_time
        t = st.astimezone(IST) if st.tzinfo else IST.localize(st)
        if (t.hour * 60 + t.minute) > PROMOTION_CUTOFF_MIN:
            break
        elig = _eligible_consecutive_top5(db, session_date, now=t)
        if key in elig:
            return t
    return None


def evaluate_case(db, symbol: str, session_date: str) -> Dict[str, Any]:
    sym = symbol.upper()
    morning = _morning_lock(db, session_date, sym)
    end = IST.localize(datetime.strptime(session_date, "%Y-%m-%d").replace(hour=14, minute=30))
    elig = _eligible_consecutive_top5(db, session_date, now=end)
    sides = sorted({side for (s, side) in elig if s == sym})
    would_promote = bool(sides)
    first_times = {side: _first_qualify_time(db, session_date, sym, side) for side in sides}

    # Variant classification vs morning lock
    if morning is None and would_promote:
        variant = "same_dir_or_late_non_promotion"
        if len(sides) >= 2:
            variant = "multi_side_late"
    elif morning is not None and would_promote:
        mside = "BEAR" if (morning["direction"] or "").upper() == "BEAR" else "BULL"
        if any(s != mside for s in sides):
            variant = "direction_flip"
        else:
            variant = "already_locked_same_side"
    else:
        variant = "would_NOT_promote"

    return {
        "symbol": sym,
        "date": session_date,
        "morning_lock": morning,
        "eligible_sides": sides,
        "first_qualify": {k: (v.isoformat() if v else None) for k, v in first_times.items()},
        "would_promote": would_promote,
        "variant": variant,
        "pass": would_promote and variant != "already_locked_same_side",
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", action="append", help="SYMBOL:YYYY-MM-DD or SYMBOL")
    args = ap.parse_args()

    cases: List[Tuple[str, Optional[str]]] = []
    if args.symbol:
        for raw in args.symbol:
            if ":" in raw:
                s, d = raw.split(":", 1)
                cases.append((s.strip().upper(), d.strip()))
            else:
                cases.append((raw.strip().upper(), None))
    else:
        cases = [(s, d) for s, d in DEFAULT_CASES]

    db = SessionLocal()
    try:
        results = []
        for sym, d in cases:
            dates = _resolve_dates(db, sym, d)
            if not dates:
                results.append(
                    {
                        "symbol": sym,
                        "date": d,
                        "would_promote": False,
                        "variant": "no_rs_data",
                        "pass": False,
                    }
                )
                continue
            for sd in dates:
                results.append(evaluate_case(db, sym, sd))
    finally:
        db.close()

    # Prefer one best day per symbol when auto-resolved: would_promote first
    by_sym: Dict[str, List[Dict[str, Any]]] = {}
    for r in results:
        by_sym.setdefault(r["symbol"], []).append(r)

    print(f"{'SYMBOL':12} {'DATE':12} {'PASS':5} {'VARIANT':32} ELIGIBLE  FIRST_QUALIFY  MORNING")
    print("-" * 110)
    passes = 0
    checked = 0
    for sym, rows in by_sym.items():
        # Show all explicit dates; for auto, show best promote day else latest
        if len(rows) > 1 and any(r.get("date") for r in DEFAULT_CASES if r[0] == sym and r[1]):
            show = rows
        elif len(rows) > 1:
            promote_rows = [r for r in rows if r.get("would_promote")]
            show = promote_rows[:1] or rows[:1]
        else:
            show = rows
        for r in show:
            checked += 1
            ok = bool(r.get("would_promote"))
            if ok:
                passes += 1
            fq = r.get("first_qualify") or {}
            fq_s = ",".join(f"{k}@{v[11:16] if v else '?'}" for k, v in fq.items()) or "—"
            m = r.get("morning_lock")
            m_s = f"{m['direction']}#{m['rank']}" if m else "none"
            print(
                f"{r['symbol']:12} {str(r.get('date') or '—'):12} "
                f"{'YES' if ok else 'NO':5} {r.get('variant',''):32} "
                f"{','.join(r.get('eligible_sides') or []) or '—':8} "
                f"{fq_s:20} {m_s}"
            )

    print("-" * 110)
    print(f"would_promote {passes}/{checked}")
    return 0 if passes == checked and checked > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
