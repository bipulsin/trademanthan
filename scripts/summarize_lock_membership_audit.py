#!/usr/bin/env python3
"""Read-only lock-membership observation summary from rs_lock_membership_audit.

Usage (on paperclip app container or local with DATABASE_URL):
  PYTHONPATH=/app python scripts/summarize_lock_membership_audit.py
  PYTHONPATH=/app python scripts/summarize_lock_membership_audit.py --days 5
  PYTHONPATH=/app python scripts/summarize_lock_membership_audit.py --date 2026-07-14

Flags:
  - Entry latency > 1 scan interval between first-of-pair and ENTRY event
  - More than one ENTRY/REMOVE cycle for the same (symbol, direction) in a day
  - R1 vs R2 removal counts
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal

IST = pytz.timezone("Asia/Kolkata")
# RS scanner cadence is ~5m; allow slack for jitter.
SCAN_INTERVAL_SEC = 5 * 60
LATENCY_FLAG_SEC = SCAN_INTERVAL_SEC + 90  # one interval + slack


def _parse_dt(v) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(IST) if v.tzinfo else IST.localize(v)
    return None


def _session_dates(db, days: int, date: Optional[str]) -> List[str]:
    if date:
        return [date[:10]]
    rows = db.execute(
        text(
            """
            SELECT DISTINCT session_date::text AS d
            FROM rs_lock_membership_audit
            WHERE session_date >= (CURRENT_DATE - CAST(:n AS int))
            ORDER BY d DESC
            """
        ),
        {"n": max(0, days - 1)},
    ).fetchall()
    return [r.d for r in rows]


def _events(db, session_date: str) -> List[Any]:
    return db.execute(
        text(
            """
            SELECT symbol, direction, event_type, rule, rank,
                   persistence_top5_frac, persistence_clean_bars,
                   detail, event_at
            FROM rs_lock_membership_audit
            WHERE session_date = CAST(:d AS date)
            ORDER BY event_at, id
            """
        ),
        {"d": session_date},
    ).fetchall()


def _scan_times(db, session_date: str) -> List[datetime]:
    rows = db.execute(
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
    out = []
    for r in rows:
        t = _parse_dt(r.scan_time)
        if t:
            out.append(t)
    return out


def _qualifying_pair_start(
    db,
    session_date: str,
    symbol: str,
    direction: str,
    entry_at: datetime,
    scan_times: List[datetime],
) -> Optional[Tuple[datetime, datetime]]:
    """Find the latest 2-scan Top-5 pair for (symbol, side) at/before entry_at."""
    side = (direction or "").upper()
    ranking = "BEARISH" if side == "BEAR" else "BULLISH"
    # Restrict to scans at or before entry
    times = [t for t in scan_times if t <= entry_at]
    if len(times) < 2:
        return None
    a, b = times[-2], times[-1]
    rows = db.execute(
        text(
            """
            SELECT scan_time
            FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date)
              AND UPPER(symbol) = :sym
              AND UPPER(ranking_type) = :rt
              AND rank_position <= 5
              AND scan_time IN (:a, :b)
            """
        ),
        {"d": session_date, "sym": symbol.upper(), "rt": ranking, "a": a, "b": b},
    ).fetchall()
    if len(rows) < 2:
        # Fall back: search backward for any consecutive pair ending at/before entry
        for i in range(len(times) - 1, 0, -1):
            a2, b2 = times[i - 1], times[i]
            if b2 > entry_at:
                continue
            hit = db.execute(
                text(
                    """
                    SELECT COUNT(*)::int AS n
                    FROM relative_strength_snapshot
                    WHERE scan_time::date = CAST(:d AS date)
                      AND UPPER(symbol) = :sym
                      AND UPPER(ranking_type) = :rt
                      AND rank_position <= 5
                      AND scan_time IN (:a, :b)
                    """
                ),
                {
                    "d": session_date,
                    "sym": symbol.upper(),
                    "rt": ranking,
                    "a": a2,
                    "b": b2,
                },
            ).fetchone()
            if hit and int(hit.n) >= 2:
                return a2, b2
        return None
    return a, b


def summarize_day(db, session_date: str) -> Dict[str, Any]:
    events = _events(db, session_date)
    scan_times = _scan_times(db, session_date)

    removals_r1 = 0
    removals_r2 = 0
    removals_other = 0
    entries = []
    latency_flags = []
    cycles: Dict[Tuple[str, str], List[str]] = defaultdict(list)

    for e in events:
        key = (str(e.symbol).upper(), str(e.direction).upper())
        cycles[key].append(str(e.event_type).upper())
        et = _parse_dt(e.event_at)
        if str(e.event_type).upper() == "REMOVE":
            rule = (e.rule or "").upper()
            if rule == "R1":
                removals_r1 += 1
            elif rule == "R2":
                removals_r2 += 1
            else:
                removals_other += 1
        elif str(e.event_type).upper() == "ENTRY" and (e.rule or "") != "morning_lock":
            entries.append(e)
            if et is None:
                continue
            pair = _qualifying_pair_start(
                db, session_date, e.symbol, e.direction, et, scan_times
            )
            if not pair:
                latency_flags.append(
                    {
                        "symbol": e.symbol,
                        "direction": e.direction,
                        "entry_at": et.strftime("%H:%M:%S"),
                        "pair_start": None,
                        "gap_sec": None,
                        "note": "could_not_resolve_qualifying_pair",
                    }
                )
                continue
            first, second = pair
            gap = (et - first).total_seconds()
            # Expected: entry roughly at second scan of the pair (~one interval after first)
            expected_max = (second - first).total_seconds() + 90
            if gap > max(LATENCY_FLAG_SEC, expected_max):
                latency_flags.append(
                    {
                        "symbol": e.symbol,
                        "direction": e.direction,
                        "entry_at": et.strftime("%H:%M:%S"),
                        "pair_start": first.strftime("%H:%M:%S"),
                        "pair_end": second.strftime("%H:%M:%S"),
                        "gap_sec": int(gap),
                        "gap_min": round(gap / 60.0, 1),
                        "note": "entry_later_than_one_scan_interval_from_pair_start",
                    }
                )

    churn_flags = []
    for (sym, side), seq in sorted(cycles.items()):
        # Count full ENTRY→REMOVE→ENTRY cycles (or REMOVE→ENTRY→REMOVE)
        entries_n = sum(1 for x in seq if x == "ENTRY")
        removes_n = sum(1 for x in seq if x == "REMOVE")
        full_cycles = min(entries_n, removes_n)
        # "more than one full cycle" = at least 2 removes after having entered, or 2+ entry+remove pairs
        if full_cycles > 1 or (entries_n > 1 and removes_n > 1):
            churn_flags.append(
                {
                    "symbol": sym,
                    "direction": side,
                    "entries": entries_n,
                    "removes": removes_n,
                    "full_cycles": full_cycles,
                    "sequence": " → ".join(seq),
                }
            )

    return {
        "date": session_date,
        "event_count": len(events),
        "intraday_entries": len(entries),
        "removals": {"R1": removals_r1, "R2": removals_r2, "other": removals_other},
        "latency_flags": latency_flags,
        "churn_flags": churn_flags,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=3, help="Look back N calendar days with audit rows")
    ap.add_argument("--date", type=str, default=None, help="Single session date YYYY-MM-DD")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        try:
            dates = _session_dates(db, args.days, args.date)
        except Exception as exc:
            print(f"No rs_lock_membership_audit (or query failed): {exc}")
            return 1
        if not dates:
            print("No audit rows in window.")
            return 0

        tot_r1 = tot_r2 = 0
        print("=" * 72)
        print("RS lock membership observation summary (read-only)")
        print("=" * 72)
        for d in dates:
            s = summarize_day(db, d)
            tot_r1 += s["removals"]["R1"]
            tot_r2 += s["removals"]["R2"]
            print(f"\n## {s['date']}  events={s['event_count']}  intraday_entries={s['intraday_entries']}")
            print(
                f"   removals: R1={s['removals']['R1']}  R2={s['removals']['R2']}"
                + (f"  other={s['removals']['other']}" if s["removals"]["other"] else "")
            )
            if s["latency_flags"]:
                print("   ENTRY latency flags (>~1 scan interval from pair start):")
                for f in s["latency_flags"]:
                    print(
                        f"     - {f['symbol']} {f['direction']} entry@{f['entry_at']} "
                        f"pair={f.get('pair_start')}→{f.get('pair_end')} "
                        f"gap={f.get('gap_min')}m  ({f.get('note')})"
                    )
            else:
                print("   ENTRY latency flags: none")
            if s["churn_flags"]:
                print("   Churn flags (>1 ENTRY/REMOVE cycle same symbol-direction):")
                for f in s["churn_flags"]:
                    print(
                        f"     - {f['symbol']} {f['direction']} "
                        f"E={f['entries']} R={f['removes']} cycles≈{f['full_cycles']}  [{f['sequence']}]"
                    )
            else:
                print("   Churn flags: none")

        print("\n" + "=" * 72)
        print(f"Observation window removal attribution: R1={tot_r1}  R2={tot_r2}")
        if tot_r1 + tot_r2:
            r1p = 100.0 * tot_r1 / (tot_r1 + tot_r2)
            print(f"  R1 share={r1p:.0f}%  R2 share={100 - r1p:.0f}%")
        print("=" * 72)
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
