#!/usr/bin/env python3
"""READY dwell / entry-guard session report (shadow + live).

Usage:
  PYTHONPATH=. python3 scripts/analyze_ready_dwell_entry_shadow.py --date 2026-07-20
  PYTHONPATH=. python3 scripts/analyze_ready_dwell_entry_shadow.py --date 2026-07-20 \\
      --baseline 2026-07-17 --normal 2026-07-15

Monday report should always pass:
  --baseline 2026-07-17   # atypical high-churn day (do NOT read as 'normal')
  --normal 2026-07-15     # fairer same-metric comparator (peak rem/h ~13)
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal

IST = pytz.timezone("Asia/Kolkata")

FLASH_JUNK_HINTS = {"BSE", "CUMMINSIND", "ALKEM"}
# Owner: 7+ removals/hour is elevated; 17-Jul peaked at 33/h (atypical).
HIGH_CHURN_PEAK_THRESHOLD = 7
ATYPICAL_PEAK_THRESHOLD = 20

# Frozen baselines from 2026-07-18 pre-live pull (also recomputed live below).
BASELINE_17JUL_NOTE = (
    "2026-07-17 was an ATYPICAL high-churn session (peak 33 lock-removals/hour; "
    "total 133 removals). Do NOT read Monday improvement vs 17-Jul as "
    "'better than a normal day' — compare also to 2026-07-15 (peak ~13/h)."
)


def _session_date(arg: Optional[str]) -> str:
    if arg:
        return arg
    return datetime.now(IST).strftime("%Y-%m-%d")


def _as_dict(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return {}
    return raw if isinstance(raw, dict) else {}


def _shadow(row_inputs: Any) -> Dict[str, Any]:
    s = _as_dict(row_inputs).get("dwell_entry_shadow") or {}
    return s if isinstance(s, dict) else {}


def _load_consistency(db, d: str):
    return db.execute(
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


def _load_state(db, d: str):
    try:
        return db.execute(
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
    except Exception:
        return []


def _removal_stats(db, d: str) -> Dict[str, Any]:
    rem = db.execute(
        text(
            """
            SELECT event_at AT TIME ZONE 'Asia/Kolkata' AS at_ist
            FROM rs_lock_membership_audit
            WHERE LOWER(event_type) = 'remove'
              AND session_date = CAST(:d AS date)
            """
        ),
        {"d": d},
    ).fetchall()
    hours: Dict[int, int] = defaultdict(int)
    for r in rem:
        if r.at_ist is None:
            continue
        h = int(r.at_ist.hour)
        if 9 <= h <= 15:
            hours[h] += 1
    peak = max(hours.values()) if hours else 0
    peak_h = max(hours, key=hours.get) if hours else None
    return {
        "removals_total": len(rem),
        "peak_removals_per_hour": peak,
        "peak_hour_ist": peak_h,
        "hours": dict(sorted(hours.items())),
        "elevated_churn": peak >= HIGH_CHURN_PEAK_THRESHOLD,
        "atypical_high_churn": peak >= ATYPICAL_PEAK_THRESHOLD,
    }


def _flow_metrics(rows: Sequence[Any]) -> Dict[str, Any]:
    """Same-metric READY flow stats used for before/after comparison."""
    by: Dict[str, List[Any]] = defaultdict(list)
    for r in rows:
        by[str(r.symbol).upper()].append(r)

    spells: List[Tuple[str, float, Optional[str]]] = []
    for sym, rs in by.items():
        i = 0
        while i < len(rs):
            if rs[i].rendered_state in ("READY", "READY(RECHECK)"):
                start = rs[i].logged_at
                j = i
                while j < len(rs) and rs[j].rendered_state in (
                    "READY",
                    "READY(RECHECK)",
                ):
                    j += 1
                end = rs[j].logged_at if j < len(rs) else rs[j - 1].logged_at
                dur = (end - start).total_seconds() / 60.0
                zd = None
                if j < len(rs):
                    zd = _as_dict(rs[j].inputs).get("zone_downgrade")
                spells.append((sym, dur, zd))
                i = j
            else:
                i += 1

    soft_polls = 0
    soft_syms = set()
    for r in rows:
        if r.pre_gate_state in ("READY", "READY(RECHECK)") and r.rendered_state not in (
            "READY",
            "READY(RECHECK)",
        ):
            if _as_dict(r.inputs).get("zone_downgrade") == "warning_stack":
                soft_polls += 1
                soft_syms.add(str(r.symbol).upper())

    durs = sorted(x[1] for x in spells)
    sub5 = sum(1 for d in durs if d < 5)
    return {
        "consistency_rows": len(rows),
        "rendered_ready_spells": len(spells),
        "spells_under_5m": sub5,
        "spells_under_5m_pct": round(100.0 * sub5 / len(spells), 1) if spells else None,
        "median_spell_min": round(durs[len(durs) // 2], 2) if durs else None,
        "spell_end_warning_stack": sum(1 for _, _, zd in spells if zd == "warning_stack"),
        "soft_kill_polls_warning_stack": soft_polls,
        "soft_kill_symbols": sorted(soft_syms),
        "soft_kill_symbol_count": len(soft_syms),
    }


def _shadow_metrics(rows: Sequence[Any]) -> Dict[str, Any]:
    outcomes: Counter = Counter()
    block_check2: set = set()
    block_check3: set = set()
    check3_only: set = set()
    dwell_ok: set = set()
    dwell_extend: set = set()
    a_block: set = set()
    b_block: set = set()
    c_block: set = set()
    live_block = 0
    live_soft_keep = 0
    shadow_n = 0
    for r in rows:
        sh = _shadow(r.inputs)
        if not sh:
            continue
        shadow_n += 1
        sym = str(r.symbol).upper()
        out = str(sh.get("outcome") or "unknown")
        outcomes[out] += 1
        if sh.get("block_check2") or (sh.get("distance") or {}).get("check2_entry_thin"):
            block_check2.add(sym)
        if sh.get("block_check3") or (sh.get("distance") or {}).get("check3_stack_thin"):
            block_check3.add(sym)
        if sh.get("check3_only") or (sh.get("distance") or {}).get("check3_only"):
            check3_only.add(sym)
        if sh.get("would_extend_dwell") or str(out).startswith("live_dwell_soft"):
            dwell_extend.add(sym)
        if sh.get("would_enter_dwell") or out in (
            "would_start_dwell",
            "would_continue_dwell",
            "live_dwell_active",
        ):
            dwell_ok.add(sym)
        sens = sh.get("threshold_sensitivity") or {}
        if (sens.get("A") or {}).get("would_block"):
            a_block.add(sym)
        if (sens.get("B") or {}).get("would_block") or (
            sh.get("live_threshold") == "B" and sh.get("distance", {}).get("would_block")
        ):
            b_block.add(sym)
        if (sens.get("C") or {}).get("would_block"):
            c_block.add(sym)
        if str(out).startswith("live_block_distance") or out == "would_block_distance":
            live_block += 1
        if "dwell_soft" in out or sh.get("would_extend_dwell"):
            live_soft_keep += 1
    return {
        "shadow_rows": shadow_n,
        "outcomes": dict(outcomes),
        "check2_syms": sorted(block_check2),
        "check3_syms": sorted(block_check3),
        "check3_only_syms": sorted(check3_only),
        "dwell_ok_syms": sorted(dwell_ok),
        "dwell_extend_syms": sorted(dwell_extend),
        "option_a_block_syms": sorted(a_block),
        "option_b_block_syms": sorted(b_block),
        "option_c_block_syms": sorted(c_block),
        "distance_block_row_events": live_block,
        "dwell_soft_row_events": live_soft_keep,
    }


def _session_bundle(db, d: str) -> Dict[str, Any]:
    rows = _load_consistency(db, d)
    return {
        "date": d,
        "churn": _removal_stats(db, d),
        "flow": _flow_metrics(rows),
        "shadow": _shadow_metrics(rows),
        "state_n": len(_load_state(db, d)),
        "rows": rows,
    }


def _print_churn_banner(churn: Dict[str, Any], d: str) -> None:
    peak = churn.get("peak_removals_per_hour") or 0
    print(f"--- Session flow / churn — {d} ---")
    print(
        f"  lock removals: total={churn.get('removals_total')}  "
        f"peak/hour={peak} (IST h={churn.get('peak_hour_ist')})  "
        f"hours={churn.get('hours')}"
    )
    if churn.get("atypical_high_churn"):
        print(
            f"  *** ATYPICAL HIGH-CHURN DAY (peak {peak}/h ≥ {ATYPICAL_PEAK_THRESHOLD}) ***"
        )
        if d == "2026-07-17":
            print(f"  {BASELINE_17JUL_NOTE}")
    elif churn.get("elevated_churn"):
        print(
            f"  note: elevated churn (peak {peak}/h ≥ {HIGH_CHURN_PEAK_THRESHOLD}/h threshold)"
        )
    else:
        print("  churn: within quieter band (<7 removals/hour peak)")
    print()


def _print_flow(flow: Dict[str, Any], label: str) -> None:
    print(f"--- Same-metric READY flow — {label} ---")
    print(f"  consistency_rows:              {flow.get('consistency_rows')}")
    print(f"  rendered READY spells:         {flow.get('rendered_ready_spells')}")
    print(
        f"  spells <5 min:                 {flow.get('spells_under_5m')} "
        f"({flow.get('spells_under_5m_pct')}%)"
    )
    print(f"  median spell (min):            {flow.get('median_spell_min')}")
    print(f"  spell ended via warning_stack: {flow.get('spell_end_warning_stack')}")
    print(
        f"  soft-kill polls (warning_stack): {flow.get('soft_kill_polls_warning_stack')} "
        f"across {flow.get('soft_kill_symbol_count')} symbols"
    )
    print()


def _print_comparison(primary: Dict[str, Any], other: Dict[str, Any], other_label: str) -> None:
    print(f"=== Same-metric comparison: {primary['date']} vs {other_label} ({other['date']}) ===")
    keys = [
        ("peak_removals_per_hour", "churn"),
        ("removals_total", "churn"),
        ("rendered_ready_spells", "flow"),
        ("spells_under_5m", "flow"),
        ("spells_under_5m_pct", "flow"),
        ("median_spell_min", "flow"),
        ("spell_end_warning_stack", "flow"),
        ("soft_kill_polls_warning_stack", "flow"),
        ("soft_kill_symbol_count", "flow"),
    ]
    print(f"  {'metric':<34} {primary['date']:>12} {other['date']:>12}")
    for key, bucket in keys:
        a = (primary.get(bucket) or {}).get(key)
        b = (other.get(bucket) or {}).get(key)
        print(f"  {key:<34} {str(a):>12} {str(b):>12}")
    if other.get("churn", {}).get("atypical_high_churn"):
        print(
            f"  WARNING: {other['date']} is atypical high-churn — "
            "do not treat deltas as 'vs normal day'."
        )
    print()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="IST session date YYYY-MM-DD (default: today)")
    ap.add_argument(
        "--baseline",
        default=None,
        help="Atypical/high-churn baseline date for same-metric compare (e.g. 2026-07-17)",
    )
    ap.add_argument(
        "--normal",
        default=None,
        help="Quieter normal-flow comparator (e.g. 2026-07-15)",
    )
    args = ap.parse_args()
    d = _session_date(args.date)

    db = SessionLocal()
    try:
        primary = _session_bundle(db, d)
        baseline = _session_bundle(db, args.baseline) if args.baseline else None
        normal = _session_bundle(db, args.normal) if args.normal else None
        state_rows = _load_state(db, d)
    finally:
        db.close()

    print(f"=== READY dwell/entry report — {d} ===")
    print()
    _print_churn_banner(primary["churn"], d)
    _print_flow(primary["flow"], d)

    if baseline:
        if baseline["date"] == "2026-07-17":
            print(f"BASELINE FLAG: {BASELINE_17JUL_NOTE}")
            print()
        _print_churn_banner(baseline["churn"], baseline["date"])
        _print_flow(baseline["flow"], f"baseline {baseline['date']}")
        _print_comparison(primary, baseline, "baseline")

    if normal:
        _print_churn_banner(normal["churn"], normal["date"])
        _print_flow(normal["flow"], f"normal-flow {normal['date']}")
        _print_comparison(primary, normal, "normal-flow")

    sh = primary["shadow"]
    print("--- Distance / dwell instrumentation ---")
    print(f"  rows with dwell_entry_shadow: {sh.get('shadow_rows')}")
    print(f"  state-table symbols:          {primary.get('state_n')}")
    print(f"  distance block row-events:    {sh.get('distance_block_row_events')}")
    print(f"  dwell soft row-events:        {sh.get('dwell_soft_row_events')}")
    print(f"  check2 symbols: {sh.get('check2_syms')}")
    print(f"  check3 symbols: {sh.get('check3_syms')}")
    print(f"  check3-ONLY:    {sh.get('check3_only_syms')}")
    print(f"  Option A block: {sh.get('option_a_block_syms')}")
    print(f"  Option B block: {sh.get('option_b_block_syms')}")
    print(f"  Option C block: {sh.get('option_c_block_syms')}")
    print()
    print("Outcome counts (row-level):")
    for k, v in Counter(sh.get("outcomes") or {}).most_common():
        print(f"  {k}: {v}")
    print()

    print(f"17-Jul flash-junk cohort cross-ref ({sorted(FLASH_JUNK_HINTS)}):")
    for sym in sorted(FLASH_JUNK_HINTS):
        tags = []
        if sym in (sh.get("check2_syms") or []):
            tags.append("check2")
        if sym in (sh.get("check3_syms") or []):
            tags.append("check3")
        if sym in (sh.get("check3_only_syms") or []):
            tags.append("check3_only")
        if sym in (sh.get("dwell_extend_syms") or []):
            tags.append("dwell_extend")
        if sym in (sh.get("dwell_ok_syms") or []):
            tags.append("dwell_ok")
        print(f"  {sym}: {', '.join(tags) if tags else 'no hit'}")
    print()
    if state_rows:
        print("State table latest outcomes:")
        for r in state_rows:
            print(
                f"  {r.symbol}: outcome={r.last_outcome} "
                f"blocked={r.distance_blocked} check3_only={r.check3_only} "
                f"since={r.shadow_ready_since}"
            )


if __name__ == "__main__":
    main()
