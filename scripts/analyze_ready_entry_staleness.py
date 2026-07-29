#!/usr/bin/env python3
"""Backfill + summary for READY NOW entry-price staleness (read-only diagnostic).

Sources historical ``kavach_ready_consistency_log`` (+ nearest ``rs_live_kavach_audit``
for LTP/EMA when missing from inputs) into ``kavach_ready_entry_staleness_log``
with source='backfill'. Does not change live gating/pricing.

Run on paperclip:
  docker compose exec -T app python scripts/analyze_ready_entry_staleness.py
  docker compose exec -T app python scripts/analyze_ready_entry_staleness.py --no-backfill
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.database import SessionLocal  # noqa: E402
from backend.services.kavach_ready_entry_staleness_log import (  # noqa: E402
    EVENT_INITIAL,
    EVENT_RECHECK,
    TABLE,
    _attempt_from_since,
    _f,
    _as_ist,
    _ten_min_slot_index,
    ensure_ready_entry_staleness_log,
    insert_staleness_row,
)

IST = pytz.timezone("Asia/Kolkata")
OUT_JSON = _ROOT / "docs" / "diagnostics" / "ready_entry_staleness_report.json"
OUT_MD = _ROOT / "docs" / "diagnostics" / "READY_ENTRY_STALENESS_REPORT.md"
GAP_FLAG_PCT = 2.0


def _wilson(successes: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    if n <= 0:
        return (0.0, 0.0)
    p = successes / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((centre - margin) / denom, (centre + margin) / denom)


def _rate(s: int, n: int) -> Dict[str, Any]:
    if n <= 0:
        return {"count": 0, "n": 0, "pct": None}
    lo, hi = _wilson(s, n)
    return {
        "count": s,
        "n": n,
        "pct": round(100.0 * s / n, 1),
        "wilson95": [round(100 * lo, 1), round(100 * hi, 1)],
    }


def backfill(db) -> Dict[str, Any]:
    ensure_ready_entry_staleness_log()
    # Skip if already backfilled recently for same clog ids via source marker.
    existing = db.execute(
        text(f"SELECT COUNT(*) FROM {TABLE} WHERE source = 'backfill'")
    ).scalar()
    if int(existing or 0) > 0:
        return {"skipped": True, "reason": "backfill already present", "existing": int(existing)}

    rows = db.execute(
        text(
            """
            SELECT id, session_date::text AS d, UPPER(symbol) AS symbol,
                   direction, rendered_state, logged_at, inputs
            FROM kavach_ready_consistency_log
            WHERE rendered_state ILIKE 'READY%'
               OR COALESCE(inputs->>'card_visible','') IN ('true','True','1')
            ORDER BY session_date, symbol, logged_at, id
            """
        )
    ).mappings().all()

    inserted = 0
    skipped = 0
    # Track per (day,sym) for event_type / sticky computed_ts
    last_by: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for r in rows:
        inp = r["inputs"] or {}
        if isinstance(inp, str):
            try:
                inp = json.loads(inp)
            except Exception:
                inp = {}
        entry = _f(inp.get("trade_entry"))
        logged_at = _as_ist(r["logged_at"])
        if logged_at is None:
            skipped += 1
            continue

        # Prefer audit nearest for LTP/EMA
        audit = db.execute(
            text(
                """
                SELECT price, ema5, ema10, confidence_grade, trade_score,
                       ABS(EXTRACT(EPOCH FROM (computed_at - CAST(:at AS timestamptz)))) AS dt
                FROM rs_live_kavach_audit
                WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :sym
                ORDER BY ABS(EXTRACT(EPOCH FROM (computed_at - CAST(:at AS timestamptz)))) ASC
                LIMIT 1
                """
            ),
            {"d": r["d"], "sym": r["symbol"], "at": logged_at.isoformat()},
        ).mappings().fetchone()

        ltp = _f((audit or {}).get("price"))
        ema5 = _f((audit or {}).get("ema5"))
        ema10 = _f((audit or {}).get("ema10"))
        grade = (audit or {}).get("confidence_grade") or inp.get("confidence")
        score = _f((audit or {}).get("trade_score") or inp.get("trade_score"))

        key = (r["d"], r["symbol"])
        prev = last_by.get(key)

        matches = False
        if entry is not None and ema5 is not None:
            matches = abs(float(entry) - round(float(ema5), 2)) <= 0.02
        if matches:
            computed_ts = logged_at
        elif prev and prev.get("entry_price") is not None and entry is not None \
                and abs(float(prev["entry_price"]) - float(entry)) <= 0.02:
            computed_ts = prev.get("entry_price_last_computed_ts") or logged_at
        else:
            computed_ts = logged_at

        gap_pct = gap_pts = None
        if entry and entry != 0 and ltp is not None:
            gap_pts = round(float(ltp) - float(entry), 4)
            gap_pct = round(100.0 * gap_pts / float(entry), 4)

        since_raw = inp.get("ready_visible_since")
        since = _as_ist(since_raw)
        attempt = _attempt_from_since(since, logged_at)

        event_type = EVENT_INITIAL if prev is None else EVENT_RECHECK
        if prev is not None:
            prev_at = _as_ist(prev.get("logged_at"))
            if prev_at and (logged_at - prev_at).total_seconds() > 20 * 60:
                event_type = EVENT_INITIAL

        row = {
            "session_date": r["d"],
            "symbol": r["symbol"],
            "direction": (r["direction"] or "LONG").upper(),
            "logged_at": logged_at,
            "event_type": event_type,
            "attempt_number": int(attempt),
            "rendered_state": r["rendered_state"],
            "card_visible": str(inp.get("card_visible")).lower() in ("true", "1"),
            "dwell_soft_hold": str(inp.get("dwell_soft_hold")).lower() in ("true", "1"),
            "trade_take_enabled": str(inp.get("trade_take_enabled")).lower() in ("true", "1"),
            "entry_price": entry,
            "entry_price_last_computed_ts": computed_ts,
            "entry_matches_ema5": matches,
            "current_ltp": ltp,
            "gap_pct": gap_pct,
            "gap_pts": gap_pts,
            "ema5_value": round(ema5, 4) if ema5 is not None else None,
            "ema10_value": round(ema10, 4) if ema10 is not None else None,
            "confidence_grade": str(grade) if grade is not None else None,
            "trade_score": score,
            "pine_readiness": inp.get("pine_readiness"),
            "atr_pct": _f(inp.get("atr_pct")),
            "source": "backfill",
            "inputs": {
                "consistency_log_id": r["id"],
                "audit_dt_sec": (audit or {}).get("dt"),
                "ready_visible_since": since_raw,
            },
        }
        rid = insert_staleness_row(db, row)
        if rid:
            inserted += 1
            last_by[key] = row
        else:
            skipped += 1

    try:
        db.commit()
    except Exception:
        db.rollback()
    return {"skipped": False, "inserted": inserted, "row_skipped": skipped, "source_rows": len(rows)}


def summarize(db) -> Dict[str, Any]:
    ensure_ready_entry_staleness_log()
    bounds = db.execute(
        text(
            f"""
            SELECT MIN(logged_at) AS mn, MAX(logged_at) AS mx, COUNT(*) AS n,
                   COUNT(*) FILTER (WHERE source='backfill') AS n_backfill,
                   COUNT(*) FILTER (WHERE source='live') AS n_live,
                   COUNT(DISTINCT session_date) AS days,
                   COUNT(DISTINCT symbol) AS syms
            FROM {TABLE}
            """
        )
    ).mappings().one()

    rows = [
        dict(r)
        for r in db.execute(
            text(
                f"""
                SELECT session_date::text AS d, symbol, direction, logged_at,
                       event_type, attempt_number, rendered_state,
                       entry_price, entry_price_last_computed_ts, entry_matches_ema5,
                       current_ltp, gap_pct, gap_pts, ema5_value, ema10_value,
                       confidence_grade, trade_score, trade_take_enabled,
                       card_visible, dwell_soft_hold, source
                FROM {TABLE}
                ORDER BY logged_at, id
                """
            )
        ).mappings()
    ]

    rechecks = [r for r in rows if r.get("event_type") == EVENT_RECHECK]
    attempt2 = [r for r in rechecks if int(r.get("attempt_number") or 0) >= 2]
    initials = [r for r in rows if r.get("event_type") == EVENT_INITIAL]

    def gap_dist(subset: List[Dict[str, Any]]) -> Dict[str, Any]:
        gaps = [abs(float(r["gap_pct"])) for r in subset if r.get("gap_pct") is not None]
        if not gaps:
            return {"n": 0}
        gaps_sorted = sorted(gaps)
        def pctile(p: float) -> float:
            i = int(round((len(gaps_sorted) - 1) * p))
            return round(gaps_sorted[i], 3)
        return {
            "n": len(gaps),
            "p50": pctile(0.5),
            "p75": pctile(0.75),
            "p90": pctile(0.9),
            "p95": pctile(0.95),
            "max": round(max(gaps), 3),
            "ge_1pct": _rate(sum(1 for g in gaps if g >= 1.0), len(gaps)),
            "ge_2pct": _rate(sum(1 for g in gaps if g >= 2.0), len(gaps)),
            "ge_5pct": _rate(sum(1 for g in gaps if g >= 5.0), len(gaps)),
        }

    # % of rechecks where entry not recalculated vs prior (sticky computed_ts)
    sticky = 0
    sticky_n = 0
    by_sym_day: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_sym_day[(r["d"], r["symbol"])].append(r)

    for key, group in by_sym_day.items():
        group = sorted(group, key=lambda x: x["logged_at"])
        for i in range(1, len(group)):
            cur, prev = group[i], group[i - 1]
            if cur.get("event_type") != EVENT_RECHECK:
                continue
            sticky_n += 1
            cts = _as_ist(cur.get("entry_price_last_computed_ts"))
            pts = _as_ist(prev.get("entry_price_last_computed_ts"))
            if cts is not None and pts is not None and abs((cts - pts).total_seconds()) < 1.0:
                sticky += 1
            elif (
                cur.get("entry_price") is not None
                and prev.get("entry_price") is not None
                and abs(float(cur["entry_price"]) - float(prev["entry_price"])) <= 0.02
                and cur.get("entry_matches_ema5") is False
            ):
                sticky += 1

    # Flag symbol-days with gap≥2% while take enabled or card visible (actionable countdown proxy)
    flagged = []
    for (d, sym), group in by_sym_day.items():
        hot = [
            r
            for r in group
            if r.get("gap_pct") is not None
            and abs(float(r["gap_pct"])) >= GAP_FLAG_PCT
            and (
                r.get("trade_take_enabled")
                or r.get("card_visible")
                or str(r.get("rendered_state") or "").startswith("READY")
            )
        ]
        if not hot:
            continue
        best = max(hot, key=lambda x: abs(float(x["gap_pct"])))
        flagged.append(
            {
                "d": d,
                "symbol": sym,
                "max_abs_gap_pct": round(abs(float(best["gap_pct"])), 3),
                "at": str(best["logged_at"]),
                "entry": best.get("entry_price"),
                "ltp": best.get("current_ltp"),
                "ema5": best.get("ema5_value"),
                "entry_matches_ema5": best.get("entry_matches_ema5"),
                "attempt": best.get("attempt_number"),
                "event_type": best.get("event_type"),
                "grade": best.get("confidence_grade"),
                "score": best.get("trade_score"),
                "take_enabled": best.get("trade_take_enabled"),
                "state": best.get("rendered_state"),
                "n_flagged_events": len(hot),
            }
        )
    flagged.sort(key=lambda x: -x["max_abs_gap_pct"])

    # entry not matching ema5 on READY cards
    mismatch = [
        r
        for r in rows
        if r.get("entry_matches_ema5") is False
        and str(r.get("rendered_state") or "").startswith("READY")
        and r.get("entry_price") is not None
        and r.get("ema5_value") is not None
    ]

    return {
        "generated_at": datetime.now(IST).isoformat(),
        "coverage": {
            "min": str(bounds["mn"]) if bounds["mn"] else None,
            "max": str(bounds["mx"]) if bounds["mx"] else None,
            "n": int(bounds["n"] or 0),
            "n_backfill": int(bounds["n_backfill"] or 0),
            "n_live": int(bounds["n_live"] or 0),
            "days": int(bounds["days"] or 0),
            "symbols": int(bounds["syms"] or 0),
        },
        "counts": {
            "initial_promotion": len(initials),
            "recheck": len(rechecks),
            "recheck_attempt_ge_2": len(attempt2),
        },
        "gap_pct_distribution": {
            "all": gap_dist(rows),
            "recheck": gap_dist(rechecks),
            "recheck_attempt_ge_2": gap_dist(attempt2),
            "initial_promotion": gap_dist(initials),
        },
        "entry_not_recalculated_on_recheck": _rate(sticky, sticky_n),
        "entry_not_matching_ema5_ready": {
            "count": len(mismatch),
            "n_ready_with_ema": sum(
                1
                for r in rows
                if str(r.get("rendered_state") or "").startswith("READY")
                and r.get("ema5_value") is not None
                and r.get("entry_price") is not None
            ),
        },
        "flagged_symbol_days_gap_ge_2pct": flagged[:50],
        "flagged_count": len(flagged),
        "definitions": {
            "gap_pct": "(ltp - entry_price) / entry_price × 100",
            "entry_price_last_computed_ts": (
                "set to event time when entry≈EMA5 (±0.02); else carried from prior "
                "same entry value (sticky)"
            ),
            "attempt_number": (
                "1 + 10m IST slots crossed since ready_visible_since "
                "(frontend Enter-within attempt proxy)"
            ),
            "event_type": "initial_promotion | recheck",
            "flag_threshold_pct": GAP_FLAG_PCT,
            "note": (
                "Frontend 'Recheck confirmed · attempt N' is sessionStorage-based; "
                "backend attempt_number approximates that via 10m slot crossings. "
                "Live dwell overrides trade_entry to live EMA5 when available — "
                "large gap with entry_matches_ema5=true means price ran away from EMA5 "
                "while card still READY/soft-held."
            ),
        },
    }


def write_md(summary: Dict[str, Any]) -> None:
    cov = summary["coverage"]
    g_all = summary["gap_pct_distribution"]["all"]
    g_r = summary["gap_pct_distribution"]["recheck"]
    g_a2 = summary["gap_pct_distribution"]["recheck_attempt_ge_2"]
    sticky = summary["entry_not_recalculated_on_recheck"]
    lines = [
        "# READY NOW entry-price staleness (shadow diagnostic)",
        "",
        f"**Generated:** {summary['generated_at']}",
        "",
        "Instrumentation only — no live entry/gating/countdown changes.",
        "",
        "## Coverage",
        "",
        f"| Metric | Value |",
        f"|---|---|",
        f"| Rows | {cov['n']} (backfill {cov['n_backfill']}, live {cov['n_live']}) |",
        f"| Window | {cov['min']} → {cov['max']} |",
        f"| Days / symbols | {cov['days']} / {cov['symbols']} |",
        f"| Initial promotions | {summary['counts']['initial_promotion']} |",
        f"| Rechecks | {summary['counts']['recheck']} (attempt≥2: {summary['counts']['recheck_attempt_ge_2']}) |",
        "",
        "## gap_pct distribution",
        "",
        "| Cohort | n | p50 | p90 | ≥1% | ≥2% | ≥5% | max |",
        "|---|---|---|---|---|---|---|---|",
    ]

    def row(label: str, g: Dict[str, Any]) -> str:
        if not g.get("n"):
            return f"| {label} | 0 | — | — | — | — | — | — |"
        return (
            f"| {label} | {g['n']} | {g['p50']} | {g['p90']} | "
            f"{g['ge_1pct']['pct']}% | {g['ge_2pct']['pct']}% | {g['ge_5pct']['pct']}% | {g['max']} |"
        )

    lines.append(row("All events", g_all))
    lines.append(row("Recheck", g_r))
    lines.append(row("Recheck attempt≥2", g_a2))
    lines.extend(
        [
            "",
            "## Entry not recalculated across recheck",
            "",
            f"**{sticky.get('pct')}%** ({sticky.get('count')}/{sticky.get('n')}) of recheck "
            "events carried the same `entry_price_last_computed_ts` as the prior event "
            "(or entry unchanged while `entry_matches_ema5=false`).",
            "",
            f"READY cards with entry ≠ live EMA5: "
            f"{summary['entry_not_matching_ema5_ready']['count']} / "
            f"{summary['entry_not_matching_ema5_ready']['n_ready_with_ema']}.",
            "",
            f"## Flagged symbol-days (|gap_pct| ≥ {GAP_FLAG_PCT}% while READY/visible)",
            "",
            f"Count: **{summary['flagged_count']}**",
            "",
        ]
    )
    flagged = summary.get("flagged_symbol_days_gap_ge_2pct") or []
    if flagged:
        lines.append(
            "| Date | Symbol | Max |gap|% | Entry | LTP | EMA5 | Match EMA5? | Attempt | Grade | Take? |"
        )
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        for f in flagged[:30]:
            lines.append(
                f"| {f['d']} | {f['symbol']} | {f['max_abs_gap_pct']} | {f['entry']} | "
                f"{f['ltp']} | {f['ema5']} | {f['entry_matches_ema5']} | {f['attempt']} | "
                f"{f['grade']} | {f['take_enabled']} |"
            )
    else:
        lines.append("_None in this window._")
    lines.extend(
        [
            "",
            "## Definitions",
            "",
            json.dumps(summary.get("definitions") or {}, indent=2),
            "",
            "## Artifacts",
            "",
            "- `docs/diagnostics/ready_entry_staleness_report.json`",
            "- Table: `kavach_ready_entry_staleness_log`",
            "- Runner: `scripts/analyze_ready_entry_staleness.py`",
            "",
        ]
    )
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-backfill", action="store_true")
    args = ap.parse_args()
    db = SessionLocal()
    try:
        ensure_ready_entry_staleness_log()
        bf = {"skipped": True, "reason": "--no-backfill"}
        if not args.no_backfill:
            print("BACKFILL…")
            bf = backfill(db)
            print("BACKFILL", json.dumps(bf, default=str))
        print("SUMMARIZE…")
        summary = summarize(db)
        summary["backfill"] = bf
        OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        OUT_JSON.write_text(json.dumps(summary, indent=2, default=str))
        write_md(summary)
        print("WROTE", OUT_JSON)
        print("WROTE", OUT_MD)
        print("FLAGGED", summary["flagged_count"])
        print("STICKY", summary["entry_not_recalculated_on_recheck"])
        print("GAP_RECHECK", summary["gap_pct_distribution"]["recheck"])
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
