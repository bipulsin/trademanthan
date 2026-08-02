#!/usr/bin/env python3
"""Top-10 hysteresis churn: B0 vs B10/B20/B35 on 2026-07-27..31.

Reconstructs per-scan directional boards from:
  - relative_strength_snapshot (Top-10 persist)
  - rs_scan_exclusion_log beyond_persist_top_n (would_be_rank + RS)

Then simulates incumbent RS bonus membership. Raw historical scores — no live API.

  python -m backend.scripts.backtest_rs_top10_hysteresis_20260727_31 \\
    --out /tmp/rs_top10_hysteresis
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.rs_universe_score_snapshot import apply_membership_ranks

IST = pytz.timezone("Asia/Kolkata")
SESSIONS = [
    "2026-07-27",
    "2026-07-28",
    "2026-07-29",
    "2026-07-30",
    "2026-07-31",
]
BONUSES = [0.0, 0.10, 0.20, 0.35]


def _ist(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return pytz.utc.localize(dt).astimezone(IST)
    return dt.astimezone(IST)


def load_boards(db, day: str) -> Dict[Any, Dict[str, List[Dict[str, Any]]]]:
    """scan_time -> {BULLISH: [rows], BEARISH: [rows]} with relative_strength + trade_score."""
    rss = db.execute(
        text(
            """
            SELECT scan_time, symbol, ranking_type, rank_position,
                   relative_strength, trade_score, confidence_grade
            FROM relative_strength_snapshot
            WHERE (scan_time AT TIME ZONE 'Asia/Kolkata')::date = CAST(:d AS date)
            ORDER BY scan_time, ranking_type, rank_position
            """
        ),
        {"d": day},
    ).mappings().all()
    excl = db.execute(
        text(
            """
            SELECT scan_time, symbol, ranking_side, would_be_rank,
                   relative_strength, trade_score, confidence_grade
            FROM rs_scan_exclusion_log
            WHERE session_date = CAST(:d AS date)
              AND exclusion_reason = 'beyond_persist_top_n'
              AND relative_strength IS NOT NULL
            ORDER BY scan_time
            """
        ),
        {"d": day},
    ).mappings().all()

    boards: Dict[Any, Dict[str, Dict[str, Dict[str, Any]]]] = defaultdict(
        lambda: {"BULLISH": {}, "BEARISH": {}}
    )
    for r in rss:
        side = str(r["ranking_type"] or "").upper()
        if "BULL" in side:
            side = "BULLISH"
        elif "BEAR" in side:
            side = "BEARISH"
        else:
            continue
        sym = str(r["symbol"] or "").upper()
        boards[r["scan_time"]][side][sym] = {
            "symbol": sym,
            "relative_strength": float(r["relative_strength"]),
            "trade_score": float(r["trade_score"] or 0),
            "confidence_grade": r["confidence_grade"],
        }
    for r in excl:
        side = str(r["ranking_side"] or "").upper()
        if "BULL" in side:
            side = "BULLISH"
        elif "BEAR" in side:
            side = "BEARISH"
        else:
            continue
        sym = str(r["symbol"] or "").upper()
        # don't overwrite RSS row
        if sym not in boards[r["scan_time"]][side]:
            boards[r["scan_time"]][side][sym] = {
                "symbol": sym,
                "relative_strength": float(r["relative_strength"]),
                "trade_score": float(r["trade_score"] or 0),
                "confidence_grade": r["confidence_grade"],
            }

    out: Dict[Any, Dict[str, List[Dict[str, Any]]]] = {}
    for st, sides in boards.items():
        out[st] = {
            "BULLISH": list(sides["BULLISH"].values()),
            "BEARISH": list(sides["BEARISH"].values()),
        }
    return out


def simulate_day(
    boards: Dict[Any, Dict[str, List[Dict[str, Any]]]], bonus: float
) -> Dict[str, Any]:
    scans = sorted(boards.keys(), key=lambda x: _ist(x))
    incumbents = {"BULLISH": set(), "BEARISH": set()}
    entries = {"BULLISH": 0, "BEARISH": 0}
    exits = {"BULLISH": 0, "BEARISH": 0}
    jaccards = {"BULLISH": [], "BEARISH": []}
    tenures: Dict[str, Dict[str, int]] = {"BULLISH": defaultdict(int), "BEARISH": defaultdict(int)}
    prev_top = {"BULLISH": set(), "BEARISH": set()}

    for st in scans:
        for side in ("BULLISH", "BEARISH"):
            ranked = apply_membership_ranks(
                [dict(r) for r in boards[st][side]],
                side=side,
                incumbents=incumbents[side],
                bonus=bonus,
            )
            top = {str(r["symbol"]).upper() for r in ranked if r.get("in_top10_membership")}
            if prev_top[side]:
                inter = len(top & prev_top[side])
                union = len(top | prev_top[side]) or 1
                jaccards[side].append(inter / union)
                entries[side] += len(top - prev_top[side])
                exits[side] += len(prev_top[side] - top)
            for s in top:
                tenures[side][s] += 1
            incumbents[side] = set(top)
            prev_top[side] = top

    def avg(xs):
        return round(sum(xs) / len(xs), 4) if xs else None

    def med_tenure(side):
        vals = list(tenures[side].values())
        if not vals:
            return None
        vals.sort()
        return vals[len(vals) // 2]

    return {
        "n_scans": len(scans),
        "bonus": bonus,
        "entries_bull": entries["BULLISH"],
        "exits_bull": exits["BULLISH"],
        "entries_bear": entries["BEARISH"],
        "exits_bear": exits["BEARISH"],
        "mean_jaccard_bull": avg(jaccards["BULLISH"]),
        "mean_jaccard_bear": avg(jaccards["BEARISH"]),
        "median_tenure_scans_bull": med_tenure("BULLISH"),
        "median_tenure_scans_bear": med_tenure("BEARISH"),
        "churn_events_bull": entries["BULLISH"] + exits["BULLISH"],
        "churn_events_bear": entries["BEARISH"] + exits["BEARISH"],
        "churn_events_total": entries["BULLISH"]
        + exits["BULLISH"]
        + entries["BEARISH"]
        + exits["BEARISH"],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument(
        "--warm-minutes-only",
        action="store_true",
        help="Keep only scans at :05/:15/:25/:35/:45/:55 (10m warm-aligned subsample).",
    )
    args = ap.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    warm_mins = {5, 15, 25, 35, 45, 55}

    db = SessionLocal()
    report: Dict[str, Any] = {
        "window": SESSIONS,
        "note": "Boards reconstructed from RSS Top-10 + beyond_persist exclusions (scored set only).",
        "bonuses": BONUSES,
        "warm_minutes_only": bool(args.warm_minutes_only),
        "by_day": {},
        "totals": {},
    }
    try:
        totals = {b: {"churn_events_total": 0, "n_scans": 0, "days": 0} for b in BONUSES}
        for day in SESSIONS:
            boards = load_boards(db, day)
            if args.warm_minutes_only:
                boards = {
                    st: sides
                    for st, sides in boards.items()
                    if _ist(st).minute in warm_mins
                }
            day_out = {}
            for b in BONUSES:
                sim = simulate_day(boards, b)
                day_out[f"B{int(b*100):02d}" if b else "B0"] = sim
                totals[b]["churn_events_total"] += sim["churn_events_total"]
                totals[b]["n_scans"] += sim["n_scans"]
                totals[b]["days"] += 1
            report["by_day"][day] = day_out
            print(day, {k: v["churn_events_total"] for k, v in day_out.items()}, flush=True)

        for b in BONUSES:
            key = f"B{int(b*100):02d}" if b else "B0"
            t = totals[b]
            report["totals"][key] = {
                "bonus": b,
                "churn_events_total": t["churn_events_total"],
                "n_scans": t["n_scans"],
                "churn_per_scan": round(t["churn_events_total"] / t["n_scans"], 3)
                if t["n_scans"]
                else None,
            }
        b0 = report["totals"]["B0"]["churn_events_total"]
        for key, t in report["totals"].items():
            if b0 and key != "B0":
                t["churn_reduction_vs_B0_pct"] = round(
                    100.0 * (1.0 - t["churn_events_total"] / b0), 1
                )
    finally:
        db.close()

    suffix = "_warm10m" if args.warm_minutes_only else ""
    (out_dir / f"hysteresis_churn{suffix}.json").write_text(
        json.dumps(report, indent=2, default=str)
    )
    lines = [
        "# RS Top-10 hysteresis churn (2026-07-27..31)"
        + (" — warm-minute subsample" if args.warm_minutes_only else ""),
        "",
        "Reconstructed boards from RSS + `beyond_persist_top_n` exclusions.",
        "",
        "| Bonus | Churn events (5d) | Per scan | vs B0 |",
        "|-------|------------------:|---------:|------:|",
    ]
    for key in sorted(report["totals"].keys()):
        t = report["totals"][key]
        lines.append(
            f"| {key} ({t['bonus']}) | {t['churn_events_total']} | {t['churn_per_scan']} | "
            f"{t.get('churn_reduction_vs_B0_pct', '—')}% |"
        )
    lines += ["", "Proposed default **B20 (0.20 RS pts)**.", ""]
    (out_dir / f"README{suffix}.md").write_text("\n".join(lines))
    print(json.dumps(report["totals"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
