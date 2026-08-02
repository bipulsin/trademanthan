#!/usr/bin/env python3
"""Step 0: Additive SQ composite threshold sensitivity on clean-10m (2026-07-27..31).

Total = 0.15*(trade_score + garuda + OW + VW + EW) + Grade_Bonus
Candidates: Garuda Top-6 + grade A/B. Informational — does not gate deploy.

  python -m backend.scripts.backtest_sq_composite_additive_20260727_31 \\
    --csv docs/diagnostics/structural_quality_backtest_v1_2_clean10m/structural_quality_backtest_clean10m.csv \\
    --out docs/diagnostics/sq_composite_additive_20260727_31
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.database import SessionLocal
from backend.services.garuda_screener.export import fetch_ready_now_promotions
from backend.services.structural_quality_score import (
    COMPONENT_WEIGHT,
    composite_total,
    grade_ab_ok,
    grade_bonus,
)

IST = pytz.timezone("Asia/Kolkata")
SESSIONS = ["2026-07-27", "2026-07-28", "2026-07-29", "2026-07-30", "2026-07-31"]
THRESHOLDS = [70, 72, 75, 78, 80, 82, 85]


def _load_csv(path: Path) -> List[Dict[str, Any]]:
    rows = []
    with path.open() as f:
        for r in csv.DictReader(f):
            if r.get("session_date") not in SESSIONS:
                continue
            if not r.get("garuda_top6_rank"):
                continue
            if not grade_ab_ok(r.get("confidence_grade")):
                continue
            rs = r.get("trade_score")
            g = r.get("garuda_rank_score")
            if rs in (None, "") or g in (None, ""):
                continue
            total = composite_total(
                rs_score=float(rs),
                garuda_score=float(g),
                ow=float(r["OW"]),
                vw=float(r["VW"]),
                ew=float(r["EW"]),
                grade=r.get("confidence_grade"),
            )
            rows.append(
                {
                    "session_date": r["session_date"],
                    "symbol": str(r["symbol"]).upper(),
                    "bar_end": r["bar_end"],
                    "bar_hhmm": r["bar_hhmm"],
                    "total": total,
                    "rs_score": float(rs),
                    "garuda_score": float(g),
                    "OW": float(r["OW"]),
                    "VW": float(r["VW"]),
                    "EW": float(r["EW"]),
                    "grade_bonus": grade_bonus(r.get("confidence_grade")),
                    "confidence_grade": r.get("confidence_grade"),
                    "garuda_top6_rank": int(r["garuda_top6_rank"]),
                }
            )
    return rows


def first_cross(
    rows: List[Dict[str, Any]], threshold: float
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    seen: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in sorted(rows, key=lambda x: (x["session_date"], x["bar_end"], x["symbol"])):
        key = (r["session_date"], r["symbol"])
        if key in seen:
            continue
        if r["total"] < threshold:
            continue
        seen[key] = r
    return seen


# First READY episodes from structural_quality_backtest.md (fallback if DB unreachable).
_FALLBACK_READY: Dict[str, List[str]] = {
    "2026-07-27": "360ONE,ABB,ADANIENSOL,BANKINDIA,DIXON,INDIGO,INFY,KFINTECH,ONGC,SHRIRAMFIN".split(","),
    "2026-07-28": "ADANIENSOL,BHEL,CGPOWER,COALINDIA,DMART,ETERNAL,INFY,LODHA,LTM,MPHASIS,NAUKRI,PERSISTENT,POWERINDIA,SBICARD,SOLARINDS,SUZLON,TATAPOWER".split(","),
    "2026-07-29": "AUROPHARMA,BHEL,CIPLA,DIVISLAB,HINDUNILVR,INFY,JINDALSTEL,KALYANKJIL,KAYNES,KPITTECH,LTF,M&M,PAGEIND,PHOENIXLTD,SAIL,SUZLON,SWIGGY,VBL".split(","),
    "2026-07-30": "360ONE,ADANIPORTS,DLF,HEROMOTOCO,KFINTECH,M&M,PAGEIND,RBLBANK,SONACOMS,TIINDIA,TVSMOTOR".split(","),
    "2026-07-31": "ABCAPITAL,BAJAJFINSV,BAJAJHLDNG,BAJFINANCE,BRITANNIA,GAIL,HYUNDAI,ITC,KALYANKJIL,KAYNES,KEI,LTM,MANKIND,SIEMENS,TIINDIA".split(","),
}


def build_actual(db) -> Dict[Tuple[str, str], Dict[str, Any]]:
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    try:
        events = fetch_ready_now_promotions(
            db, start_date=SESSIONS[0], end_date=SESSIONS[-1]
        )
        for e in events:
            if int(e.get("entry_seq") or 1) != 1:
                continue
            d = str(e.get("session_date") or "")[:10]
            sym = str(e.get("symbol") or "").upper()
            if d not in SESSIONS or not sym:
                continue
            key = (d, sym)
            if key in out:
                continue
            out[key] = {
                "session_date": d,
                "symbol": sym,
                "promoted_at": e.get("promoted_at"),
                "promoted_hhmm": None,
            }
            try:
                pt = e.get("promoted_at")
                if isinstance(pt, datetime):
                    pdt = pt.astimezone(IST) if pt.tzinfo else IST.localize(pt)
                    out[key]["promoted_hhmm"] = pdt.strftime("%H:%M")
            except Exception:
                pass
    except Exception as exc:
        print(f"DB READY fetch failed ({exc}); using documented fallback list", flush=True)
    if not out:
        for d, syms in _FALLBACK_READY.items():
            for sym in syms:
                out[(d, sym)] = {"session_date": d, "symbol": sym, "promoted_at": None, "promoted_hhmm": None}
    return out


def compare(
    proposed: Dict[Tuple[str, str], Dict[str, Any]],
    actual: Dict[Tuple[str, str], Dict[str, Any]],
) -> Dict[str, Any]:
    p, a = set(proposed), set(actual)
    tp, fp, fn = p & a, p - a, a - p
    prec = len(tp) / len(p) if p else None
    rec = len(tp) / len(a) if a else None
    return {
        "proposed_n": len(p),
        "actual_n": len(a),
        "TP": len(tp),
        "FP": len(fp),
        "FN": len(fn),
        "precision": round(prec, 4) if prec is not None else None,
        "recall": round(rec, 4) if rec is not None else None,
        "tp_list": sorted(f"{d}|{s}" for d, s in tp),
        "fp_list": sorted(f"{d}|{s}" for d, s in fp)[:30],
        "fn_list": sorted(f"{d}|{s}" for d, s in fn)[:30],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--csv",
        default="docs/diagnostics/structural_quality_backtest_v1_2_clean10m/structural_quality_backtest_clean10m.csv",
    )
    ap.add_argument("--out", default="docs/diagnostics/sq_composite_additive_20260727_31")
    args = ap.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = _load_csv(Path(args.csv))
    try:
        db = SessionLocal()
        try:
            actual = build_actual(db)
        finally:
            db.close()
    except Exception as exc:
        print(f"SessionLocal failed ({exc}); using documented fallback READY list", flush=True)
        actual = {}
        for d, syms in _FALLBACK_READY.items():
            for sym in syms:
                actual[(d, sym)] = {
                    "session_date": d,
                    "symbol": sym,
                    "promoted_at": None,
                    "promoted_hhmm": None,
                }

    curve = []
    by_day_75 = {}
    for thr in THRESHOLDS:
        prop = first_cross(rows, float(thr))
        cmp = compare(prop, actual)
        # per-day for thr
        day_stats = {}
        for day in SESSIONS:
            p_day = {k: v for k, v in prop.items() if k[0] == day}
            a_day = {k: v for k, v in actual.items() if k[0] == day}
            day_stats[day] = compare(p_day, a_day)
        entry = {"threshold": thr, **{k: cmp[k] for k in ("proposed_n", "actual_n", "TP", "FP", "FN", "precision", "recall")}, "by_day": day_stats}
        curve.append(entry)
        if thr == 75:
            by_day_75 = day_stats

    payload = {
        "formula": f"Total = {COMPONENT_WEIGHT}*(RS_trade_score + Garuda + OW + VW + EW) + Grade_Bonus",
        "grade_bonus": {"A+": 25, "A": 20, "B": 15, "C": 10, "D/D!": 0},
        "candidates": "garuda_top6_rank NOT NULL AND grade A/B AND trade_score+garuda present",
        "window": SESSIONS,
        "candidate_rows": len(rows),
        "actual_ready_first_episodes": len(actual),
        "threshold_curve": curve,
        "thr_75_by_day": by_day_75,
        "note": "Informational only — does not gate live deploy. RS_Score = trade_score (0–100).",
    }
    (out_dir / "sq_composite_additive.json").write_text(json.dumps(payload, indent=2, default=str))

    lines = [
        "# SQ Additive Composite — threshold sensitivity (2026-07-27..31 clean-10m)",
        "",
        f"Formula: `{payload['formula']}`",
        f"Candidates: {payload['candidates']}",
        f"Actual READY first episodes: **{len(actual)}** · candidate rows: **{len(rows)}**",
        "",
        "| thr | proposed | TP | FP | FN | precision | recall |",
        "|----:|---------:|---:|---:|---:|----------:|-------:|",
    ]
    for e in curve:
        lines.append(
            f"| {e['threshold']} | {e['proposed_n']} | {e['TP']} | {e['FP']} | {e['FN']} | "
            f"{e['precision']} | {e['recall']} |"
        )
    lines += ["", "## thr=75 by day", "", "| day | proposed | TP | FP | FN | precision |", "|-----|---------:|---:|---:|---:|----------:|"]
    for day in SESSIONS:
        d = by_day_75.get(day) or {}
        lines.append(
            f"| {day} | {d.get('proposed_n')} | {d.get('TP')} | {d.get('FP')} | {d.get('FN')} | {d.get('precision')} |"
        )
    j31 = by_day_75.get("2026-07-31") or {}
    lines += [
        "",
        f"**2026-07-31 @ thr=75:** precision={j31.get('precision')} "
        f"(TP {j31.get('TP')} / proposed {j31.get('proposed_n')}).",
        "",
        "Informational only — live deploy proceeds regardless.",
        "",
    ]
    (out_dir / "README.md").write_text("\n".join(lines))
    print(json.dumps({"curve": [{k: e[k] for k in e if k != "by_day"} for e in curve], "j31_75": j31}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
