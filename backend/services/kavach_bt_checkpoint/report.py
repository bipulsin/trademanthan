"""Cohort summaries + recommendation stubs for Kavach BT checkpoint."""
from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any, Dict, List, Optional

from backend.services.kavach_bt_checkpoint.pullback import pb_bucket


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _cohort_metrics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rs = [_f(r.get("r_realized")) for r in rows]
    rs = [x for x in rs if x is not None]
    mfes = [_f(r.get("mfe_r")) for r in rows]
    mfes = [x for x in mfes if x is not None]
    maes = [_f(r.get("mae_r")) for r in rows]
    maes = [x for x in maes if x is not None]
    pnls = [_f(r.get("pnl")) for r in rows]
    pnls = [x for x in pnls if x is not None]
    wins = [x for x in rs if x > 0]
    return {
        "n": len(rows),
        "win_rate": round(100.0 * len(wins) / len(rs), 1) if rs else None,
        "avg_r": round(mean(rs), 4) if rs else None,
        "total_pnl": round(sum(pnls), 2) if pnls else None,
        "avg_mfe": round(mean(mfes), 4) if mfes else None,
        "avg_mae": round(mean(maes), 4) if maes else None,
    }


def build_summary_rows(details: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # overall
    m = _cohort_metrics(details)
    out.append(
        {
            "cohort_type": "overall",
            "cohort_key": "all",
            **m,
            "recommendation_text": None,
        }
    )

    # pullback v2
    by_pb: Dict[str, List] = defaultdict(list)
    for r in details:
        by_pb[pb_bucket(r.get("pb_v2"))].append(r)
    best_pb = None
    best_avg = None
    for k, rows in sorted(by_pb.items()):
        mm = _cohort_metrics(rows)
        out.append({"cohort_type": "pullback_v2", "cohort_key": k, **mm, "recommendation_text": None})
        if mm.get("avg_r") is not None and (best_avg is None or mm["avg_r"] > best_avg):
            best_avg = mm["avg_r"]
            best_pb = k

    # pullback legacy
    by_leg: Dict[str, List] = defaultdict(list)
    for r in details:
        by_leg[pb_bucket(r.get("pb_legacy"))].append(r)
    for k, rows in sorted(by_leg.items()):
        out.append(
            {
                "cohort_type": "pullback_legacy",
                "cohort_key": k,
                **_cohort_metrics(rows),
                "recommendation_text": None,
            }
        )

    # hard block
    for flag, key in ((True, "blocked"), (False, "allowed")):
        rows = [r for r in details if bool(r.get("pb_hard_blocked")) is flag]
        out.append(
            {
                "cohort_type": "pb_hard_block",
                "cohort_key": key,
                **_cohort_metrics(rows),
                "recommendation_text": None,
            }
        )

    # resistance
    for flag, key in ((True, "confluence"), (False, "clear")):
        rows = [r for r in details if bool(r.get("res_confluence")) is flag]
        out.append(
            {
                "cohort_type": "resistance",
                "cohort_key": key,
                **_cohort_metrics(rows),
                "recommendation_text": None,
            }
        )

    # exits — compare avg exit_r by method using simulated columns
    for method, col in (("A", "exit_a_r"), ("B", "exit_b_r"), ("C", "exit_c_r")):
        vals = [_f(r.get(col)) for r in details]
        vals = [v for v in vals if v is not None]
        wins = [v for v in vals if v > 0]
        out.append(
            {
                "cohort_type": "exit_method",
                "cohort_key": method,
                "n": len(vals),
                "win_rate": round(100.0 * len(wins) / len(vals), 1) if vals else None,
                "avg_r": round(mean(vals), 4) if vals else None,
                "total_pnl": None,
                "avg_mfe": None,
                "avg_mae": None,
                "recommendation_text": None,
            }
        )

    # best_exit_method distribution
    by_best: Dict[str, List] = defaultdict(list)
    for r in details:
        by_best[str(r.get("best_exit_method") or "NA")].append(r)
    for k, rows in sorted(by_best.items()):
        out.append(
            {
                "cohort_type": "best_exit",
                "cohort_key": k,
                **_cohort_metrics(rows),
                "recommendation_text": None,
            }
        )

    # Garuda
    by_g: Dict[str, List] = defaultdict(list)
    for r in details:
        by_g[str(r.get("garuda_confluence") or "NOT_AVAILABLE")].append(r)
    for k, rows in sorted(by_g.items()):
        out.append(
            {
                "cohort_type": "garuda",
                "cohort_key": k,
                **_cohort_metrics(rows),
                "recommendation_text": None,
            }
        )

    # rank-wise (MATCH only)
    by_rank: Dict[str, List] = defaultdict(list)
    for r in details:
        if r.get("garuda_confluence") != "MATCH":
            continue
        rk = r.get("garuda_rank")
        by_rank[str(rk) if rk is not None else "NA"].append(r)
    for k, rows in sorted(by_rank.items(), key=lambda x: x[0]):
        out.append(
            {
                "cohort_type": "garuda_rank",
                "cohort_key": k,
                **_cohort_metrics(rows),
                "recommendation_text": None,
            }
        )

    # recommendations
    res_on = next((x for x in out if x["cohort_type"] == "resistance" and x["cohort_key"] == "confluence"), None)
    res_off = next((x for x in out if x["cohort_type"] == "resistance" and x["cohort_key"] == "clear"), None)
    g_match = next((x for x in out if x["cohort_type"] == "garuda" and x["cohort_key"] == "MATCH"), None)
    g_nomatch = next((x for x in out if x["cohort_type"] == "garuda" and x["cohort_key"] == "NO_MATCH"), None)
    exit_rows = {x["cohort_key"]: x for x in out if x["cohort_type"] == "exit_method"}

    pb_rec = (
        f"Prefer pullback_v2={best_pb} (highest avg R among cohorts with data). "
        f"Keep pullback 5+ as hard-block pending more samples."
        if best_pb
        else "Insufficient pullback cohort data; keep 5+ hard-block in research display."
    )
    res_rec = "Keep resistance confluence warning-only."
    if res_on and res_off and res_on.get("avg_r") is not None and res_off.get("avg_r") is not None:
        if res_on["avg_r"] < res_off["avg_r"]:
            res_rec = (
                f"Resistance confluence shows weaker avg R ({res_on['avg_r']} vs {res_off['avg_r']}); "
                "keep warning-only and consider stronger grade penalty."
            )
        else:
            res_rec = (
                "Resistance confluence did not degrade avg R in this sample; remain warning-only."
            )

    exit_rec = "Compare A/B/C on dashboard; do not flip live exit yet."
    if exit_rows:
        ranked = sorted(
            ((k, v.get("avg_r")) for k, v in exit_rows.items() if v.get("avg_r") is not None),
            key=lambda x: x[1],
            reverse=True,
        )
        if ranked:
            exit_rec = (
                f"Best avg exit_r in sample: method {ranked[0][0]} ({ranked[0][1]}). "
                "If B leads, candidate for future R26 live engine after more data."
            )

    garuda_rec = "Keep Garuda shadow-only (Rule 24)."
    if (
        g_match
        and g_nomatch
        and g_match.get("avg_r") is not None
        and g_nomatch.get("avg_r") is not None
        and g_match.get("n", 0) >= 3
        and g_nomatch.get("n", 0) >= 3
    ):
        if g_match["avg_r"] > g_nomatch["avg_r"]:
            garuda_rec = (
                f"MATCH avg R ({g_match['avg_r']}) > NO_MATCH ({g_nomatch['avg_r']}); "
                "remain shadow; consider soft filter later with more data."
            )
        else:
            garuda_rec = (
                "MATCH did not beat NO_MATCH on avg R; remain unvalidated / shadow-only."
            )

    out.append(
        {
            "cohort_type": "recommendation",
            "cohort_key": "pullback",
            "n": 0,
            "win_rate": None,
            "avg_r": None,
            "total_pnl": None,
            "avg_mfe": None,
            "avg_mae": None,
            "recommendation_text": pb_rec,
        }
    )
    out.append(
        {
            "cohort_type": "recommendation",
            "cohort_key": "resistance",
            "n": 0,
            "win_rate": None,
            "avg_r": None,
            "total_pnl": None,
            "avg_mfe": None,
            "avg_mae": None,
            "recommendation_text": res_rec,
        }
    )
    out.append(
        {
            "cohort_type": "recommendation",
            "cohort_key": "exit",
            "n": 0,
            "win_rate": None,
            "avg_r": None,
            "total_pnl": None,
            "avg_mfe": None,
            "avg_mae": None,
            "recommendation_text": exit_rec,
        }
    )
    out.append(
        {
            "cohort_type": "recommendation",
            "cohort_key": "garuda",
            "n": 0,
            "win_rate": None,
            "avg_r": None,
            "total_pnl": None,
            "avg_mfe": None,
            "avg_mae": None,
            "recommendation_text": garuda_rec,
        }
    )
    return out
