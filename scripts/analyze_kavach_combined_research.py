#!/usr/bin/env python3
"""Kavach combined research package — Parts A & B (read-only, no live changes).

Uses stored tables only: relative_strength_snapshot, rs_scanner_history,
rs_anchor_snapshot, rs_fast_watch, rs_conviction_promotion_log, daily_snapshot.

Run: PYTHONPATH=. python3 scripts/analyze_kavach_combined_research.py
"""
from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402

from backend.database import SessionLocal  # noqa: E402
from backend.services.kavach_momentum_ignition_validate import (  # noqa: E402
    _credibility_label,
    _wilson_ci,
)
from scripts.analyze_rs_fast_watch_discard import (  # noqa: E402
    BULL_EQ,
    BEAR_EQ,
    _analyze,
    _buy_eq,
    _core_members,
    _morning_locked,
    _production_scope,
    _side_from_ranking,
)
from scripts.analyze_rs_selection_quality import (  # noqa: E402
    SESSION_OPEN_MIN,
    _atr_rupees,
    _load_maturity,
    _load_selections,
    _load_session_end,
    classify_outcome,
)

CASE_SYMBOLS = ("NAUKRI", "SWIGGY", "JUBLFOOD")
RELOCK_CHECKPOINTS = (
    ("09:25", 9 * 60 + 25),
    ("09:45", 9 * 60 + 45),
    ("10:15", 10 * 60 + 15),
    ("10:45", 10 * 60 + 45),
)


def _rate(items: List[str], label: str) -> Dict[str, Any]:
    n = len(items)
    if not n:
        return {"n": 0, "rate": None, "wilson_ci": [None, None], "credibility": "insufficient_sample"}
    hits = sum(1 for x in items if x == label)
    rate = round(hits / n, 4)
    lo, hi = _wilson_ci(hits, n)
    return {"n": n, "hits": hits, "rate": rate, "wilson_ci": [lo, hi]}


def _flip_rate_by_maturity(db) -> Dict[str, Any]:
    picks = _load_selections(db)
    by_tag: Dict[str, List[str]] = defaultdict(list)
    fresh_cont: List[str] = []
    ext_str: List[str] = []
    for p in picks:
        mat = _load_maturity(db, p["session_date"], p["symbol"])
        end = _load_session_end(db, p["session_date"], p["symbol"])
        if not end or end["end_price"] <= 0:
            continue
        atr_r = _atr_rupees(p["sel_price"], mat["atr14_pct"])
        outcome = classify_outcome(
            p["side"], p["sel_price"], end["end_price"], atr_r,
            sel_ema10=p.get("sel_ema10"), end_ema10=end.get("end_ema10"),
        )
        tag = (mat.get("maturity_tag") or "FRESH").upper()
        by_tag[tag].append(outcome)
        if tag in ("FRESH", "CONTINUING"):
            fresh_cont.append(outcome)
        elif tag in ("EXTENDED", "STRETCHED", "CLIMACTIC"):
            ext_str.append(outcome)

    baseline_flip = _rate([o for tags in by_tag.values() for o in tags], "flip")
    base_rate = baseline_flip["rate"]

    def tag_block(outcomes: List[str]) -> Dict[str, Any]:
        flip = _rate(outcomes, "flip")
        ft = _rate(outcomes, "followed_through")
        side = _rate(outcomes, "sideways")
        flip["vs_baseline_flip_credibility"] = _credibility_label(
            flip.get("hits") or 0, flip["n"], base_rate
        )
        return {
            "outcomes": dict((k, outcomes.count(k)) for k in sorted(set(outcomes))),
            "flip": flip,
            "followed_through": ft,
            "sideways": side,
        }

    return {
        "n_picks": sum(len(v) for v in by_tag.values()),
        "by_tag": {k: tag_block(v) for k, v in sorted(by_tag.items())},
        "fresh_continuing_combined": tag_block(fresh_cont),
        "extended_stretched_combined": tag_block(ext_str),
        "baseline_flip_rate": baseline_flip,
        "interpretation": (
            "Higher flip rate in EXTENDED/STRETCHED supports maturity tag as reversal-risk signal; "
            "if CIs overlap baseline, tag has limited standalone predictive value."
        ),
    }


def _load_first_flips(db) -> List[Dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT scan_time::date AS session_date, scan_time, symbol,
                   ranking_type, kavach_state, trade_score, confidence_grade
            FROM relative_strength_snapshot
            WHERE kavach_state IS NOT NULL
              AND EXTRACT(HOUR FROM scan_time AT TIME ZONE 'Asia/Kolkata') * 60
                  + EXTRACT(MINUTE FROM scan_time AT TIME ZONE 'Asia/Kolkata') >= 555
            ORDER BY session_date, symbol, scan_time
            """
        )
    ).fetchall()
    first_flip: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for r in rows:
        side = _side_from_ranking(r.ranking_type)
        if not _buy_eq(r.kavach_state, side):
            continue
        key = (str(r.session_date), r.symbol, side)
        if key not in first_flip:
            first_flip[key] = {
                "session_date": str(r.session_date),
                "symbol": r.symbol,
                "side": side,
                "first_flip_time": r.scan_time,
                "kavach_state": r.kavach_state,
            }
    return list(first_flip.values())


def _ever_top5_that_day(db, session_date: str, symbol: str) -> bool:
    r = db.execute(
        text(
            """
            SELECT 1 FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date) AND symbol = :sym
              AND rank_position <= 5 LIMIT 1
            """
        ),
        {"d": session_date, "sym": symbol},
    ).fetchone()
    return r is not None


def _expanded_scope(db, session_date: str, symbol: str) -> bool:
    """Any symbol that touched top-5 intraday (widest scope measurable from snapshots)."""
    return _ever_top5_that_day(db, session_date, symbol)


def _full_universe_discard_proxy(db) -> Dict[str, Any]:
    events = _load_first_flips(db)
    core_cache: Dict[Tuple[str, str], Set[str]] = {}
    for ev in events:
        ck = (ev["session_date"], ev["side"])
        if ck not in core_cache:
            core_cache[ck] = _core_members(db, ev["session_date"], ev["side"])

    locked_or_top5 = [e for e in events if _production_scope(
        db, e["session_date"], e["symbol"], e["first_flip_time"]
    )]
    expanded = [e for e in events if _expanded_scope(db, e["session_date"], e["symbol"])]

    prod = _analyze(locked_or_top5, core_cache)
    exp = _analyze(expanded, core_cache)

    # Missed: symbols with meaningful session move that NEVER appear in top-5 that day
    missed_rows = db.execute(
        text(
            """
            WITH day_syms AS (
                SELECT DISTINCT scan_time::date AS d, symbol
                FROM relative_strength_snapshot
            ),
            movers AS (
                SELECT scan_time::date AS d, symbol,
                       MAX(current_price) - MIN(current_price) AS range_abs,
                       MIN(current_price) AS lo, MAX(current_price) AS hi
                FROM relative_strength_snapshot
                WHERE current_price > 0
                GROUP BY scan_time::date, symbol
            ),
            never_top5 AS (
                SELECT m.d, m.symbol, m.range_abs, m.lo, m.hi
                FROM movers m
                LEFT JOIN (
                    SELECT DISTINCT scan_time::date AS d, symbol
                    FROM relative_strength_snapshot WHERE rank_position <= 5
                ) t ON t.d = m.d AND t.symbol = m.symbol
                WHERE t.symbol IS NULL AND m.range_abs / NULLIF(m.lo, 0) > 0.015
            )
            SELECT * FROM never_top5 ORDER BY d DESC, range_abs DESC LIMIT 20
            """
        )
    ).fetchall()

    return {
        "data_limitation": (
            "Full 203-symbol Kavach flips are NOT persisted. Snapshot history only contains "
            "symbols while in top-5 (~10 rows/scan). 'expanded_intraday_top5' is the widest "
            "measurable scope without candle replay. True full-universe discard requires "
            "recomputing Kavach from cached candles (after-hours only)."
        ),
        "locked_or_top5_scope": prod,
        "expanded_intraday_top5_scope": exp,
        "never_top5_large_move_sample": [
            {"date": str(r.d), "symbol": r.symbol, "range_pct": round(r.range_abs / r.lo * 100, 2)}
            for r in missed_rows[:10]
        ],
        "note_on_never_top5_query": (
            "Query only sees symbols that appear in ANY snapshot row (still top-5 history). "
            "Cannot detect symbols that never ranked at all."
        ),
    }


def _case_study_symbols(db) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for sym in CASE_SYMBOLS:
        rows = db.execute(
            text(
                """
                SELECT scan_time, ranking_type, rank_position, relative_strength,
                       kavach_state, trade_score, current_price
                FROM relative_strength_snapshot
                WHERE symbol = :sym
                ORDER BY scan_time
                """
            ),
            {"sym": sym},
        ).fetchall()
        lock_rows = db.execute(
            text(
                """
                SELECT snapshot_date, direction FROM daily_snapshot
                WHERE symbol = :sym ORDER BY snapshot_date
                """
            ),
            {"sym": sym},
        ).fetchall()
        fw = db.execute(
            text(
                "SELECT session_date, direction, first_flip_at FROM rs_fast_watch WHERE symbol = :sym"
            ),
            {"sym": sym},
        ).fetchall()
        scans = []
        for r in rows:
            t = r.scan_time
            mins = t.astimezone(__import__("pytz").timezone("Asia/Kolkata")).hour * 60 + t.minute
            scans.append({
                "date": str(t.date()),
                "ist_time": t.astimezone(__import__("pytz").timezone("Asia/Kolkata")).strftime("%H:%M"),
                "mins": mins,
                "side": r.ranking_type,
                "rank": int(r.rank_position or 0),
                "rs": float(r.relative_strength) if r.relative_strength is not None else None,
                "kavach": r.kavach_state,
                "score": r.trade_score,
            })
        first_top5 = next((s for s in scans if s["rank"] <= 5), None)
        out[sym] = {
            "morning_locks": [{"date": str(l.snapshot_date), "dir": l.direction} for l in lock_rows],
            "fast_watch": [{"date": str(f.session_date), "dir": f.direction, "flip": str(f.first_flip_at)} for f in fw],
            "first_top5_appearance": first_top5,
            "scan_count": len(scans),
            "recent_scans": scans[-8:],
        }
    return out


def _vw_score(rs: float, vol_ratio: float, side: str) -> float:
    """Defensible volume-weight: RS scaled by capped volume_ratio (0.6–2.0)."""
    mult = min(2.0, max(0.6, float(vol_ratio or 1.0)))
    if side == "BEAR":
        return -float(rs) * mult  # lower RS wins; invert for ranking
    return float(rs) * mult


def _volume_weighted_backtest(db) -> Dict[str, Any]:
    """B1: at morning first-scan, compare actual vs volume-weighted top-5 among intraday candidates."""
    days = db.execute(
        text(
            """
            SELECT DISTINCT scan_time::date AS d FROM relative_strength_snapshot ORDER BY d
            """
        )
    ).fetchall()
    actual_outcomes: List[str] = []
    vw_outcomes: List[str] = []
    changed_picks = 0
    total_picks = 0
    rs_inverted_wins = 0  # lower RS% actual pick that followed through

    for day in days:
        sd = str(day.d)
        for ranking in ("BULLISH", "BEARISH"):
            side = "BEAR" if ranking == "BEARISH" else "BULL"
            # First scan >= 09:20
            first = db.execute(
                text(
                    """
                    SELECT MIN(scan_time) AS t FROM relative_strength_snapshot
                    WHERE scan_time::date = CAST(:d AS date) AND ranking_type = :rt
                      AND EXTRACT(HOUR FROM scan_time AT TIME ZONE 'Asia/Kolkata') * 60
                          + EXTRACT(MINUTE FROM scan_time AT TIME ZONE 'Asia/Kolkata') >= :open
                    """
                ),
                {"d": sd, "rt": ranking, "open": SESSION_OPEN_MIN},
            ).scalar()
            if not first:
                continue

            # Candidate pool: all symbols seen in top-5 during 09:20–10:00 window
            pool_rows = db.execute(
                text(
                    """
                    SELECT DISTINCT ON (symbol) symbol, relative_strength, volume_ratio,
                           current_price, ema10, rank_position
                    FROM relative_strength_snapshot
                    WHERE scan_time::date = CAST(:d AS date) AND ranking_type = :rt
                      AND rank_position <= 5
                      AND EXTRACT(HOUR FROM scan_time AT TIME ZONE 'Asia/Kolkata') * 60
                          + EXTRACT(MINUTE FROM scan_time AT TIME ZONE 'Asia/Kolkata')
                          BETWEEN :open AND :end
                    ORDER BY symbol, scan_time
                    """
                ),
                {"d": sd, "rt": ranking, "open": SESSION_OPEN_MIN, "end": 10 * 60},
            ).mappings().all()

            if len(pool_rows) < 5:
                continue

            actual = sorted(pool_rows, key=lambda r: int(r["rank_position"] or 99))[:5]
            actual_syms = {r["symbol"] for r in actual}

            vw_ranked = sorted(
                pool_rows,
                key=lambda r: _vw_score(
                    float(r["relative_strength"] or 0),
                    float(r["volume_ratio"] or 1),
                    side,
                ),
                reverse=(side == "BULL"),
            )[:5]
            vw_syms = {r["symbol"] for r in vw_ranked}

            for act, vw in zip(actual, vw_ranked):
                total_picks += 1
                if act["symbol"] != vw["symbol"]:
                    changed_picks += 1
                for sym_row, bucket in ((act, actual_outcomes), (vw, vw_outcomes)):
                    sym = sym_row["symbol"]
                    price = float(sym_row["current_price"] or 0)
                    end = _load_session_end(db, sd, sym)
                    mat = _load_maturity(db, sd, sym)
                    if not end or price <= 0:
                        continue
                    atr_r = _atr_rupees(price, mat["atr14_pct"])
                    outcome = classify_outcome(side, price, end["end_price"], atr_r)
                    bucket.append(outcome)
                    if bucket is actual_outcomes and outcome == "followed_through":
                        rs_vals = [float(r["relative_strength"] or 0) for r in pool_rows]
                        if side == "BULL" and float(act["relative_strength"] or 0) < sorted(rs_vals, reverse=True)[2]:
                            rs_inverted_wins += 1

    base_ft = _rate(actual_outcomes, "followed_through")
    vw_ft = _rate(vw_outcomes, "followed_through")
    base_flip = _rate(actual_outcomes, "flip")
    vw_flip = _rate(vw_outcomes, "flip")

    return {
        "formula": "vw_score = RS% × clamp(volume_ratio, 0.6, 2.0); BEAR uses −RS% × mult",
        "candidate_pool": "Symbols appearing in top-5 during 09:20–10:00 (stored data limit)",
        "total_pick_slots_compared": total_picks,
        "slots_where_vw_changes_symbol": changed_picks,
        "change_rate": round(changed_picks / total_picks, 3) if total_picks else None,
        "actual_morning_top5": {
            "followed_through": base_ft,
            "flip": base_flip,
            "sideways": _rate(actual_outcomes, "sideways"),
        },
        "volume_weighted_top5": {
            "followed_through": vw_ft,
            "flip": vw_flip,
            "ft_lift_vs_actual": (
                round((vw_ft["rate"] or 0) - (base_ft["rate"] or 0), 4)
                if vw_ft["rate"] is not None and base_ft["rate"] is not None else None
            ),
            "ft_credibility_vs_actual": _credibility_label(
                vw_ft.get("hits") or 0, vw_ft["n"], base_ft["rate"]
            ),
        },
        "inverted_rs_follow_through_count": rs_inverted_wins,
        "volume_confounding_note": (
            "If vw ranking improves FT rate with credible CI, Monday's inverted-RS pattern may be "
            "partially volume-confounded. Small sample — treat as exploratory."
        ),
    }


def _nearest_scan_top5(db, session_date: str, ranking: str, target_min: int) -> Optional[Dict[str, Any]]:
    row = db.execute(
        text(
            """
            SELECT scan_time,
                   EXTRACT(HOUR FROM scan_time AT TIME ZONE 'Asia/Kolkata') * 60
                     + EXTRACT(MINUTE FROM scan_time AT TIME ZONE 'Asia/Kolkata') AS mins
            FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date) AND ranking_type = :rt
            GROUP BY scan_time
            ORDER BY ABS(
                (EXTRACT(HOUR FROM scan_time AT TIME ZONE 'Asia/Kolkata') * 60
                 + EXTRACT(MINUTE FROM scan_time AT TIME ZONE 'Asia/Kolkata')) - :tgt
            ), scan_time
            LIMIT 1
            """
        ),
        {"d": session_date, "rt": ranking, "tgt": target_min},
    ).mappings().first()
    if not row:
        return None
    syms = db.execute(
        text(
            """
            SELECT symbol, rank_position, relative_strength, current_price, ema10
            FROM relative_strength_snapshot
            WHERE scan_time = :t AND ranking_type = :rt AND rank_position <= 5
            ORDER BY rank_position
            """
        ),
        {"t": row["scan_time"], "rt": ranking},
    ).mappings().all()
    return {
        "target_label": next(l for l, m in RELOCK_CHECKPOINTS if m == target_min),
        "scan_time": str(row["scan_time"]),
        "ist_mins": int(row["mins"]),
        "symbols": [s["symbol"] for s in syms],
        "rows": [dict(s) for s in syms],
    }


def _relock_timing_backtest(db) -> Dict[str, Any]:
    days = [str(r.d) for r in db.execute(
        text("SELECT DISTINCT scan_time::date AS d FROM relative_strength_snapshot ORDER BY d")
    ).fetchall()]

    churn_by_cp: Dict[str, List[int]] = defaultdict(list)
    catches: Dict[str, Dict[str, Optional[str]]] = {s: {} for s in CASE_SYMBOLS}
    outcome_by_cp: Dict[str, List[str]] = defaultdict(list)

    morning_lock = {}
    for sd in days:
        locks = _morning_locked(db, sd)
        morning_lock[sd] = locks
        prev_bull: Set[str] = set()
        prev_bear: Set[str] = set()
        for label, tgt in RELOCK_CHECKPOINTS:
            for ranking, side, prev in (
                ("BULLISH", "BULL", prev_bull),
                ("BEARISH", "BEAR", prev_bear),
            ):
                snap = _nearest_scan_top5(db, sd, ranking, tgt)
                if not snap:
                    continue
                syms = set(snap["symbols"])
                if prev:
                    churn_by_cp[label].append(len(syms - prev))
                if ranking == "BULLISH":
                    prev_bull = syms
                else:
                    prev_bear = syms
                for cs in CASE_SYMBOLS:
                    if cs in syms and cs not in catches[cs]:
                        catches[cs][sd] = label
                for row in snap["rows"]:
                    sym = row["symbol"]
                    price = float(row["current_price"] or 0)
                    end = _load_session_end(db, sd, sym)
                    mat = _load_maturity(db, sd, sym)
                    if end and price > 0:
                        outcome_by_cp[label].append(
                            classify_outcome(
                                side, price, end["end_price"],
                                _atr_rupees(price, mat["atr14_pct"]),
                                sel_ema10=row.get("ema10"),
                                end_ema10=end.get("end_ema10"),
                            )
                        )

    cp_summary = {}
    for label, _ in RELOCK_CHECKPOINTS:
        outs = outcome_by_cp[label]
        cp_summary[label] = {
            "avg_new_symbols_vs_prior": (
                round(sum(churn_by_cp[label]) / len(churn_by_cp[label]), 2)
                if churn_by_cp[label] else None
            ),
            "followed_through": _rate(outs, "followed_through"),
            "flip": _rate(outs, "flip"),
            "sideways": _rate(outs, "sideways"),
        }

    return {
        "checkpoints": [l for l, _ in RELOCK_CHECKPOINTS],
        "session_days": len(days),
        "checkpoint_summary": cp_summary,
        "case_symbol_first_checkpoint_in_top5": catches,
        "morning_lock_vs_0945_note": (
            "09:25 uses nearest scan to checkpoint; morning lock is daily_snapshot at 09:25 IST."
        ),
    }


def _combined_b1_b2(db, b1: Dict, b2: Dict) -> Dict[str, Any]:
    b1_lift = (b1.get("volume_weighted_top5") or {}).get("ft_lift_vs_actual")
    b1_cred = (b1.get("volume_weighted_top5") or {}).get("ft_credibility_vs_actual")
    cp = b2.get("checkpoint_summary") or {}
    early = cp.get("09:45") or {}
    late = cp.get("09:25") or {}
    relock_ft_lift = None
    if early.get("followed_through", {}).get("rate") and late.get("followed_through", {}).get("rate"):
        relock_ft_lift = round(
            early["followed_through"]["rate"] - late["followed_through"]["rate"], 4
        )
    both_promising = (
        (b1_lift or 0) > 0.02 and b1_cred == "credible_positive"
    ) or (relock_ft_lift or 0) > 0.05
    return {
        "b1_ft_lift": b1_lift,
        "b1_credibility": b1_cred,
        "b2_0945_vs_0925_ft_lift": relock_ft_lift,
        "combined_test_feasible": both_promising,
        "combined_note": (
            "With only ~8 session days of snapshot history, combined B3 is underpowered. "
            "If both effects look positive in shadow run, test intersection live rather than "
            "historically here."
        ),
    }


def _recommendations(a2: Dict, a4: Dict, b1: Dict, b2: Dict, b3: Dict) -> Dict[str, Any]:
    prod_dr = (a2.get("locked_or_top5_scope") or {}).get("discard_rate")
    exp_dr = (a2.get("expanded_intraday_top5_scope") or {}).get("discard_rate")
    ext_flip = ((a4.get("extended_stretched_combined") or {}).get("flip") or {}).get("rate")
    fresh_flip = ((a4.get("fresh_continuing_combined") or {}).get("flip") or {}).get("rate")

    a3 = "Do not widen Fast Watch scope beyond locked_or_top5."
    if exp_dr is not None and prod_dr is not None and exp_dr < prod_dr + 0.05:
        a3 = (
            "Expanded intraday-top-5 scope does not materially improve discard rate vs "
            "locked_or_top5. Widening to full universe likely increases noise further — "
            "keep locked_or_top5; consider after-hours candle replay to quantify true "
            "full-universe miss rate before any surfacing change."
        )

    b4_change = "Insufficient evidence for live selection change; continue shadow data collection."
    if (b1.get("volume_weighted_top5") or {}).get("ft_credibility_vs_actual") == "credible_positive":
        b4_change = (
            "Volume-weighting shows credible FT lift in exploratory window — pursue shadow "
            "parallel Top-5 (volume-weighted) logged alongside live RS for 3–4 weeks before "
            "any swap."
        )

    return {
        "part_a3_surfacing": a3,
        "part_a4_maturity": (
            f"EXTENDED/STRETCHED flip rate {ext_flip} vs FRESH/CONTINUING {fresh_flip} — "
            "see Wilson CIs in a4; use as soft conviction reducer unless CI credibly higher."
        ),
        "part_b4_selection": b4_change,
        "shadow_run_proposal": {
            "duration_weeks": 4,
            "components": [
                "Log volume-weighted shadow Top-5 at 09:25, 09:45, 10:15 alongside live lock",
                "Persist full-universe Kavach state 1×/day after close (no intraday API load)",
                "Compare shadow vs live outcomes weekly; no user-facing change until CI credible",
            ],
            "never": "Direct swap of 09:25 lock based on backtest alone",
        },
    }


def main() -> int:
    db = SessionLocal()
    try:
        a2 = _full_universe_discard_proxy(db)
        a4 = _flip_rate_by_maturity(db)
        cases = _case_study_symbols(db)
        b1 = _volume_weighted_backtest(db)
        b2 = _relock_timing_backtest(db)
        b3 = _combined_b1_b2(db, b1, b2)
        rec = _recommendations(a2, a4, b1, b2, b3)

        report = {
            "ok": True,
            "part_a": {
                "a1_full_universe_kavach": {
                    "computed_each_scan": True,
                    "universe_size": "~203 current-month NSE futures (arbitrage_master)",
                    "per_symbol": "EMA/VWAP/Supertrend/MACD/ADX + evaluate_kavach + volume ratios",
                    "trade_score_confidence": "Only on Kavach-directional symbols inside _rank()",
                    "persisted": "Top-5 bull + top-5 bear only (relative_strength_snapshot)",
                    "maturity_tags": "Top-5 only → rs_scanner_history",
                    "verdict": (
                        "Full-universe Kavach IS computed every 5 min in memory but NOT stored. "
                        "Gap is persistence/exposure, not compute. Smallest lift: after-hours "
                        "archive of directional candidates, not re-running detection."
                    ),
                    "code_refs": [
                        "backend/services/relative_strength_scanner.py: run_relative_strength_scan",
                        "backend/services/kavach_engine.py: evaluate_kavach",
                    ],
                },
                "a2_discard_and_missed": a2,
                "a2_case_studies": cases,
                "a3_surfacing_proposal": rec["part_a3_surfacing"],
                "a4_maturity_backtest": a4,
            },
            "part_b": {
                "b1_volume_weighted_rs": b1,
                "b2_relock_timing": b2,
                "b3_combined": b3,
                "b4_recommendation": rec["part_b4_selection"],
            },
            "recommendations": rec,
            "data_window": {
                "snapshot_days": db.execute(
                    text("SELECT COUNT(DISTINCT scan_time::date) FROM relative_strength_snapshot")
                ).scalar(),
                "history_days": db.execute(
                    text("SELECT MIN(date), MAX(date) FROM rs_scanner_history")
                ).fetchone(),
            },
        }
        print(json.dumps(report, indent=2, default=str))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
