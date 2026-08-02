#!/usr/bin/env python3
"""Structural Quality Score v1.2 — corrected VWAP/EMA + EW/grade LOCF.

Data fixes vs v1.1:
  - VWAP from 1m typical price (H+L+C)/3 × volume, session-anchored 09:15 IST
  - EMA5/EMA10 on **close only**, seeded from prior session final EMA (carry-forward)
  - ema_reliable: prior-session EMA seed is exact from bar 1 (6-bar buffer removed 2026-08-02)

Formula fixes:
  - EW: if EMA5 already on qualifying side at first evaluated bar → EW=100 (start aligned)
  - Confidence grade / trade_score: LOCF with rs_score_stale_minutes

Still backtest-only — no live wiring.

  python -m backend.scripts.backtest_structural_quality_v1_2 \\
    --out /tmp/structural_quality_backtest_v1_2
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.scripts.backtest_structural_quality_v1 import (
    GRADE_OK,
    SESSIONS,
    SCORE_FROM,
    SESSION_LAST,
    _dir_sign,
    _f,
    _grade_ok,
    _ist,
    _norm_grade,
    _side,
    build_actual_ready,
    compare_sets,
    first_promotions,
    load_garuda_safe,
    load_universe,
    overextension_weight,
    step_vw,
)
from backend.services.vajra.indicators import cumulative_vwap, ema_series

IST = pytz.timezone("Asia/Kolkata")
SESSION_OPEN = dtime(9, 15)
EMA_RELIABLE_AFTER_BARS = 0  # removed 2026-08-02: prior-session seed is exact from bar 1


def ema_seeded(values: Sequence[float], period: int, seed: float) -> List[float]:
    """Close-only EMA starting from prior-session seed (not bar-1 close)."""
    if not values:
        return []
    k = 2.0 / (max(1, int(period)) + 1.0)
    out: List[float] = []
    ema_v = float(seed)
    for v in values:
        ema_v = float(v) * k + ema_v * (1.0 - k)
        out.append(ema_v)
    return out


def _prev_session(day: str) -> str:
    """Previous calendar day (sessions list aware when possible)."""
    if day in SESSIONS:
        i = SESSIONS.index(day)
        if i > 0:
            return SESSIONS[i - 1]
    dt = datetime.strptime(day, "%Y-%m-%d") - timedelta(days=1)
    # skip weekends roughly
    while dt.weekday() >= 5:
        dt -= timedelta(days=1)
    return dt.strftime("%Y-%m-%d")


def load_1m_range(
    db, iks: Sequence[str], d0: str, d1: str
) -> Dict[str, List[Dict[str, Any]]]:
    if not iks:
        return {}
    rows = db.execute(
        text(
            """
            SELECT instrument_key, candle_time, open, high, low, close, volume
            FROM upstox_ws_intraday_1m
            WHERE instrument_key = ANY(:iks)
              AND candle_time >= CAST(:a AS timestamptz)
              AND candle_time < CAST(:b AS timestamptz)
            ORDER BY instrument_key, candle_time
            """
        ),
        {
            "iks": list(iks),
            "a": f"{d0} 09:00:00+05:30",
            "b": f"{d1} 16:00:00+05:30",
        },
    ).mappings().all()
    by: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by[r["instrument_key"]].append(dict(r))
    return by


def filter_session_1m(rows: List[Dict[str, Any]], day: str) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        t = _ist(r["candle_time"])
        if t is None or t.strftime("%Y-%m-%d") != day or t.year < 2000:
            continue
        if not (SESSION_OPEN <= t.time() < dtime(15, 30)):
            continue
        out.append({**r, "t": t})
    return out


def build_10m_ohlcv(day_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[datetime, Dict[str, Any]] = {}
    for r in day_rows:
        t = r["t"]
        mins = (t.hour * 60 + t.minute) - (SESSION_OPEN.hour * 60 + SESSION_OPEN.minute)
        if mins < 0:
            continue
        idx = mins // 10
        bar_end = t.replace(
            hour=SESSION_OPEN.hour, minute=SESSION_OPEN.minute, second=0, microsecond=0
        ) + timedelta(minutes=10 * (idx + 1))
        o, h, l, c = _f(r["open"]), _f(r["high"]), _f(r["low"]), _f(r["close"])
        if None in (o, h, l, c):
            continue
        v = _f(r["volume"]) or 0.0
        b = buckets.get(bar_end)
        if b is None:
            buckets[bar_end] = {
                "bar_end": bar_end,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
                "n1m": 1,
                "_first": t,
                "_last": t,
            }
        else:
            b["high"] = max(b["high"], h)
            b["low"] = min(b["low"], l)
            b["volume"] += v
            b["n1m"] += 1
            if t < b["_first"]:
                b["_first"], b["open"] = t, o
            if t > b["_last"]:
                b["_last"], b["close"] = t, c
    bars = sorted(buckets.values(), key=lambda x: x["bar_end"])
    for b in bars:
        b.pop("_first", None)
        b.pop("_last", None)
    return bars


def vwap_1m_at_bar_ends(day_rows: List[Dict[str, Any]]) -> Dict[datetime, float]:
    """Session VWAP: Σ(typical×vol)/Σvol from 09:15 on 1m bars; sample at 10m ends."""
    cum_pv = 0.0
    cum_v = 0.0
    ends: Dict[datetime, float] = {}
    for r in day_rows:
        h, l, c = float(r["high"]), float(r["low"]), float(r["close"])
        v = float(r["volume"] or 0.0)
        tp = (h + l + c) / 3.0
        cum_pv += tp * v
        cum_v += v
        t = r["t"]
        mins = (t.hour * 60 + t.minute) - (SESSION_OPEN.hour * 60 + SESSION_OPEN.minute)
        idx = mins // 10
        bar_end = t.replace(
            hour=SESSION_OPEN.hour, minute=SESSION_OPEN.minute, second=0, microsecond=0
        ) + timedelta(minutes=10 * (idx + 1))
        ends[bar_end] = (cum_pv / cum_v) if cum_v > 0 else c
    return ends


def enrich_bars_v12(
    day_rows: List[Dict[str, Any]],
    prev_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    bars = build_10m_ohlcv(day_rows)
    if not bars:
        return []
    vwap_map = vwap_1m_at_bar_ends(day_rows)
    closes = [float(b["close"]) for b in bars]
    prev_bars = build_10m_ohlcv(prev_rows)
    prev_closes = [float(b["close"]) for b in prev_bars]
    if prev_closes:
        seed5 = ema_series(prev_closes, 5)[-1]
        seed10 = ema_series(prev_closes, 10)[-1]
        ema_seed_source = "prior_session_final"
    else:
        seed5 = closes[0]
        seed10 = closes[0]
        ema_seed_source = "fallback_first_close"
    e5 = ema_seeded(closes, 5, seed5)
    e10 = ema_seeded(closes, 10, seed10)
    # legacy fresh for before/after columns
    e5_fresh = ema_series(closes, 5)
    old_vw = cumulative_vwap(
        [float(b["high"]) for b in bars],
        [float(b["low"]) for b in bars],
        closes,
        [float(b["volume"]) for b in bars],
    )
    session_open = float(bars[0]["open"])
    out = []
    for i, b in enumerate(bars):
        be = b["bar_end"]
        out.append(
            {
                **b,
                "vwap": float(vwap_map.get(be, old_vw[i])),
                "vwap_legacy_10m_tp": float(old_vw[i]),
                "ema5": float(e5[i]),
                "ema10": float(e10[i]),
                "ema5_legacy_fresh": float(e5_fresh[i]),
                "ema_seed5": float(seed5),
                "ema_seed10": float(seed10),
                "ema_seed_source": ema_seed_source,
                "ema_input": "close_only",
                "vwap_input": "1m_typical_price_HLC3",
                "ema_reliable": i >= EMA_RELIABLE_AFTER_BARS,
                "session_bar_idx": i,
                "session_open": session_open,
            }
        )
    return out


def step_ew_v12(
    state: Dict[str, Any],
    *,
    ema5: float,
    vwap: float,
    dir_sign: int,
    is_first_eval: bool,
    ema_reliable: bool = True,
) -> Tuple[float, Optional[str]]:
    """EW arms only on a genuine observed EMA5/VWAP crossover in qualifying direction.

    ``start_aligned`` free-100 removed (2026-08-02): already-on-side at first bar
    stays EW=0. Unreliable EMA bars never arm/decay.
    """
    side = _side(ema5, vwap)
    if not ema_reliable:
        if side != 0:
            state["prev_side"] = side
        return float(state.get("ew") or 0.0), None

    if is_first_eval and not state.get("armed"):
        if side != 0:
            state["prev_side"] = side
        return float(state.get("ew") or 0.0), None

    event = None
    prev = int(state.get("prev_side") or 0)
    if prev != 0 and side != 0 and side != prev:
        event = "bullish" if side > 0 else "bearish"
        if not state.get("armed"):
            if dir_sign != 0 and side == dir_sign:
                state["armed"] = True
                state["cross_count"] = 1
                state["ew"] = 100.0
        else:
            state["cross_count"] = int(state.get("cross_count") or 0) + 1
            state["ew"] = float(max(0.0, float(state.get("ew") or 0.0) - 20.0))
    if side != 0:
        state["prev_side"] = side
    return float(state.get("ew") or 0.0), event


def load_grades_series(
    db, session_date: str
) -> Dict[str, List[Tuple[datetime, str, Any]]]:
    by: Dict[str, List[Tuple[datetime, str, Any]]] = defaultdict(list)
    try:
        audits = db.execute(
            text(
                """
                SELECT UPPER(TRIM(symbol)) AS symbol,
                       bar_evaluated_at AS ts,
                       confidence_grade, trade_score
                FROM rs_live_kavach_audit
                WHERE session_date = CAST(:d AS date)
                ORDER BY bar_evaluated_at
                """
            ),
            {"d": session_date},
        ).mappings().all()
        for r in audits:
            ts = _ist(r["ts"])
            if ts:
                by[r["symbol"]].append((ts, r["confidence_grade"], r["trade_score"]))
    except Exception:
        db.rollback()
    try:
        snaps = db.execute(
            text(
                """
                SELECT UPPER(TRIM(symbol)) AS symbol,
                       scan_time AS ts,
                       confidence_grade, trade_score
                FROM relative_strength_snapshot
                WHERE scan_time >= CAST(:a AS timestamptz)
                  AND scan_time < CAST(:b AS timestamptz)
                ORDER BY scan_time
                """
            ),
            {
                "a": f"{session_date} 09:00:00+05:30",
                "b": f"{session_date} 16:00:00+05:30",
            },
        ).mappings().all()
        for r in snaps:
            ts = _ist(r["ts"])
            if ts:
                by[r["symbol"]].append(
                    (ts, r.get("confidence_grade"), r.get("trade_score"))
                )
    except Exception:
        db.rollback()
    for sym in by:
        by[sym].sort(key=lambda x: x[0])
    return by


def grade_locf(
    series: List[Tuple[datetime, str, Any]], at: datetime
) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[str]]:
    """Last observation at-or-before `at`; return grade, score, stale_minutes, source_ts."""
    last = None
    for ts, g, sc in series:
        if ts <= at:
            last = (ts, g, sc)
        else:
            break
    if last is None:
        return None, None, None, None
    ts, g, sc = last
    stale = (at - ts).total_seconds() / 60.0
    return g, _f(sc), round(stale, 1), ts.isoformat()


def _mult_ow_vw(rank: Optional[float], ow: float, vw: float) -> Tuple[Optional[float], Optional[float]]:
    if rank is None:
        return None, None
    mult = (ow / 100.0) * (vw / 100.0)
    return round(mult, 6), round(float(rank) * mult, 4)


def _garuda_timeline(garuda, sym):
    items = [(hhmm, g) for (s, hhmm), g in garuda.items() if s == sym]
    return sorted(items, key=lambda x: x[0])


def run_session(
    db,
    session_date: str,
    universe: List[Tuple[str, str]],
    by_ik_all: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    prev = _prev_session(session_date)
    sym_by_ik = {ik: sym for sym, ik in universe}
    garuda = load_garuda_safe(db, session_date)
    grades = load_grades_series(db, session_date)
    rows_out: List[Dict[str, Any]] = []
    print(f"  [{session_date}] scoring…", flush=True)

    for ik, raw in by_ik_all.items():
        sym = sym_by_ik.get(ik)
        if not sym:
            continue
        day_rows = filter_session_1m(raw, session_date)
        prev_rows = filter_session_1m(raw, prev)
        bars = enrich_bars_v12(day_rows, prev_rows)
        if len(bars) < 2:
            continue

        g_timeline = _garuda_timeline(garuda, sym)
        last_rank = None
        last_top6 = None
        last_side = "NEUTRAL"
        for _, g0 in g_timeline:
            s0 = str(g0.get("side") or "").upper()
            if s0 in ("LONG", "SHORT"):
                last_side = s0
                break

        vw_state = 50.0
        ew_state: Dict[str, Any] = {
            "ew": 0.0,
            "armed": False,
            "cross_count": 0,
            "prev_side": 0,
        }
        g_idx = 0
        first_eval = True
        grade_series = grades.get(sym) or []

        for i, b in enumerate(bars):
            be: datetime = b["bar_end"]
            hhmm = be.strftime("%H:%M")
            while g_idx < len(g_timeline) and g_timeline[g_idx][0] <= hhmm:
                g_row = g_timeline[g_idx][1]
                r = _f(g_row.get("rank_score"))
                if r is not None:
                    last_rank = r
                if g_row.get("top6_rank") is not None:
                    last_top6 = g_row.get("top6_rank")
                s0 = str(g_row.get("side") or "").upper()
                if s0 in ("LONG", "SHORT"):
                    last_side = s0
                g_idx += 1

            side = last_side
            dir_sign = _dir_sign(side)
            o, c, vwap = float(b["open"]), float(b["close"]), float(b["vwap"])
            ema5 = float(b["ema5"])

            # Always advance VW/EW state from session start (incl. pre-09:35)
            is_first_candle = i == 0
            vw_state, vw_cls = step_vw(
                vw_state,
                open_=o,
                close=c,
                vwap=vwap,
                dir_sign=dir_sign,
                is_first_candle=is_first_candle,
            )

            # First *evaluated* bar (SCORE_FROM+) gets start-aligned EW check
            in_window = SCORE_FROM <= be.time() <= SESSION_LAST
            if in_window:
                ew_val, ew_event = step_ew_v12(
                    ew_state,
                    ema5=ema5,
                    vwap=vwap,
                    dir_sign=dir_sign,
                    is_first_eval=first_eval,
                    ema_reliable=bool(b.get("ema_reliable", True)),
                )
                if b.get("ema_reliable", True):
                    first_eval = False
            else:
                # still track EMA side for crossover detection after 09:35
                side_e = _side(ema5, vwap)
                if side_e != 0:
                    ew_state["prev_side"] = side_e
                ew_val, ew_event = float(ew_state.get("ew") or 0.0), None

            stretch, ow = overextension_weight(c, float(b["session_open"]))
            if not in_window:
                continue

            mult, final = _mult_ow_vw(last_rank, ow, vw_state)
            _, score_ow = _mult_ow_vw(last_rank, ow, 100.0)
            _, score_vw = _mult_ow_vw(last_rank, 100.0, vw_state)

            grade, tscore, stale_min, grade_ts = grade_locf(grade_series, be)
            rows_out.append(
                {
                    "session_date": session_date,
                    "symbol": sym,
                    "bar_end": be.isoformat(),
                    "bar_hhmm": hhmm,
                    "side": side,
                    "dir_sign": dir_sign,
                    "session_open": round(float(b["session_open"]), 4),
                    "open": round(o, 4),
                    "high": round(float(b["high"]), 4),
                    "low": round(float(b["low"]), 4),
                    "close": round(c, 4),
                    "vwap": round(vwap, 4),
                    "vwap_legacy_10m_tp": round(float(b["vwap_legacy_10m_tp"]), 4),
                    "ema5": round(ema5, 4),
                    "ema10": round(float(b["ema10"]), 4),
                    "ema5_legacy_fresh": round(float(b["ema5_legacy_fresh"]), 4),
                    "ema_reliable": b["ema_reliable"],
                    "ema_seed_source": b["ema_seed_source"],
                    "ema_input": "close_only",
                    "vwap_input": "1m_typical_price_HLC3",
                    "stretch_pct": stretch,
                    "OW": ow,
                    "VW": round(vw_state, 4),
                    "vw_classification": vw_cls,
                    "EW": round(ew_val, 4),
                    "ew_unlocked": bool(ew_state.get("armed")),
                    "ew_event": ew_event,
                    "ew_cross_count": int(ew_state.get("cross_count") or 0),
                    "garuda_rank_score": last_rank,
                    "garuda_top6_rank": last_top6,
                    "structural_multiplier": mult,
                    "final_score": final,
                    "score_ow_only": score_ow,
                    "score_vw_only": score_vw,
                    "confidence_grade": grade,
                    "trade_score": tscore,
                    "rs_score_stale_minutes": stale_min,
                    "rs_grade_source_ts": grade_ts,
                    "grade_ok": _grade_ok(grade) if grade else None,
                    "n1m_in_bar": b.get("n1m"),
                    "data_source": "upstox_ws_intraday_1m→10m_v1.2",
                    "formula_version": "v1.2",
                }
            )
    print(f"  [{session_date}] rows={len(rows_out)}", flush=True)
    return rows_out


def f1(p, r):
    if not p or not r:
        return 0.0
    return 2 * p * r / (p + r)


def sensitivity(rows, actual, thresholds):
    out = []
    for thr in thresholds:
        prop = first_promotions(
            rows, score_key="final_score", threshold=float(thr), require_grade=True
        )
        cmp_ = compare_sets(prop, actual)
        prec = (
            round(cmp_["true_positive_n"] / cmp_["proposed_n"], 3)
            if cmp_["proposed_n"]
            else None
        )
        rec = (
            round(cmp_["true_positive_n"] / cmp_["actual_n"], 3)
            if cmp_["actual_n"]
            else None
        )
        out.append(
            {
                "threshold": float(thr),
                **{k: cmp_[k] for k in (
                    "proposed_n",
                    "actual_n",
                    "true_positive_n",
                    "false_positive_n",
                    "false_negative_n",
                )},
                "precision": prec,
                "recall": rec,
                "f1": round(f1(prec, rec), 3),
            }
        )
    return out


V11_REF_THR10 = {
    "threshold": 10.0,
    "true_positive_n": 67,
    "false_positive_n": 335,
    "false_negative_n": 4,
    "precision": 0.167,
    "recall": 0.944,
}


def write_md(path: Path, payload: Dict[str, Any]) -> None:
    ver = payload.get("verification") or []
    sens = payload.get("threshold_sensitivity") or []
    best = payload.get("best_f1") or {}
    lines = [
        "# Structural Quality Score v1.2 — Corrected VWAP/EMA + EW/grade LOCF",
        "",
        "**LIVE PROMOTION NOT WIRED.**",
        "",
        "## Data integrity fixes",
        "",
        "1. **VWAP:** session-anchored from **09:15 IST**, typical price `(H+L+C)/3 × volume` "
        "accumulated on **1m** bars, sampled at each 10m `bar_end`. "
        "(Prior v1.1 used 10m-aggregated H/L/C — correct formula family, but incomplete `n1m` "
        "buckets skewed early bars; 1m path is the fix.)",
        "2. **EMA5/EMA10:** computed on **close price only** (unchanged definition). "
        "Seeded from **prior session final EMA** (carry-forward), not reset-to-close on bar 1. "
        "Prior-session EMA seed is exact from bar 1 (`EMA_RELIABLE_AFTER_BARS=0`).",
        "",
        "## Formula fixes",
        "",
        "- **EW start-aligned:** if EMA5 already on qualifying side of VWAP at first evaluated bar → `EW=100`.",
        "- **RS grade/trade_score LOCF** with `rs_score_stale_minutes`.",
        "",
        "## Verification (before → after)",
        "",
        "Manual targets: TVSMOTOR 2026-07-31 VWAP ~4255–4257 at 09:35/09:45; "
        "EMA5 must not equal bar-1 close after seeding fix.",
        "",
    ]
    for v in ver:
        lines += [
            f"### {v['symbol']} {v['session_date']}",
            "",
            f"- EMA seed5={v.get('ema_seed5_from_prev')} seed10={v.get('ema_seed10_from_prev')} "
            f"(`{v.get('ema_input')}`)",
            f"- VWAP input: `{v.get('vwap_input')}`",
            "",
            "| hhmm | n1m | close | VWAP old→new | EMA5 old→new | old EMA5==close? |",
            "|-----:|----:|------:|--------------|--------------|:----------------:|",
        ]
        for b in v.get("bars") or []:
            lines.append(
                f"| {b['hhmm']} | {b['n1m']} | {b['close']} | "
                f"{b['vwap_old_10m_tp']}→{b['vwap_new_1m_tp']} | "
                f"{b['ema5_old_fresh']}→{b['ema5_new_carry']} | "
                f"{b['ema5_equals_close_old']} |"
            )
        lines.append("")

    lines += [
        "## Threshold sensitivity (v1.2 primary: LOCF rank × OW×VW, EW badge)",
        "",
        f"- Best F1: thr={best.get('threshold')} F1={best.get('f1')} "
        f"P={best.get('precision')} R={best.get('recall')} "
        f"(TP {best.get('true_positive_n')}/FP {best.get('false_positive_n')}/FN {best.get('false_negative_n')})",
        "",
        "| thr | proposed | TP | FP | FN | P | R | F1 | vs v1.1@10 ΔP | ΔR |",
        "|----:|---------:|---:|---:|---:|--:|--:|---:|-------------:|---:|",
    ]
    for s in sens:
        dp = dr = ""
        if s["threshold"] == 10.0:
            dp = round(s["precision"] - V11_REF_THR10["precision"], 3)
            dr = round(s["recall"] - V11_REF_THR10["recall"], 3)
        lines.append(
            f"| {s['threshold']} | {s['proposed_n']} | {s['true_positive_n']} | "
            f"{s['false_positive_n']} | {s['false_negative_n']} | {s['precision']} | "
            f"{s['recall']} | {s['f1']} | {dp} | {dr} |"
        )

    abl = payload.get("ablation_at_best") or {}
    lines += ["", "## Ablation at best-F1 threshold", ""]
    for k, v in abl.items():
        lines.append(
            f"- **{k}:** TP={v.get('true_positive_n')} FP={v.get('false_positive_n')} "
            f"FN={v.get('false_negative_n')} P={v.get('precision')} R={v.get('recall')}"
        )

    ba = payload.get("before_after_thr10") or {}
    lines += [
        "",
        "## Before/after @ threshold 10",
        "",
        "| config | TP | FP | FN | P | R |",
        "|--------|---:|---:|---:|--:|--:|",
        f"| v1.1 (prior) | {V11_REF_THR10['true_positive_n']} | {V11_REF_THR10['false_positive_n']} | "
        f"{V11_REF_THR10['false_negative_n']} | {V11_REF_THR10['precision']} | {V11_REF_THR10['recall']} |",
    ]
    v12 = ba.get("v12") or {}
    lines.append(
        f"| v1.2 corrected | {v12.get('true_positive_n')} | {v12.get('false_positive_n')} | "
        f"{v12.get('false_negative_n')} | {v12.get('precision')} | {v12.get('recall')} |"
    )
    lines += [
        "",
        "## Non-requirements",
        "",
        "- No live wiring, dashboard, or deploy",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="docs/diagnostics/structural_quality_backtest_v1_2")
    ap.add_argument("--sessions", default=",".join(SESSIONS))
    args = ap.parse_args()
    sessions = [s.strip() for s in args.sessions.split(",") if s.strip()]
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # verification first
    from backend.scripts.verify_vwap_ema_reconstruction import verify_symbol

    db = SessionLocal()
    try:
        verification = []
        for sym, day, prev in [
            ("TVSMOTOR", "2026-07-31", "2026-07-30"),
            ("M&M", "2026-07-31", "2026-07-30"),
            ("BAJFINANCE", "2026-07-31", "2026-07-30"),
            ("SIEMENS", "2026-07-31", "2026-07-30"),
            ("LODHA", "2026-07-28", "2026-07-27"),
        ]:
            verification.append(verify_symbol(db, sym, day, prev))

        universe = load_universe(db)
        # load 1m with prior-day buffer for EMA seed
        d0 = _prev_session(sessions[0])
        # go one more day back for first session seed quality
        d0 = _prev_session(d0)
        d1 = sessions[-1]
        print(f"loading 1m {d0}→{d1} for {len(universe)} symbols…", flush=True)
        by_ik = load_1m_range(db, [ik for _, ik in universe], d0, d1)
        print(f"instruments with bars: {len(by_ik)}", flush=True)

        all_rows: List[Dict[str, Any]] = []
        for sd in sessions:
            all_rows.extend(run_session(db, sd, universe, by_ik))
        actual = build_actual_ready(db)
    finally:
        db.close()

    thresholds = [5, 8, 10, 12, 15, 18, 20, 25, 30, 35, 40, 45, 50, 55, 60]
    grade_ok_scores = sorted(
        r["final_score"]
        for r in all_rows
        if r.get("final_score") is not None and r.get("grade_ok")
    )
    for p in (10, 25, 50, 75, 90, 95):
        if grade_ok_scores:
            idx = min(len(grade_ok_scores) - 1, int(len(grade_ok_scores) * p / 100))
            thresholds.append(round(grade_ok_scores[idx], 1))
    thresholds = sorted(set(float(t) for t in thresholds))

    sens = sensitivity(all_rows, actual, thresholds)
    best = max(sens, key=lambda s: (s["f1"], s.get("precision") or 0, s["threshold"]))
    thr = best["threshold"]

    ablation = {}
    for label, key, ew_req in [
        ("v12_full_ow_vw", "final_score", False),
        ("v12_plus_ew_unlock", "final_score", True),
        ("OW_alone", "score_ow_only", False),
        ("VW_alone", "score_vw_only", False),
    ]:
        src = [r for r in all_rows if r.get("ew_unlocked")] if ew_req else all_rows
        prop = first_promotions(src, score_key=key, threshold=thr, require_grade=True)
        c = compare_sets(prop, actual)
        c["precision"] = (
            round(c["true_positive_n"] / c["proposed_n"], 3) if c["proposed_n"] else None
        )
        c["recall"] = (
            round(c["true_positive_n"] / c["actual_n"], 3) if c["actual_n"] else None
        )
        ablation[label] = c

    v12_10 = next((s for s in sens if s["threshold"] == 10.0), {})

    # CSV
    csv_path = out_dir / "structural_quality_backtest.csv"
    if all_rows:
        keys = list(all_rows[0].keys())
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(all_rows)

    with (out_dir / "threshold_curve.csv").open("w", newline="", encoding="utf-8") as f:
        fields = [
            "threshold",
            "proposed_n",
            "true_positive_n",
            "false_positive_n",
            "false_negative_n",
            "precision",
            "recall",
            "f1",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for s in sens:
            w.writerow({k: s.get(k) for k in fields})

    payload = {
        "version": "v1.2",
        "live_promotion_wired": False,
        "data_fixes": {
            "vwap": "1m typical price (H+L+C)/3 × vol from 09:15 IST",
            "ema5_ema10": "close-only, prior-session seed carry-forward",
            "ema_reliable_after_bars": EMA_RELIABLE_AFTER_BARS,
        },
        "formula_fixes": {
            "ew_start_aligned": False,
            "ew_requires_observed_cross": True,
            "ew_respects_ema_reliable": True,
            "rs_grade_locf": True,
        },
        "verification": verification,
        "row_count": len(all_rows),
        "actual_ready_n": len(actual),
        "threshold_sensitivity": sens,
        "best_f1": best,
        "ablation_at_best": ablation,
        "before_after_thr10": {"v11": V11_REF_THR10, "v12": v12_10},
        "ew_start_aligned_eval_count": sum(
            1 for r in all_rows if r.get("ew_event") == "start_aligned"
        ),
        "grade_locf_non_null": sum(
            1 for r in all_rows if r.get("confidence_grade") is not None
        ),
    }
    (out_dir / "structural_quality_backtest.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8"
    )
    write_md(out_dir / "structural_quality_backtest.md", payload)

    # also dump verification alone
    (out_dir / "verification_before_after.json").write_text(
        json.dumps(verification, indent=2, default=str), encoding="utf-8"
    )

    print(f"wrote {csv_path} rows={len(all_rows)}")
    print(f"best_f1={best}")
    print(f"@10 v11={V11_REF_THR10} v12={v12_10}")
    print(f"ew_start_aligned events={payload['ew_start_aligned_eval_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
