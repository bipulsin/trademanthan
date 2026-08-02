#!/usr/bin/env python3
"""Re-fetch FO universe native Upstox V3 minutes/10 for 2026-07-27..31.

Pipeline (read-only vs live WS tables — writes only under --out):
  1) Gap-audit upstox_ws_intraday_1m vs expected 375 bars/session
  2) Fetch native minutes/10 into separate JSONL dataset (no DB overwrite)
  3) Spot-check M&M (+ peers) VWAP/EMA5 vs known TV / stored-derived
  4) Recompute structural OW/VW/EW/final_score (v1.2 logic on native 10m TP VWAP)
  5) Before/after vs existing v1.2 CSV (stored 1m path)

  python -m backend.scripts.refetch_fo_native10m_sq_clean_20260727_31 \\
    --out /tmp/sq_clean10m \\
    --v12-csv /path/to/structural_quality_backtest.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import time
from collections import defaultdict
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.scripts.backtest_structural_quality_v1 import (
    SESSIONS,
    SCORE_FROM,
    SESSION_LAST,
    _dir_sign,
    _f,
    _grade_ok,
    _ist,
    load_garuda_safe,
    load_universe,
    overextension_weight,
    step_vw,
)
from backend.scripts.backtest_structural_quality_v1_2 import (
    EMA_RELIABLE_AFTER_BARS,
    _garuda_timeline,
    _mult_ow_vw,
    _prev_session,
    ema_seeded,
    enrich_bars_v12,
    filter_session_1m,
    grade_locf,
    load_1m_range,
    load_grades_series,
    step_ew_v12,
)
from backend.services.upstox_service import UpstoxService, _parse_ts_to_aware_ist
from backend.services.vajra.indicators import cumulative_vwap, ema_series

IST = pytz.timezone("Asia/Kolkata")
SESSION_OPEN = dtime(9, 15)
EXPECTED_1M = 375
WARMUP_FROM = "2026-07-24"  # Fri before window — EMA seed for 07-27
END_DAY = "2026-07-31"
PROBLEM_LO = "09:35"
PROBLEM_HI = "10:25"

# Manual TV refs from M&M diagnostic
TV_MM = {
    "09:25": {"vwap": 3322.63, "ema5": 3317.13},
    "09:35": {"vwap": 3338.53, "ema5": 3337.78},
    "09:45": {"vwap": 3345.95, "ema5": 3355.59},
}

SPOT_SYMS = ["M&M", "TVSMOTOR", "BAJFINANCE", "SIEMENS"]


def session_minutes(day: str) -> List[str]:
    base = IST.localize(datetime.strptime(f"{day} 09:15:00", "%Y-%m-%d %H:%M:%S"))
    return [(base + timedelta(minutes=i)).strftime("%H:%M") for i in range(EXPECTED_1M)]


def gap_audit(db, universe: List[Tuple[str, str]]) -> Dict[str, Any]:
    print("Step 1: universe gap audit…", flush=True)
    iks = [ik for _, ik in universe if ik]
    sym_by_ik = {ik: sym for sym, ik in universe}
    # Count per ik×day via SQL for speed
    rows = db.execute(
        text(
            """
            SELECT instrument_key,
                   (candle_time AT TIME ZONE 'Asia/Kolkata')::date AS d,
                   COUNT(*) AS n,
                   COUNT(DISTINCT date_trunc('minute', candle_time AT TIME ZONE 'Asia/Kolkata')) AS n_distinct
            FROM upstox_ws_intraday_1m
            WHERE instrument_key = ANY(:iks)
              AND candle_time >= CAST(:a AS timestamptz)
              AND candle_time < CAST(:b AS timestamptz)
              AND (candle_time AT TIME ZONE 'Asia/Kolkata')::time >= TIME '09:15'
              AND (candle_time AT TIME ZONE 'Asia/Kolkata')::time < TIME '15:30'
            GROUP BY 1, 2
            """
        ),
        {
            "iks": iks,
            "a": f"{SESSIONS[0]} 09:00:00+05:30",
            "b": f"{SESSIONS[-1]} 16:00:00+05:30",
        },
    ).mappings().all()
    counts: Dict[Tuple[str, str], int] = {}
    for r in rows:
        d = r["d"].isoformat() if hasattr(r["d"], "isoformat") else str(r["d"])
        counts[(r["instrument_key"], d)] = int(r["n_distinct"] or r["n"] or 0)

    # Missing minutes detail only for worst / sample — pull sparse symbols
    per_day_sym: List[Dict[str, Any]] = []
    missing_in_problem = 0
    total_missing = 0
    worst: List[Dict[str, Any]] = []

    for sym, ik in universe:
        for day in SESSIONS:
            n = counts.get((ik, day), 0)
            miss = EXPECTED_1M - n
            if miss < 0:
                miss = 0  # extras / dups ignored at count level
            total_missing += max(0, EXPECTED_1M - n) if n < EXPECTED_1M else 0
            rec = {
                "symbol": sym,
                "ik": ik,
                "day": day,
                "n_bars": n,
                "missing": max(0, EXPECTED_1M - n) if n <= EXPECTED_1M else 0,
                "pct_complete": round(min(n, EXPECTED_1M) / EXPECTED_1M, 4),
            }
            per_day_sym.append(rec)
            if rec["missing"] > 0:
                worst.append(rec)

    worst.sort(key=lambda x: -x["missing"])

    # Detailed missing minutes for top-25 worst + SPOT_SYMS
    detail_targets = set()
    for w in worst[:25]:
        detail_targets.add((w["symbol"], w["ik"], w["day"]))
    for sym in SPOT_SYMS:
        ik = next((i for s, i in universe if s == sym), None)
        if ik:
            for day in SESSIONS:
                detail_targets.add((sym, ik, day))

    problem_cluster = []
    for sym, ik, day in sorted(detail_targets):
        times = db.execute(
            text(
                """
                SELECT DISTINCT to_char(candle_time AT TIME ZONE 'Asia/Kolkata', 'HH24:MI') AS hhmm
                FROM upstox_ws_intraday_1m
                WHERE instrument_key = :ik
                  AND candle_time >= CAST(:a AS timestamptz)
                  AND candle_time < CAST(:b AS timestamptz)
                  AND (candle_time AT TIME ZONE 'Asia/Kolkata')::time >= TIME '09:15'
                  AND (candle_time AT TIME ZONE 'Asia/Kolkata')::time < TIME '15:30'
                """
            ),
            {"ik": ik, "a": f"{day} 09:00:00+05:30", "b": f"{day} 16:00:00+05:30"},
        ).fetchall()
        have = {r[0] for r in times}
        expected = session_minutes(day)
        missing = [m for m in expected if m not in have]
        in_prob = [m for m in missing if PROBLEM_LO <= m <= PROBLEM_HI]
        missing_in_problem += len(in_prob)
        if missing:
            problem_cluster.append(
                {
                    "symbol": sym,
                    "day": day,
                    "missing_n": len(missing),
                    "missing_in_0935_1025": len(in_prob),
                    "frac_gaps_in_problem_window": round(len(in_prob) / len(missing), 3),
                    "missing_minutes": missing,
                }
            )

    n_pairs = len(universe) * len(SESSIONS)
    incomplete = sum(1 for r in per_day_sym if r["missing"] > 0)
    summary = {
        "expected_1m_per_session": EXPECTED_1M,
        "symbol_day_pairs": n_pairs,
        "incomplete_symbol_days": incomplete,
        "frac_incomplete": round(incomplete / n_pairs, 4) if n_pairs else None,
        "total_missing_minutes_sum": sum(r["missing"] for r in per_day_sym),
        "mean_missing_when_incomplete": round(
            sum(r["missing"] for r in per_day_sym if r["missing"] > 0)
            / max(1, incomplete),
            2,
        ),
        "worst_25": worst[:25],
        "spot_symbol_days": [r for r in per_day_sym if r["symbol"] in SPOT_SYMS],
        "problem_window_detail_sample": problem_cluster,
        "among_detailed_gaps_count_in_0935_1025": missing_in_problem,
        "note": "problem_window_detail_sample covers worst-25 + spot symbols only",
    }
    # Cluster stats on detailed sample
    with_gaps = [p for p in problem_cluster if p["missing_n"] > 0]
    if with_gaps:
        summary["detail_sample_mean_frac_gaps_in_problem_window"] = round(
            sum(p["frac_gaps_in_problem_window"] for p in with_gaps) / len(with_gaps), 3
        )
        summary["detail_sample_pct_with_any_gap_in_problem_window"] = round(
            sum(1 for p in with_gaps if p["missing_in_0935_1025"] > 0) / len(with_gaps), 3
        )
    print(
        f"  incomplete {incomplete}/{n_pairs} symbol-days; "
        f"worst missing={worst[0]['missing'] if worst else 0} ({worst[0]['symbol'] if worst else '-'} {worst[0]['day'] if worst else ''})",
        flush=True,
    )
    return summary


def normalize_10m_bar_end(t: datetime) -> Optional[datetime]:
    """Upstox V3 minutes/10 stamps are bar starts → bar_end = t+10m."""
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    be = t + timedelta(minutes=10)
    if be.time() < dtime(9, 25) or be.time() > dtime(15, 30):
        # try interpret as already bar_end
        if dtime(9, 25) <= t.time() <= dtime(15, 30):
            return t.replace(second=0, microsecond=0)
        return None
    return be.replace(second=0, microsecond=0)


def fetch_native_10m(universe: List[Tuple[str, str]], out_jsonl: Path, sleep_s: float) -> Dict[str, Any]:
    print("Step 2: native minutes/10 re-fetch…", flush=True)
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    meta = {
        "api": "Upstox V3 historical-candle",
        "interval": "minutes/10",
        "from": WARMUP_FROM,
        "to": END_DAY,
        "aggregation": False,
        "symbols_ok": 0,
        "symbols_fail": 0,
        "bars_written": 0,
        "failures": [],
    }
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with out_jsonl.open("w") as fh:
        for i, (sym, ik) in enumerate(universe):
            try:
                raw = ux._fetch_historical_v3_candles(ik, "minutes/10", END_DAY, WARMUP_FROM) or []
            except Exception as e:
                meta["symbols_fail"] += 1
                meta["failures"].append({"symbol": sym, "ik": ik, "error": str(e)[:200]})
                time.sleep(sleep_s)
                continue
            if not raw:
                meta["symbols_fail"] += 1
                meta["failures"].append({"symbol": sym, "ik": ik, "error": "empty"})
                time.sleep(sleep_s)
                continue
            n = 0
            for c in raw:
                t = _parse_ts_to_aware_ist(c.get("timestamp"))
                if t is None:
                    continue
                be = normalize_10m_bar_end(t)
                if be is None:
                    continue
                day = be.strftime("%Y-%m-%d")
                # keep warmup Friday + window
                if day < WARMUP_FROM or day > END_DAY:
                    continue
                if be.weekday() >= 5:
                    continue
                rec = {
                    "symbol": sym,
                    "instrument_key": ik,
                    "session_date": day,
                    "bar_end": be.isoformat(),
                    "bar_hhmm": be.strftime("%H:%M"),
                    "open": _f(c.get("open")),
                    "high": _f(c.get("high")),
                    "low": _f(c.get("low")),
                    "close": _f(c.get("close")),
                    "volume": _f(c.get("volume")) or 0.0,
                    "oi": _f(c.get("oi")),
                    "source": "upstox_v3_minutes_10",
                    "raw_timestamp": str(c.get("timestamp")),
                }
                if None in (rec["open"], rec["high"], rec["low"], rec["close"]):
                    continue
                fh.write(json.dumps(rec) + "\n")
                n += 1
            meta["symbols_ok"] += 1
            meta["bars_written"] += n
            if (i + 1) % 25 == 0:
                print(f"  fetched {i+1}/{len(universe)} … ok={meta['symbols_ok']} fail={meta['symbols_fail']}", flush=True)
            time.sleep(sleep_s)
    print(f"  done: ok={meta['symbols_ok']} fail={meta['symbols_fail']} bars={meta['bars_written']}", flush=True)
    return meta


def load_native_jsonl(path: Path) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """symbol -> session_date -> bars sorted (deduped by bar_hhmm)."""
    by: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    with path.open() as fh:
        for line in fh:
            r = json.loads(line)
            be = datetime.fromisoformat(r["bar_end"])
            if be.tzinfo is None:
                be = IST.localize(be)
            r["bar_end_dt"] = be
            by[r["symbol"]][r["session_date"]].append(r)
    for sym in by:
        for day in by[sym]:
            # keep last write per hhmm
            uniq: Dict[str, Dict[str, Any]] = {}
            for r in sorted(by[sym][day], key=lambda x: x["bar_end_dt"]):
                uniq[r["bar_hhmm"]] = r
            by[sym][day] = sorted(uniq.values(), key=lambda x: x["bar_end_dt"])
    return by


def enrich_native_10m(day_bars: List[Dict[str, Any]], prev_bars: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """v1.2-equivalent indicators on native 10m: TP VWAP + close EMA carry."""
    if not day_bars:
        return []
    closes = [float(b["close"]) for b in day_bars]
    prev_closes = [float(b["close"]) for b in prev_bars] if prev_bars else []
    if prev_closes:
        seed5 = ema_series(prev_closes, 5)[-1]
        seed10 = ema_series(prev_closes, 10)[-1]
        seed_src = "prior_session_final_native10m"
    else:
        seed5 = closes[0]
        seed10 = closes[0]
        seed_src = "fallback_first_close"
    e5 = ema_seeded(closes, 5, seed5)
    e10 = ema_seeded(closes, 10, seed10)
    vw = cumulative_vwap(
        [float(b["high"]) for b in day_bars],
        [float(b["low"]) for b in day_bars],
        closes,
        [float(b["volume"]) for b in day_bars],
    )
    session_open = float(day_bars[0]["open"])
    out = []
    for i, b in enumerate(day_bars):
        out.append(
            {
                "bar_end": b["bar_end_dt"],
                "open": float(b["open"]),
                "high": float(b["high"]),
                "low": float(b["low"]),
                "close": float(b["close"]),
                "volume": float(b["volume"]),
                "vwap": float(vw[i]),
                "ema5": float(e5[i]),
                "ema10": float(e10[i]),
                "ema_seed5": float(seed5),
                "ema_seed10": float(seed10),
                "ema_seed_source": seed_src,
                "ema_input": "close_only",
                "vwap_input": "native_10m_typical_price_HLC3",
                "ema_reliable": i >= EMA_RELIABLE_AFTER_BARS,
                "session_bar_idx": i,
                "session_open": session_open,
                "n1m": None,
                "data_source": "upstox_v3_minutes_10",
            }
        )
    return out


def spot_checks(
    native: Dict[str, Dict[str, List[Dict[str, Any]]]],
    stored_by_ik: Dict[str, List[Dict[str, Any]]],
    universe: List[Tuple[str, str]],
) -> Dict[str, Any]:
    print("Step 3: spot-checks…", flush=True)
    ik_by_sym = {s: ik for s, ik in universe}
    out: Dict[str, Any] = {"symbols": {}, "tv_mm_deltas": {}}
    focus_day = "2026-07-31"
    focus_hh = ["09:25", "09:35", "09:45", "09:55", "10:05", "10:15", "10:25"]

    for sym in SPOT_SYMS:
        ik = ik_by_sym.get(sym)
        nat_day = native.get(sym, {}).get(focus_day) or []
        nat_prev = native.get(sym, {}).get(_prev_session(focus_day)) or []
        nat_enr = enrich_native_10m(nat_day, nat_prev)
        nat_by = {b["bar_end"].strftime("%H:%M"): b for b in nat_enr}

        stored_enr = []
        if ik and ik in stored_by_ik:
            day_rows = filter_session_1m(stored_by_ik[ik], focus_day)
            prev_rows = filter_session_1m(stored_by_ik[ik], _prev_session(focus_day))
            stored_enr = enrich_bars_v12(day_rows, prev_rows)
        st_by = {b["bar_end"].strftime("%H:%M"): b for b in stored_enr}

        rows = []
        for hh in focus_hh:
            n = nat_by.get(hh)
            s = st_by.get(hh)
            row = {"hhmm": hh, "native": None, "stored_v12": None}
            if n:
                row["native"] = {
                    "ohlc": [round(n["open"], 2), round(n["high"], 2), round(n["low"], 2), round(n["close"], 2)],
                    "vwap": round(n["vwap"], 2),
                    "ema5": round(n["ema5"], 2),
                    "volume": round(n["volume"], 2),
                }
            if s:
                row["stored_v12"] = {
                    "n1m": s.get("n1m"),
                    "ohlc": [round(s["open"], 2), round(s["high"], 2), round(s["low"], 2), round(s["close"], 2)],
                    "vwap": round(s["vwap"], 2),
                    "ema5": round(s["ema5"], 2),
                }
            if n and s:
                row["d_close"] = round(n["close"] - s["close"], 2)
                row["d_vwap"] = round(n["vwap"] - s["vwap"], 2)
                row["d_ema5"] = round(n["ema5"] - s["ema5"], 2)
            rows.append(row)
        out["symbols"][sym] = {
            "native_bars_day": len(nat_day),
            "stored_10m_bars": len(stored_enr),
            "focus": rows,
        }

    # M&M vs TV
    mm = out["symbols"].get("M&M", {}).get("focus") or []
    deltas = []
    for row in mm:
        hh = row["hhmm"]
        if hh not in TV_MM or not row.get("native"):
            continue
        tv = TV_MM[hh]
        deltas.append(
            {
                "hhmm": hh,
                "dvwap": round(row["native"]["vwap"] - tv["vwap"], 2),
                "dema5": round(row["native"]["ema5"] - tv["ema5"], 2),
            }
        )
    out["tv_mm_deltas"] = deltas
    if deltas:
        out["tv_mm_mean_abs_ema5"] = round(sum(abs(d["dema5"]) for d in deltas) / len(deltas), 3)
        out["tv_mm_mean_abs_vwap"] = round(sum(abs(d["dvwap"]) for d in deltas) / len(deltas), 3)
    return out


def score_bars(
    db,
    session_date: str,
    universe: List[Tuple[str, str]],
    bars_by_sym: Dict[str, List[Dict[str, Any]]],
    *,
    formula_version: str,
) -> List[Dict[str, Any]]:
    garuda = load_garuda_safe(db, session_date)
    grades = load_grades_series(db, session_date)
    rows_out: List[Dict[str, Any]] = []
    for sym, _ik in universe:
        bars = bars_by_sym.get(sym) or []
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
        ew_state: Dict[str, Any] = {"ew": 0.0, "armed": False, "cross_count": 0, "prev_side": 0}
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
            is_first_candle = i == 0
            vw_state, vw_cls = step_vw(
                vw_state, open_=o, close=c, vwap=vwap, dir_sign=dir_sign, is_first_candle=is_first_candle
            )
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
                ew_val, ew_event = float(ew_state.get("ew") or 0.0), None

            if not in_window:
                continue
            if not b.get("ema_reliable", True):
                # still emit but flag — v1.2 emitted with ema_reliable column
                pass

            stretch, ow = overextension_weight(c, float(b["session_open"]))
            mult, final = _mult_ow_vw(last_rank, ow, vw_state)
            # v1.2 used OW*VW only for structural_multiplier when EW is badge;
            # check existing — actually v1.1+ EW as badge not multiplier
            structural_multiplier = mult
            final_score = final
            # Include EW in reporting; match v1.2 file columns
            g, sc, stale, gts = grade_locf(grade_series, be)

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
                    "ema5": round(ema5, 4),
                    "ema10": round(float(b["ema10"]), 4),
                    "ema_reliable": bool(b.get("ema_reliable")),
                    "stretch_pct": round(stretch, 4),
                    "OW": round(ow, 4),
                    "VW": round(vw_state, 4),
                    "vw_classification": vw_cls,
                    "EW": round(ew_val, 4),
                    "ew_unlocked": bool(ew_state.get("armed")),
                    "ew_event": ew_event,
                    "ew_cross_count": int(ew_state.get("cross_count") or 0),
                    "garuda_rank_score": last_rank,
                    "garuda_top6_rank": last_top6,
                    "structural_multiplier": structural_multiplier,
                    "final_score": final_score,
                    "confidence_grade": g,
                    "trade_score": sc,
                    "rs_score_stale_minutes": stale,
                    "rs_grade_source_ts": gts,
                    "grade_ok": _grade_ok(g),
                    "n1m_in_bar": b.get("n1m"),
                    "data_source": b.get("data_source") or formula_version,
                    "formula_version": formula_version,
                    "vwap_input": b.get("vwap_input"),
                }
            )
    return rows_out


def before_after(
    v12_csv: Optional[Path],
    clean_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    print("Step 5: before/after…", flush=True)
    if not v12_csv or not v12_csv.exists():
        return {"error": f"missing v12 csv: {v12_csv}"}

    old_by: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    with v12_csv.open() as fh:
        for r in csv.DictReader(fh):
            key = (r["session_date"], r["symbol"], r["bar_hhmm"])
            old_by[key] = r

    paired = []
    only_new = 0
    only_old = 0
    abs_dfs = []
    abs_dow = []
    abs_dvw = []
    abs_dew = []
    abs_dc = []
    abs_dema = []
    abs_dvwap = []
    big_shifts = []

    new_keys = set()
    for r in clean_rows:
        key = (r["session_date"], r["symbol"], r["bar_hhmm"])
        new_keys.add(key)
        o = old_by.get(key)
        if not o:
            only_new += 1
            continue
        def ff(x):
            try:
                return float(x) if x not in (None, "") else None
            except Exception:
                return None

        dfs = None
        if r.get("final_score") is not None and ff(o.get("final_score")) is not None:
            dfs = r["final_score"] - ff(o["final_score"])
            abs_dfs.append(abs(dfs))
        for arr, a, b in (
            (abs_dow, r.get("OW"), ff(o.get("OW"))),
            (abs_dvw, r.get("VW"), ff(o.get("VW"))),
            (abs_dew, r.get("EW"), ff(o.get("EW"))),
            (abs_dc, r.get("close"), ff(o.get("close"))),
            (abs_dema, r.get("ema5"), ff(o.get("ema5"))),
            (abs_dvwap, r.get("vwap"), ff(o.get("vwap"))),
        ):
            if a is not None and b is not None:
                arr.append(abs(float(a) - float(b)))

        if dfs is not None and abs(dfs) >= 5:
            big_shifts.append(
                {
                    "key": f"{key[0]}|{key[1]}|{key[2]}",
                    "d_final": round(dfs, 3),
                    "old_final": ff(o.get("final_score")),
                    "new_final": r.get("final_score"),
                    "d_close": round(float(r["close"]) - ff(o["close"]), 2) if ff(o.get("close")) is not None else None,
                    "d_ema5": round(float(r["ema5"]) - ff(o["ema5"]), 2) if ff(o.get("ema5")) is not None else None,
                    "d_vwap": round(float(r["vwap"]) - ff(o["vwap"]), 2) if ff(o.get("vwap")) is not None else None,
                    "old_n1m": o.get("n1m_in_bar"),
                }
            )

    for key in old_by:
        if key[0] in SESSIONS and key not in new_keys:
            only_old += 1

    big_shifts.sort(key=lambda x: -abs(x["d_final"]))

    def mean(xs):
        return round(sum(xs) / len(xs), 4) if xs else None

    def pct(xs, thr):
        return round(sum(1 for x in xs if x >= thr) / len(xs), 4) if xs else None

    return {
        "paired_rows": len(abs_dfs) if abs_dfs else len(clean_rows) - only_new,
        "only_in_clean": only_new,
        "only_in_v12_stored": only_old,
        "mean_abs_d_final_score": mean(abs_dfs),
        "median_abs_d_final_score": round(sorted(abs_dfs)[len(abs_dfs) // 2], 4) if abs_dfs else None,
        "pct_final_abs_ge_1": pct(abs_dfs, 1),
        "pct_final_abs_ge_5": pct(abs_dfs, 5),
        "pct_final_abs_ge_10": pct(abs_dfs, 10),
        "mean_abs_d_OW": mean(abs_dow),
        "mean_abs_d_VW": mean(abs_dvw),
        "mean_abs_d_EW": mean(abs_dew),
        "mean_abs_d_close": mean(abs_dc),
        "mean_abs_d_ema5": mean(abs_dema),
        "mean_abs_d_vwap": mean(abs_dvwap),
        "top_final_shifts": big_shifts[:30],
        "mm_0731_paired": [
            x
            for x in big_shifts
            if x["key"].startswith("2026-07-31|M&M|")
        ]
        + [
            {
                "note": "include even small MM shifts",
            }
        ],
    }


def write_md(out_dir: Path, gap, fetch_meta, spots, compare, n_clean: int) -> None:
    lines = [
        "# Structural Quality — clean native 10m re-fetch (2026-07-27..31)",
        "",
        "**LIVE PROMOTION NOT WIRED.** Separate dataset from `upstox_ws_intraday_1m` (not overwritten).",
        "",
        "## Step 1 — Gap audit (stored 1m)",
        "",
        f"- Symbol-days incomplete: **{gap.get('incomplete_symbol_days')}/{gap.get('symbol_day_pairs')}** "
        f"({gap.get('frac_incomplete')})",
        f"- Sum of missing minutes: **{gap.get('total_missing_minutes_sum')}**",
        f"- Mean missing when incomplete: **{gap.get('mean_missing_when_incomplete')}**",
        f"- Detail sample: mean fraction of gaps in 09:35–10:25 = "
        f"**{gap.get('detail_sample_mean_frac_gaps_in_problem_window')}**; "
        f"share of gapped symbol-days with ≥1 gap in that window = "
        f"**{gap.get('detail_sample_pct_with_any_gap_in_problem_window')}**",
        "",
        "### Worst 10 symbol-days",
        "",
        "| symbol | day | n_bars | missing |",
        "|--------|-----|-------:|--------:|",
    ]
    for w in (gap.get("worst_25") or [])[:10]:
        lines.append(f"| {w['symbol']} | {w['day']} | {w['n_bars']} | {w['missing']} |")
    lines += [
        "",
        "## Step 2 — Native `minutes/10` fetch",
        "",
        f"- Interval: `{fetch_meta.get('interval')}` (no 1m aggregation)",
        f"- Range: {fetch_meta.get('from')} → {fetch_meta.get('to')}",
        f"- Symbols ok/fail: **{fetch_meta.get('symbols_ok')}/{fetch_meta.get('symbols_fail')}**",
        f"- Bars written: **{fetch_meta.get('bars_written')}** → `fo_native_10m.jsonl`",
        "",
        "## Step 3 — Spot-checks",
        "",
        f"- M&M vs TV mean |ΔEMA5|={spots.get('tv_mm_mean_abs_ema5')} |ΔVWAP|={spots.get('tv_mm_mean_abs_vwap')}",
        "",
    ]
    for sym, block in (spots.get("symbols") or {}).items():
        lines.append(f"### {sym} 2026-07-31 (native vs stored v1.2)")
        lines.append("")
        lines.append(f"native bars={block.get('native_bars_day')} stored_10m={block.get('stored_10m_bars')}")
        lines.append("")
        lines.append("| hhmm | d_close | d_vwap | d_ema5 | native VWAP/EMA5 | stored n1m |")
        lines.append("|------|--------:|-------:|-------:|------------------|------------|")
        for r in block.get("focus") or []:
            n = r.get("native") or {}
            lines.append(
                f"| {r['hhmm']} | {r.get('d_close')} | {r.get('d_vwap')} | {r.get('d_ema5')} | "
                f"{n.get('vwap')}/{n.get('ema5')} | {(r.get('stored_v12') or {}).get('n1m')} |"
            )
        lines.append("")
    lines += [
        "## Step 4–5 — Structural scores before/after",
        "",
        f"- Clean scored rows: **{n_clean}**",
        f"- Paired with v1.2 stored path: see `before_after.json`",
        f"- mean |Δ final_score| = **{compare.get('mean_abs_d_final_score')}**",
        f"- median |Δ final_score| = **{compare.get('median_abs_d_final_score')}**",
        f"- % |Δfinal| ≥5 = **{compare.get('pct_final_abs_ge_5')}**; ≥10 = **{compare.get('pct_final_abs_ge_10')}**",
        f"- mean |Δ OW/VW/EW| = {compare.get('mean_abs_d_OW')} / {compare.get('mean_abs_d_VW')} / {compare.get('mean_abs_d_EW')}",
        f"- mean |Δ close/ema5/vwap| = {compare.get('mean_abs_d_close')} / {compare.get('mean_abs_d_ema5')} / {compare.get('mean_abs_d_vwap')}",
        "",
        "## Interpretation",
        "",
        "This closes **websocket 1m gaps**, not tip-stale grade contamination. "
        "Grade-gated metrics still need a clean post-`c115a77` week.",
        "",
    ]
    (out_dir / "README.md").write_text("\n".join(lines))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--v12-csv", default="")
    ap.add_argument("--sleep", type=float, default=0.35)
    ap.add_argument("--skip-fetch", action="store_true")
    args = ap.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl = out_dir / "fo_native_10m.jsonl"
    v12_csv = Path(args.v12_csv) if args.v12_csv else None

    db = SessionLocal()
    try:
        universe = load_universe(db)
        print(f"Universe: {len(universe)} symbols", flush=True)

        gap = gap_audit(db, universe)
        (out_dir / "gap_audit.json").write_text(json.dumps(gap, indent=2, default=str))

        if args.skip_fetch and jsonl.exists():
            fetch_meta = json.loads((out_dir / "fetch_meta.json").read_text())
            print("Skipping fetch (using existing jsonl)", flush=True)
        else:
            fetch_meta = fetch_native_10m(universe, jsonl, args.sleep)
            (out_dir / "fetch_meta.json").write_text(json.dumps(fetch_meta, indent=2))

        native = load_native_jsonl(jsonl)

        # Stored 1m for spot + optional — only spot IKs to save RAM? Load all for fairness on compare path we use v12 csv
        spot_iks = [ik for s, ik in universe if s in SPOT_SYMS]
        stored = load_1m_range(db, spot_iks, WARMUP_FROM, END_DAY)
        spots = spot_checks(native, stored, universe)
        (out_dir / "spot_checks.json").write_text(json.dumps(spots, indent=2, default=str))

        print("Step 4: recompute structural scores on native 10m…", flush=True)
        all_rows: List[Dict[str, Any]] = []
        for day in SESSIONS:
            prev = _prev_session(day)
            bars_by_sym = {}
            for sym, _ik in universe:
                day_bars = native.get(sym, {}).get(day) or []
                prev_bars = native.get(sym, {}).get(prev) or []
                bars_by_sym[sym] = enrich_native_10m(day_bars, prev_bars)
            rows = score_bars(db, day, universe, bars_by_sym, formula_version="v1.2_clean_native10m")
            all_rows.extend(rows)
            print(f"  [{day}] rows={len(rows)}", flush=True)

        csv_path = out_dir / "structural_quality_backtest_clean10m.csv"
        if all_rows:
            cols = list(all_rows[0].keys())
            with csv_path.open("w", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=cols)
                w.writeheader()
                w.writerows(all_rows)

        compare = before_after(v12_csv, all_rows)
        # Enrich MM small shifts
        if v12_csv and v12_csv.exists():
            mm_rows = []
            old_by = {}
            with v12_csv.open() as fh:
                for r in csv.DictReader(fh):
                    if r["session_date"] == "2026-07-31" and r["symbol"] == "M&M":
                        old_by[r["bar_hhmm"]] = r
            for r in all_rows:
                if r["session_date"] != "2026-07-31" or r["symbol"] != "M&M":
                    continue
                o = old_by.get(r["bar_hhmm"])
                if not o:
                    continue
                try:
                    dfs = float(r["final_score"] or 0) - float(o.get("final_score") or 0)
                except Exception:
                    dfs = None
                mm_rows.append(
                    {
                        "hhmm": r["bar_hhmm"],
                        "d_final": round(dfs, 3) if dfs is not None else None,
                        "d_close": round(float(r["close"]) - float(o["close"]), 2),
                        "d_ema5": round(float(r["ema5"]) - float(o["ema5"]), 2),
                        "d_vwap": round(float(r["vwap"]) - float(o["vwap"]), 2),
                        "old_n1m": o.get("n1m_in_bar"),
                        "OW_old_new": f"{o.get('OW')}→{r.get('OW')}",
                        "EW_old_new": f"{o.get('EW')}→{r.get('EW')}",
                    }
                )
            compare["mm_0731_all_bars"] = mm_rows

        (out_dir / "before_after.json").write_text(json.dumps(compare, indent=2, default=str))
        write_md(out_dir, gap, fetch_meta, spots, compare, len(all_rows))

        # Compact summary json
        summary = {
            "gap": {
                k: gap[k]
                for k in (
                    "incomplete_symbol_days",
                    "symbol_day_pairs",
                    "frac_incomplete",
                    "total_missing_minutes_sum",
                    "mean_missing_when_incomplete",
                    "detail_sample_mean_frac_gaps_in_problem_window",
                    "detail_sample_pct_with_any_gap_in_problem_window",
                    "worst_25",
                )
                if k in gap
            },
            "fetch": fetch_meta,
            "spot_tv_mm": {
                "mean_abs_ema5": spots.get("tv_mm_mean_abs_ema5"),
                "mean_abs_vwap": spots.get("tv_mm_mean_abs_vwap"),
                "deltas": spots.get("tv_mm_deltas"),
            },
            "before_after": {k: compare[k] for k in compare if k != "top_final_shifts"},
            "top_final_shifts_10": (compare.get("top_final_shifts") or [])[:10],
        }
        (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
        print("DONE", json.dumps(summary["before_after"], indent=2)[:800], flush=True)
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
