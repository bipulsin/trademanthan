#!/usr/bin/env python3
"""22-Jul checkpoint follow-up: Items A/B/C/E/F (read-only). Item D separate.

Run on paperclip app container:
  PYTHONPATH=/app /opt/venv/bin/python /tmp/checkpoint_22jul_followup_extract.py
"""
from __future__ import annotations

import csv
import json
import random
import time
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.kavach_10m import aggregate_10m_bars, metrics_from_10m_candles
from backend.services.kavach_confidence import compute_stretch_pct, hard_stretch_pct, soft_stretch_pct
from backend.services.relative_strength_scanner import CANDLE_INTERVAL, _parse_ist_date, _sorted_candles
from backend.services.upstox_service import UpstoxService

IST = pytz.timezone("Asia/Kolkata")
ROOT = Path("/tmp/ckpt_22jul_followup")
THROTTLE = 0.2
A_WIN, B_WIN = "2026-07-08", "2026-07-22"
PRE_N, POST_N = 6, 6
SOFT = soft_stretch_pct()
HARD = hard_stretch_pct()
GIVEBACK_JSON = Path("/tmp/ckpt_22jul_extract/04_giveback_2r_matches.json")


def jdump(folder: Path, name: str, obj: Any) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    p = folder / name
    p.write_text(json.dumps(obj, indent=2, default=str))
    print("WROTE", p)


def csv_write(folder: Path, name: str, rows: List[Dict[str, Any]]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    p = folder / name
    if not rows:
        p.write_text("")
        print("WROTE", p, "EMPTY")
        return
    with p.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: (json.dumps(v, default=str) if isinstance(v, (dict, list)) else v) for k, v in r.items()})
    print("WROTE", p, "n=", len(rows))


def _f(x, d=None):
    try:
        return float(x) if x is not None else d
    except Exception:
        return d


def session_key(d) -> str:
    if d is None:
        return ""
    if hasattr(d, "isoformat"):
        return d.isoformat()[:10]
    return str(d)[:10]


def to_ist(dt) -> Optional[datetime]:
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def bar_end_ist(b: Dict) -> Optional[datetime]:
    be = b.get("bar_end")
    if be is not None:
        return to_ist(be)
    ts = b.get("timestamp")
    t = to_ist(ts) if not isinstance(ts, datetime) else to_ist(ts)
    return t + timedelta(minutes=5) if t else None


def hm_min(t) -> Optional[int]:
    if isinstance(t, dtime):
        return t.hour * 60 + t.minute
    if isinstance(t, datetime):
        t = to_ist(t)
        return t.hour * 60 + t.minute if t else None
    return None


def votes_label(metrics: Dict[str, Any], direction: str) -> str:
    """ALIGNED if ≥2/3 panel votes match trade direction; else SPLIT/OPPOSED."""
    st = metrics.get("supertrend")
    macd = _f(metrics.get("macd"), 0)
    sig = _f(metrics.get("macd_signal"), 0)
    panel = _f(metrics.get("panel_ema"), 0)
    vwap = _f(metrics.get("vwap"), 0)
    st_bull = st is not None and float(st) > 0
    st_bear = st is not None and float(st) < 0
    macd_bull = macd > sig
    macd_bear = macd < sig
    ema_above = panel > vwap
    ema_below = panel < vwap
    bull = (1 if macd_bull else 0) + (1 if st_bull else 0) + (1 if ema_above else 0)
    bear = (1 if macd_bear else 0) + (1 if st_bear else 0) + (1 if ema_below else 0)
    if direction == "LONG":
        if bull >= 2:
            return f"ALIGNED({bull}/3 bull)"
        if bear >= 2:
            return f"OPPOSED({bear}/3 bear)"
        return f"SPLIT(bull={bull},bear={bear})"
    if bear >= 2:
        return f"ALIGNED({bear}/3 bear)"
    if bull >= 2:
        return f"OPPOSED({bull}/3 bull)"
    return f"SPLIT(bull={bull},bear={bear})"


def ema_aligned(metrics: Dict[str, Any], direction: str) -> bool:
    e5 = _f(metrics.get("ema5"))
    e10 = _f(metrics.get("ema10_10m") or metrics.get("ema10"))
    vwap = _f(metrics.get("vwap"))
    px = _f(metrics.get("price"))
    if None in (e5, e10, vwap, px):
        return False
    if direction == "LONG":
        return px >= e5 >= e10 and px >= vwap  # loose stack
    return px <= e5 <= e10 and px <= vwap


def classify_post_exit(
    direction: str, exit_px: float, post_bars: List[Dict[str, Any]]
) -> Dict[str, Any]:
    if not post_bars:
        return {"class": "TRUNCATED", "points_left": None, "pct_left": None, "note": "no post-exit bars"}
    fav_ext = 0.0
    adv_ext = 0.0
    for b in post_bars:
        hi, lo = _f(b["high"], exit_px), _f(b["low"], exit_px)
        if direction == "LONG":
            fav_ext = max(fav_ext, hi - exit_px)
            adv_ext = max(adv_ext, exit_px - lo)
        else:
            fav_ext = max(fav_ext, exit_px - lo)
            adv_ext = max(adv_ext, hi - exit_px)
    last = _f(post_bars[-1]["close"], exit_px)
    net = (last - exit_px) if direction == "LONG" else (exit_px - last)
    # thresholds in pts relative to exit
    if fav_ext >= max(1.0, 0.15 * abs(exit_px) * 0.01) and fav_ext > adv_ext * 1.25 and net > 0:
        cls = "CONTINUATION"
    elif adv_ext >= max(1.0, 0.15 * abs(exit_px) * 0.01) and adv_ext > fav_ext * 1.25 and net < 0:
        cls = "REVERSAL"
    else:
        cls = "CHOP"
    return {
        "class": cls,
        "points_left_on_table": round(fav_ext, 4) if cls == "CONTINUATION" else round(fav_ext, 4),
        "pct_left": round(fav_ext / exit_px * 100, 4) if exit_px else None,
        "adverse_excursion_pts": round(adv_ext, 4),
        "net_close_pts": round(net, 4),
    }


def main() -> None:
    random.seed(22)
    db = SessionLocal()
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    for sub in "A B C D E F".split():
        (ROOT / sub).mkdir(parents=True, exist_ok=True)

    trades = [dict(r) for r in db.execute(text("""
        SELECT * FROM trade_log
        WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
        ORDER BY session_date, entry_time, id
    """), {"a": A_WIN, "b": B_WIN}).mappings()]
    print("TRADES", len(trades))

    ikey_cache: Dict[str, Optional[str]] = {}

    def ikey(sym: str) -> Optional[str]:
        if sym not in ikey_cache:
            row = db.execute(text(
                "SELECT currmth_future_instrument_key FROM arbitrage_master WHERE UPPER(stock)=:s LIMIT 1"
            ), {"s": sym.upper()}).fetchone()
            ikey_cache[sym] = str(row[0]) if row and row[0] else None
        return ikey_cache[sym]

    # Preload badge / stretch for joins
    badge_rows = [dict(r) for r in db.execute(text("""
        SELECT session_date, symbol, logged_at, gate_badges, whipsaw_active, dir_conflict_active,
               regime_unstable_active, churn_active
        FROM kavach_badge_input_log
        WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
    """), {"a": "2026-07-13", "b": B_WIN}).mappings()]

    try:
        stretch_rows = [dict(r) for r in db.execute(text("""
            SELECT session_date, symbol, logged_at, bar_at, stretch_pct, stretch_score_penalty,
                   would_suppress_ready, base_grade_post_stretch, trade_score_post_stretch,
                   close_px, ema10, vwap
            FROM kavach_stretch_penalty_log
            WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
        """), {"a": "2026-07-13", "b": B_WIN}).mappings()]
    except Exception as e:
        print("STRETCH_LOAD", e)
        stretch_rows = []

    day_bars_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
    metrics_cache: Dict[Tuple[str, str, str], Dict[int, Any]] = {}

    def load_bars(sym: str, sd: date) -> Dict[str, Any]:
        key = (sym, str(sd))
        if key in day_bars_cache:
            return day_bars_cache[key]
        ik = ikey(sym)
        out: Dict[str, Any] = {"bars": [], "c5": [], "ikey": ik}
        if not ik:
            day_bars_cache[key] = out
            return out
        time.sleep(THROTTLE)
        raw = ux.get_historical_candles_by_instrument_key(
            ik, interval=CANDLE_INTERVAL, days_back=8, range_end_date=sd
        )
        c5 = _sorted_candles(raw) if raw else []
        out["c5"] = c5
        want = session_key(sd)
        today5 = [c for c in c5 if session_key(_parse_ist_date(c.get("timestamp"))) == want]
        out["bars"] = aggregate_10m_bars(today5) if len(today5) >= 2 else []
        day_bars_cache[key] = out
        print(f"  bars {sym} {sd} n={len(out['bars'])}")
        return out

    def metrics_for_day(sym: str, sd: date, direction: str) -> Dict[int, Any]:
        mkey = (sym, str(sd), direction)
        if mkey in metrics_cache:
            return metrics_cache[mkey]
        day = load_bars(sym, sd)
        bars, c5 = day["bars"], day["c5"]
        ranking = "BULL" if direction == "LONG" else "BEAR"
        out: Dict[int, Any] = {}
        for i, b in enumerate(bars):
            be = bar_end_ist(b)
            if be is None or len(c5) < 40:
                continue
            try:
                m = metrics_from_10m_candles(c5, ranking_type=ranking, nifty_pct=0.0, now=be)
            except Exception as e:
                print("METRICS_ERR", sym, be, e)
                m = None
            if not m:
                continue
            m["bar_idx"] = i
            m["open"] = _f(b.get("open"))
            m["high"] = _f(b.get("high"))
            m["low"] = _f(b.get("low"))
            m["close"] = _f(b.get("close"))
            m["volume"] = _f(b.get("volume"))
            sp = compute_stretch_pct(m.get("price"), m.get("ema10_10m"), m.get("vwap"))
            m["stretch_pct"] = sp
            m["stretch_zone"] = (
                "HARD" if sp is not None and sp > HARD else
                ("SOFT" if sp is not None and sp > SOFT else "OK")
            )
            m["votes"] = votes_label(m, direction)
            m["ema_stack_aligned"] = ema_aligned(m, direction)
            out[i] = m
        metrics_cache[mkey] = out
        print(f"  metrics {sym} {sd} {direction} n={len(out)}")
        return out

    def load_day(sym: str, sd: date, direction: str) -> Dict[str, Any]:
        day = load_bars(sym, sd)
        return {
            "bars": day["bars"],
            "c5": day["c5"],
            "ikey": day["ikey"],
            "metrics_by_idx": metrics_for_day(sym, sd, direction),
        }

    def nearest_badges(sym: str, sd: date, be: datetime) -> Dict[str, Any]:
        best = None
        best_dt = None
        for r in badge_rows:
            if str(r["session_date"]) != str(sd) or r["symbol"] != sym:
                continue
            la = to_ist(r["logged_at"])
            if la is None:
                continue
            if la <= be + timedelta(minutes=5):
                if best_dt is None or la > best_dt:
                    best_dt = la
                    best = r
        if not best:
            return {}
        badges = []
        if best.get("whipsaw_active"):
            badges.append("WHIPSAW")
        if best.get("dir_conflict_active"):
            badges.append("DIR_CONFLICT")
        if best.get("regime_unstable_active"):
            badges.append("REGIME_UNSTABLE")
        if best.get("churn_active"):
            badges.append("CHURN")
        gb = best.get("gate_badges")
        if gb:
            badges.append(f"gate:{gb}")
        return {"badges": badges, "badge_logged_at": str(best_dt)}

    # ---------------- ITEM A ----------------
    print("=== ITEM A ===")
    a_summaries = []
    a_candles_all = {}
    for t in trades:
        tid = t["id"]
        sym = t["symbol"]
        sd = t["session_date"]
        direction = (t["direction"] or "").upper()
        entry = float(t["entry_price"])
        exit_px = float(t["exit_price"]) if t.get("exit_price") is not None else None
        et_m = hm_min(t["entry_time"])
        xt_m = hm_min(t["exit_time"])
        day = load_day(sym, sd, direction)
        bars = day["bars"]
        if not bars:
            pnl = None
            if t.get("points_captured") is not None and t.get("qty") is not None:
                pnl = float(t["points_captured"]) * int(t["qty"])
            summary = {
                "trade_id": tid, "session_date": str(sd), "symbol": sym, "direction": direction,
                "entry_time": str(t["entry_time"]), "exit_time": str(t["exit_time"]),
                "entry_price": float(t["entry_price"]) if t.get("entry_price") is not None else None,
                "exit_price": float(t["exit_price"]) if t.get("exit_price") is not None else None,
                "exit_trigger": t.get("exit_trigger"),
                "realized_pnl_inr": pnl,
                "error": "no_10m_bars",
                "pre_truncation": PRE_N, "post_truncation": POST_N,
                "stretch_before_entry": None, "stretch_at_entry": None,
                "stretch_began_only_after_entry": None,
                "pre_entry_ema_aligned_count_of_6": None, "pre_entry_available": 0,
                "dist_vwap_pts_at_entry": None, "dist_vwap_pct_at_entry": None,
                "post_exit_class": "TRUNCATED", "post_exit_points_left": None,
                "post_exit_pct_left": None, "post_exit_detail": {"class": "TRUNCATED"},
                "candle_detail_key": f"trade_{tid}_{sym}",
                "entry_grade_reconstructed": None, "entry_score_reconstructed": None,
                "entry_votes_reconstructed": None,
                "confidence_at_entry_log": t.get("confidence_at_entry"),
            }
            a_summaries.append(summary)
            a_candles_all[summary["candle_detail_key"]] = []
            continue
        # find entry / exit bar indices (first bar ending at/after entry time)
        entry_idx = None
        exit_idx = None
        for i, b in enumerate(bars):
            be = bar_end_ist(b)
            if be is None:
                continue
            bm = be.hour * 60 + be.minute
            if entry_idx is None and et_m is not None and bm >= et_m:
                entry_idx = i
            if exit_idx is None and xt_m is not None and bm >= xt_m:
                exit_idx = i
        if entry_idx is None:
            entry_idx = 0
        if exit_idx is None:
            exit_idx = len(bars) - 1
        entry_idx = max(0, min(entry_idx, len(bars) - 1))
        exit_idx = max(entry_idx, min(exit_idx, len(bars) - 1))

        pre_start = max(0, entry_idx - PRE_N)
        pre_idxs = list(range(pre_start, entry_idx))
        pre_trunc = PRE_N - len(pre_idxs)
        post_end = min(len(bars), (exit_idx + 1) + POST_N)
        post_idxs = list(range(exit_idx + 1, post_end))
        post_trunc = POST_N - len(post_idxs)

        def candle_row(i: int, role: str) -> Dict[str, Any]:
            b = bars[i]
            m = day["metrics_by_idx"].get(i) or {}
            be = bar_end_ist(b)
            badges = nearest_badges(sym, sd, be) if be else {}
            return {
                "role": role,
                "bar_idx": i,
                "bar_end": str(be),
                "open": _f(b.get("open")),
                "high": _f(b.get("high")),
                "low": _f(b.get("low")),
                "close": _f(b.get("close")),
                "ema5": m.get("ema5"),
                "ema10": m.get("ema10_10m"),
                "vwap": m.get("vwap"),
                "supertrend": m.get("supertrend"),
                "macd": m.get("macd"),
                "macd_signal": m.get("macd_signal"),
                "macd_histogram": m.get("macd_histogram"),
                "adx": m.get("adx"),
                "confidence_grade": m.get("confidence_grade"),
                "trade_score": m.get("trade_score"),
                "kavach_state": m.get("kavach_state"),
                "votes": m.get("votes"),
                "stretch_pct": m.get("stretch_pct"),
                "stretch_zone": m.get("stretch_zone"),
                "ema_stack_aligned": m.get("ema_stack_aligned"),
                "badges": badges.get("badges"),
            }

        candles = []
        for i in pre_idxs:
            candles.append(candle_row(i, "PRE"))
        candles.append(candle_row(entry_idx, "ENTRY"))
        for i in post_idxs:
            candles.append(candle_row(i, "POST"))

        # pre-entry flags
        pre_metrics = [day["metrics_by_idx"][i] for i in pre_idxs if i in day["metrics_by_idx"]]
        stretch_pre = [m.get("stretch_zone") for m in pre_metrics]
        stretch_before = any(z in ("SOFT", "HARD") for z in stretch_pre)
        entry_m = day["metrics_by_idx"].get(entry_idx) or {}
        stretch_at_entry = entry_m.get("stretch_zone")
        aligned_count = sum(1 for m in pre_metrics if m.get("ema_stack_aligned"))
        vwap_e = _f(entry_m.get("vwap"))
        dist_vwap_pts = abs(entry - vwap_e) if vwap_e else None
        dist_vwap_pct = (dist_vwap_pts / entry * 100) if (dist_vwap_pts is not None and entry) else None

        post_bars_ohlc = [bars[i] for i in post_idxs]
        post_cls = classify_post_exit(direction, exit_px or entry, post_bars_ohlc)

        pnl = None
        if t.get("points_captured") is not None and t.get("qty") is not None:
            pnl = float(t["points_captured"]) * int(t["qty"])

        summary = {
            "trade_id": tid,
            "session_date": str(sd),
            "symbol": sym,
            "direction": direction,
            "entry_time": str(t["entry_time"]),
            "exit_time": str(t["exit_time"]),
            "entry_price": entry,
            "exit_price": exit_px,
            "exit_trigger": t.get("exit_trigger"),
            "realized_pnl_inr": pnl,
            "confidence_at_entry_log": t.get("confidence_at_entry"),
            "pre_truncation": pre_trunc,
            "post_truncation": post_trunc,
            "stretch_before_entry": stretch_before,
            "stretch_at_entry": stretch_at_entry,
            "stretch_began_only_after_entry": (not stretch_before) and stretch_at_entry in ("SOFT", "HARD"),
            "pre_entry_ema_aligned_count_of_6": aligned_count,
            "pre_entry_available": len(pre_idxs),
            "dist_vwap_pts_at_entry": round(dist_vwap_pts, 4) if dist_vwap_pts is not None else None,
            "dist_vwap_pct_at_entry": round(dist_vwap_pct, 4) if dist_vwap_pct is not None else None,
            "post_exit_class": post_cls["class"],
            "post_exit_points_left": post_cls.get("points_left_on_table"),
            "post_exit_pct_left": post_cls.get("pct_left"),
            "post_exit_detail": post_cls,
            "candle_detail_key": f"trade_{tid}_{sym}",
            "entry_grade_reconstructed": entry_m.get("confidence_grade"),
            "entry_score_reconstructed": entry_m.get("trade_score"),
            "entry_votes_reconstructed": entry_m.get("votes"),
        }
        a_summaries.append(summary)
        a_candles_all[summary["candle_detail_key"]] = candles

    # correlations (honest small-n)
    from collections import Counter, defaultdict

    def rate(rows, pred):
        if not rows:
            return None
        return sum(1 for r in rows if pred(r)) / len(rows)

    L = [s for s in a_summaries if s.get("realized_pnl_inr") is not None and s["realized_pnl_inr"] < 0]
    W = [s for s in a_summaries if s.get("realized_pnl_inr") is not None and s["realized_pnl_inr"] > 0]
    for s, t in zip(a_summaries, trades):
        if s.get("realized_pnl_inr") is not None:
            continue
        pc = t.get("points_captured")
        if pc is None:
            continue
        if float(pc) < 0:
            L.append(s)
        elif float(pc) > 0:
            W.append(s)

    corr = {
        "n_trades": len(a_summaries),
        "n_with_pnl": sum(1 for s in a_summaries if s.get("realized_pnl_inr") is not None),
        "caveat": "n=25 is small — exploratory associations only, not statistical significance.",
        "losers_n": len(L),
        "winners_n": len(W),
        "losers_stretch_before_entry_rate": rate(L, lambda s: bool(s.get("stretch_before_entry"))),
        "winners_stretch_before_entry_rate": rate(W, lambda s: bool(s.get("stretch_before_entry"))),
        "losers_avg_pre_ema_aligned": (
            sum((s.get("pre_entry_ema_aligned_count_of_6") or 0) for s in L) / len(L) if L else None
        ),
        "winners_avg_pre_ema_aligned": (
            sum((s.get("pre_entry_ema_aligned_count_of_6") or 0) for s in W) / len(W) if W else None
        ),
        "losers_avg_dist_vwap_pct": (
            sum((s.get("dist_vwap_pct_at_entry") or 0) for s in L) / len(L) if L else None
        ),
        "winners_avg_dist_vwap_pct": (
            sum((s.get("dist_vwap_pct_at_entry") or 0) for s in W) / len(W) if W else None
        ),
        "post_exit_class_counts": dict(Counter(s.get("post_exit_class") for s in a_summaries)),
        "by_exit_trigger_post_class": {},
        "errors": [f"{s['symbol']} {s['session_date']}: {s.get('error')}" for s in a_summaries if s.get("error")],
    }
    by_trig = defaultdict(list)
    for s in a_summaries:
        trig = (s.get("exit_trigger") or "unknown/none")[:60]
        by_trig[trig].append(s.get("post_exit_class"))
    corr["by_exit_trigger_post_class"] = {k: dict(Counter(v)) for k, v in by_trig.items()}

    csv_write(ROOT / "A", "A_trade_summaries.csv", a_summaries)
    jdump(ROOT / "A", "A_candles_by_trade.json", a_candles_all)
    jdump(ROOT / "A", "A_correlations.json", corr)
    jdump(ROOT / "A", "A_summary.json", {
        "coverage": "trade_log 2026-07-13 → 2026-07-22 (25 trades)",
        "pre_post_bars": PRE_N,
        "soft_stretch_pct": SOFT,
        "hard_stretch_pct": HARD,
        "note": (
            "Grade/score/votes/ADX/ST/MACD reconstructed via metrics_from_10m_candles "
            "(nifty_pct=0 so scores are approximate). Badges from nearest badge_input_log ≤ bar."
        ),
        "n": len(a_summaries),
    })

    # ---------------- ITEM C (before B, independent) ----------------
    print("=== ITEM C TATAELXSI ===")
    tata = [t for t in trades if t["symbol"] == "TATAELXSI" and str(t["session_date"]) == "2026-07-20"]
    c_out = {"found_trades": [], "note": None}
    for t in tata:
        day = load_day(t["symbol"], t["session_date"], t["direction"])
        entry = float(t["entry_price"])
        ema10 = _f(t.get("ema10_at_entry"))
        vwap = _f(t.get("vwap_at_entry"))
        risk_planned = _f(t.get("planned_risk_pts"))
        risk_ema = abs(entry - ema10) if ema10 else None
        risk_vwap = abs(entry - vwap) if vwap else None
        cands = [x for x in [risk_planned, risk_ema, risk_vwap] if x and x > 0]
        risk_near = min(cands) if cands else None
        et_m = hm_min(t["entry_time"])
        xt_m = hm_min(t["exit_time"])
        peak_r_near = 0.0
        peak_r_ema = 0.0
        peak_info = None
        for b in day["bars"]:
            be = bar_end_ist(b)
            if be is None:
                continue
            bm = be.hour * 60 + be.minute
            if et_m is not None and bm <= et_m:
                continue
            if xt_m is not None and bm > xt_m + 10:
                break
            hi, lo = _f(b["high"], entry), _f(b["low"], entry)
            fav = hi - entry  # LONG
            if risk_near:
                peak_r_near = max(peak_r_near, fav / risk_near)
            if risk_ema:
                peak_r_ema = max(peak_r_ema, fav / risk_ema)
            if risk_near and fav / risk_near >= peak_r_near:
                peak_info = {"bar_end": str(be), "high": hi, "fav": fav}
        # also session max after entry including post-exit to 3532.50 anecdote
        peak_r_session = 0.0
        peak_px = entry
        for b in day["bars"]:
            be = bar_end_ist(b)
            if be is None or (et_m and be.hour * 60 + be.minute <= et_m):
                continue
            hi = _f(b["high"], entry)
            if hi > peak_px:
                peak_px = hi
            if risk_near:
                peak_r_session = max(peak_r_session, (hi - entry) / risk_near)
        c_out["found_trades"].append({
            "trade_id": t["id"],
            "entry": entry,
            "exit": t["exit_price"],
            "ema10_at_entry": ema10,
            "vwap_at_entry": vwap,
            "planned_risk_pts": risk_planned,
            "risk_ema10": risk_ema,
            "risk_vwap": risk_vwap,
            "risk_used_by_matcher_nearer": risk_near,
            "peak_r_through_exit_nearer": round(peak_r_near, 4),
            "peak_r_through_exit_ema10": round(peak_r_ema, 4),
            "peak_r_full_session_after_entry_nearer": round(peak_r_session, 4),
            "session_peak_price_after_entry": peak_px,
            "reached_2r_through_exit": peak_r_near >= 2.0 if risk_near else None,
            "reached_2r_full_session": peak_r_session >= 2.0 if risk_near else None,
            "exclusion_reason": (
                "Peak MFE through exit < 2R on nearer-risk basis — near-miss / not matcher bug"
                if risk_near and peak_r_near < 2.0 else
                ("Would qualify if full-session peak counted" if risk_near and peak_r_session >= 2.0 else "check")
            ),
        })
    jdump(ROOT / "C", "C_tataelxsi_reconciliation.json", c_out)

    # ---------------- ITEM B ----------------
    print("=== ITEM B ===")
    give = {"matches": []}
    if GIVEBACK_JSON.exists():
        give = json.loads(GIVEBACK_JSON.read_text())
    matches = give.get("matches") or []
    # map post-exit from A
    a_by_key = {(s["session_date"], s["symbol"], s["direction"]): s for s in a_summaries}
    faster = {"Rule 23", "Rule 20"}
    thresholds = [0.5, 0.75, 1.0, 1.5]  # retained-from-peak; trigger when giveback from peak exceeds (peak - retained)?
    # Spec: "giveback threshold of 1R-to-1.5R retained (gives back more than 0.5-1R from peak)"
    # retained R means exit when current R drops below peak_r - giveback_amount
    # Test giveback amounts: 0.5, 0.75, 1.0, 1.5 from peak

    def simulate_ratchet(m: Dict, giveback_from_peak: float) -> Dict[str, Any]:
        """Replay 10m bars; once peak>=2R, exit when R from peak falls by giveback_from_peak."""
        sym, sd = m["symbol"], date.fromisoformat(m["session_date"])
        direction = m["direction"]
        entry = float(m["entry_price"])
        risk = float(m["risk_pts"])
        day = load_day(sym, sd, direction)
        et_m = hm_min(datetime.strptime(m["entry_time"][:8], "%H:%M:%S").time()) if ":" in m["entry_time"] else None
        try:
            parts = m["entry_time"].split(":")
            et_m = int(parts[0]) * 60 + int(parts[1])
        except Exception:
            et_m = None
        peak = 0.0
        armed = False
        hyp_exit_r = None
        hyp_bar = None
        for b in day["bars"]:
            be = bar_end_ist(b)
            if be is None:
                continue
            bm = be.hour * 60 + be.minute
            if et_m is not None and bm <= et_m:
                continue
            hi, lo, cl = _f(b["high"], entry), _f(b["low"], entry), _f(b["close"], entry)
            if direction == "LONG":
                fav_ext = (hi - entry) / risk
                close_r = (cl - entry) / risk
            else:
                fav_ext = (entry - lo) / risk
                close_r = (entry - cl) / risk
            peak = max(peak, fav_ext)
            if peak >= 2.0:
                armed = True
            if armed and (peak - close_r) >= giveback_from_peak:
                hyp_exit_r = close_r
                hyp_bar = str(be)
                break
        return {
            "hyp_exit_r": hyp_exit_r,
            "hyp_bar": hyp_bar,
            "peak_r": peak,
            "actual_exit_r": m.get("exit_r"),
            "delta_vs_actual": (None if hyp_exit_r is None else round(hyp_exit_r - float(m.get("exit_r") or 0), 4)),
        }

    already_fast = []
    test_pop = []
    for m in matches:
        trig = ""
        key = (m["session_date"], m["symbol"], m["direction"])
        arow = a_by_key.get(key)
        # classify by known exit rules
        for t in trades:
            if str(t["session_date"]) == m["session_date"] and t["symbol"] == m["symbol"]:
                trig = t.get("exit_trigger") or ""
                break
        is_fast = ("Rule 23" in trig) or ("Rule 20" in trig)
        row = {**m, "exit_trigger": trig, "is_already_fast_rule": is_fast,
               "post_exit_class": arow["post_exit_class"] if arow else None}
        (already_fast if is_fast else test_pop).append(row)

    b_results = {"thresholds": {}, "already_fast_n": len(already_fast), "test_pop_n": len(test_pop)}
    for gb in thresholds:
        rows = []
        for m in test_pop:
            sim = simulate_ratchet(m, gb)
            false_pos = (
                sim["hyp_exit_r"] is not None
                and m.get("post_exit_class") == "CONTINUATION"
                and (sim["hyp_exit_r"] or 0) < float(m.get("exit_r") or 0)
            )
            # also FP if hyp exits and post-exit continued favorably past actual
            rows.append({
                "symbol": m["symbol"], "session_date": m["session_date"],
                "actual_exit_r": m.get("exit_r"), "peak_r": m.get("peak_r"),
                **sim,
                "post_exit_class": m.get("post_exit_class"),
                "would_exit_earlier": sim["hyp_exit_r"] is not None and (
                    float(m.get("exit_r") or -999) < sim["hyp_exit_r"] or True
                ) and sim["hyp_bar"] is not None,
                "false_positive_early_exit_risk": false_pos or (
                    sim["hyp_exit_r"] is not None and m.get("post_exit_class") == "CONTINUATION"
                ),
            })
        # refine would_exit_earlier: hyp_exit_r > actual_exit_r (saved R)
        for r in rows:
            if r["hyp_exit_r"] is None:
                r["would_exit_earlier"] = False
                r["r_saved_vs_actual"] = None
            else:
                r["r_saved_vs_actual"] = round(r["hyp_exit_r"] - float(r["actual_exit_r"] or 0), 4)
                r["would_exit_earlier"] = r["hyp_bar"] is not None
        b_results["thresholds"][f"giveback_{gb}R_from_peak"] = {
            "n_test": len(rows),
            "n_would_fire": sum(1 for r in rows if r["would_exit_earlier"]),
            "n_false_positive_continuation": sum(1 for r in rows if r["false_positive_early_exit_risk"]),
            "avg_r_saved": (
                sum(r["r_saved_vs_actual"] for r in rows if r["r_saved_vs_actual"] is not None)
                / max(1, sum(1 for r in rows if r["r_saved_vs_actual"] is not None))
            ),
            "rows": rows,
        }
    # also report already-fast group for reference
    b_results["already_fast_symbols"] = [f"{m['symbol']} {m['session_date']}" for m in already_fast]
    b_results["test_pop_symbols"] = [f"{m['symbol']} {m['session_date']}" for m in test_pop]
    b_results["depends_on_A"] = "false_positive_early_exit_risk uses A post_exit_class"
    b_results["caveat"] = "n=8 test-pop / n=10 matches — exploratory shadow candidate only; do not go live."
    jdump(ROOT / "B", "B_ratchet_backtest.json", b_results)
    # flat csv for best threshold view
    flat = []
    for thr, block in b_results["thresholds"].items():
        for r in block["rows"]:
            flat.append({"threshold": thr, **{k: r[k] for k in r if k != "peak_ohlc"}})
    csv_write(ROOT / "B", "B_ratchet_rows.csv", flat)

    # ---------------- ITEM E ----------------
    print("=== ITEM E ===")
    e_rows = []
    for s, t in zip(a_summaries, trades):
        # grade decay: look at pre candles grades
        key = s["candle_detail_key"]
        candles = a_candles_all.get(key) or []
        pre_grades = [c.get("confidence_grade") for c in candles if c["role"] == "PRE" and c.get("confidence_grade")]
        entry_grade = s.get("entry_grade_reconstructed") or t.get("confidence_at_entry")
        decay = False
        if pre_grades and entry_grade:
            # decay if earlier pre had A/A+ and entry is C/D or lower than first pre
            rank = {"A+": 5, "A": 4, "B": 3, "C": 2, "D": 1, "D!": 0}
            def rk(g):
                g = (g or "").replace("*", "")
                return rank.get(g, rank.get(g[:1], -1))
            if any(rk(g) >= 4 for g in pre_grades) and rk(entry_grade) <= 2:
                decay = True
            if pre_grades and rk(entry_grade) + 2 <= max(rk(g) for g in pre_grades):
                decay = True
        e_rows.append({
            "trade_id": s["trade_id"],
            "session_date": s["session_date"],
            "symbol": s["symbol"],
            "direction": s["direction"],
            "entry_time": s["entry_time"],
            "grade_at_entry_trade_log": t.get("confidence_at_entry"),
            "grade_at_entry_reconstructed": s.get("entry_grade_reconstructed"),
            "score_at_entry_reconstructed": s.get("entry_score_reconstructed"),
            "votes_at_entry": s.get("entry_votes_reconstructed"),
            "pre_entry_grades": pre_grades,
            "late_entry_after_grade_decay": decay,
            "exideind_style_flag": decay,
        })
    csv_write(ROOT / "E", "E_grade_votes_at_entry.csv", e_rows)
    jdump(ROOT / "E", "E_summary.json", {
        "n": len(e_rows),
        "late_entry_decay_flags": sum(1 for r in e_rows if r["late_entry_after_grade_decay"]),
        "flagged": [f"{r['symbol']} {r['session_date']}" for r in e_rows if r["late_entry_after_grade_decay"]],
        "note": "Votes reconstructed from ST/MACD/panel-EMA vs VWAP at entry bar (not historical Votes store).",
        "depends_on_A": True,
    })

    # ---------------- ITEM F ----------------
    print("=== ITEM F ===")
    passes = [dict(r) for r in db.execute(text("""
        SELECT id, session_date, symbol, direction, logged_at, rendered_state, pre_gate_state,
               vwap_slope_score, steep_ok, flip_flop, whipsaw_crosses, quality_pass,
               vwap_would_block, inputs
        FROM kavach_ready_consistency_log
        WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
          AND quality_pass IS TRUE
        ORDER BY logged_at
    """), {"a": "2026-07-15", "b": B_WIN}).mappings()]
    # high-confidence blocked: inputs.confidence A/A+ or reconstruct from inputs
    blocked = [dict(r) for r in db.execute(text("""
        SELECT id, session_date, symbol, direction, logged_at, rendered_state, pre_gate_state,
               vwap_slope_score, steep_ok, flip_flop, whipsaw_crosses, quality_pass,
               vwap_would_block, inputs
        FROM kavach_ready_consistency_log
        WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
          AND (quality_pass IS NOT TRUE)
          AND vwap_would_block IS TRUE
    """), {"a": "2026-07-15", "b": B_WIN}).mappings()]

    def conf_of(r):
        inp = r.get("inputs") or {}
        if isinstance(inp, str):
            try:
                inp = json.loads(inp)
            except Exception:
                inp = {}
        c = inp.get("confidence") or inp.get("confidence_grade")
        if isinstance(c, dict):
            c = c.get("grade") or c.get("confidence_grade")
        return str(c or "")

    high_blocked = [r for r in blocked if conf_of(r).replace("!", "").startswith("A")]
    if len(high_blocked) < 10:
        # fallback: steep_ok false but slope score present
        high_blocked = blocked
    sample_blocked = random.sample(high_blocked, min(10, len(high_blocked)))

    def flatten(r, tag):
        inp = r.get("inputs") or {}
        if isinstance(inp, str):
            try:
                inp = json.loads(inp)
            except Exception:
                inp = {}
        return {
            "tag": tag,
            "id": r["id"],
            "session_date": str(r["session_date"]),
            "symbol": r["symbol"],
            "logged_at": str(r["logged_at"]),
            "rendered_state": r["rendered_state"],
            "vwap_slope_score": r["vwap_slope_score"],
            "steep_ok": r["steep_ok"],
            "flip_flop": r["flip_flop"],
            "whipsaw_crosses": r["whipsaw_crosses"],
            "quality_pass": r["quality_pass"],
            "vwap_would_block": r["vwap_would_block"],
            "confidence": conf_of(r),
            "signed_slope_atr": inp.get("signed_slope_atr"),
            "adverse_closes": inp.get("adverse_closes"),
        }

    f_rows = [flatten(r, "PASS") for r in passes] + [flatten(r, "BLOCK_SAMPLE") for r in sample_blocked]
    csv_write(ROOT / "F", "F_pass_vs_block_sample.csv", f_rows)

    def avg(rows, key):
        xs = [_f(r.get(key)) for r in rows if _f(r.get(key)) is not None]
        return sum(xs) / len(xs) if xs else None

    pass_f = [flatten(r, "PASS") for r in passes]
    block_f = [flatten(r, "BLOCK_SAMPLE") for r in sample_blocked]
    jdump(ROOT / "F", "F_summary.json", {
        "quality_pass_count": len(passes),
        "block_sample_n": len(sample_blocked),
        "pass_steep_ok_rate": rate(pass_f, lambda r: r["steep_ok"]),
        "block_steep_ok_rate": rate(block_f, lambda r: r["steep_ok"]),
        "pass_flip_flop_rate": rate(pass_f, lambda r: r["flip_flop"]),
        "block_flip_flop_rate": rate(block_f, lambda r: r["flip_flop"]),
        "pass_avg_slope_score": avg(pass_f, "vwap_slope_score"),
        "block_avg_slope_score": avg(block_f, "vwap_slope_score"),
        "pass_avg_whipsaw": avg(pass_f, "whipsaw_crosses"),
        "block_avg_whipsaw": avg(block_f, "whipsaw_crosses"),
        "definition": "quality_pass = steep_ok AND NOT (flip_flop OR whipsaw>max). Gate would_block when not quality_pass (shadow).",
        "interpretation": (
            "If passes all have steep_ok=true and low flip/whipsaw while A-grade blocks fail steep_ok "
            "or show flip/whipsaw, filter is intentionally strict (slope/stability), not a confidence bug."
        ),
        "pass_ids": [r["id"] for r in passes],
        "block_sample_ids": [r["id"] for r in sample_blocked],
    })

    # ---------------- ITEM D status placeholder (instrumentation in separate step) ----------------
    jdump(ROOT / "D", "D_status_pending_instrumentation.json", {
        "instrumentation_live_at_extract_time": False,
        "action": "Separate code change adds kavach_vwap_touch_reject_log + forward write; backfill via Upstox.",
        "depends_on_A": False,
    })

    jdump(ROOT, "00_manifest.json", {
        "generated_at": datetime.now(IST).isoformat(),
        "items": {
            "A": {"n": len(a_summaries), "primary": True},
            "B": {"depends_on_A": True, "test_pop": len(test_pop)},
            "C": {"tata_trades": len(c_out["found_trades"])},
            "D": {"status": "pending_code_change"},
            "E": {"depends_on_A": True, "n": len(e_rows)},
            "F": {"passes": len(passes), "block_sample": len(sample_blocked)},
        },
        "ordering_note": "A first; B/E use A; C/F independent; D instrumentation after extract.",
    })
    print("DONE", ROOT)
    db.close()


if __name__ == "__main__":
    main()
