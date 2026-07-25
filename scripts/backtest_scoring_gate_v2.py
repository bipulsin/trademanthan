#!/usr/bin/env python3
"""BACKTEST v2 — offline only (no live changes).

PART 1  ATR-consumption as a READY-family *suppression* gate on already-locked
        symbols (leave ranking / morning lock / promotion untouched).
        Variants: threshold ∈ {75, 80, 85}% AND progression NOT increasing.

PART 2  Take-Trade override for grade A/A+ READY when the ONLY soft blocker is
        direction_imbalance (warning_stack stays blocked). Extended date range.
        Optional EMA5–EMA10 gap structural filter.

Run on paperclip app container:
  PYTHONPATH=/app /opt/venv/bin/python /tmp/backtest_scoring_gate_v2.py
"""
from __future__ import annotations

import json
import math
import os
from collections import defaultdict
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal

IST = pytz.timezone("Asia/Kolkata")
OUT = Path(os.environ.get("SGB_OUT", "/tmp/scoring_gate_backtest_v2"))

# Part1 window (extend through 07-25 if data exists)
P1_START = "2026-07-20"
P1_END = os.environ.get("SGB_P1_END", "2026-07-25")

# Part2: pull as far back as consistency log allows (capped)
P2_END = os.environ.get("SGB_P2_END", "2026-07-25")
P2_START_FLOOR = "2026-07-15"  # consistency log began ~15 Jul

WARMUP_FROM = "2026-07-10"
SESSION_OPEN = dtime(9, 15)
SESSION_LAST_BAR = dtime(15, 25)
SQUAREOFF = dtime(15, 15)
MIN_STOP_FRAC = 0.003
THRESHOLDS = [75.0, 80.0, 85.0]
CLEAN_TRENDERS_0724 = ["COFORGE", "MPHASIS", "KPITTECH", "TATAELXSI", "ASTRAL"]
GAP_FILTERS_PCT = [0.0, 0.05, 0.10, 0.15]  # |EMA5-EMA10|/price * 100


def q(sql: str, **params) -> List[Dict[str, Any]]:
    with SessionLocal() as s:
        return [dict(r) for r in s.execute(text(sql), params).mappings().all()]


def ist(dt):
    if dt is None:
        return None
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    return dt.astimezone(IST)


def hm(dt):
    d = ist(dt)
    return d.strftime("%H:%M:%S") if d else None


def f(v):
    try:
        if v is None:
            return None
        x = float(v)
        return None if math.isnan(x) or math.isinf(x) else x
    except (TypeError, ValueError):
        return None


def pct(a, b):
    return round(100.0 * a / b, 1) if b else None


def median(xs):
    if not xs:
        return None
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def ema_series(closes, length):
    if not closes:
        return []
    k = 2 / (length + 1)
    out = [None] * len(closes)
    s = 0.0
    for i, c in enumerate(closes):
        if i < length - 1:
            s += c
            continue
        if i == length - 1:
            s = (s + c) / length
            out[i] = s
            continue
        s = c * k + s * (1 - k)
        out[i] = s
    return out


# ─── data loads ───
def date_bounds():
    row = q(
        """
        SELECT MIN(session_date)::text AS mn, MAX(session_date)::text AS mx,
               COUNT(*) AS n
        FROM kavach_ready_consistency_log
        """
    )[0]
    return row


def load_renders(a, b):
    return q(
        """
        SELECT session_date::text AS session_date, symbol, direction,
               rendered_state, logged_at, in_lock, lock_rank,
               inputs->>'confidence' AS grade,
               inputs->>'trade_entry' AS trade_entry,
               inputs->>'trade_sl' AS trade_sl,
               inputs->>'trade_take_enabled' AS take_enabled,
               inputs->>'trade_take_disable_reason' AS trade_take_disable_reason,
               inputs->>'trade_state_reason' AS trade_state_reason,
               inputs->>'zone_downgrade' AS zone_downgrade,
               inputs->>'dwell_soft_hold' AS soft_hold,
               inputs->>'atr_consumed_pct_from_open' AS atr_consumed_pct_from_open,
               inputs->'atr_consumed'->>'move_from_open' AS move_from_open,
               inputs->'dwell_entry_shadow'->'distance'->>'lot' AS lot,
               inputs->'dwell_entry_shadow'->'distance'->>'would_block' AS dist_block,
               inputs->'dwell_entry_shadow'->'distance'->>'check1_beyond_ema10' AS c1_beyond_ema10,
               inputs->'dwell_entry_shadow'->'distance'->>'check2_entry_thin' AS c2_entry_thin,
               inputs->'dwell_entry_shadow'->'distance'->>'check3_stack_thin' AS c3_stack_thin,
               inputs->>'ema5' AS ema5_inp,
               inputs->>'ema10' AS ema10_inp
        FROM kavach_ready_consistency_log
        WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
        ORDER BY session_date, symbol, logged_at
        """,
        a=a, b=b,
    )


def lock_symbols_from_renders(renders):
    """Locked/promoted universe from consistency_log.in_lock (same source as checklist path)."""
    out = defaultdict(set)
    for r in renders:
        if r.get("in_lock"):
            out[str(r["session_date"])].add((r["symbol"] or "").upper())
    # Fallback: any READY-family symbol is checklist-eligible
    for r in renders:
        st = (r.get("rendered_state") or "").upper()
        if st.startswith("READY"):
            out[str(r["session_date"])].add((r["symbol"] or "").upper())
    return out


def load_atr(a, b):
    rows = q(
        """
        SELECT date::text AS session_date, UPPER(symbol) AS symbol,
               MAX(atr14_pct) AS atr14_pct
        FROM rs_scanner_history
        WHERE date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
          AND atr14_pct IS NOT NULL AND atr14_pct > 0
        GROUP BY date, UPPER(symbol)
        """,
        a=a, b=b,
    )
    m = {}
    for r in rows:
        v = f(r["atr14_pct"])
        if v and v > 0:
            m[(r["session_date"], r["symbol"])] = v
    return m


def load_kavach_audit(a, b):
    return q(
        """
        SELECT session_date::text AS session_date, UPPER(symbol) AS symbol,
               bar_evaluated_at, confidence_grade, kavach_state, price, ema5, ema10
        FROM rs_live_kavach_audit
        WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
        ORDER BY session_date, symbol, bar_evaluated_at
        """,
        a=a, b=b,
    )


MAP_SQL = """
SELECT symbol, instrument_key FROM rs_universe_kavach_archive WHERE instrument_key IS NOT NULL
UNION SELECT underlying, instrument_key FROM daily_futures_screening WHERE instrument_key IS NOT NULL
UNION SELECT symbol, instrument_key FROM rs_shadow_selection_log WHERE instrument_key IS NOT NULL
UNION SELECT symbol, instrument_key FROM rs_scan_exclusion_log WHERE instrument_key IS NOT NULL
UNION SELECT underlying_symbol, instrument_key FROM oi_heatmap_latest WHERE instrument_key IS NOT NULL
UNION SELECT stock, instrument_key FROM premarket_watchlist WHERE instrument_key IS NOT NULL
UNION SELECT stock, instrument_key FROM vajra_futures_rating WHERE instrument_key IS NOT NULL
"""


def load_symbol_candidates():
    out = defaultdict(list)
    for r in q(MAP_SQL):
        sym = (r["symbol"] or "").strip().upper()
        ik = (r["instrument_key"] or "").strip()
        if sym and ik and ik not in out[sym]:
            out[sym].append(ik)
    return dict(out)


def load_1m(iks, a_day, b_day):
    return q(
        """
        SELECT instrument_key, candle_time, open, high, low, close, volume
        FROM upstox_ws_intraday_1m
        WHERE instrument_key = ANY(:iks)
          AND candle_time >= :a AND candle_time < :b
        ORDER BY instrument_key, candle_time
        """,
        iks=list(iks),
        a=f"{a_day} 00:00:00+05:30",
        b=f"{b_day} 23:59:59+05:30",
    )


def build_10m(rows_1m):
    buckets = {}
    for r in rows_1m:
        t = ist(r["candle_time"])
        if t is None or t.year < 2000:
            continue
        if not (SESSION_OPEN <= t.time() < dtime(15, 30)):
            continue
        mins = (t.hour * 60 + t.minute) - (SESSION_OPEN.hour * 60 + SESSION_OPEN.minute)
        if mins < 0:
            continue
        idx = mins // 10
        bar_end = t.replace(hour=SESSION_OPEN.hour, minute=SESSION_OPEN.minute,
                            second=0, microsecond=0) + timedelta(minutes=10 * (idx + 1))
        key = (r["instrument_key"], t.strftime("%Y-%m-%d"), bar_end)
        o, h, l, c = f(r["open"]), f(r["high"]), f(r["low"]), f(r["close"])
        if None in (o, h, l, c):
            continue
        b = buckets.get(key)
        if b is None:
            buckets[key] = {"bar_end": bar_end, "open": o, "high": h, "low": l,
                            "close": c, "volume": f(r["volume"]) or 0.0,
                            "_first": t, "_last": t}
        else:
            b["high"] = max(b["high"], h)
            b["low"] = min(b["low"], l)
            b["volume"] += f(r["volume"]) or 0.0
            if t < b["_first"]:
                b["_first"], b["open"] = t, o
            if t > b["_last"]:
                b["_last"], b["close"] = t, c

    by_ik = defaultdict(lambda: defaultdict(list))
    for (ik, day, _), b in buckets.items():
        b.pop("_first", None)
        b.pop("_last", None)
        by_ik[ik][day].append(b)

    out = {}
    for ik, days in by_ik.items():
        ordered = sorted(days)
        flat = []
        for d in ordered:
            days[d].sort(key=lambda x: x["bar_end"])
            flat.extend(days[d])
        closes = [b["close"] for b in flat]
        e5, e10 = ema_series(closes, 5), ema_series(closes, 10)
        for i, b in enumerate(flat):
            b["ema5"], b["ema10"] = e5[i], e10[i]
        out[ik] = {d: days[d] for d in ordered}
    return out


def pick_instrument(symbol, day, candidates, bars, audit_prices):
    live = [ik for ik in candidates if bars.get(ik, {}).get(day)]
    if not live:
        return None, "no_candles"
    sig = audit_prices.get((day, symbol)) or []
    if sig:
        best, best_n = None, -1
        for ik in live:
            db = bars[ik][day]
            n = 0
            for t, px in sig:
                b = next((x for x in db if abs((x["bar_end"] - t).total_seconds()) < 60), None)
                if b and abs(b["close"] - px) / max(px, 1e-6) < 0.002:
                    n += 1
            if n > best_n:
                best, best_n = ik, n
        if best and best_n > 0:
            return best, "price_signature"
    # max volume
    best = max(live, key=lambda ik: sum(b["volume"] for b in bars[ik][day]))
    return best, "max_volume"


def simulate_trade(day_bars, signal_at, direction, entry, stop):
    R = abs(entry - stop)
    if R <= 0:
        return None
    is_long = direction.upper() == "LONG"
    fwd = [b for b in day_bars if b["bar_end"] > signal_at and b["bar_end"].time() <= SESSION_LAST_BAR]
    if not fwd:
        return None
    reached_1r = False
    for i, b in enumerate(fwd):
        c, hi, lo = b["close"], b["high"], b["low"]
        if b["bar_end"].time() >= SQUAREOFF:
            pts = (c - entry) if is_long else (entry - c)
            return _res("squareoff_1515", b, entry, c, pts, R, i + 1, reached_1r)
        if (lo <= stop) if is_long else (hi >= stop):
            pts = (stop - entry) if is_long else (entry - stop)
            return _res("stop_hit", b, entry, stop, pts, R, i + 1, reached_1r)
        if not reached_1r and ((c >= entry + R) if is_long else (c <= entry - R)):
            reached_1r = True
        ref = b["ema5"] if reached_1r else b["ema10"]
        if ref is None:
            continue
        against = (c < ref) if is_long else (c > ref)
        if against:
            pts = (c - entry) if is_long else (entry - c)
            tag = "ema5_close_after_1R" if reached_1r else "ema10_close_before_1R"
            return _res(tag, b, entry, c, pts, R, i + 1, reached_1r)
    b = fwd[-1]
    pts = (b["close"] - entry) if is_long else (entry - b["close"])
    return _res("session_end", b, entry, b["close"], pts, R, len(fwd), reached_1r)


def _res(reason, bar, entry, exit_px, pts, R, bars_held, reached_1r):
    return {
        "exit_reason": reason, "exit_at": hm(bar["bar_end"]),
        "entry": round(entry, 2), "exit_price": round(exit_px, 2),
        "pts": round(pts, 2), "r_multiple": round(pts / R, 3),
        "risk_pts": round(R, 2), "bars_held": bars_held, "reached_1R": reached_1r,
    }


def aggregate(trades, key=None):
    if not trades:
        return {"n": 0}
    wins = [t for t in trades if t["pts"] > 0]
    losses = [t for t in trades if t["pts"] <= 0]
    rs_ = [t["r_multiple"] for t in trades]
    inr = [t["pnl_inr"] for t in trades if t.get("pnl_inr") is not None]
    out = {
        "n": len(trades), "wins": len(wins), "losses": len(losses),
        "win_rate_pct": pct(len(wins), len(trades)),
        "avg_r": round(sum(rs_) / len(rs_), 3),
        "median_r": round(median(rs_), 3),
        "total_r": round(sum(rs_), 2),
        "avg_win_r": round(sum(t["r_multiple"] for t in wins) / len(wins), 3) if wins else None,
        "avg_loss_r": round(sum(t["r_multiple"] for t in losses) / len(losses), 3) if losses else None,
        "total_pts": round(sum(t["pts"] for t in trades), 2),
        "total_pnl_inr": round(sum(inr), 0) if inr else None,
        "n_with_lot": len(inr),
        "reached_1R": sum(1 for t in trades if t.get("reached_1R")),
        "exit_reasons": {k: sum(1 for t in trades if t["exit_reason"] == k)
                         for k in sorted({t["exit_reason"] for t in trades})},
    }
    if key:
        grp = {}
        for v in sorted({str(t.get(key)) for t in trades}):
            sub = [t for t in trades if str(t.get(key)) == v]
            w = [t for t in sub if t["pts"] > 0]
            g_inr = [t["pnl_inr"] for t in sub if t.get("pnl_inr") is not None]
            grp[v] = {
                "n": len(sub), "wins": len(w), "win_rate_pct": pct(len(w), len(sub)),
                "avg_r": round(sum(t["r_multiple"] for t in sub) / len(sub), 3),
                "median_r": round(median([t["r_multiple"] for t in sub]), 3),
                "total_r": round(sum(t["r_multiple"] for t in sub), 2),
                "total_pnl_inr": round(sum(g_inr), 0) if g_inr else None,
            }
        out[f"by_{key}"] = grp
    return out


def classify_gates(r):
    hard, soft = [], []
    reason = (r.get("trade_take_disable_reason") or r.get("trade_state_reason") or "")
    ru = reason.upper()
    zd = (r.get("zone_downgrade") or "").lower()
    if "WINDOW" in ru or "14:30" in reason or "AFTER" in ru and "WINDOW" in ru:
        hard.append("entry_window_closed")
    if (r.get("dist_block") or "").lower() == "true" or "TOO CLOSE" in ru or "DISTANCE" in ru:
        hard.append("entry_distance_stop_validity")
    if (r.get("c1_beyond_ema10") or "").lower() == "true":
        hard.append("beyond_ema10")
    if "WARNING" in ru or "warning_stack" in zd or "dwell hold (warning" in reason.lower():
        soft.append("warning_stack")
    if "DIRECTION" in ru or "IMBALANCE" in ru or "direction_imbalance" in zd:
        soft.append("direction_imbalance")
    # soft_hold alone often warning
    if not soft and (r.get("soft_hold") or "").lower() == "true":
        if "direction" in reason.lower():
            soft.append("direction_imbalance")
        else:
            soft.append("warning_stack")
    return {"hard": sorted(set(hard)), "soft": soft, "reason": reason}


def is_ready_family(state):
    s = (state or "").upper()
    return s.startswith("READY")


def derive_atr_consumed(bar, day_open, atr_pct):
    if not bar or day_open is None or not atr_pct or atr_pct <= 0:
        return None
    atr_pts = atr_pct / 100.0 * day_open
    if atr_pts <= 0:
        return None
    return abs(bar["close"] - day_open) / atr_pts * 100.0


def progression_increasing(hist_ac, hist_signed, direction):
    """True if atr_consumed rising vs 1-2 bars prior AND move still with direction."""
    if len(hist_ac) < 2:
        return False
    cur, prev = hist_ac[-1], hist_ac[-2]
    if cur is None or prev is None:
        return False
    rising = cur > prev + 0.5  # 0.5pp noise floor
    if len(hist_ac) >= 3 and hist_ac[-3] is not None:
        rising = rising or (cur > hist_ac[-3] + 0.5 and cur >= prev)
    if not rising:
        return False
    # signed move aligned
    if not hist_signed or hist_signed[-1] is None:
        return rising
    sig = hist_signed[-1]
    if (direction or "LONG").upper() == "SHORT":
        return sig < 0
    return sig > 0


# ─── PART 1 ───
def part1(renders, locked, atr_map, bars, sym_ik, day_open, audit):
    ready = [r for r in renders if is_ready_family(r["rendered_state"])]
    # Build per-symbol bar atr history from day bars for progression
    # Also attach atr at each READY render
    results = {}
    for thr in THRESHOLDS:
        suppressed = []
        kept = []
        by_sym = defaultdict(int)
        first_kick = {}  # ICICIGI etc
        false_neg = []  # clean trenders wrongly suppressed

        # hist keyed by (day, sym)
        hist_ac = defaultdict(list)
        hist_signed = defaultdict(list)
        hist_t = defaultdict(list)

        # Walk READY renders in time order; also fill hist from 10m bars before each
        for r in sorted(ready, key=lambda x: (str(x["session_date"]), ist(x["logged_at"]))):
            day = str(r["session_date"])
            sym = (r["symbol"] or "").upper()
            if sym not in locked.get(day, set()):
                continue  # post-lock only
            t = ist(r["logged_at"])
            ik = sym_ik.get((day, sym))
            db = bars.get(ik, {}).get(day) if ik else None
            op = day_open.get((day, sym))
            atr_pct = atr_map.get((day, sym))

            # refresh hist from bars up to signal
            if db and op and atr_pct:
                hist_ac[(day, sym)] = []
                hist_signed[(day, sym)] = []
                hist_t[(day, sym)] = []
                for b in db:
                    if b["bar_end"] > t:
                        break
                    ac = derive_atr_consumed(b, op, atr_pct)
                    hist_ac[(day, sym)].append(ac)
                    hist_signed[(day, sym)].append(
                        (b["close"] - op) / op * 100.0 if op else None
                    )
                    hist_t[(day, sym)].append(b["bar_end"])

            # prefer production atr field
            ac = f(r["atr_consumed_pct_from_open"])
            if ac is None and hist_ac[(day, sym)]:
                ac = hist_ac[(day, sym)][-1]
            # ensure hist ends with this ac for progression
            if ac is not None:
                if not hist_ac[(day, sym)] or hist_ac[(day, sym)][-1] != ac:
                    hist_ac[(day, sym)].append(ac)
                    mv = f(r["move_from_open"])
                    if mv is None and op and f(r["trade_entry"]):
                        mv = (f(r["trade_entry"]) - op) / op * 100.0
                    hist_signed[(day, sym)].append(mv)

            progressing = progression_increasing(
                hist_ac[(day, sym)], hist_signed[(day, sym)], r.get("direction")
            )
            would_suppress = (
                ac is not None and ac >= thr and (not progressing)
            )
            row = {
                "session": day, "symbol": sym, "direction": r.get("direction"),
                "grade": r.get("grade"), "signal_ist": hm(t),
                "atr_consumed_pct": round(ac, 2) if ac is not None else None,
                "progression": progressing,
                "suppressed": would_suppress,
            }
            if would_suppress:
                by_sym[sym] += 1
                key = (day, sym)
                if key not in first_kick:
                    first_kick[key] = row["signal_ist"]
                # simulate trade
                entry, stop = f(r["trade_entry"]), f(r["trade_sl"])
                lot = int(f(r["lot"]) or 0)
                if entry and stop and db:
                    # structural stop floor
                    gap = abs(entry - stop)
                    if gap < MIN_STOP_FRAC * entry:
                        stop = entry - MIN_STOP_FRAC * entry if (r.get("direction") or "LONG").upper() == "LONG" else entry + MIN_STOP_FRAC * entry
                    sim = simulate_trade(db, t, r.get("direction") or "LONG", entry, stop)
                    if sim:
                        row.update(sim)
                        row["pnl_inr"] = round(sim["pts"] * lot, 0) if lot else None
                        row["lot"] = lot
                        suppressed.append(row)
                else:
                    suppressed.append(row)
                if day == "2026-07-24" and sym in CLEAN_TRENDERS_0724:
                    false_neg.append(row)
            else:
                kept.append(row)

        # ICICIGI detail
        ic_sup = [s for s in suppressed if s["symbol"] == "ICICIGI" and s["session"] == "2026-07-24"]
        ic_total = sum(
            1 for r in ready
            if str(r["session_date"]) == "2026-07-24"
            and (r["symbol"] or "").upper() == "ICICIGI"
            and "ICICIGI" in locked.get("2026-07-24", set())
        )
        # if ICICIGI not in locked set but had READY — still count from ready
        if ic_total == 0:
            ic_total = sum(
                1 for r in ready
                if str(r["session_date"]) == "2026-07-24"
                and (r["symbol"] or "").upper() == "ICICIGI"
            )
            ic_sup = [s for s in suppressed if s["symbol"] == "ICICIGI" and s["session"] == "2026-07-24"]
            # also recompute suppress counting ICICIGI even if lock miss — treat READY as locked-eligible
            # (consistency log implies checklist path)
            pass

        # For ICICIGI: if not in locked set, include via READY presence override
        # Re-run ICICIGI-only if needed
        if not ic_sup and ic_total:
            # ICICIGI had READY but wasn't in lock sets — still apply rule as "checklist-eligible"
            pass

        trades_with_r = [s for s in suppressed if s.get("r_multiple") is not None]
        pos = [s for s in trades_with_r if s["r_multiple"] > 0]
        neg = [s for s in trades_with_r if s["r_multiple"] <= 0]

        # Separation score: maximize |neg_r| suppressed / (1 + pos_r suppressed)
        sep = {
            "suppressed_positive_r_n": len(pos),
            "suppressed_negative_r_n": len(neg),
            "suppressed_positive_total_r": round(sum(s["r_multiple"] for s in pos), 2) if pos else 0,
            "suppressed_negative_total_r": round(sum(s["r_multiple"] for s in neg), 2) if neg else 0,
        }
        # Good separation = suppress lots of negative R, little positive R
        # score = (-neg_total_r) - pos_total_r  (higher better)
        sep["separation_score"] = round(
            (-sep["suppressed_negative_total_r"]) - sep["suppressed_positive_total_r"], 2
        )

        results[f"thr_{int(thr)}"] = {
            "threshold_pct": thr,
            "n_ready_locked_eligible": sum(
                1 for r in ready
                if (r["symbol"] or "").upper() in locked.get(str(r["session_date"]), set())
                or True  # see note — we also count all READY as checklist-path
            ),
            "n_ready_family_total": len(ready),
            "n_suppressed": len(suppressed),
            "n_suppressed_with_sim": len(trades_with_r),
            "by_symbol": dict(sorted(by_sym.items(), key=lambda x: -x[1])),
            "suppressed_agg": aggregate(trades_with_r),
            "separation": sep,
            "icicigi_20260724": {
                "n_ready_total": sum(
                    1 for r in ready
                    if str(r["session_date"]) == "2026-07-24"
                    and (r["symbol"] or "").upper() == "ICICIGI"
                ),
                "n_suppressed": len(ic_sup),
                "first_suppress_ist": first_kick.get(("2026-07-24", "ICICIGI"))
                or (ic_sup[0]["signal_ist"] if ic_sup else None),
                "suppressed_agg": aggregate([s for s in ic_sup if s.get("r_multiple") is not None]),
            },
            "false_neg_clean_trenders_0724": {
                "symbols_checked": CLEAN_TRENDERS_0724,
                "n_suppressed_renders": len(false_neg),
                "rows": false_neg[:40],
                "agg": aggregate([s for s in false_neg if s.get("r_multiple") is not None]),
            },
        }

    # Also: treat ALL READY as eligible (consistency log = checklist path) — more accurate
    # Recompute with eligibility = any READY (not just lock set), since lock tables may miss promotions
    results_all_ready = {}
    for thr in THRESHOLDS:
        suppressed = []
        by_sym = defaultdict(int)
        first_kick = {}
        false_neg = []
        hist_ac = defaultdict(list)
        hist_signed = defaultdict(list)

        for r in sorted(ready, key=lambda x: (str(x["session_date"]), ist(x["logged_at"]))):
            day = str(r["session_date"])
            sym = (r["symbol"] or "").upper()
            t = ist(r["logged_at"])
            ik = sym_ik.get((day, sym))
            db = bars.get(ik, {}).get(day) if ik else None
            op = day_open.get((day, sym))
            atr_pct = atr_map.get((day, sym))

            if db and op and atr_pct:
                hist_ac[(day, sym)] = []
                hist_signed[(day, sym)] = []
                for b in db:
                    if b["bar_end"] > t:
                        break
                    ac = derive_atr_consumed(b, op, atr_pct)
                    hist_ac[(day, sym)].append(ac)
                    hist_signed[(day, sym)].append((b["close"] - op) / op * 100.0)

            ac = f(r["atr_consumed_pct_from_open"])
            if ac is None and hist_ac[(day, sym)]:
                ac = hist_ac[(day, sym)][-1]
            if ac is not None:
                hist_ac[(day, sym)].append(ac)
                mv = f(r["move_from_open"])
                if mv is None and op and f(r["trade_entry"]):
                    mv = (f(r["trade_entry"]) - op) / op * 100.0
                hist_signed[(day, sym)].append(mv)

            progressing = progression_increasing(
                hist_ac[(day, sym)], hist_signed[(day, sym)], r.get("direction")
            )
            would_suppress = ac is not None and ac >= thr and (not progressing)
            if not would_suppress:
                continue
            by_sym[sym] += 1
            if (day, sym) not in first_kick:
                first_kick[(day, sym)] = hm(t)
            row = {
                "session": day, "symbol": sym, "direction": r.get("direction"),
                "grade": r.get("grade"), "signal_ist": hm(t),
                "atr_consumed_pct": round(ac, 2) if ac is not None else None,
                "progression": progressing,
            }
            entry, stop = f(r["trade_entry"]), f(r["trade_sl"])
            lot = int(f(r["lot"]) or 0)
            if entry and stop and db:
                gap = abs(entry - stop)
                if gap < MIN_STOP_FRAC * entry:
                    stop = (
                        entry - MIN_STOP_FRAC * entry
                        if (r.get("direction") or "LONG").upper() == "LONG"
                        else entry + MIN_STOP_FRAC * entry
                    )
                sim = simulate_trade(db, t, r.get("direction") or "LONG", entry, stop)
                if sim:
                    row.update(sim)
                    row["pnl_inr"] = round(sim["pts"] * lot, 0) if lot else None
            suppressed.append(row)
            if day == "2026-07-24" and sym in CLEAN_TRENDERS_0724:
                false_neg.append(row)

        trades_with_r = [s for s in suppressed if s.get("r_multiple") is not None]
        pos = [s for s in trades_with_r if s["r_multiple"] > 0]
        neg = [s for s in trades_with_r if s["r_multiple"] <= 0]
        sep = {
            "suppressed_positive_r_n": len(pos),
            "suppressed_negative_r_n": len(neg),
            "suppressed_positive_total_r": round(sum(s["r_multiple"] for s in pos), 2) if pos else 0,
            "suppressed_negative_total_r": round(sum(s["r_multiple"] for s in neg), 2) if neg else 0,
        }
        sep["separation_score"] = round(
            (-sep["suppressed_negative_total_r"]) - sep["suppressed_positive_total_r"], 2
        )
        # Dedup first-per-symbol-session for cleaner trade stats
        first_ss = {}
        for s in sorted(trades_with_r, key=lambda x: (x["session"], x["symbol"], x["signal_ist"])):
            k = (s["session"], s["symbol"])
            if k not in first_ss:
                first_ss[k] = s
        first_trades = list(first_ss.values())

        ic_sup = [s for s in suppressed if s["symbol"] == "ICICIGI" and s["session"] == "2026-07-24"]
        results_all_ready[f"thr_{int(thr)}"] = {
            "threshold_pct": thr,
            "eligibility": "all_READY_family_renders (checklist path)",
            "n_ready_family_total": len(ready),
            "n_suppressed_renders": len(suppressed),
            "n_suppressed_with_sim": len(trades_with_r),
            "by_symbol_top20": dict(sorted(by_sym.items(), key=lambda x: -x[1])[:20]),
            "suppressed_all_renders_agg": aggregate(trades_with_r),
            "suppressed_first_per_symbol_session_agg": aggregate(first_trades),
            "separation": sep,
            "icicigi_20260724": {
                "n_ready_total": sum(
                    1 for r in ready
                    if str(r["session_date"]) == "2026-07-24"
                    and (r["symbol"] or "").upper() == "ICICIGI"
                ),
                "n_suppressed": len(ic_sup),
                "first_suppress_ist": first_kick.get(("2026-07-24", "ICICIGI")),
                "suppressed_agg": aggregate([s for s in ic_sup if s.get("r_multiple") is not None]),
                "sample": ic_sup[:5],
            },
            "false_neg_clean_trenders_0724": {
                "symbols_checked": CLEAN_TRENDERS_0724,
                "n_suppressed_renders": len(false_neg),
                "agg": aggregate([s for s in false_neg if s.get("r_multiple") is not None]),
                "by_symbol": {
                    s: sum(1 for x in false_neg if x["symbol"] == s) for s in CLEAN_TRENDERS_0724
                },
            },
        }

    # pick best threshold by separation_score on all_ready path
    best = max(
        results_all_ready.keys(),
        key=lambda k: results_all_ready[k]["separation"]["separation_score"],
    )
    return {
        "note": (
            "Primary results use all READY-family consistency-log rows (checklist path). "
            "Lock-set-only variant retained for sensitivity."
        ),
        "primary": results_all_ready,
        "lock_set_only": results,
        "best_threshold_by_separation_score": best,
    }


# ─── PART 2 ───
def part2(renders, bars, sym_ik, audit):
    ready_states = {"READY", "READY(RECHECK)"}
    grades = {"A", "A+"}

    traj = defaultdict(list)
    for a in audit:
        traj[(str(a["session_date"]), (a["symbol"] or "").upper())].append(
            {"t": ist(a["bar_evaluated_at"]), "grade": a["confidence_grade"], "state": a["kavach_state"]}
        )
    for v in traj.values():
        v.sort(key=lambda x: x["t"])

    di_flips = []
    funnel = {
        "render_rows_total": len(renders),
        "grade_A_ready_family": 0,
        "already_take_enabled": 0,
        "hard_blocked": 0,
        "warning_stack_only_skipped": 0,
        "direction_imbalance_flip": 0,
        "other_soft_or_none": 0,
    }
    for r in renders:
        if not is_ready_family(r["rendered_state"]) or (r.get("grade") or "") not in grades:
            continue
        # normalize READY(RECHECK)
        if (r["rendered_state"] or "").upper() not in ready_states and not (
            r["rendered_state"] or ""
        ).upper().startswith("READY"):
            continue
        funnel["grade_A_ready_family"] += 1
        if (r.get("take_enabled") or "").lower() == "true":
            funnel["already_take_enabled"] += 1
            continue
        g = classify_gates(r)
        if g["hard"]:
            funnel["hard_blocked"] += 1
            continue
        soft = set(g["soft"])
        if soft == {"direction_imbalance"} or (
            "direction_imbalance" in soft and "warning_stack" not in soft
        ):
            funnel["direction_imbalance_flip"] += 1
            di_flips.append(r)
        elif "warning_stack" in soft:
            funnel["warning_stack_only_skipped"] += 1
        else:
            funnel["other_soft_or_none"] += 1

    # episodes
    by_ss = defaultdict(list)
    for r in di_flips:
        by_ss[(str(r["session_date"]), (r["symbol"] or "").upper())].append(r)
    episodes = []
    for (day, sym), rows in sorted(by_ss.items()):
        rows.sort(key=lambda x: ist(x["logged_at"]))
        cur = []
        for r in rows:
            if cur and (ist(r["logged_at"]) - ist(cur[-1]["logged_at"])) > timedelta(minutes=15):
                episodes.append({"day": day, "sym": sym, "rows": cur})
                cur = []
            cur.append(r)
        if cur:
            episodes.append({"day": day, "sym": sym, "rows": cur})

    def ema_gap_pct(r, day, sym):
        e5, e10 = f(r.get("ema5_inp")), f(r.get("ema10_inp"))
        entry = f(r.get("trade_entry"))
        if e5 is None or e10 is None or not entry:
            # from bars
            ik = sym_ik.get((day, sym))
            db = bars.get(ik, {}).get(day) if ik else None
            t = ist(r["logged_at"])
            if db and t:
                prev = [b for b in db if b["bar_end"] <= t]
                if prev and prev[-1].get("ema5") and prev[-1].get("ema10"):
                    e5, e10 = prev[-1]["ema5"], prev[-1]["ema10"]
                    entry = entry or prev[-1]["close"]
        if e5 is None or e10 is None or not entry:
            return None
        return abs(e5 - e10) / entry * 100.0

    def run(gap_min_pct):
        seen, trades, skipped = set(), [], []
        for ep in episodes:
            day, sym = ep["day"], ep["sym"]
            if (day, sym) in seen:
                continue
            r0 = ep["rows"][0]
            gap = ema_gap_pct(r0, day, sym)
            if gap_min_pct > 0 and (gap is None or gap < gap_min_pct):
                skipped.append({"session": day, "symbol": sym, "why": "thin_ema_gap", "gap_pct": gap})
                continue
            seen.add((day, sym))
            entry, stop = f(r0["trade_entry"]), f(r0["trade_sl"])
            lot = int(f(r0["lot"]) or 0)
            ik = sym_ik.get((day, sym))
            db = bars.get(ik, {}).get(day) if ik else None
            if not (entry and stop and db):
                skipped.append({"session": day, "symbol": sym, "why": "missing_entry_sl_bars"})
                continue
            sig = ist(r0["logged_at"])
            sim = simulate_trade(db, sig, r0.get("direction") or "LONG", entry, stop)
            if not sim:
                skipped.append({"session": day, "symbol": sym, "why": "no_forward_bars"})
                continue
            after = [x for x in traj.get((day, sym), []) if x["t"] and x["t"] > sig][:6]
            collapse = next((x for x in after if (x["grade"] or "") in ("D", "D!")), None)
            hour = sig.hour if sig else None
            trades.append({
                "session": day, "symbol": sym, "direction": r0.get("direction"),
                "grade": r0.get("grade"), "signal_ist": hm(sig),
                "ema_gap_pct": round(gap, 4) if gap is not None else None,
                "hour_ist": hour,
                "lot": lot, "stop": round(stop, 2), **sim,
                "pnl_inr": round(sim["pts"] * lot, 0) if lot else None,
                "protective_grade_collapse": bool(collapse),
                "protective_flag": bool(collapse and sim["pts"] < 0),
            })
        return {
            "gap_min_pct": gap_min_pct,
            "n_skipped": len(skipped),
            "trades": trades,
            "agg": aggregate(trades),
            "agg_by_direction": aggregate(trades, "direction"),
            "agg_by_grade": aggregate(trades, "grade"),
            "agg_by_hour": aggregate(trades, "hour_ist"),
            "agg_by_session": aggregate(trades, "session"),
        }

    variants = {f"gap_min_{g:g}pct": run(g) for g in GAP_FILTERS_PCT}

    # sessions with any DI flip
    sessions = sorted({str(r["session_date"]) for r in di_flips})
    n_sess = len(sessions)
    n_first = variants["gap_min_0pct"]["agg"]["n"]
    # project days to n=30
    rate = n_first / n_sess if n_sess else 0
    days_for_30 = int(math.ceil(30 / rate)) if rate > 0 else None

    return {
        "funnel": funnel,
        "n_di_episodes": len(episodes),
        "n_sessions_with_di_flip": n_sess,
        "sessions": sessions,
        "sample_size_projection": {
            "current_first_trades_n": n_first,
            "sessions_covered": n_sess,
            "trades_per_session": round(rate, 2),
            "sessions_needed_for_n30": days_for_30,
            "additional_sessions_needed": max(0, (days_for_30 or 30) - n_sess) if days_for_30 else None,
        },
        "variants": variants,
    }


def write(path, obj):
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")
    print(f"      wrote {path.name} ({path.stat().st_size:,} bytes)")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    started = datetime.now(IST).isoformat()
    bounds = date_bounds()
    p2_start = max(P2_START_FLOOR, bounds["mn"] or P2_START_FLOOR)
    p2_end = min(P2_END, bounds["mx"] or P2_END)
    p1_end = min(P1_END, bounds["mx"] or P1_END)
    print(f"consistency_log bounds: {bounds}")
    print(f"P1 {P1_START}..{p1_end}  P2 {p2_start}..{p2_end}")

    print("[1] load renders / locks / atr / audit ...")
    # Use widest window for bars
    renders_p1 = load_renders(P1_START, p1_end)
    renders_p2 = load_renders(p2_start, p2_end)
    locked = lock_symbols_from_renders(renders_p1)
    atr_map = load_atr(WARMUP_FROM, p2_end)
    audit = load_kavach_audit(p2_start, p2_end)

    print("[2] instruments + 1m candles ...")
    cand = load_symbol_candidates()
    need = defaultdict(set)
    for r in renders_p2 + renders_p1:
        need[str(r["session_date"])].add((r["symbol"] or "").upper())
    for a in audit:
        need[str(a["session_date"])].add(a["symbol"])
    all_iks = sorted({ik for day, syms in need.items() for s in syms for ik in cand.get(s, [])})
    print(f"      symbol-days={sum(len(v) for v in need.values())} iks={len(all_iks)}")
    rows_1m = load_1m(all_iks, WARMUP_FROM, p2_end)
    print(f"      1m rows={len(rows_1m):,}")
    bars = build_10m(rows_1m)

    audit_prices = defaultdict(list)
    for a in audit:
        p, t = f(a["price"]), ist(a["bar_evaluated_at"])
        if p and t:
            audit_prices[(str(a["session_date"]), a["symbol"])].append((t, p))

    sym_ik = {}
    for day, syms in need.items():
        for s in syms:
            ik, _ = pick_instrument(s, day, cand.get(s, []), bars, audit_prices)
            if ik:
                sym_ik[(day, s)] = ik

    day_open = {}
    for (day, s), ik in sym_ik.items():
        db = bars.get(ik, {}).get(day) or []
        if db:
            day_open[(day, s)] = db[0]["open"]

    print("[3] PART 1 — READY suppression ...")
    p1 = part1(renders_p1, locked, atr_map, bars, sym_ik, day_open, audit)

    print("[4] PART 2 — DI-only override ...")
    p2 = part2(renders_p2, bars, sym_ik, audit)

    # Recommendations
    best_thr = p1["best_threshold_by_separation_score"]
    best_p1 = p1["primary"][best_thr]
    fn = best_p1["false_neg_clean_trenders_0724"]
    sep = best_p1["separation"]
    p1_go = (
        sep["suppressed_negative_r_n"] > sep["suppressed_positive_r_n"] * 1.5
        and fn["n_suppressed_renders"] == 0
        and best_p1["suppressed_first_per_symbol_session_agg"].get("avg_r", 0) < 0
    )

    base = p2["variants"]["gap_min_0pct"]["agg"]
    # pick best gap filter by median_r then avg_r among n>=5
    best_gap_k, best_gap = "gap_min_0pct", base
    for k, v in p2["variants"].items():
        a = v["agg"]
        if a.get("n", 0) < 5:
            continue
        cur = best_gap
        if (a.get("median_r") or -999) > (cur.get("median_r") or -999) or (
            a.get("median_r") == cur.get("median_r")
            and (a.get("avg_r") or -999) > (cur.get("avg_r") or -999)
        ):
            best_gap_k, best_gap = k, a

    p2_n = base.get("n", 0)
    p2_go = (
        p2_n >= 30
        and (base.get("median_r") or -999) > 0
        and (base.get("win_rate_pct") or 0) >= 45
    )

    rec = {
        "part1": {
            "best_threshold": best_thr,
            "go_live": bool(p1_go),
            "verdict": (
                "GO — suppression separates negative-R READY from clean trenders"
                if p1_go
                else "NO-GO / needs refinement — see README"
            ),
            "key_stats": {
                "n_suppressed_renders": best_p1["n_suppressed_renders"],
                "first_ss_agg": best_p1["suppressed_first_per_symbol_session_agg"],
                "separation": sep,
                "false_neg_0724": fn,
                "icicigi": best_p1["icicigi_20260724"],
            },
        },
        "part2": {
            "best_variant": best_gap_k,
            "go_live": bool(p2_go),
            "verdict": (
                "GO — direction_imbalance-only override meets decision thresholds"
                if p2_go
                else "NO-GO for live — sample and/or edge insufficient; shadow more days"
            ),
            "sample_projection": p2["sample_size_projection"],
            "base_agg_gap0": base,
            "best_gap_agg": best_gap,
        },
    }

    manifest = {
        "generated_at_ist": started,
        "purpose": "BACKTEST v2 only — no live changes",
        "consistency_log_bounds": bounds,
        "part1_sessions": [P1_START, p1_end],
        "part2_sessions": [p2_start, p2_end],
        "row_counts": {
            "renders_p1": len(renders_p1),
            "renders_p2": len(renders_p2),
            "locked_symbol_days": sum(len(v) for v in locked.values()),
            "1m_rows": len(rows_1m),
            "instruments_resolved": len(sym_ik),
        },
        "recommendations": rec,
    }
    write(OUT / "00_manifest.json", manifest)
    write(OUT / "part1_ready_suppression.json", p1)
    write(OUT / "part2_direction_imbalance_override.json", p2)
    write(OUT / "recommendations.json", rec)
    print("\nDone ->", OUT)
    print(json.dumps(rec, indent=2, default=str))


if __name__ == "__main__":
    main()
