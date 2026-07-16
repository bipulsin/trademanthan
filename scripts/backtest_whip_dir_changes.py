#!/usr/bin/env python3
"""Backtest VWAP WHIPSAW + DIR CONFLICT ≥2 badge vs Jul 13–16 shadow data.

Run on app host:
  python3 scripts/backtest_whip_dir_changes.py
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _classify(fracs, first_n, max_n):
    if len(fracs) < 3:
        return "insufficient"
    avg = sum(fracs) / len(fracs)
    late = fracs[len(fracs) // 2 :]
    early = fracs[: max(1, len(fracs) // 2)]
    late_avg = sum(late) / len(late)
    early_avg = sum(early) / len(early)
    if late_avg >= 0.6 and late_avg >= early_avg - 0.05:
        return "sustained_clean_after_whip"
    if avg <= 0.4 and late_avg <= 0.45:
        return "continued_weak_structure"
    if late_avg < 0.5 and max_n > first_n + 1:
        return "continued_whipsaw_worsening"
    return "mixed"


def _events_ema5(candles, session_date, is_long, near_atr, atr):
    """Inline legacy EMA5 definition (works before deploy)."""
    from backend.services.daily_checklist_chop_gates import _session_day_bars_10m
    from backend.services.vajra.indicators import ema_series

    bars = _session_day_bars_10m(candles, session_date)
    if len(bars) < 3 or not atr or atr <= 0:
        return []
    closes = [float(b["close"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    ema5_s = ema_series(closes, 5)
    events = []
    i = 4
    while i < len(bars) - 1:
        e5 = ema5_s[i]
        touched = (lows[i] <= e5 + near_atr * atr) and (highs[i] >= e5 - near_atr * atr)
        if not touched:
            i += 1
            continue
        reverse_j = None
        for j in (i + 1, i + 2):
            if j >= len(bars):
                break
            c = closes[j]
            if is_long and c < ema5_s[j]:
                reverse_j = j
                break
            if (not is_long) and c > ema5_s[j]:
                reverse_j = j
                break
        if reverse_j is not None:
            events.append(
                {
                    "reverse_ts": str(
                        bars[reverse_j].get("timestamp") or bars[reverse_j].get("bar_end")
                    ),
                    "touch_ts": str(bars[i].get("timestamp") or bars[i].get("bar_end")),
                }
            )
            i = reverse_j + 1
        else:
            i += 1
    return events


def _events_vwap(candles, session_date, is_long):
    """Inline new VWAP definition (works before deploy)."""
    from backend.services.daily_checklist_chop_gates import _session_day_bars_10m
    from backend.services.vajra.indicators import cumulative_vwap

    bars = _session_day_bars_10m(candles, session_date)
    if len(bars) < 3:
        return []
    closes = [float(b["close"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    volumes = [float(b.get("volume") or 0.0) for b in bars]
    vwaps = cumulative_vwap(highs, lows, closes, volumes)
    events = []
    i = 0
    while i < len(bars) - 1:
        on_lock = (closes[i] > vwaps[i]) if is_long else (closes[i] < vwaps[i])
        if not on_lock:
            i += 1
            continue
        reverse_j = None
        for j in (i + 1, i + 2):
            if j >= len(bars):
                break
            against = (closes[j] < vwaps[j]) if is_long else (closes[j] > vwaps[j])
            if against:
                reverse_j = j
                break
        if reverse_j is not None:
            events.append(
                {
                    "reverse_ts": str(
                        bars[reverse_j].get("timestamp") or bars[reverse_j].get("bar_end")
                    ),
                    "touch_ts": str(bars[i].get("timestamp") or bars[i].get("bar_end")),
                }
            )
            i = reverse_j + 1
        else:
            i += 1
    return events


def main() -> int:
    from sqlalchemy import text

    from backend.database import SessionLocal
    from backend.services.relative_strength_scanner import (
        CANDLE_DAYS_BACK,
        CANDLE_INTERVAL,
        MIN_BARS,
        _sorted_candles,
    )
    from backend.services.rs_conviction_candles import load_instrument_atr_maps
    from backend.services.kavach_universe_vwap_scan import _atr_map
    from backend.services.upstox_service import UpstoxService
    from backend.config import settings

    db = SessionLocal()
    try:
        pairs = db.execute(
            text(
                """
                SELECT DISTINCT (event_at AT TIME ZONE 'Asia/Kolkata')::date AS d,
                       UPPER(symbol) AS symbol,
                       CASE
                         WHEN UPPER(COALESCE(direction, '')) IN ('BEAR', 'SHORT')
                         THEN 'SHORT' ELSE 'LONG'
                       END AS direction
                FROM rs_lock_membership_audit
                WHERE event_type = 'entry'
                  AND (event_at AT TIME ZONE 'Asia/Kolkata')::date
                      BETWEEN DATE '2026-07-13' AND DATE '2026-07-16'
                """
            )
        ).fetchall()
        align = db.execute(
            text(
                """
                SELECT session_date, symbol, logged_at, alignment_score, alignment_max
                FROM kavach_structural_alignment_log
                WHERE session_date BETWEEN DATE '2026-07-13' AND DATE '2026-07-16'
                ORDER BY session_date, symbol, logged_at
                """
            )
        ).fetchall()
        badge = db.execute(
            text(
                """
                SELECT session_date, symbol, logged_at, dir_conflict_active,
                       COALESCE((dir_conflict->>'conflict_count')::int, 0) AS n_opp,
                       dir_conflict->'opposing_fields' AS opposing,
                       dir_conflict->'sides' AS sides
                FROM kavach_badge_input_log
                WHERE session_date BETWEEN DATE '2026-07-13' AND DATE '2026-07-16'
                ORDER BY session_date, symbol, logged_at
                """
            )
        ).fetchall()
    finally:
        db.close()

    align_by = defaultdict(list)
    for a in align:
        mx = float(a.alignment_max or 5) or 5.0
        frac = float(a.alignment_score or 0) / mx
        align_by[(str(a.session_date), a.symbol)].append((a.logged_at, frac))

    # --- Change 3: DIR CONFLICT from existing shadow rows ---
    by_sym = defaultdict(list)
    for r in badge:
        by_sym[(str(r.session_date), r.symbol)].append(r)

    def _mom_oppose(opposing) -> int:
        if opposing is None:
            return 0
        if isinstance(opposing, str):
            try:
                opposing = json.loads(opposing)
            except Exception:
                opposing = []
        return sum(
            1
            for f in (opposing or [])
            if str(f) in ("trend", "ema_vs_vwap", "supertrend", "macd")
        )

    old_badge_rows = sum(1 for r in badge if r.dir_conflict_active)
    new_badge_rows = sum(
        1 for r in badge if r.dir_conflict_active and _mom_oppose(r.opposing) >= 2
    )
    # Also count rows that would show via suppress (n_opp inflated / kavach) —
    # approximate: old active AND (mom>=2 OR n_opp>=2 from non-momentum)
    new_badge_rows_strict = sum(1 for r in badge if _mom_oppose(r.opposing) >= 2)

    # Episodes that escalated 1→2+: would they still be captured later?
    escalated_captured_later = 0
    escalated_total = 0
    single_eps = 0
    for key, seq in by_sym.items():
        prev = False
        for r in seq:
            active = bool(r.dir_conflict_active)
            mom = _mom_oppose(r.opposing)
            if active and not prev and int(r.n_opp or 0) <= 1 and mom <= 1:
                single_eps += 1
                # look ahead 90m for mom>=2
                end = r.logged_at + timedelta(minutes=90)
                hit = False
                for r2 in seq:
                    if r2.logged_at <= r.logged_at:
                        continue
                    if r2.logged_at > end:
                        break
                    if _mom_oppose(r2.opposing) >= 2:
                        hit = True
                        break
                if hit:
                    escalated_captured_later += 1
                    escalated_total += 1
                else:
                    # count as escalated-style if never cleared and stayed single
                    escalated_total += 0
            prev = active

    # Recompute escalated_total as episodes that reached mom>=2 within 90m after single start
    esc_num = escalated_captured_later
    esc_den = 0
    for key, seq in by_sym.items():
        prev = False
        for r in seq:
            active = bool(r.dir_conflict_active)
            mom = _mom_oppose(r.opposing)
            if active and not prev and mom <= 1:
                esc_den += 1
            prev = active

    print(
        "=== CHANGE 3 DIR CONFLICT ===",
        json.dumps(
            {
                "old_badge_active_rows": old_badge_rows,
                "new_badge_rows_mom_ge2_among_old_active": new_badge_rows,
                "new_badge_rows_any_mom_ge2": new_badge_rows_strict,
                "badge_row_reduction_pct": round(
                    100 * (1 - new_badge_rows / max(old_badge_rows, 1)), 1
                ),
                "single_vote_episodes": esc_den,
                "single_eps_that_reach_2of3_within_90m": esc_num,
                "early_warning_retained_pct": round(
                    100 * esc_num / max(esc_den, 1), 1
                ),
            },
            indent=2,
        ),
        flush=True,
    )

    # --- Change 2: recompute whip EMA5 vs VWAP on candles ---
    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    db = SessionLocal()
    try:
        syms = sorted({r.symbol for r in pairs})
        ikey_map, _ = load_instrument_atr_maps(db, set(syms))
        atr_map = _atr_map(db, syms) or {}
    finally:
        db.close()

    ema_classes: Counter = Counter()
    vwap_classes: Counter = Counter()
    compared = 0
    for i, r in enumerate(sorted(pairs, key=lambda x: (str(x.d), x.symbol)), 1):
        d, sym, direction = str(r.d), r.symbol, r.direction
        ikey = ikey_map.get(sym)
        if not ikey:
            continue
        atr_pct = float(atr_map.get(sym) or 1.0)
        try:
            # Avoid range_end_date — some FO keys omit the end session day.
            raw = upstox.get_historical_candles_by_instrument_key(
                ikey,
                interval=CANDLE_INTERVAL,
                days_back=max(CANDLE_DAYS_BACK, 10),
            )
        except Exception as exc:
            print(f"  fetch fail {sym} {d}: {exc}", flush=True)
            continue
        from backend.services.relative_strength_scanner import _parse_ist_date

        candles = [
            c
            for c in _sorted_candles(raw or [])
            if (_parse_ist_date(c.get("timestamp")) or "") <= d
        ]
        if len(candles) < MIN_BARS:
            continue
        if not any(_parse_ist_date(c.get("timestamp")) == d for c in candles):
            print(f"  skip {sym} {d}: no session bars", flush=True)
            continue
        # price for atr abs
        last = float(candles[-1].get("close") or 0) or 1.0
        atr = last * atr_pct / 100.0
        is_long = direction != "SHORT"
        ema_ev = _events_ema5(
            candles, session_date=d, is_long=is_long, near_atr=0.35, atr=atr
        )
        vwap_ev = _events_vwap(candles, session_date=d, is_long=is_long)
        aligns = align_by.get((d, sym), [])

        def _case(events):
            if len(events) < 2:
                return None
            # first time count hits 2 → use reverse_ts of 2nd event
            t0 = events[1].get("reverse_ts") or events[1].get("touch_ts")
            if not t0:
                return None
            try:
                raw_ts = str(t0).replace("Z", "+00:00")
                t0d = __import__("datetime").datetime.fromisoformat(raw_ts)
            except Exception:
                return None
            fracs = [f for ts, f in aligns if ts >= t0d]
            if not fracs and aligns:
                # if timestamps tz-mismatch, use all post mid-session
                fracs = [f for _, f in aligns]
            first_n, max_n = 2, len(events)
            return _classify(fracs, first_n, max_n)

        ce = _case(ema_ev)
        cv = _case(vwap_ev)
        if ce:
            ema_classes[ce] += 1
        if cv:
            vwap_classes[cv] += 1
        if ce or cv:
            compared += 1
        if i % 40 == 0:
            print(f"  whip progress {i}/{len(pairs)}", flush=True)

    def _pct(ctr, key):
        n = sum(ctr.values())
        return round(100 * ctr.get(key, 0) / max(n, 1), 1)

    print(
        "=== CHANGE 2 WHIPSAW EMA5 vs VWAP ===",
        json.dumps(
            {
                "pairs": len(pairs),
                "sessions_with_flag_ema5": sum(ema_classes.values()),
                "sessions_with_flag_vwap": sum(vwap_classes.values()),
                "ema5_classes": dict(ema_classes),
                "vwap_classes": dict(vwap_classes),
                "ema5_clean_pct": _pct(ema_classes, "sustained_clean_after_whip"),
                "vwap_clean_pct": _pct(vwap_classes, "sustained_clean_after_whip"),
                "ema5_weak_or_worsening_pct": round(
                    _pct(ema_classes, "continued_weak_structure")
                    + _pct(ema_classes, "continued_whipsaw_worsening"),
                    1,
                ),
                "vwap_weak_or_worsening_pct": round(
                    _pct(vwap_classes, "continued_weak_structure")
                    + _pct(vwap_classes, "continued_whipsaw_worsening"),
                    1,
                ),
                "threshold": 2,
            },
            indent=2,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
