#!/usr/bin/env python3
"""Part 1+2 validation: Garuda/RS side dynamics + 2-candle VWAP flip confirm replay."""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.daily_checklist_snapshot import _load_candles_for_symbol
from backend.services.vwap_side_gate import closed_10m_session_bars

IST = pytz.timezone("Asia/Kolkata")
DAY = date(2026, 8, 3)
SYMS = ["FORTIS", "CHOLAFIN", "BAJAJFINSV", "INOXWIND"]
MANUAL_TV = {"FORTIS": "2026-08-03T11:15:00"}


def _signed_delta(close: float, vwap: float) -> float:
    return close - vwap


def session_10m_vwap_series(candles) -> List[Dict[str, Any]]:
    closed = closed_10m_session_bars(
        candles, now=IST.localize(datetime(2026, 8, 3, 15, 35))
    )
    out = []
    pv = vv = 0.0
    for b in closed:
        h = float(b["high"])
        l = float(b["low"])
        c = float(b["close"])
        v = float(b.get("volume") or 1.0)
        tp = (h + l + c) / 3.0
        pv += tp * v
        vv += v
        vwap = pv / vv if vv else None
        be = b.get("bar_end")
        out.append(
            {
                "bar_end": str(be),
                "close": c,
                "vwap": round(vwap, 4) if vwap else None,
                "delta": round(_signed_delta(c, vwap), 4) if vwap else None,
            }
        )
    return out


def find_2candle_flips(
    series: List[Dict[str, Any]], *, from_side: str = "LONG"
) -> List[Dict[str, Any]]:
    """Detect confirmed flips away from from_side via 2-candle VWAP acceleration."""
    flips = []
    for i in range(1, len(series)):
        a, b = series[i - 1], series[i]
        d0, d1 = a.get("delta"), b.get("delta")
        if d0 is None or d1 is None:
            continue
        if from_side == "LONG":
            # flip candle below, confirm extends further below
            if d0 < 0 and d1 < 0 and d1 < d0:
                flips.append(
                    {
                        "new_side": "SHORT",
                        "flip_bar_end": a["bar_end"],
                        "flip_close": a["close"],
                        "flip_vwap": a["vwap"],
                        "flip_delta": d0,
                        "confirm_bar_end": b["bar_end"],
                        "confirm_close": b["close"],
                        "confirm_vwap": b["vwap"],
                        "confirm_delta": d1,
                    }
                )
        else:
            if d0 > 0 and d1 > 0 and d1 > d0:
                flips.append(
                    {
                        "new_side": "LONG",
                        "flip_bar_end": a["bar_end"],
                        "flip_close": a["close"],
                        "flip_vwap": a["vwap"],
                        "flip_delta": d0,
                        "confirm_bar_end": b["bar_end"],
                        "confirm_close": b["close"],
                        "confirm_vwap": b["vwap"],
                        "confirm_delta": d1,
                    }
                )
    return flips


def count_whipsaws(flips: List[Dict[str, Any]], series: List[Dict[str, Any]]) -> int:
    """How many confirmed flips reverse within next 2 closed 10m bars."""
    by_end = {r["bar_end"]: i for i, r in enumerate(series)}
    n = 0
    for f in flips:
        i = by_end.get(f["confirm_bar_end"])
        if i is None:
            continue
        new = f["new_side"]
        # reverse if within 2 bars price returns to old side of VWAP
        for j in range(i + 1, min(i + 3, len(series))):
            d = series[j].get("delta")
            if d is None:
                continue
            if new == "SHORT" and d > 0:
                n += 1
                break
            if new == "LONG" and d < 0:
                n += 1
                break
    return n


def main() -> None:
    out: Dict[str, Any] = {"session": str(DAY), "symbols": {}, "root_cause_note": None}
    db = SessionLocal()
    try:
        with engine.connect() as conn:
            for sym in SYMS:
                candles = _load_candles_for_symbol(db, sym) or []
                series = session_10m_vwap_series(candles)
                g = conn.execute(
                    text(
                        """
                        SELECT bar_end, side, top6_rank, rank_score
                        FROM garuda_screener_log
                        WHERE session_date=:d AND UPPER(symbol)=:s
                        ORDER BY bar_end
                        """
                    ),
                    {"d": DAY, "s": sym},
                ).mappings().all()
                g_top6 = [dict(r) for r in g if r["top6_rank"] is not None]
                g_sides = sorted({r["side"] for r in g if r["side"]})
                g_top6_sides = sorted({r["side"] for r in g_top6 if r["side"]})

                rs = conn.execute(
                    text(
                        """
                        SELECT scan_time AT TIME ZONE 'Asia/Kolkata' AS t,
                               ranking_type, confidence_grade, trade_score
                        FROM rs_universe_score_snapshot
                        WHERE session_date=:d AND UPPER(symbol)=:s
                        ORDER BY scan_time
                        """
                    ),
                    {"d": DAY, "s": sym},
                ).mappings().all()
                rs_flips = []
                prev = None
                for r in rs:
                    rt = r["ranking_type"]
                    if prev is not None and rt != prev:
                        rs_flips.append(
                            {
                                "t": str(r["t"]),
                                "from": prev,
                                "to": rt,
                                "grade": r["confidence_grade"],
                                "score": float(r["trade_score"])
                                if r["trade_score"] is not None
                                else None,
                            }
                        )
                    prev = rt

                # Morning assigned LONG for these four — confirm SHORT flips
                short_confirms = find_2candle_flips(series, from_side="LONG")
                first = short_confirms[0] if short_confirms else None
                whipsaw = count_whipsaws(short_confirms, series)

                # Garuda side at/after first confirm
                g_at_confirm = None
                if first:
                    for r in g_top6:
                        if str(r["bar_end"]) <= first["confirm_bar_end"]:
                            g_at_confirm = {
                                "bar_end": str(r["bar_end"]),
                                "side": r["side"],
                                "top6_rank": r["top6_rank"],
                            }

                entry = {
                    "garuda_all_sides": g_sides,
                    "garuda_top6_sides": g_top6_sides,
                    "garuda_top6_count": len(g_top6),
                    "garuda_top6_sample": [
                        {
                            "bar_end": str(r["bar_end"]),
                            "side": r["side"],
                            "rank": r["top6_rank"],
                        }
                        for r in g_top6[:8]
                    ]
                    + (
                        [
                            {
                                "bar_end": str(r["bar_end"]),
                                "side": r["side"],
                                "rank": r["top6_rank"],
                            }
                            for r in g_top6[-3:]
                        ]
                        if len(g_top6) > 8
                        else []
                    ),
                    "rs_flip_count": len(rs_flips),
                    "rs_flips_sample": rs_flips[:15],
                    "first_short_2candle_confirm": first,
                    "short_2candle_confirm_count": len(short_confirms),
                    "whipsaw_within_2bars": whipsaw,
                    "garuda_top6_at_or_before_confirm": g_at_confirm,
                    "manual_tv": MANUAL_TV.get(sym),
                }
                if first and MANUAL_TV.get(sym):
                    try:
                        raw = str(first["confirm_bar_end"]).replace("Z", "+00:00")
                        ct = datetime.fromisoformat(raw[:32] if len(raw) > 19 else raw)
                        if ct.tzinfo is None:
                            ct = IST.localize(ct)
                        else:
                            ct = ct.astimezone(IST)
                        tv = IST.localize(datetime.fromisoformat(MANUAL_TV[sym]))
                        entry["lag_vs_manual_tv_min"] = round(
                            (ct - tv).total_seconds() / 60.0, 1
                        )
                    except Exception as exc:
                        entry["lag_parse_error"] = str(exc)
                out["symbols"][sym] = entry

        # Universe-wide whipsaw rate for false-positive estimate (same day, all symbols with candles in lock? use distinct garuda symbols)
        with engine.connect() as conn:
            syms = [
                r[0]
                for r in conn.execute(
                    text(
                        """
                        SELECT DISTINCT UPPER(symbol) FROM garuda_screener_log
                        WHERE session_date=:d AND top6_rank IS NOT NULL
                        """
                    ),
                    {"d": DAY},
                ).fetchall()
            ]
        total_confirms = 0
        total_whipsaw = 0
        for sym in syms:
            candles = _load_candles_for_symbol(db, sym) or []
            if not candles:
                continue
            series = session_10m_vwap_series(candles)
            # both directions: count confirms from whatever morning side via both scanners
            for side0 in ("LONG", "SHORT"):
                flips = find_2candle_flips(series, from_side=side0)
                total_confirms += len(flips)
                total_whipsaw += count_whipsaws(flips, series)
        out["universe_top6_day"] = {
            "n_symbols": len(syms),
            "total_2candle_confirms_both_dirs": total_confirms,
            "whipsaw_within_2_bars": total_whipsaw,
            "whipsaw_rate": round(total_whipsaw / total_confirms, 3)
            if total_confirms
            else None,
        }
        out["root_cause_note"] = (
            "Garuda side and RS ranking_type are recomputed every cycle — not frozen at "
            "first qualification. FORTIS never got SHORT Top-6 because Garuda's vote "
            "(imbalance or VWAP+EMA+ROC majority) stayed LONG; RS did go BEARISH but "
            "only with grade D. No Garuda/RS flip-flop gate exists today; B20 only "
            "affects within-side Top-10 membership."
        )
    finally:
        db.close()
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()
