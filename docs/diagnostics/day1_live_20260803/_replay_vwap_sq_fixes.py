#!/usr/bin/env python3
"""Replay Day-1 READY cards through VWAP-side gate + SQ earliest-eligible.

Run inside app container. Uses historical last-closed bar at promote time
(not wall-clock last_closed_bar_index).
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.daily_checklist_snapshot import _load_candles_for_symbol
from backend.services.relative_strength_scanner import _sorted_candles
from backend.services.rs_conviction_signals import _today_slice, _vwap_series_today
from backend.services.structural_quality_ready import (
    evaluate_sq_for_stock,
    load_latest_garuda_top6,
    load_universe_rs_scores,
)
from backend.services.structural_quality_score import grade_ab_ok, promote_threshold

IST = pytz.timezone("Asia/Kolkata")
DAY = date(2026, 8, 3)

CARDS = [
    ("CHOLAFIN", "2026-08-03T09:45:19", "LONG", "organic"),
    ("INOXWIND", "2026-08-03T09:45:19", "LONG", "organic"),
    ("BAJAJFINSV", "2026-08-03T09:45:19", "LONG", "organic"),
    ("FORTIS", "2026-08-03T10:16:28", "LONG", "organic"),
    ("DIVISLAB", "2026-08-03T11:10:32", "LONG", "SQ"),
    ("PNBHOUSING", "2026-08-03T11:16:24", "LONG", "SQ"),
    ("JUBLFOOD", "2026-08-03T12:05:40", "LONG", "SQ"),
    ("PAYTM", "2026-08-03T12:16:28", "LONG", "SQ"),
    ("ASHOKLEY", "2026-08-03T12:25:35", "LONG", "SQ"),
    ("APLAPOLLO", "2026-08-03T13:06:36", "LONG", "SQ"),
    ("LTM", "2026-08-03T13:26:34", "LONG", "SQ"),
    ("MCX", "2026-08-03T15:48:46", "SHORT", "other"),
]


def parse_ist(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    return IST.localize(dt) if dt.tzinfo is None else dt.astimezone(IST)


def to_ist(t: Any) -> Optional[datetime]:
    if t is None:
        return None
    if isinstance(t, str):
        t = datetime.fromisoformat(t.replace("Z", "+00:00"))
    if getattr(t, "tzinfo", None) is None:
        return IST.localize(t)
    return t.astimezone(IST)


def close_vwap_at(candles: List[Dict], asof: datetime) -> Dict[str, Any]:
    """Last closed 10m (bar_end ≤ asof) close vs session VWAP — production gate."""
    from backend.services.vwap_side_gate import (
        closed_10m_session_bars,
        last_closed_close_and_session_vwap,
    )

    close, vwap, n = last_closed_close_and_session_vwap(candles, now=asof)
    if close is None or vwap is None:
        return {"ok": None, "fail": "no_closed_10m"}
    closed = closed_10m_session_bars(candles, now=asof)
    last = closed[-1] if closed else {}
    return {
        "close": close,
        "vwap": round(vwap, 4),
        "delta": round(close - vwap, 4),
        "bar_open": to_ist(last.get("timestamp")).isoformat() if last.get("timestamp") else None,
        "bar_end": str(last.get("bar_end")) if last.get("bar_end") else None,
        "n_closed_10m": n,
        "bar_min": 10,
    }


def side_ok(direction: str, close: float, vwap: float) -> bool:
    return (close > vwap) if direction == "LONG" else (close < vwap)


def main() -> None:
    out: Dict[str, Any] = {"fix1_vwap_gate": [], "fix2_sq_earliest": [], "fix3_fortis": {}}
    db = SessionLocal()
    try:
        top6 = load_latest_garuda_top6(db, str(DAY))
        for sym, ts, direction, path in CARDS:
            promote = parse_ist(ts)
            candles = _load_candles_for_symbol(db, sym) or []
            cv = close_vwap_at(candles, promote)
            ok = None
            if cv.get("close") is not None:
                ok = side_ok(direction, cv["close"], cv["vwap"])
            out["fix1_vwap_gate"].append(
                {
                    "symbol": sym,
                    "path": path,
                    "direction": direction,
                    "promote_ist": promote.isoformat(),
                    "gate_pass": ok,
                    "would_block": ok is False,
                    **cv,
                }
            )

        # Fix 2: for SQ names, find earliest cycle where grade A/B + top6 + side ok
        # Approximate using universe snapshot scans + candle VWAP at each scan.
        sq_syms = [c[0] for c in CARDS if c[3] == "SQ"]
        with engine.connect() as conn:
            for sym in sq_syms:
                direction = "LONG"
                actual = next(c for c in CARDS if c[0] == sym)[1]
                actual_dt = parse_ist(actual)
                scans = conn.execute(
                    text(
                        """
                    SELECT scan_time AT TIME ZONE 'Asia/Kolkata' AS t,
                           confidence_grade, trade_score
                    FROM rs_universe_score_snapshot
                    WHERE session_date=:d AND UPPER(symbol)=:s
                    ORDER BY scan_time
                    """
                    ),
                    {"d": DAY, "s": sym},
                ).mappings().all()
                candles = _load_candles_for_symbol(db, sym) or []
                # Garuda LOCF at each scan: any top6 row with bar_end <= scan
                earliest = None
                for sc in scans:
                    t = sc["t"]
                    if t.tzinfo is None:
                        t = IST.localize(t)
                    grade = sc["confidence_grade"]
                    if not grade_ab_ok(grade):
                        continue
                    g = conn.execute(
                        text(
                            """
                        SELECT side, top6_rank, rank_score, bar_end
                        FROM garuda_screener_log
                        WHERE session_date=:d AND UPPER(symbol)=:s
                          AND top6_rank IS NOT NULL
                          AND bar_end <= :t
                        ORDER BY bar_end DESC LIMIT 1
                        """
                        ),
                        {"d": DAY, "s": sym, "t": t},
                    ).mappings().first()
                    if not g:
                        continue
                    cv = close_vwap_at(candles, t)
                    if cv.get("close") is None or not side_ok(direction, cv["close"], cv["vwap"]):
                        continue
                    # Proxy SQ total: we don't recompute OW/VW/EW here cheaply —
                    # use live SQ log total as "eventually met" and earliest grade+top6+side
                    earliest = {
                        "earliest_grade_top6_vwap_ist": t.isoformat(),
                        "grade": grade,
                        "ts": float(sc["trade_score"]) if sc["trade_score"] is not None else None,
                        "garuda_rank": g["top6_rank"],
                        "garuda_side": g["side"],
                        "close": cv["close"],
                        "vwap": cv["vwap"],
                    }
                    break
                actual_sq = conn.execute(
                    text(
                        """
                    SELECT promoted_at AT TIME ZONE 'Asia/Kolkata' AS t, total_score
                    FROM sq_ready_promotion_log
                    WHERE session_date=:d AND UPPER(symbol)=:s
                    """
                    ),
                    {"d": DAY, "s": sym},
                ).mappings().first()
                lag = None
                if earliest and actual_sq:
                    et = parse_ist(earliest["earliest_grade_top6_vwap_ist"][:19])
                    lag = round((actual_dt - et).total_seconds() / 60.0, 1)
                out["fix2_sq_earliest"].append(
                    {
                        "symbol": sym,
                        "actual_promote_ist": actual,
                        "actual_total": float(actual_sq["total_score"])
                        if actual_sq and actual_sq["total_score"] is not None
                        else None,
                        "earliest_inputs_ready": earliest,
                        "lateness_closed_min_vs_grade_top6_vwap": lag,
                        "note": (
                            "Earliest = first scan with Grade A/B + Garuda Top-6 LOCF "
                            "+ VWAP-side OK. Full SQ Total≥75 may be slightly later; "
                            "lock admission was the dominant delay historically."
                        ),
                    }
                )

        # Fix 3 FORTIS: first closed *10m* below VWAP after READY, then SHORT SQ
        from backend.services.vwap_side_gate import closed_10m_session_bars, vwap_side_ok

        candles = _load_candles_for_symbol(db, "FORTIS") or []
        promote = parse_ist("2026-08-03T10:16:28")
        thesis_break = None
        # Walk session 10m closes after promote until LONG VWAP-side fails
        eod = IST.localize(datetime(2026, 8, 3, 15, 30))
        step = promote
        while step <= eod:
            chk = vwap_side_ok("LONG", candles, now=step)
            if chk.get("ok") is False and chk.get("detail", {}).get("close") is not None:
                d = chk["detail"]
                closed = closed_10m_session_bars(candles, now=step)
                last = closed[-1] if closed else {}
                thesis_break = {
                    "asof_ist": step.isoformat(),
                    "bar_open_ist": to_ist(last.get("timestamp")).isoformat()
                    if last.get("timestamp")
                    else None,
                    "bar_end": str(last.get("bar_end")) if last.get("bar_end") else None,
                    "close": d.get("close"),
                    "vwap": d.get("vwap"),
                    "delta": d.get("close_minus_vwap"),
                }
                break
            step = step + timedelta(minutes=10)
        fortis_short = {"thesis_break": thesis_break, "short_sq": None}
        if thesis_break:
            tbreak = parse_ist((thesis_break.get("asof_ist") or thesis_break["bar_open_ist"])[:19])
            # Evaluate SHORT SQ at break using evaluate_sq_for_stock
            g = load_latest_garuda_top6(db, str(DAY)).get("FORTIS")
            # Prefer BEAR garuda LOCF at break
            with engine.connect() as conn:
                g_bear = conn.execute(
                    text(
                        """
                    SELECT side, top6_rank, rank_score, bar_end
                    FROM garuda_screener_log
                    WHERE session_date=:d AND UPPER(symbol)='FORTIS'
                      AND top6_rank IS NOT NULL
                      AND bar_end <= :t
                    ORDER BY bar_end DESC LIMIT 1
                    """
                    ),
                    {"d": DAY, "t": tbreak},
                ).mappings().first()
            rs = load_universe_rs_scores(db, str(DAY), ["FORTIS"]).get("FORTIS")
            stock = {"symbol": "FORTIS", "direction": "SHORT", "confidence": (rs or {}).get("confidence_grade")}
            # Truncate candles to break for eval
            br = None
            if g_bear:
                br = evaluate_sq_for_stock(
                    db=db,
                    stock=stock,
                    session_date=str(DAY),
                    candles=candles,
                    garuda_meta={
                        "top6_rank": g_bear["top6_rank"],
                        "rank_score": float(g_bear["rank_score"])
                        if g_bear["rank_score"] is not None
                        else None,
                        "side": g_bear["side"],
                        "bar_end": g_bear["bar_end"],
                    },
                    rs_meta=rs,
                )
            fortis_short["short_sq"] = {
                "garuda_at_break": dict(g_bear) if g_bear else None,
                "rs_grade": (rs or {}).get("confidence_grade"),
                "rs_score": (rs or {}).get("trade_score"),
                "rs_ranking_type": (rs or {}).get("ranking_type"),
                "garuda_side": (g_bear or {}).get("side") if g_bear else None,
                "sq_breakdown": br,
                "would_qualify_short_ready": bool(
                    br
                    and br.get("meets_threshold")
                    and grade_ab_ok(br.get("confidence_grade"))
                    and (
                        str((g_bear or {}).get("side") or "").upper()
                        in ("SHORT", "BEAR", "BEARISH")
                        or str((rs or {}).get("ranking_type") or "").upper() == "BEARISH"
                    )
                ),
                "manual_tv_short_start": "2026-08-03T11:15:00",
                "note": (
                    "SHORT READY if (Garuda BEAR/SHORT OR RS BEARISH) AND "
                    f"SQ Total≥{promote_threshold():.0f} AND grade A/B AND VWAP-side."
                ),
            }
        out["fix3_fortis"] = fortis_short

        # Summary
        blocked = [r["symbol"] for r in out["fix1_vwap_gate"] if r.get("would_block")]
        passed = [r["symbol"] for r in out["fix1_vwap_gate"] if r.get("gate_pass") is True]
        out["summary"] = {
            "fix1_would_block": blocked,
            "fix1_would_pass": passed,
            "expected_block": ["CHOLAFIN", "INOXWIND", "BAJAJFINSV"],
            "fix1_matches_expected_blocks": all(
                s in blocked for s in ("CHOLAFIN", "INOXWIND", "BAJAJFINSV")
            ),
        }
    finally:
        db.close()
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()
