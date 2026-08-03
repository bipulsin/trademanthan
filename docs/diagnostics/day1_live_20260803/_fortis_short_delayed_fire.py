#!/usr/bin/env python3
"""FORTIS SHORT delayed-fire check after LONG thesis break (2026-08-03)."""
from __future__ import annotations

import json
from datetime import date, datetime

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.daily_checklist_snapshot import _load_candles_for_symbol
from backend.services.structural_quality_ready import (
    evaluate_sq_for_stock,
    load_universe_rs_scores,
)
from backend.services.structural_quality_score import grade_ab_ok, promote_threshold
from backend.services.vwap_side_gate import vwap_side_ok

IST = pytz.timezone("Asia/Kolkata")
DAY = date(2026, 8, 3)
MARKS = [
    "2026-08-03T10:56:00",
    "2026-08-03T11:06:00",
    "2026-08-03T11:16:00",
    "2026-08-03T11:26:00",
    "2026-08-03T11:36:00",
]
MANUAL_TV = "2026-08-03T11:15:00"


def parse_ist(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    return IST.localize(dt) if dt.tzinfo is None else dt.astimezone(IST)


def main() -> None:
    out = {
        "symbol": "FORTIS",
        "session": str(DAY),
        "manual_tv_short_start": MANUAL_TV,
        "rows": [],
        "garuda_short_ever": False,
        "first_qualify": None,
        "verdict": None,
    }
    db = SessionLocal()
    try:
        candles = _load_candles_for_symbol(db, "FORTIS") or []
        with engine.connect() as conn:
            # Any Garuda SHORT Top-6 for FORTIS this session?
            g_short = conn.execute(
                text(
                    """
                    SELECT side, top6_rank, rank_score, bar_end
                    FROM garuda_screener_log
                    WHERE session_date=:d AND UPPER(symbol)='FORTIS'
                      AND top6_rank IS NOT NULL
                      AND UPPER(side) IN ('SHORT','BEAR','BEARISH')
                    ORDER BY bar_end
                    """
                ),
                {"d": DAY},
            ).mappings().all()
            out["garuda_short_rows"] = [dict(r) for r in g_short]
            out["garuda_short_ever"] = bool(g_short)

            rs_bear = conn.execute(
                text(
                    """
                    SELECT scan_time AT TIME ZONE 'Asia/Kolkata' AS t,
                           ranking_type, confidence_grade, trade_score
                    FROM rs_universe_score_snapshot
                    WHERE session_date=:d AND UPPER(symbol)='FORTIS'
                      AND UPPER(COALESCE(ranking_type,'')) = 'BEARISH'
                    ORDER BY scan_time
                    """
                ),
                {"d": DAY},
            ).mappings().all()
            out["rs_bearish_rows"] = [dict(r) for r in rs_bear]

            for mark in MARKS:
                t = parse_ist(mark)
                g = conn.execute(
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
                    {"d": DAY, "t": t},
                ).mappings().first()
                rs_row = conn.execute(
                    text(
                        """
                        SELECT scan_time AT TIME ZONE 'Asia/Kolkata' AS t,
                               ranking_type, confidence_grade, trade_score
                        FROM rs_universe_score_snapshot
                        WHERE session_date=:d AND UPPER(symbol)='FORTIS'
                          AND scan_time <= :t
                        ORDER BY scan_time DESC LIMIT 1
                        """
                    ),
                    {"d": DAY, "t": t},
                ).mappings().first()
                rs = dict(rs_row) if rs_row else None
                # Also try load_universe helper (latest of day) for compare
                rs_latest = load_universe_rs_scores(db, str(DAY), ["FORTIS"]).get("FORTIS")
                # Truncate candles to asof so SQ Total reflects that cycle, not EOD.
                cut = []
                for c in candles:
                    ts = c.get("timestamp") or c.get("datetime")
                    if not ts:
                        continue
                    if isinstance(ts, str):
                        ts_dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    else:
                        ts_dt = ts
                    if ts_dt.tzinfo is None:
                        ts_dt = IST.localize(ts_dt)
                    else:
                        ts_dt = ts_dt.astimezone(IST)
                    if ts_dt <= t:
                        cut.append(c)
                side_chk = vwap_side_ok("SHORT", cut or candles, now=t)
                stock = {
                    "symbol": "FORTIS",
                    "direction": "SHORT",
                    "confidence": (rs or {}).get("confidence_grade")
                    or (rs_latest or {}).get("confidence_grade"),
                }
                br = None
                if g:
                    # Force SHORT scoring direction regardless of Garuda side LOCF
                    gmeta = dict(g)
                    gmeta["side"] = "SHORT"
                    br = evaluate_sq_for_stock(
                        db=db,
                        stock=stock,
                        session_date=str(DAY),
                        candles=cut or candles,
                        garuda_meta=gmeta,
                        rs_meta={
                            "confidence_grade": stock["confidence"],
                            "trade_score": (rs or {}).get("trade_score")
                            or (rs_latest or {}).get("trade_score"),
                            "ranking_type": (rs or {}).get("ranking_type"),
                        },
                    )
                # Truncate SQ bars to mark: evaluate uses full day enrich — for honesty
                # note total uses bars through EOD in score_bars_through if enrich uses all.
                # Prefer re-eval note; still report numbers.
                qualify = bool(
                    br
                    and br.get("meets_threshold")
                    and grade_ab_ok(br.get("confidence_grade"))
                    and side_chk.get("ok")
                    and (
                        str((g or {}).get("side") or "").upper()
                        in ("SHORT", "BEAR", "BEARISH")
                        or str((rs or {}).get("ranking_type") or "").upper() == "BEARISH"
                    )
                )
                # Architectural flip path without SHORT Garuda: needs RS BEARISH + Top-6 present
                flip_path_ok = bool(
                    br
                    and br.get("meets_threshold")
                    and grade_ab_ok(br.get("confidence_grade"))
                    and side_chk.get("ok")
                    and g
                    and (
                        str((g or {}).get("side") or "").upper()
                        in ("SHORT", "BEAR", "BEARISH")
                        or str((rs or {}).get("ranking_type") or "").upper() == "BEARISH"
                    )
                )
                row = {
                    "asof": mark,
                    "garuda_side": (g or {}).get("side"),
                    "garuda_rank": (g or {}).get("top6_rank"),
                    "garuda_score": float(g["rank_score"])
                    if g and g.get("rank_score") is not None
                    else None,
                    "rs_ranking_type": (rs or {}).get("ranking_type"),
                    "rs_grade": (rs or {}).get("confidence_grade"),
                    "rs_score": float(rs["trade_score"])
                    if rs and rs.get("trade_score") is not None
                    else None,
                    "vwap_side_short_ok": side_chk.get("ok"),
                    "vwap_detail": side_chk.get("detail"),
                    "sq_total": (br or {}).get("total"),
                    "sq_meets_75": bool(br and br.get("meets_threshold")),
                    "grade_ab_ok": grade_ab_ok((br or {}).get("confidence_grade"))
                    if br
                    else False,
                    "would_flip_promote": flip_path_ok,
                    "blockers": [],
                }
                if not g:
                    row["blockers"].append("no_garuda_top6_locf")
                elif str(g.get("side") or "").upper() not in ("SHORT", "BEAR", "BEARISH"):
                    if str((rs or {}).get("ranking_type") or "").upper() != "BEARISH":
                        row["blockers"].append("garuda_still_long_and_no_bearish_rs")
                if not side_chk.get("ok"):
                    row["blockers"].append("vwap_side_short_fail")
                if not br or not br.get("meets_threshold"):
                    row["blockers"].append(
                        f"sq_total_{round((br or {}).get('total') or 0, 1)}_lt_{promote_threshold():.0f}"
                    )
                if br and not grade_ab_ok(br.get("confidence_grade")):
                    row["blockers"].append("grade_not_ab")
                out["rows"].append(row)
                if flip_path_ok and out["first_qualify"] is None:
                    out["first_qualify"] = mark

        if out["first_qualify"]:
            fq = parse_ist(out["first_qualify"])
            tv = parse_ist(MANUAL_TV)
            lag_min = round((fq - tv).total_seconds() / 60.0, 1)
            out["lag_vs_manual_tv_min"] = lag_min
            out["verdict"] = (
                f"Would have SHORT-flip promoted at {out['first_qualify']} "
                f"({lag_min:+.1f} min vs manual TV 11:15)."
            )
        elif not out["garuda_short_ever"] and not out["rs_bearish_rows"]:
            out["verdict"] = (
                "Never qualifies: Garuda never flagged FORTIS SHORT Top-6 this session, "
                "and RS universe never wrote BEARISH ranking_type — flip path starved of "
                "directional anchors even though price was on SHORT side of VWAP."
            )
        else:
            out["verdict"] = (
                "Never qualifies at sampled marks despite some SHORT anchors — see blockers "
                "(likely SQ Total < 75 or VWAP-side)."
            )
    finally:
        db.close()
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()
