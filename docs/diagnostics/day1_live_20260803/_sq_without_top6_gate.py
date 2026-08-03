"""SQ without Top-6: symbols that never had top6_rank but would meet SQ≥75."""
from __future__ import annotations

import json
from datetime import date

from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.daily_checklist_snapshot import _load_candles_for_symbol
from backend.services.structural_quality_ready import (
    evaluate_sq_for_stock,
    load_latest_garuda_scores,
    load_universe_rs_scores,
)
from backend.services.structural_quality_score import grade_ab_ok, promote_threshold
from backend.services.vwap_side_gate import vwap_side_ok

DAY = date(2026, 8, 3)


def main() -> None:
    out: dict = {"session": str(DAY), "threshold": promote_threshold(), "rows": []}
    db = SessionLocal()
    try:
        with engine.connect() as conn:
            ever_top6 = {
                r[0]
                for r in conn.execute(
                    text(
                        """
                        SELECT DISTINCT UPPER(symbol)
                        FROM garuda_screener_log
                        WHERE session_date=:d AND top6_rank IS NOT NULL
                        """
                    ),
                    {"d": DAY},
                ).fetchall()
            }
            # Symbols with at least one rank_score today
            scored = {
                r[0]
                for r in conn.execute(
                    text(
                        """
                        SELECT DISTINCT UPPER(symbol)
                        FROM garuda_screener_log
                        WHERE session_date=:d AND rank_score IS NOT NULL
                        """
                    ),
                    {"d": DAY},
                ).fetchall()
            }
        never_top6 = sorted(scored - ever_top6)
        out["n_scored"] = len(scored)
        out["n_ever_top6"] = len(ever_top6)
        out["n_never_top6"] = len(never_top6)

        universe = load_latest_garuda_scores(db, str(DAY))
        rs_map = load_universe_rs_scores(db, str(DAY), never_top6)
        would_promote = []
        grade_ab_never = []
        for sym in never_top6:
            g = universe.get(sym)
            rs = rs_map.get(sym) or {}
            grade = rs.get("confidence_grade")
            if grade_ab_ok(grade):
                grade_ab_never.append(sym)
            candles = _load_candles_for_symbol(db, sym) or []
            stock = {
                "symbol": sym,
                "direction": "SHORT"
                if str((g or {}).get("side") or "").upper() in ("SHORT", "BEAR", "BEARISH")
                else "LONG",
                "confidence": grade,
            }
            br = evaluate_sq_for_stock(
                db=db,
                stock=stock,
                session_date=str(DAY),
                candles=candles,
                garuda_meta=g,
                rs_meta=rs,
            )
            if not br:
                continue
            side_ok = vwap_side_ok(stock["direction"], candles).get("ok")
            row = {
                "symbol": sym,
                "side": (g or {}).get("side"),
                "garuda_score": (g or {}).get("rank_score"),
                "grade": grade,
                "rs_score": rs.get("trade_score"),
                "sq_total": br.get("total"),
                "meets_75": br.get("meets_threshold"),
                "vwap_side_ok": side_ok,
                "would_promote": bool(br.get("meets_threshold") and side_ok),
                "OW": br.get("OW"),
                "VW": br.get("VW"),
                "EW": br.get("EW"),
            }
            out["rows"].append(row)
            if row["would_promote"]:
                would_promote.append(row)

        out["n_grade_ab_never_top6"] = len(grade_ab_never)
        out["n_would_promote_never_top6"] = len(would_promote)
        out["would_promote"] = would_promote
        # Also: how many Top-6-gated promotes existed historically
        with engine.connect() as conn:
            hist = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM sq_ready_promotion_log WHERE session_date=:d
                    """
                ),
                {"d": DAY},
            ).scalar()
        out["historical_sq_promotes"] = int(hist or 0)
        out["verdict"] = (
            f"Garuda scored {len(scored)} symbols; {len(ever_top6)} ever Top-6; "
            f"{len(never_top6)} never Top-6. Of never-Top-6, {len(grade_ab_never)} had "
            f"grade A/B at LOCF, and {len(would_promote)} would have SQ≥{promote_threshold():.0f} "
            f"+ VWAP-side (invisible under Top-6 gate)."
        )
    finally:
        db.close()
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()
