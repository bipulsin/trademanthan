#!/usr/bin/env python3
"""Day-1 live extract for 2026-08-03 — run inside app container."""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import text

from backend.database import engine

DAY = date(2026, 8, 3)
OUT = Path("/tmp/day1_full.json")


def iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


def main() -> None:
    out: dict = {}
    with engine.connect() as c:
        # --- READY from badge (denser) ---
        ready = c.execute(
            text(
                """
            SELECT symbol, direction,
                   min(logged_at AT TIME ZONE 'Asia/Kolkata') AS first_ready,
                   count(*) AS ready_polls,
                   (array_agg(inputs->>'confidence' ORDER BY logged_at))[1] AS first_grade,
                   (array_agg(COALESCE((inputs->>'trade_score')::float,
                                       (inputs->>'dashboard_score')::float)
                              ORDER BY logged_at))[1] AS first_ts,
                   bool_or(COALESCE((inputs->>'promoted_via_structural_score')::boolean,false)) AS any_via_sq,
                   bool_or(COALESCE((inputs->>'trade_take_enabled')::boolean,false)) AS any_take
            FROM kavach_badge_input_log
            WHERE session_date=:d AND trade_state IN ('READY','READY(RECHECK)')
            GROUP BY symbol, direction
            ORDER BY first_ready
            """
            ),
            {"d": DAY},
        ).mappings().all()
        out["ready_badge"] = [{**dict(r), "first_ready": iso(r["first_ready"])} for r in ready]

        # --- TAKE from consistency + badge ---
        take = c.execute(
            text(
                """
            SELECT symbol, direction,
                   min(logged_at AT TIME ZONE 'Asia/Kolkata') AS first_take,
                   (array_agg(rendered_state ORDER BY logged_at))[1] AS state,
                   (array_agg(inputs->>'confidence' ORDER BY logged_at))[1] AS grade,
                   (array_agg(COALESCE((inputs->>'trade_score')::float,
                                       (inputs->>'dashboard_score')::float)
                              ORDER BY logged_at))[1] AS trade_score,
                   bool_or(COALESCE((inputs->>'promoted_via_structural_score')::boolean,false)) AS any_via_sq
            FROM kavach_ready_consistency_log
            WHERE session_date=:d AND (inputs->>'trade_take_enabled')='true'
            GROUP BY symbol, direction
            ORDER BY first_take
            """
            ),
            {"d": DAY},
        ).mappings().all()
        out["take_consistency"] = [{**dict(r), "first_take": iso(r["first_take"])} for r in take]

        take_b = c.execute(
            text(
                """
            SELECT symbol, direction,
                   min(logged_at AT TIME ZONE 'Asia/Kolkata') AS first_take,
                   (array_agg(trade_state ORDER BY logged_at))[1] AS state,
                   (array_agg(inputs->>'confidence' ORDER BY logged_at))[1] AS grade,
                   (array_agg(COALESCE((inputs->>'trade_score')::float,
                                       (inputs->>'dashboard_score')::float)
                              ORDER BY logged_at))[1] AS trade_score,
                   bool_or(COALESCE((inputs->>'promoted_via_structural_score')::boolean,false)) AS any_via_sq
            FROM kavach_badge_input_log
            WHERE session_date=:d AND (inputs->>'trade_take_enabled')='true'
            GROUP BY symbol, direction
            ORDER BY first_take
            """
            ),
            {"d": DAY},
        ).mappings().all()
        out["take_badge"] = [{**dict(r), "first_take": iso(r["first_take"])} for r in take_b]

        # --- SQ promotions ---
        sq = c.execute(
            text(
                """
            SELECT symbol, direction,
                   promoted_at AT TIME ZONE 'Asia/Kolkata' AS promoted_ist,
                   total_score, rs_score, garuda_score, ow, vw, ew, grade_bonus,
                   confidence_grade, garuda_top6_rank, also_organic,
                   pre_state, rendered_state, score_breakdown
            FROM sq_ready_promotion_log
            WHERE session_date=:d
            ORDER BY promoted_at
            """
            ),
            {"d": DAY},
        ).mappings().all()
        out["sq"] = []
        for r in sq:
            d = dict(r)
            d["promoted_ist"] = iso(d["promoted_ist"])
            out["sq"].append(d)

        # --- Universe coverage ---
        cycles = c.execute(
            text(
                """
            SELECT scan_time AT TIME ZONE 'Asia/Kolkata' AS scan_ist,
                   count(*) AS n_rows,
                   count(*) FILTER (WHERE exclusion_reason IS NULL) AS scored,
                   count(*) FILTER (WHERE exclusion_reason IS NOT NULL) AS unscored
            FROM rs_universe_score_snapshot
            WHERE session_date=:d
            GROUP BY scan_time
            ORDER BY scan_time
            """
            ),
            {"d": DAY},
        ).mappings().all()
        out["cycles"] = [{**dict(r), "scan_ist": iso(r["scan_ist"])} for r in cycles]
        scored = [r["scored"] for r in out["cycles"]]
        rows = [r["n_rows"] for r in out["cycles"]]
        out["cov_summary"] = {
            "n_scans": len(cycles),
            "min_scored": min(scored) if scored else None,
            "max_scored": max(scored) if scored else None,
            "avg_scored": round(sum(scored) / len(scored), 1) if scored else None,
            "avg_rows": round(sum(rows) / len(rows), 1) if rows else None,
            "avg_coverage_pct": round(
                100.0 * sum(scored) / sum(rows), 2
            )
            if rows and sum(rows)
            else None,
        }
        excl = c.execute(
            text(
                """
            SELECT exclusion_reason, count(*) AS n
            FROM rs_universe_score_snapshot
            WHERE session_date=:d AND exclusion_reason IS NOT NULL
            GROUP BY 1 ORDER BY n DESC
            """
            ),
            {"d": DAY},
        ).mappings().all()
        out["excl"] = [dict(r) for r in excl]

        # --- Top10 churn ---
        top10 = c.execute(
            text(
                """
            SELECT scan_time AT TIME ZONE 'Asia/Kolkata' AS scan_ist,
                   ranking_type, symbol
            FROM rs_universe_score_snapshot
            WHERE session_date=:d AND in_top10_membership=true
            ORDER BY scan_time, ranking_type
            """
            ),
            {"d": DAY},
        ).mappings().all()
        by_scan: dict = defaultdict(lambda: {"BULLISH": set(), "BEARISH": set()})
        for r in top10:
            st = iso(r["scan_ist"])
            by_scan[st][r["ranking_type"]].add(r["symbol"])
        times = sorted(by_scan.keys())
        events = []
        pb = pr = None
        for t in times:
            b = by_scan[t]["BULLISH"]
            be = by_scan[t]["BEARISH"]
            if pb is not None:
                if b - pb or pb - b:
                    events.append(
                        {"t": t, "side": "BULL", "in": sorted(b - pb), "out": sorted(pb - b)}
                    )
                if be - pr or pr - be:
                    events.append(
                        {
                            "t": t,
                            "side": "BEAR",
                            "in": sorted(be - pr),
                            "out": sorted(pr - be),
                        }
                    )
            pb, pr = b, be
        out["top10_events"] = events
        out["top10_summary"] = {
            "n_scans": len(times),
            "bull_in": sum(len(e["in"]) for e in events if e["side"] == "BULL"),
            "bull_out": sum(len(e["out"]) for e in events if e["side"] == "BULL"),
            "bear_in": sum(len(e["in"]) for e in events if e["side"] == "BEAR"),
            "bear_out": sum(len(e["out"]) for e in events if e["side"] == "BEAR"),
        }
        bonus = c.execute(
            text(
                """
            SELECT count(*) FILTER (WHERE incumbent_bonus_applied) AS n_bonus,
                   count(*) FILTER (WHERE in_top10_membership) AS n_top10
            FROM rs_universe_score_snapshot WHERE session_date=:d
            """
            ),
            {"d": DAY},
        ).mappings().one()
        out["bonus"] = dict(bonus)

        outside = c.execute(
            text(
                """
            SELECT symbol, ranking_type, count(*) AS n,
                   min(scan_time AT TIME ZONE 'Asia/Kolkata') AS first_ist,
                   max(confidence_grade) AS g, max(trade_score) AS ts
            FROM rs_universe_score_snapshot
            WHERE session_date=:d AND exclusion_reason IS NULL
              AND confidence_grade IN ('A+','A','B')
              AND COALESCE(in_top10_membership,false)=false
            GROUP BY symbol, ranking_type
            HAVING count(*) >= 2
            ORDER BY n DESC
            LIMIT 20
            """
            ),
            {"d": DAY},
        ).mappings().all()
        out["grades_outside"] = [
            {**dict(r), "first_ist": iso(r["first_ist"])} for r in outside
        ]

        # Early A/A+ with dist to EMA10
        early = c.execute(
            text(
                """
            SELECT symbol,
                   scan_time AT TIME ZONE 'Asia/Kolkata' AS scan_ist,
                   confidence_grade, trade_score, current_price, ema10,
                   CASE WHEN ema10>0 THEN abs(current_price-ema10)/ema10*100 END AS dist_ema10_pct
            FROM rs_universe_score_snapshot
            WHERE session_date=:d AND exclusion_reason IS NULL
              AND scan_time AT TIME ZONE 'Asia/Kolkata'
                  < TIMESTAMPTZ '2026-08-03 10:55:00+05:30'
              AND confidence_grade IN ('A+','A')
              AND current_price IS NOT NULL AND ema10 IS NOT NULL
            ORDER BY scan_time, symbol
            LIMIT 120
            """
            ),
            {"d": DAY},
        ).mappings().all()
        out["early_A"] = [{**dict(r), "scan_ist": iso(r["scan_ist"])} for r in early]
        inflated = [
            x
            for x in out["early_A"]
            if (x.get("dist_ema10_pct") or 0) >= 0.50
            and x.get("confidence_grade") in ("A+", "A")
        ]
        out["early_A_high_stretch_still_A"] = inflated[:30]
        out["early_A_high_stretch_count"] = len(inflated)

        # Price follow-through for SQ symbols from universe snapshots
        sq_ft = []
        for srow in out["sq"]:
            sym = srow["symbol"]
            prom = srow["promoted_ist"]
            # price at promote ± windows from snapshot
            px = c.execute(
                text(
                    """
                SELECT scan_time AT TIME ZONE 'Asia/Kolkata' AS scan_ist,
                       current_price, ema10, confidence_grade
                FROM rs_universe_score_snapshot
                WHERE session_date=:d AND symbol=:s
                  AND current_price IS NOT NULL
                ORDER BY scan_time
                """
                ),
                {"d": DAY, "s": sym},
            ).mappings().all()
            prices = [{**dict(r), "scan_ist": iso(r["scan_ist"])} for r in px]
            # find nearest at/after promote
            after = [p for p in prices if p["scan_ist"] >= prom[:19].replace("T", " ") or p["scan_ist"] >= prom]
            # simplify: parse loosely
            def _key(p):
                return str(p["scan_ist"])

            after_sorted = sorted(prices, key=_key)
            # pick price at first scan >= promote hour
            prom_hhmm = prom[11:16] if len(prom) > 16 else ""
            base = None
            for p in after_sorted:
                if str(p["scan_ist"])[11:16] >= prom_hhmm and str(p["scan_ist"])[:10] == "2026-08-03":
                    if str(p["scan_ist"]) >= prom[:19].replace("T", " "):
                        base = p
                        break
            if base is None and after_sorted:
                # fallback nearest
                base = min(after_sorted, key=lambda p: abs(hash(str(p["scan_ist"])) % 10**9))
            # better: use datetime compare
            try:
                prom_dt = datetime.fromisoformat(prom.replace("Z", "+00:00"))
                if prom_dt.tzinfo is None:
                    from zoneinfo import ZoneInfo

                    prom_dt = prom_dt.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
            except Exception:
                prom_dt = None
            base = None
            later = []
            for p in prices:
                try:
                    t = datetime.fromisoformat(str(p["scan_ist"]))
                    if t.tzinfo is None:
                        from zoneinfo import ZoneInfo

                        t = t.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
                except Exception:
                    continue
                if prom_dt and t >= prom_dt:
                    if base is None:
                        base = {**p, "_t": t}
                    later.append({**p, "_t": t})
            ft = {
                "symbol": sym,
                "promoted_ist": prom,
                "total_score": srow["total_score"],
                "pre_state": srow["pre_state"],
                "also_organic": srow["also_organic"],
            }
            if base and base.get("current_price"):
                bp = float(base["current_price"])
                ft["price_at_promote"] = bp
                for mins, label in ((30, "m30"), (60, "m60")):
                    target = base["_t"] + timedelta(minutes=mins)
                    cand = [x for x in later if x["_t"] >= target]
                    if cand:
                        cp = float(cand[0]["current_price"])
                        ft[f"price_{label}"] = cp
                        ft[f"chg_{label}_pct"] = round((cp / bp - 1.0) * 100.0, 3)
            sq_ft.append(ft)
        out["sq_followthrough"] = sq_ft

    OUT.write_text(json.dumps(out, indent=2, default=str))
    print(
        "wrote",
        OUT,
        "ready",
        len(out["ready_badge"]),
        "take",
        len(out["take_consistency"]),
        "sq",
        len(out["sq"]),
        "cycles",
        len(out["cycles"]),
    )


if __name__ == "__main__":
    main()
