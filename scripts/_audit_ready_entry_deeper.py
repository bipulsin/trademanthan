#!/usr/bin/env python3
"""Deeper READY entry path audit: live miss, audit age, tip lag, freeze streaks."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.daily_checklist_snapshot import _load_candles_for_symbol
from backend.services.kavach_10m import metrics_from_10m_candles
from backend.services.relative_strength_scanner import RANKING_BEARISH, RANKING_BULLISH

IST = timezone(timedelta(hours=5, minutes=30))
DEPLOY = datetime(2026, 7, 31, 5, 47, 0, tzinfo=timezone.utc)
SESSION = "2026-07-31"


def _f(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def main() -> None:
    db = SessionLocal()
    rows = db.execute(
        text(
            """
            SELECT symbol, direction, rendered_state, logged_at, inputs
            FROM kavach_ready_consistency_log
            WHERE session_date = CAST(:d AS date)
              AND rendered_state ILIKE '%READY%'
            ORDER BY symbol, logged_at
            """
        ),
        {"d": SESSION},
    ).mappings().all()

    print("=== Entry freeze streaks (same entry across consecutive logs while expected EMA moves) ===")
    by_sym: Dict[str, List] = defaultdict(list)
    for r in rows:
        by_sym[str(r["symbol"]).upper()].append(r)

    candle_map = {}
    for sym in by_sym:
        candle_map[sym] = _load_candles_for_symbol(db, sym) or []

    for sym, arr in sorted(by_sym.items()):
        entries = []
        for r in arr:
            inp = r["inputs"] or {}
            entry = _f(inp.get("trade_entry"))
            dwell = inp.get("dwell_entry_shadow") or {}
            live = dwell.get("live_levels") or {}
            audit = dwell.get("audit_levels") or {}
            logged = r["logged_at"]
            if logged.tzinfo is None:
                logged = logged.replace(tzinfo=timezone.utc)
            entries.append(
                {
                    "at": logged.astimezone(IST),
                    "entry": entry,
                    "live_e5": _f(live.get("ema5")),
                    "live_px": _f(live.get("price")),
                    "bar_at": live.get("bar_at"),
                    "audit_e5": _f(audit.get("ema5")),
                    "audit_src": audit.get("source"),
                    "post": logged >= DEPLOY,
                }
            )

        # freeze: consecutive identical entry lasting > 3 polls
        freeze_runs = []
        i = 0
        while i < len(entries):
            j = i + 1
            while j < len(entries) and entries[j]["entry"] == entries[i]["entry"]:
                j += 1
            if j - i >= 3 and entries[i]["entry"] is not None:
                freeze_runs.append((entries[i], entries[j - 1], j - i))
            i = j

        print(f"\n{sym}: polls={len(entries)} freeze_runs(>=3 identical entry)={len(freeze_runs)}")
        for a, b, n in freeze_runs:
            # expected ema at start vs end
            def exp_at(ts):
                cut = []
                for c in candle_map[sym]:
                    tss = str(c.get("timestamp") or "")
                    try:
                        dt = datetime.fromisoformat(tss.replace("Z", "+00:00")).astimezone(IST)
                    except Exception:
                        continue
                    if dt <= ts:
                        cut.append(c)
                if len(cut) < 40:
                    return None
                m = metrics_from_10m_candles(
                    cut, ranking_type=RANKING_BULLISH, nifty_pct=0.0, include_forming=True, now=ts
                )
                return _f((m or {}).get("ema5"))

            e0 = exp_at(a["at"])
            e1 = exp_at(b["at"])
            moved = None
            if e0 and e1:
                moved = abs(e1 - e0)
            print(
                f"  entry={a['entry']} x{n} from {a['at'].strftime('%H:%M')}->"
                f"{b['at'].strftime('%H:%M')} live_e5={a['live_e5']}->"
                f"{b['live_e5']} audit={a['audit_e5']} exp_ema {e0}->{e1} "
                f"ema_moved={round(moved,4) if moved is not None else None} "
                f"post={a['post']}/{b['post']}"
            )

        # show last 5 polls
        print("  last polls:")
        for e in entries[-5:]:
            print(
                f"    {e['at'].strftime('%H:%M:%S')} entry={e['entry']} "
                f"live_e5={e['live_e5']} live_px={e['live_px']} "
                f"bar_at={e['bar_at']} audit_e5={e['audit_e5']} src={e['audit_src']}"
            )

    # Compare displayed entry to *price* (user TV compare) vs EMA5
    print("\n=== NOW: entry vs LTP vs EMA5 (what TV viewer sees) ===")
    for sym in sorted(by_sym):
        last = by_sym[sym][-1]
        inp = last["inputs"] or {}
        entry = _f(inp.get("trade_entry"))
        c = candle_map[sym]
        m = metrics_from_10m_candles(
            c, ranking_type=RANKING_BULLISH, nifty_pct=0.0, include_forming=True
        ) if c else None
        e5 = _f((m or {}).get("ema5"))
        px = _f((m or {}).get("price"))
        # last close
        tip = c[-1] if c else {}
        print(
            f"  {sym}: card_entry={entry} ema5={round(e5,2) if e5 else None} "
            f"ltp={px} tip_close={tip.get('close')} tip_ts={tip.get('timestamp')} "
            f"entry_vs_ltp%={round(100*abs(entry-px)/px,3) if entry and px else None} "
            f"entry_vs_ema5%={round(100*abs(entry-e5)/e5,3) if entry and e5 else None}"
        )

    # How often does apply path have candles? Check shadow live source
    print("\n=== live_levels.source distribution today ===")
    src = db.execute(
        text(
            """
            SELECT
              inputs->'dwell_entry_shadow'->'live_levels'->>'source' AS src,
              (inputs->'dwell_entry_shadow'->'live_levels'->>'ema5') IS NOT NULL AS has_e5,
              COUNT(*) AS n
            FROM kavach_ready_consistency_log
            WHERE session_date = CAST(:d AS date)
              AND rendered_state ILIKE '%READY%'
            GROUP BY 1, 2
            ORDER BY 3 DESC
            """
        ),
        {"d": SESSION},
    ).mappings().all()
    for r in src:
        print(dict(r))

    # audit bar age from rs_live vs consistency entry
    print("\n=== rs_live_kavach_audit freshness (latest per READY symbol) ===")
    for sym in sorted(by_sym):
        r = db.execute(
            text(
                """
                SELECT computed_at, bar_evaluated_at, price, ema5
                FROM rs_live_kavach_audit
                WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :s
                ORDER BY computed_at DESC LIMIT 1
                """
            ),
            {"d": SESSION, "s": sym},
        ).mappings().first()
        if not r:
            print(f"  {sym}: NO rs_live")
            continue
        ca = r["computed_at"]
        if ca.tzinfo is None:
            ca = ca.replace(tzinfo=timezone.utc)
        age_min = (datetime.now(timezone.utc) - ca.astimezone(timezone.utc)).total_seconds() / 60
        print(
            f"  {sym}: computed_at={ca.astimezone(IST).strftime('%H:%M:%S')} "
            f"age_min={age_min:.1f} bar={r['bar_evaluated_at']} "
            f"ema5={round(float(r['ema5']),4) if r['ema5'] else None} px={r['price']}"
        )

    # Historical freeze: fraction of READY rows where entry equals prior row entry
    # and live ema5 is null (audit sticky)
    print("\n=== HISTORICAL sticky-entry rate (entry unchanged vs prior poll, live_e5 null) ===")
    hist = db.execute(
        text(
            """
            WITH ordered AS (
              SELECT session_date, symbol, logged_at,
                     (inputs->>'trade_entry')::float AS entry,
                     inputs->'dwell_entry_shadow'->'live_levels'->>'ema5' AS live_e5,
                     LAG((inputs->>'trade_entry')::float) OVER (
                       PARTITION BY session_date, symbol ORDER BY logged_at
                     ) AS prev_entry
              FROM kavach_ready_consistency_log
              WHERE session_date >= CURRENT_DATE - 14
                AND rendered_state ILIKE '%READY%'
            )
            SELECT session_date::text AS d,
                   COUNT(*) AS n,
                   COUNT(*) FILTER (
                     WHERE prev_entry IS NOT NULL AND entry = prev_entry
                       AND live_e5 IS NULL
                   ) AS sticky_audit,
                   COUNT(*) FILTER (
                     WHERE prev_entry IS NOT NULL AND entry = prev_entry
                   ) AS sticky_any,
                   COUNT(*) FILTER (WHERE live_e5 IS NULL) AS live_miss
            FROM ordered
            GROUP BY 1
            ORDER BY 1
            """
        )
    ).mappings().all()
    for h in hist:
        n = int(h["n"] or 0) or 1
        print(
            f"  {h['d']}: sticky_any={h['sticky_any']}/{h['n']} "
            f"({100*int(h['sticky_any'])/n:.0f}%) "
            f"sticky_audit_path={h['sticky_audit']}/{h['n']} "
            f"({100*int(h['sticky_audit'])/n:.0f}%) "
            f"live_miss={h['live_miss']}/{h['n']} "
            f"({100*int(h['live_miss'])/n:.0f}%)"
        )

    db.close()


if __name__ == "__main__":
    main()
