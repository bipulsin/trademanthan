#!/usr/bin/env python3
"""Fast Watch discard rate: BUY/READY flips that never reached Core same session.

Measures what % of chart-level BUY-equivalent first-flips (from RS snapshots)
would have fired Fast Watch but Core never included that symbol that session.

Core membership proxy per (session_date, side):
  - Any symbol in rs_conviction_board for that date/side (if retained)
  - UNION symbols in promotion_log (promote.symbol + promote.replaced_symbol)
  - UNION symbols with opening_anchor > 0 in rs_conviction_scoring_log (morning anchor)
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pytz

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402

from backend.database import SessionLocal  # noqa: E402

IST = pytz.timezone("Asia/Kolkata")
BULL_EQ = frozenset({"BUY", "READY"})
BEAR_EQ = frozenset({"SELL", "READY SHORT"})


def _side_from_ranking(rt: str) -> str:
    return "BEAR" if (rt or "").upper() == "BEARISH" else "BULL"


def _buy_eq(state: str, side: str) -> bool:
    k = (state or "").upper()
    return k in (BEAR_EQ if side == "BEAR" else BULL_EQ)


def _morning_locked(db, session_date: str) -> Set[str]:
    rows = db.execute(
        text(
            """
            SELECT symbol FROM daily_snapshot WHERE snapshot_date = CAST(:d AS date)
            """
        ),
        {"d": session_date},
    ).fetchall()
    return {r.symbol for r in rows}


def _in_top5_at_flip(db, session_date: str, symbol: str, flip_time) -> bool:
    r = db.execute(
        text(
            """
            SELECT 1 FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date)
              AND symbol = :sym
              AND scan_time = :t
              AND rank_position <= 5
            """
        ),
        {"d": session_date, "sym": symbol, "t": flip_time},
    ).fetchone()
    return r is not None


def _production_scope(db, session_date: str, symbol: str, flip_time) -> bool:
    """locked_or_top5: morning lock ∪ in top-5 at flip scan."""
    if symbol in _morning_locked(db, session_date):
        return True
    return _in_top5_at_flip(db, session_date, symbol, flip_time)


def _morning_core_proxy(db, session_date: str, side: str) -> Set[str]:
    """First-scan-of-day top-5 per side (09:20+ IST) as morning Core proxy."""
    ranking = "BEARISH" if side == "BEAR" else "BULLISH"
    rows = db.execute(
        text(
            """
            WITH first AS (
                SELECT MIN(scan_time) AS t
                FROM relative_strength_snapshot
                WHERE scan_time::date = CAST(:d AS date)
                  AND ranking_type = :rt
                  AND EXTRACT(HOUR FROM scan_time AT TIME ZONE 'Asia/Kolkata') * 60
                      + EXTRACT(MINUTE FROM scan_time AT TIME ZONE 'Asia/Kolkata') >= 560
            )
            SELECT symbol FROM relative_strength_snapshot s
            INNER JOIN first f ON s.scan_time = f.t
            WHERE s.ranking_type = :rt AND s.rank_position <= 5
            """
        ),
        {"d": session_date, "rt": ranking},
    ).fetchall()
    return {r.symbol for r in rows}


def _core_members(db, session_date: str, side: str) -> Set[str]:
    syms: Set[str] = set()
    syms |= _morning_core_proxy(db, session_date, side)
    rows = db.execute(
        text(
            """
            SELECT symbol FROM rs_conviction_board
            WHERE session_date = CAST(:d AS date) AND side = :side
            """
        ),
        {"d": session_date, "side": side},
    ).fetchall()
    for r in rows:
        syms.add(r.symbol)

    promos = db.execute(
        text(
            """
            SELECT symbol, replaced_symbol FROM rs_conviction_promotion_log
            WHERE session_date = CAST(:d AS date) AND side = :side AND event_type = 'promote'
            """
        ),
        {"d": session_date, "side": side},
    ).fetchall()
    for p in promos:
        syms.add(p.symbol)
        if p.replaced_symbol:
            syms.add(p.replaced_symbol)

    anchors = db.execute(
        text(
            """
            SELECT DISTINCT symbol FROM rs_conviction_scoring_log
            WHERE session_date = CAST(:d AS date) AND side = :side AND opening_anchor > 0
            """
        ),
        {"d": session_date, "side": side},
    ).fetchall()
    for a in anchors:
        syms.add(a.symbol)
    return syms


def _analyze(events: List[Dict], core_cache: Dict[Tuple[str, str], Set[str]]) -> Dict[str, Any]:
    never_core = reached_core = 0
    never_list: List[Dict] = []
    reached_list: List[Dict] = []
    for ev in events:
        ck = (ev["session_date"], ev["side"])
        core = core_cache.get(ck, set())
        if ev["symbol"] in core:
            reached_core += 1
            reached_list.append(ev)
        else:
            never_core += 1
            never_list.append(ev)
    n = len(events)
    dr = round(never_core / n, 4) if n else None
    return {
        "n": n,
        "reached": reached_core,
        "never": never_core,
        "discard_rate": dr,
        "discard_rate_pct": round(dr * 100, 1) if dr is not None else None,
        "never_sample": never_list[:5],
        "reached_sample": reached_list[:5],
    }


def main() -> int:
    db = SessionLocal()
    try:
        days = db.execute(
            text(
                """
                SELECT DISTINCT scan_time::date AS d
                FROM relative_strength_snapshot
                ORDER BY d
                """
            )
        ).fetchall()
        session_days = [str(r.d) for r in days]

        # Per (date, symbol, side): first BUY-equivalent flip scan
        rows = db.execute(
            text(
                """
                SELECT scan_time::date AS session_date, scan_time, symbol,
                       ranking_type, kavach_state, trade_score, confidence_grade
                FROM relative_strength_snapshot
                WHERE kavach_state IS NOT NULL
                  AND EXTRACT(HOUR FROM scan_time AT TIME ZONE 'Asia/Kolkata') * 60
                      + EXTRACT(MINUTE FROM scan_time AT TIME ZONE 'Asia/Kolkata') >= 555
                ORDER BY session_date, symbol, scan_time
                """
            )
        ).fetchall()
        # 555 = 09:15 — session scans only

        first_flip: Dict[Tuple[str, str, str], Any] = {}
        for r in rows:
            side = _side_from_ranking(r.ranking_type)
            if not _buy_eq(r.kavach_state, side):
                continue
            key = (str(r.session_date), r.symbol, side)
            if key not in first_flip:
                first_flip[key] = {
                    "session_date": str(r.session_date),
                    "symbol": r.symbol,
                    "side": side,
                    "first_flip_time": r.scan_time,
                    "kavach_state": r.kavach_state,
                    "trade_score": r.trade_score,
                    "confidence_grade": r.confidence_grade,
                }

        events = list(first_flip.values())
        if not events:
            print(json.dumps({"ok": False, "error": "no BUY/READY first-flips in snapshot history"}))
            return 1

        scoped_events = [
            e for e in events
            if _production_scope(db, e["session_date"], e["symbol"], e["first_flip_time"])
        ]
        scoped_off_lock = [
            e for e in scoped_events
            if e["symbol"] not in _morning_locked(db, e["session_date"])
        ]

        core_cache: Dict[Tuple[str, str], Set[str]] = {}
        for ev in events:
            ck = (ev["session_date"], ev["side"])
            if ck not in core_cache:
                core_cache[ck] = _core_members(db, ev["session_date"], ev["side"])

        all_days = _analyze(events, core_cache)
        scoped_all = _analyze(scoped_events, core_cache)
        scoped_ui = _analyze(scoped_off_lock, core_cache)
        jul6 = _analyze([e for e in events if e["session_date"] == "2026-07-06"], core_cache)
        jul7 = _analyze([e for e in events if e["session_date"] == "2026-07-07"], core_cache)
        jul7_scoped_ui = _analyze(
            [e for e in scoped_off_lock if e["session_date"] == "2026-07-07"], core_cache
        )
        naukri_jul7 = [e for e in events if e["session_date"] == "2026-07-07" and e["symbol"] == "NAUKRI"]

        discard_rate = scoped_ui["discard_rate"]
        verdict = "ship_with_full_visibility"
        if discard_rate is not None and discard_rate > 0.5:
            verdict = "ship_beta_or_default_off"
            note = (
                f"Discard rate {discard_rate:.0%} > 50%: Fast Watch alerts on many symbols "
                "Core never included. Ship default-off + beta label, or restrict to locked-list symbols only."
            )
        elif discard_rate is not None and discard_rate > 0.35:
            verdict = "ship_with_beta_label"
            note = "Moderate discard — explicit unconfirmed labeling + muted alerts."
        else:
            note = "Lower discard — still not entry authority."

        report = {
            "ok": True,
            "methodology": {
                "flip_definition": "First BUY/READY (bull) or SELL/READY SHORT (bear) per symbol/side/day in RS snapshots",
                "production_scope": "morning daily_snapshot lock ∪ in RS top-5 at flip scan (locked_or_top5)",
                "ui_scope": "production_scope minus symbols already on morning lock (visibility gap)",
                "core_reached": "Morning top-5 proxy + rs_conviction_board + promotion_log + opening_anchor scoring_log",
                "session_days": len(session_days),
                "date_range": [session_days[0], session_days[-1]] if session_days else None,
            },
            "all_flips_universe": all_days,
            "production_scope_all": scoped_all,
            "production_scope_off_lock_ui": scoped_ui,
            "jul6_only_reliable_scoring_log": jul6,
            "jul7_all_flips": jul7,
            "jul7_off_lock_ui": jul7_scoped_ui,
            "naukri_jul7_flip": naukri_jul7[:1],
            "verdict": verdict,
            "verdict_note": note,
            "fast_watch_recommendation": (
                "Ship default-off + beta label on checklist and dashboard; "
                "scope locked_or_top5; UI shows off-lock flips only; muted alerts"
            ),
        }
        print(json.dumps(report, indent=2, default=str))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
