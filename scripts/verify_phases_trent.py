"""Phase 1–3 verification replay for 08-Jul-2026 (TRENT + LAURUSLABS)."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pytz

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

IST = pytz.timezone("Asia/Kolkata")


def _load_candles(symbol: str, session_date: str):
    from backend.database import SessionLocal
    from backend.services.rs_conviction_candles import candles_cache_only, load_instrument_atr_maps

    db = SessionLocal()
    try:
        ikey_map, _ = load_instrument_atr_maps(db, {symbol})
        ikey = ikey_map.get(symbol.upper())
        if not ikey:
            return None, None
        candles = candles_cache_only(ikey)
        return candles, ikey
    finally:
        db.close()


def phase1_timeline(symbol: str, session_date: str) -> dict:
    from backend.services.kavach_10m import timeline_states
    from backend.services.kavach_engine import RANKING_BEARISH, RANKING_BULLISH
    from backend.services.daily_checklist_snapshot import locked_direction_map
    from backend.database import SessionLocal

    candles, ikey = _load_candles(symbol, session_date)
    if not candles:
        return {"ok": False, "error": f"No candles for {symbol} ({ikey})"}

    db = SessionLocal()
    try:
        lock_dirs = locked_direction_map(db, session_date)
        lock = lock_dirs.get(symbol.upper(), "LONG")
    finally:
        db.close()

    ranking = RANKING_BEARISH if lock == "SHORT" else RANKING_BULLISH
    rows = timeline_states(candles, ranking_type=ranking, start_min=9 * 60 + 15, end_min=13 * 60 + 40)
    buy_near_1035 = [
        r for r in rows
        if r["bar_end_ist"] in ("10:35", "10:25", "10:45")
        and r["kavach_state"] in ("BUY", "READY")
    ]
    transitions = []
    prev = None
    for r in rows:
        if prev and prev != r["kavach_state"]:
            transitions.append({"at": r["bar_end_ist"], "from": prev, "to": r["kavach_state"]})
        prev = r["kavach_state"]

    return {
        "ok": True,
        "symbol": symbol,
        "instrument_key": ikey,
        "lock_direction": lock,
        "timeline": rows,
        "transitions": transitions,
        "buy_near_1035": buy_near_1035,
        "phase1_pass": len(buy_near_1035) > 0,
    }


def phase2_replay(symbol: str, session_date: str) -> dict:
    from backend.services.kavach_10m import metrics_from_10m_candles, timeline_states
    from backend.services.kavach_engine import RANKING_BEARISH
    from backend.services.rs_fast_watch import is_edge_flip, kavach_direction, _is_reversal
    from backend.services.daily_checklist_snapshot import locked_direction_map
    from backend.database import SessionLocal

    candles, _ = _load_candles(symbol, session_date)
    if not candles:
        return {"ok": False, "error": "no candles"}

    db = SessionLocal()
    try:
        lock = locked_direction_map(db, session_date).get(symbol.upper(), "SHORT")
    finally:
        db.close()

    rows = timeline_states(candles, ranking_type=RANKING_BEARISH, start_min=9 * 60 + 15, end_min=13 * 60 + 40)
    flips = []
    prev = None
    for r in rows:
        bar_end = r["bar_end_ist"]
        h, m = map(int, bar_end.split(":"))
        now = IST.localize(datetime.strptime(session_date, "%Y-%m-%d").replace(hour=h, minute=m))
        mets = metrics_from_10m_candles(candles, ranking_type=RANKING_BEARISH, nifty_pct=0.0, now=now)
        if not mets:
            continue
        new = mets["kavach_state"]
        if is_edge_flip(prev, new):
            flips.append({
                "at": bar_end,
                "prev": prev,
                "new": new,
                "direction": kavach_direction(new),
                "is_reversal": _is_reversal(new, lock),
                "grade": mets.get("confidence_grade"),
                "price": mets.get("price"),
            })
        prev = new

    trent_long_rev = [f for f in flips if f.get("direction") == "LONG" and f.get("is_reversal")]
    return {
        "ok": True,
        "flips": flips,
        "reversal_flips": trent_long_rev,
        "phase2_pass": any(
            f.get("is_reversal") and f.get("direction") == "LONG" and f["at"] in ("10:35", "10:25", "10:45")
            for f in flips
        ),
    }


def phase3_replay(session_date: str) -> dict:
    from backend.services.rs_go_board import replay_go_board_day

    return replay_go_board_day(session_date)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--session-date", default="2026-07-08")
    ap.add_argument("--phase", type=int, choices=(1, 2, 3, 0), default=0)
    args = ap.parse_args()
    out = {"session_date": args.session_date}
    if args.phase in (0, 1):
        out["phase1_trent"] = phase1_timeline("TRENT", args.session_date)
    if args.phase in (0, 2):
        out["phase2_trent"] = phase2_replay("TRENT", args.session_date)
    if args.phase in (0, 3):
        out["phase3"] = phase3_replay(args.session_date)
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
