#!/usr/bin/env python3
"""Reconstruct rs_lock_membership_audit for a session from RS snapshots + Upstox 5m.

Used to fix 2026-07-13 where morning lock was delayed to 14:45 (checklist crash),
leaving only a single artificial morning_lock dump and no real entry/remove trail.

Replays the same live functions:
  lock_morning_snapshot → promote_intraday_from_rs (R1/R2 + intraday_2scan)

at each historical RS scan_time through 14:30, with Upstox historical candles for R1.

Does NOT change live gate logic. Leaves other session_dates untouched.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

IST = pytz.timezone("Asia/Kolkata")
SESSION = "2026-07-13"


def _as_ist(dt: Any) -> datetime:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
    raise TypeError(type(dt))


def _top5_at_scan(db, session_date: str, scan_time) -> Tuple[List[Dict], List[Dict]]:
    rows = db.execute(
        text(
            """
            SELECT UPPER(symbol) AS symbol, ranking_type, rank_position, relative_strength
            FROM relative_strength_snapshot
            WHERE scan_time = :st
              AND scan_time::date = CAST(:d AS date)
              AND rank_position IS NOT NULL
              AND rank_position <= 5
            ORDER BY ranking_type, rank_position
            """
        ),
        {"st": scan_time, "d": session_date},
    ).fetchall()
    bull: List[Dict] = []
    bear: List[Dict] = []
    for r in rows:
        item = {
            "symbol": str(r.symbol).upper(),
            "relative_strength": r.relative_strength,
            "rank_position": int(r.rank_position),
        }
        if (r.ranking_type or "").upper() == "BEARISH":
            bear.append(item)
        else:
            bull.append(item)
    bull.sort(key=lambda x: x["rank_position"])
    bear.sort(key=lambda x: x["rank_position"])
    return bull[:5], bear[:5]


def _scan_times(db, session_date: str) -> List[datetime]:
    rows = db.execute(
        text(
            """
            SELECT DISTINCT scan_time
            FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date)
            ORDER BY scan_time
            """
        ),
        {"d": session_date},
    ).fetchall()
    out: List[datetime] = []
    for r in rows:
        out.append(_as_ist(r.scan_time))
    return out


def _top10_symbols(db, session_date: str) -> List[str]:
    rows = db.execute(
        text(
            """
            SELECT DISTINCT UPPER(symbol) AS symbol
            FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date)
              AND rank_position IS NOT NULL
              AND rank_position <= 10
            """
        ),
        {"d": session_date},
    ).fetchall()
    return [str(r.symbol).upper() for r in rows if r.symbol]


def _prefetch_candles(
    db, symbols: List[str], session_date: str, *, pace: float
) -> Dict[str, List[Dict[str, Any]]]:
    from backend.config import settings
    from backend.services.rs_conviction_candles import load_instrument_atr_maps
    from backend.services.relative_strength_scanner import (
        CANDLE_INTERVAL,
        MIN_BARS,
        _sorted_candles,
    )
    from backend.services.upstox_service import UpstoxService

    d = date.fromisoformat(session_date)
    ikeys, _ = load_instrument_atr_maps(db, set(symbols))
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    cache: Dict[str, List[Dict[str, Any]]] = {}
    for i, sym in enumerate(symbols, start=1):
        ik = ikeys.get(sym)
        if not ik:
            print(f"  skip {sym}: no instrument key", flush=True)
            continue
        try:
            raw = ux.get_historical_candles_by_instrument_key(
                ik,
                interval=CANDLE_INTERVAL,
                days_back=5,
                range_end_date=d,
            )
        except Exception as exc:
            print(f"  fetch fail {sym}: {exc}", flush=True)
            raw = None
        if pace > 0:
            time.sleep(pace)
        if raw and len(raw) >= MIN_BARS:
            cache[sym] = _sorted_candles(raw)
        if i % 25 == 0 or i == len(symbols):
            print(f"  candles {i}/{len(symbols)} cached={len(cache)}", flush=True)
    return cache


def _install_candle_override(candle_cache: Dict[str, List[Dict[str, Any]]]) -> None:
    import backend.services.daily_checklist_snapshot as snap

    def _override(db, symbol: str):
        return candle_cache.get(str(symbol or "").upper())

    snap._load_candles_for_symbol = _override  # type: ignore[assignment]


def _noop_trade_exit(*_a, **_k):
    return {"updated": 0}


def reconstruct(session_date: str, *, pace: float, dry_run: bool) -> Dict[str, Any]:
    from backend.database import SessionLocal
    from backend.services.daily_checklist_snapshot import (
        LOCK_MINUTES_IST,
        PROMOTION_CUTOFF_MIN,
        clear_snapshot_for_date,
        lock_morning_snapshot,
        promote_intraday_from_rs,
    )

    db = SessionLocal()
    summary: Dict[str, Any] = {"session_date": session_date, "ok": False}
    try:
        scans = _scan_times(db, session_date)
        morning_scans = [
            t for t in scans if (t.hour * 60 + t.minute) >= LOCK_MINUTES_IST
        ]
        if not morning_scans:
            summary["error"] = "no RS scans at/after 09:25"
            return summary
        lock_at = morning_scans[0]
        bull, bear = _top5_at_scan(db, session_date, lock_at)
        if len(bull) < 5 or len(bear) < 5:
            summary["error"] = f"incomplete Top-5 at {lock_at.isoformat()}: bull={len(bull)} bear={len(bear)}"
            summary["bull"] = bull
            summary["bear"] = bear
            return summary

        print(f"morning lock at {lock_at.isoformat()}", flush=True)
        print(f"  BULL: {[b['symbol'] for b in bull]}", flush=True)
        print(f"  BEAR: {[b['symbol'] for b in bear]}", flush=True)

        syms = _top10_symbols(db, session_date)
        print(f"prefetch candles for {len(syms)} Top-10-ever symbols…", flush=True)
        candle_cache = _prefetch_candles(db, syms, session_date, pace=pace)
        _install_candle_override(candle_cache)

        # Avoid touching live open-trade EXIT_NOW during historical remove replay.
        import backend.services.kavach_open_trades as ot

        ot.mark_open_trades_exit_on_lock_removal = _noop_trade_exit  # type: ignore[assignment]

        if dry_run:
            summary.update(
                {
                    "ok": True,
                    "dry_run": True,
                    "lock_at": lock_at.isoformat(),
                    "bull": [b["symbol"] for b in bull],
                    "bear": [b["symbol"] for b in bear],
                    "candle_symbols": len(candle_cache),
                    "promote_ticks": sum(
                        1
                        for t in scans
                        if t >= lock_at and (t.hour * 60 + t.minute) <= PROMOTION_CUTOFF_MIN
                    ),
                }
            )
            return summary

        # Replace only this session's audit + lock tables.
        db.execute(
            text("DELETE FROM rs_lock_membership_audit WHERE session_date = CAST(:d AS date)"),
            {"d": session_date},
        )
        clear_snapshot_for_date(db, session_date)
        db.commit()

        lock_morning_snapshot(
            db,
            session_date,
            bull,
            bear,
            locked_by="historical_replay",
            now=lock_at,
        )
        db.commit()

        promote_ticks = [
            t
            for t in scans
            if t >= lock_at and (t.hour * 60 + t.minute) <= PROMOTION_CUTOFF_MIN
        ]
        # Skip the morning lock scan itself for promote (state already seeded);
        # still run promotes from the *next* scan onward.
        for i, tick in enumerate(promote_ticks):
            if tick == lock_at and i == 0:
                # First tick = morning lock moment; still run promote in case
                # 2-scan already qualifies at that same stamp (rare).
                pass
            out = promote_intraday_from_rs(db, session_date, now=tick)
            db.commit()
            if out.get("promoted") or out.get("removed") or out.get("updated"):
                print(
                    f"  {tick.strftime('%H:%M:%S')} promoted={len(out.get('promoted') or [])} "
                    f"removed={len(out.get('removed') or [])} updated={len(out.get('updated') or [])}",
                    flush=True,
                )

        audit_n = db.execute(
            text(
                """
                SELECT COUNT(*) AS n,
                       COUNT(*) FILTER (WHERE event_type='entry') AS entries,
                       COUNT(*) FILTER (WHERE event_type='remove') AS removes,
                       COUNT(DISTINCT event_at) AS timestamps
                FROM rs_lock_membership_audit
                WHERE session_date = CAST(:d AS date)
                """
            ),
            {"d": session_date},
        ).fetchone()
        snap_n = db.execute(
            text("SELECT COUNT(*) FROM daily_snapshot WHERE snapshot_date = CAST(:d AS date)"),
            {"d": session_date},
        ).scalar()
        lock_row = db.execute(
            text("SELECT locked_at, locked_by FROM snapshot_lock WHERE lock_date = CAST(:d AS date)"),
            {"d": session_date},
        ).fetchone()

        summary.update(
            {
                "ok": True,
                "lock_at": lock_at.isoformat(),
                "bull": [b["symbol"] for b in bull],
                "bear": [b["symbol"] for b in bear],
                "candle_symbols": len(candle_cache),
                "promote_ticks": len(promote_ticks),
                "audit_rows": int(audit_n.n) if audit_n else 0,
                "audit_entries": int(audit_n.entries) if audit_n else 0,
                "audit_removes": int(audit_n.removes) if audit_n else 0,
                "audit_timestamps": int(audit_n.timestamps) if audit_n else 0,
                "daily_snapshot_rows": int(snap_n or 0),
                "snapshot_lock": {
                    "locked_at": lock_row.locked_at.isoformat() if lock_row else None,
                    "locked_by": lock_row.locked_by if lock_row else None,
                },
            }
        )
        return summary
    except Exception as exc:
        db.rollback()
        summary["error"] = str(exc)
        return summary
    finally:
        db.close()


def recompute_universe_in_lock(session_date: str) -> Dict[str, Any]:
    """Recompute in_lock_at_time on kavach_universe_vwap_scan from corrected audit."""
    from backend.database import SessionLocal
    from backend.services.kavach_universe_vwap_scan import (
        _lock_membership_timeline,
        _locked_at,
    )

    db = SessionLocal()
    try:
        timeline = _lock_membership_timeline(db, session_date)
        if not timeline:
            return {"ok": False, "error": "empty lock timeline after reconstruct"}
        rows = db.execute(
            text(
                """
                SELECT id, UPPER(symbol) AS symbol, logged_at, in_lock_at_time
                FROM kavach_universe_vwap_scan
                WHERE session_date = CAST(:d AS date)
                """
            ),
            {"d": session_date},
        ).fetchall()
        flipped = 0
        true_n = 0
        updates: List[Tuple[bool, int]] = []
        for r in rows:
            lat = r.logged_at
            if isinstance(lat, datetime) and lat.tzinfo is None:
                lat = IST.localize(lat)
            elif isinstance(lat, datetime):
                lat = lat.astimezone(IST)
            is_in = str(r.symbol).upper() in _locked_at(timeline, lat)
            if is_in:
                true_n += 1
            if bool(r.in_lock_at_time) != is_in:
                flipped += 1
                updates.append((is_in, int(r.id)))
        for is_in, rid in updates:
            db.execute(
                text(
                    """
                    UPDATE kavach_universe_vwap_scan
                    SET in_lock_at_time = :il
                    WHERE id = :id
                    """
                ),
                {"il": is_in, "id": rid},
            )
        db.commit()

        steep = db.execute(
            text(
                """
                SELECT
                  COUNT(*) FILTER (WHERE steep_ok) AS steep,
                  COUNT(*) FILTER (WHERE steep_ok AND NOT in_lock_at_time) AS steep_out,
                  COUNT(DISTINCT symbol) FILTER (WHERE steep_ok AND NOT in_lock_at_time) AS steep_out_syms,
                  COUNT(*) FILTER (WHERE in_lock_at_time) AS in_lock_rows
                FROM kavach_universe_vwap_scan
                WHERE session_date = CAST(:d AS date)
                """
            ),
            {"d": session_date},
        ).fetchone()
        return {
            "ok": True,
            "rows": len(rows),
            "flipped": flipped,
            "in_lock_true": true_n,
            "steep": int(steep.steep or 0),
            "steep_out": int(steep.steep_out or 0),
            "steep_out_syms": int(steep.steep_out_syms or 0),
            "in_lock_rows": int(steep.in_lock_rows or 0),
            "timeline_events": len(timeline),
        }
    except Exception as exc:
        db.rollback()
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--date", default=SESSION)
    p.add_argument("--pace", type=float, default=0.2)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-recompute", action="store_true")
    args = p.parse_args()

    print(f"=== reconstruct lock timeline {args.date} ===", flush=True)
    out = reconstruct(args.date, pace=args.pace, dry_run=args.dry_run)
    print(json.dumps(out, indent=2, default=str), flush=True)
    if not out.get("ok"):
        return 1
    if args.dry_run or args.skip_recompute:
        return 0
    print("=== recompute in_lock_at_time on universe VWAP scan ===", flush=True)
    re = recompute_universe_in_lock(args.date)
    print(json.dumps(re, indent=2, default=str), flush=True)
    return 0 if re.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
