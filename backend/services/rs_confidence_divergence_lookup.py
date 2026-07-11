"""Read-only RS–confidence divergence lookup (symbol + date).

Pulls lock / scan / audit / Fast Watch / GO Board rows. Does not write to
snapshot, lock, promotion, or trading tables.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
ENTRY_CUTOFF_MIN = 14 * 60 + 30  # 14:30 IST hard entry cutoff


def _iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    return str(v)


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _minutes_ist(ts: Any) -> Optional[int]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        t = ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
        return t.hour * 60 + t.minute
    return None


def _grade_sequence(bars: List[Dict[str, Any]]) -> List[str]:
    seq: List[str] = []
    for b in bars:
        g = (b.get("confidence_grade") or "").strip()
        if not g:
            continue
        if not seq or seq[-1] != g:
            seq.append(g)
    return seq


def _norm_side(ranking_type: Optional[str], direction: Optional[str] = None) -> Optional[str]:
    raw = (ranking_type or direction or "").strip().upper()
    if not raw:
        return None
    if raw in ("BEARISH", "BEAR", "SHORT"):
        return "BEARISH"
    if raw in ("BULLISH", "BULL", "LONG"):
        return "BULLISH"
    return raw


def _classify(
    *,
    ever_locked: bool,
    best_rank: Optional[int],
    close_miss_threshold: int,
    ever_in_top5: bool = False,
    lock_direction_0925: Optional[str] = None,
    top5_directions: Optional[List[str]] = None,
) -> Dict[str, str]:
    """Return {classification, classification_note}."""
    if ever_locked:
        return {
            "classification": "RS confirmed",
            "classification_note": "Symbol appears in daily_snapshot morning lock (Top-5 BULL or BEAR).",
        }

    sides = [_norm_side(s) for s in (top5_directions or []) if _norm_side(s)]
    sides = [s for s in sides if s]
    unique_sides = sorted(set(sides))
    lock_side = _norm_side(lock_direction_0925)

    if ever_in_top5:
        # Direction flip vs first 09:25-side (or multi-side Top-5 presence) while never promoted into lock.
        flipped = False
        if lock_side and any(s != lock_side for s in unique_sides):
            flipped = True
        elif len(unique_sides) >= 2:
            flipped = True
        if flipped:
            note = (
                "Appeared in Top-5 RS scans (in_top5=true) but never entered daily_snapshot lock. "
                f"09:25 / early side was {lock_side or 'unknown'}; later/qualifying Top-5 side(s): "
                f"{', '.join(unique_sides) or 'unknown'}. "
                "Promotion/re-lock may only evaluate within the 09:25 direction — direction flips "
                "are a separate gap from plain close-miss ranking."
            )
            return {
                "classification": "RS confirmed but not promoted / direction flip",
                "classification_note": note,
            }
        return {
            "classification": "RS confirmed but not promoted",
            "classification_note": (
                "Appeared in Top-5 RS scans but never entered daily_snapshot lock "
                "(same-side presence without morning lock membership)."
            ),
        }

    if best_rank is not None and best_rank <= close_miss_threshold:
        return {
            "classification": "RS close miss",
            "classification_note": (
                f"Never in Top-5 scans / lock, but best observed rank #{best_rank} "
                f"is within close-miss threshold ≤{close_miss_threshold}."
            ),
        }
    return {
        "classification": "RS genuine laggard",
        "classification_note": (
            "Never locked and never (or never near) Top-5 — RS and Kavach confidence "
            "are measuring different populations for this name."
        ),
    }


# Common Chartink / truncated / alternate tickers → arbitrage_master.stock
_SYMBOL_ALIASES = {
    "UNIONBNK": "UNIONBANK",
    "UNIONBANK": "UNIONBANK",
    "HPCL": "HINDPETRO",
    "HINDPETRO": "HINDPETRO",
    "LTIM": "LTIM",
    "LTIMIND": "LTIM",
    "LTIMINDTREE": "LTIM",
    "PERSISTENTSYS": "PERSISTENT",
}


def _resolve_symbol(db, symbol: str) -> Dict[str, Any]:
    """Normalize symbol to arbitrage_master / RS table form; suggest neighbors on miss."""
    raw = (symbol or "").strip().upper()
    if not raw:
        return {"input": raw, "resolved": None, "matched_via": None, "suggestions": []}

    candidates = []
    if raw in _SYMBOL_ALIASES:
        candidates.append(_SYMBOL_ALIASES[raw])
    candidates.append(raw)
    # Strip common suffixes people type
    for suf in ("-EQ", ".NS", ".NSE", "EQ"):
        if raw.endswith(suf) and len(raw) > len(suf):
            candidates.append(raw[: -len(suf)])
    candidates = list(dict.fromkeys(candidates))

    for cand in candidates:
        hit = db.execute(
            text("SELECT stock FROM arbitrage_master WHERE UPPER(stock) = :s LIMIT 1"),
            {"s": cand},
        ).fetchone()
        if hit:
            return {
                "input": raw,
                "resolved": str(hit.stock).upper(),
                "matched_via": "exact" if cand == raw else "alias",
                "suggestions": [],
            }
        # Also accept symbols that appear in RS scans even if not in master
        try:
            rs_hit = db.execute(
                text(
                    """
                    SELECT UPPER(symbol) AS symbol
                    FROM relative_strength_snapshot
                    WHERE UPPER(symbol) = :s
                    LIMIT 1
                    """
                ),
                {"s": cand},
            ).fetchone()
        except Exception:
            db.rollback()
            rs_hit = None
        if rs_hit:
            return {
                "input": raw,
                "resolved": str(rs_hit.symbol).upper(),
                "matched_via": "exact" if cand == raw else "alias",
                "suggestions": [],
            }

    # Fuzzy: prefix / contains against master + recent RS symbols
    suggestions: List[str] = []
    try:
        rows = db.execute(
            text(
                """
                SELECT stock FROM arbitrage_master
                WHERE UPPER(stock) LIKE :p OR UPPER(stock) LIKE :c
                ORDER BY
                  CASE WHEN UPPER(stock) LIKE :p THEN 0 ELSE 1 END,
                  LENGTH(stock), stock
                LIMIT 8
                """
            ),
            {"p": raw[:4] + "%", "c": "%" + raw + "%"},
        ).fetchall()
        suggestions.extend(str(r.stock).upper() for r in rows)
    except Exception:
        db.rollback()

    try:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT UPPER(symbol) AS symbol
                FROM relative_strength_snapshot
                WHERE UPPER(symbol) LIKE :p OR UPPER(symbol) LIKE :c
                ORDER BY symbol
                LIMIT 8
                """
            ),
            {"p": raw[:4] + "%", "c": "%" + raw + "%"},
        ).fetchall()
        suggestions.extend(str(r.symbol).upper() for r in rows)
    except Exception:
        db.rollback()

    # de-dupe preserve order
    seen = set()
    uniq = []
    for s in suggestions:
        if s and s not in seen:
            seen.add(s)
            uniq.append(s)

    # If exactly one strong alias-like suggestion (starts with same stem), auto-resolve
    stem = raw[:6]
    strong = [s for s in uniq if s.startswith(stem) or stem.startswith(s[:4])]
    if len(strong) == 1:
        return {
            "input": raw,
            "resolved": strong[0],
            "matched_via": "fuzzy_unique",
            "suggestions": uniq,
        }

    return {
        "input": raw,
        "resolved": None,
        "matched_via": None,
        "suggestions": uniq[:8],
    }


def lookup_symbol_day(
    symbol: str,
    session_date: str,
    *,
    close_miss_threshold: int = 8,
    include_upstox: bool = False,
) -> Dict[str, Any]:
    """Return auto-filled divergence report for one symbol/date (read-only)."""
    sym = (symbol or "").strip().upper()
    sd = (session_date or "").strip()[:10]
    thr = max(1, int(close_miss_threshold or 8))
    if not sym or not sd:
        return {"ok": False, "error": "symbol and date are required"}

    db = SessionLocal()
    try:
        resolution = _resolve_symbol(db, sym)
        input_symbol = sym
        if resolution.get("resolved"):
            sym = resolution["resolved"]

        lock_meta = db.execute(
            text("SELECT locked_at, locked_by FROM snapshot_lock WHERE lock_date = CAST(:d AS date)"),
            {"d": sd},
        ).fetchone()

        lock_row = db.execute(
            text(
                """
                SELECT symbol, direction, rank, rs_score, locked_at
                FROM daily_snapshot
                WHERE snapshot_date = CAST(:d AS date) AND UPPER(symbol) = :sym
                ORDER BY rank
                LIMIT 1
                """
            ),
            {"d": sd, "sym": sym},
        ).fetchone()

        lock = {
            "day_locked": lock_meta is not None,
            "day_locked_at": _iso(lock_meta.locked_at) if lock_meta else None,
            "day_locked_by": lock_meta.locked_by if lock_meta else None,
            "symbol_in_lock": lock_row is not None,
            "direction": lock_row.direction if lock_row else None,
            "rank": int(lock_row.rank) if lock_row and lock_row.rank is not None else None,
            "rs_score": _f(lock_row.rs_score) if lock_row else None,
            "locked_at": _iso(lock_row.locked_at) if lock_row else None,
        }

        # Morning lock as first checkpoint when present
        rs_checkpoints: List[Dict[str, Any]] = []
        if lock_row is not None:
            rs_checkpoints.append(
                {
                    "checkpoint": "09:25 lock",
                    "scan_time": _iso(lock_row.locked_at) or f"{sd}T09:25:00+05:30",
                    "source": "daily_snapshot",
                    "ranking_type": "BEARISH" if (lock_row.direction or "").upper() == "BEAR" else "BULLISH",
                    "rank": int(lock_row.rank) if lock_row.rank is not None else None,
                    "rs_pct": _f(lock_row.rs_score),
                    "in_top5": True,
                    "locked": True,
                }
            )

        scan_rows = db.execute(
            text(
                """
                SELECT scan_time, ranking_type, rank_position, relative_strength,
                       trade_score, confidence_grade, kavach_state, volume_label,
                       market_regime, adx, current_price
                FROM relative_strength_snapshot
                WHERE scan_time::date = CAST(:d AS date)
                  AND UPPER(symbol) = :sym
                ORDER BY scan_time
                """
            ),
            {"d": sd, "sym": sym},
        ).fetchall()

        for r in scan_rows:
            mins = _minutes_ist(r.scan_time)
            if mins is not None and mins > ENTRY_CUTOFF_MIN:
                continue
            label = "scan"
            if r.scan_time is not None:
                t = r.scan_time.astimezone(IST) if r.scan_time.tzinfo else IST.localize(r.scan_time)
                label = t.strftime("%H:%M") + " refresh"
            rs_checkpoints.append(
                {
                    "checkpoint": label,
                    "scan_time": _iso(r.scan_time),
                    "source": "relative_strength_snapshot",
                    "ranking_type": r.ranking_type,
                    "rank": int(r.rank_position) if r.rank_position is not None else None,
                    "rs_pct": _f(r.relative_strength),
                    "in_top5": (
                        r.rank_position is not None and int(r.rank_position) <= 5
                    ),
                    "locked": False,
                    "confidence_grade": r.confidence_grade,
                    "trade_score": _f(r.trade_score),
                    "kavach_state": r.kavach_state,
                    "volume_label": r.volume_label,
                    "market_regime": r.market_regime,
                    "adx": _f(r.adx),
                    "price": _f(r.current_price),
                }
            )

        # Anchor archives (fixed labels) — fill gaps when snapshot row missing at those times
        try:
            anchors = db.execute(
                text(
                    """
                    SELECT capture_label, capture_time, rank_position, direction,
                           relative_strength, confidence_grade, trade_score
                    FROM rs_anchor_snapshot
                    WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :sym
                    ORDER BY capture_time
                    """
                ),
                {"d": sd, "sym": sym},
            ).fetchall()
        except Exception:
            db.rollback()
            anchors = []

        seen_labels = {c["checkpoint"] for c in rs_checkpoints}
        for a in anchors:
            label = f"{a.capture_label} anchor"
            if label in seen_labels:
                continue
            mins = _minutes_ist(a.capture_time)
            if mins is not None and mins > ENTRY_CUTOFF_MIN:
                continue
            rs_checkpoints.append(
                {
                    "checkpoint": label,
                    "scan_time": _iso(a.capture_time),
                    "source": "rs_anchor_snapshot",
                    "ranking_type": "BEARISH" if (a.direction or "").upper() in ("BEAR", "BEARISH", "SHORT") else "BULLISH",
                    "rank": int(a.rank_position) if a.rank_position is not None else None,
                    "rs_pct": _f(a.relative_strength),
                    "in_top5": (
                        a.rank_position is not None and int(a.rank_position) <= 5
                    ),
                    "locked": a.capture_label == "09:25",
                    "confidence_grade": a.confidence_grade,
                    "trade_score": _f(a.trade_score),
                }
            )

        rs_checkpoints.sort(key=lambda c: c.get("scan_time") or "")

        audit_rows = db.execute(
            text(
                """
                SELECT bar_evaluated_at, lock_direction, kavach_state, prev_kavach_state,
                       trade_score, confidence_grade, volume_label, vwap_purity_pct,
                       market_regime, adx, ema5, ema10, vwap, price, timeframe
                FROM rs_live_kavach_audit
                WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :sym
                ORDER BY bar_evaluated_at, id
                """
            ),
            {"d": sd, "sym": sym},
        ).fetchall()

        confidence_bars = [
            {
                "bar_evaluated_at": _iso(r.bar_evaluated_at),
                "time_label": (
                    r.bar_evaluated_at.astimezone(IST).strftime("%H:%M")
                    if isinstance(r.bar_evaluated_at, datetime)
                    else None
                ),
                "lock_direction": r.lock_direction,
                "kavach_state": r.kavach_state,
                "prev_kavach_state": r.prev_kavach_state,
                "trade_score": _f(r.trade_score),
                "confidence_grade": r.confidence_grade,
                "volume_label": r.volume_label,
                "vwap_purity_pct": _f(r.vwap_purity_pct),
                "market_regime": r.market_regime,
                "adx": _f(r.adx),
                "ema5": _f(r.ema5),
                "ema10": _f(r.ema10),
                "vwap": _f(r.vwap),
                "price": _f(r.price),
                "timeframe": r.timeframe or "10m",
                "source": "rs_live_kavach_audit",
            }
            for r in audit_rows
        ]

        # If audit empty but symbol appeared in RS scans, surface scan confidence as secondary
        confidence_from_scans: List[Dict[str, Any]] = []
        if not confidence_bars:
            for c in rs_checkpoints:
                if c.get("source") != "relative_strength_snapshot":
                    continue
                if not c.get("confidence_grade") and c.get("trade_score") is None:
                    continue
                confidence_from_scans.append(
                    {
                        "bar_evaluated_at": c.get("scan_time"),
                        "time_label": (c.get("checkpoint") or "").replace(" refresh", ""),
                        "confidence_grade": c.get("confidence_grade"),
                        "trade_score": c.get("trade_score"),
                        "market_regime": c.get("market_regime"),
                        "volume_label": c.get("volume_label"),
                        "adx": c.get("adx"),
                        "kavach_state": c.get("kavach_state"),
                        "price": c.get("price"),
                        "source": "relative_strength_snapshot",
                        "note": "From RS top-5 scan row — not 10m live audit",
                    }
                )

        fw_rows = db.execute(
            text(
                """
                SELECT direction, first_flip_at, kavach_state, prev_kavach_state,
                       trade_score, confidence_grade, is_reversal, lock_direction, flip_price
                FROM rs_fast_watch
                WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :sym
                ORDER BY first_flip_at
                """
            ),
            {"d": sd, "sym": sym},
        ).fetchall()
        fast_watch = {
            "present": len(fw_rows) > 0,
            "rows": [
                {
                    "direction": r.direction,
                    "first_flip_at": _iso(r.first_flip_at),
                    "kavach_state": r.kavach_state,
                    "prev_kavach_state": r.prev_kavach_state,
                    "trade_score": _f(r.trade_score),
                    "confidence_grade": r.confidence_grade,
                    "is_reversal": bool(r.is_reversal),
                    "lock_direction": r.lock_direction,
                    "flip_price": _f(r.flip_price),
                }
                for r in fw_rows
            ],
        }

        go_rows = []
        try:
            go_rows = db.execute(
                text(
                    """
                    SELECT evaluated_at, side, outcome, filter_reason, window_label,
                           confidence_grade, kavach_state, is_reversal, price, adx
                    FROM rs_go_board_shadow_log
                    WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :sym
                    ORDER BY evaluated_at
                    """
                ),
                {"d": sd, "sym": sym},
            ).fetchall()
        except Exception:
            db.rollback()
            go_rows = []

        go_shown = [r for r in go_rows if (r.outcome or "").lower() == "shown"]
        go_board = {
            "present": len(go_shown) > 0,
            "evaluated": len(go_rows) > 0,
            "note": (
                None
                if go_rows
                else "No rs_go_board_shadow_log rows (live GO Board may not persist shadow logs)"
            ),
            "rows": [
                {
                    "evaluated_at": _iso(r.evaluated_at),
                    "side": r.side,
                    "outcome": r.outcome,
                    "filter_reason": r.filter_reason,
                    "window_label": r.window_label,
                    "confidence_grade": r.confidence_grade,
                    "kavach_state": r.kavach_state,
                    "is_reversal": bool(r.is_reversal),
                    "price": _f(r.price),
                    "adx": _f(r.adx),
                }
                for r in go_rows
            ],
        }

        checklist = None
        try:
            cl = db.execute(
                text(
                    """
                    SELECT confidence, dashboard_score, dashboard_kavach, decision, section,
                           gate_score, indicator_as_of, live_rs_direction
                    FROM daily_checklist
                    WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :sym
                    LIMIT 1
                    """
                ),
                {"d": sd, "sym": sym},
            ).fetchone()
            if cl:
                checklist = {
                    "confidence": cl.confidence,
                    "dashboard_score": _f(cl.dashboard_score),
                    "dashboard_kavach": cl.dashboard_kavach,
                    "decision": cl.decision,
                    "section": cl.section,
                    "gate_score": cl.gate_score,
                    "indicator_as_of": _iso(cl.indicator_as_of),
                    "live_rs_direction": cl.live_rs_direction,
                }
        except Exception:
            db.rollback()
            checklist = None

        # Optional linked trades (Daily Futures user trades)
        trades: List[Dict[str, Any]] = []
        try:
            trows = db.execute(
                text(
                    """
                    SELECT t.entry_time, t.entry_price, t.exit_time, t.exit_price,
                           t.pnl_rupees, t.pnl_points, t.direction
                    FROM daily_futures_user_trade t
                    JOIN daily_futures_screening s ON s.id = t.screening_id
                    WHERE s.trade_date = CAST(:d AS date)
                      AND UPPER(t.underlying) = :sym
                    ORDER BY t.entry_time
                    """
                ),
                {"d": sd, "sym": sym},
            ).fetchall()
            trades = [
                {
                    "source": "daily_futures_user_trade",
                    "direction": r.direction,
                    "entry_time": _iso(r.entry_time),
                    "entry_price": _f(r.entry_price),
                    "exit_time": _iso(r.exit_time),
                    "exit_price": _f(r.exit_price),
                    "pnl_rupees": _f(r.pnl_rupees),
                    "pnl_points": _f(r.pnl_points),
                }
                for r in trows
            ]
        except Exception:
            db.rollback()
            trades = []

        ranks = [c["rank"] for c in rs_checkpoints if isinstance(c.get("rank"), int)]
        best_rank = min(ranks) if ranks else None
        ever_locked = bool(lock["symbol_in_lock"])
        ever_in_top5 = any(bool(c.get("in_top5")) for c in rs_checkpoints)
        top5_directions = [
            c.get("ranking_type")
            for c in rs_checkpoints
            if c.get("in_top5") and c.get("ranking_type")
        ]

        # 09:25 side: daily_snapshot direction, else earliest / ~09:25 checkpoint side
        lock_direction_0925: Optional[str] = None
        if lock_row is not None:
            lock_direction_0925 = (
                "BEARISH" if (lock_row.direction or "").upper() in ("BEAR", "BEARISH", "SHORT") else "BULLISH"
            )
        else:
            for c in rs_checkpoints:
                label = str(c.get("checkpoint") or "")
                if "09:25" in label or label.startswith("09:2"):
                    lock_direction_0925 = c.get("ranking_type")
                    break
            if not lock_direction_0925 and rs_checkpoints:
                lock_direction_0925 = rs_checkpoints[0].get("ranking_type")

        classified = _classify(
            ever_locked=ever_locked,
            best_rank=best_rank,
            close_miss_threshold=thr,
            ever_in_top5=ever_in_top5,
            lock_direction_0925=lock_direction_0925,
            top5_directions=top5_directions,
        )

        bars_for_prog = confidence_bars if confidence_bars else confidence_from_scans
        progression = _grade_sequence(bars_for_prog)

        has_any = bool(
            lock_row
            or scan_rows
            or audit_rows
            or fw_rows
            or go_rows
            or checklist
            or anchors
        )

        upstox_fallback = None
        if include_upstox or not has_any:
            upstox_fallback = _upstox_ohlc_fallback(db, sym, sd)

        empty_msg = None
        if not has_any:
            sug = resolution.get("suggestions") or []
            if sug:
                empty_msg = (
                    f"No Kavach data found for '{input_symbol}' on {sd}. "
                    f"Did you mean: {', '.join(sug)}?"
                )
            elif resolution.get("resolved") and input_symbol != sym:
                empty_msg = (
                    f"No Kavach data found for resolved symbol {sym} "
                    f"(from input '{input_symbol}') on {sd} — not tracked in RS scan "
                    "or confidence audit this session"
                )
            else:
                empty_msg = (
                    "No Kavach data found for this symbol/date — not tracked in RS scan "
                    "or confidence audit this session"
                )

        return {
            "ok": True,
            "symbol": sym,
            "input_symbol": input_symbol,
            "symbol_resolution": {
                "input": input_symbol,
                "resolved": sym if has_any or resolution.get("resolved") else resolution.get("resolved"),
                "matched_via": resolution.get("matched_via"),
                "suggestions": resolution.get("suggestions") or [],
            },
            "date": sd,
            "close_miss_threshold": thr,
            "data_found": has_any,
            "message": empty_msg,
            "lock": lock,
            "lock_direction_0925": lock_direction_0925,
            "ever_in_top5": ever_in_top5,
            "rs_checkpoints": rs_checkpoints,
            "confidence_bars": confidence_bars,
            "confidence_from_scans": confidence_from_scans,
            "confidence_progression": progression,
            "confidence_progression_label": " → ".join(progression) if progression else "—",
            "best_rs_rank": best_rank,
            "best_rs_rank_label": f"#{best_rank}" if best_rank is not None else "not visible",
            "classification": classified["classification"],
            "classification_note": classified["classification_note"],
            "fast_watch": fast_watch,
            "go_board": go_board,
            "checklist": checklist,
            "trades_system": trades,
            "upstox_fallback": upstox_fallback,
            "notes": {
                "rs_scan_scope": "relative_strength_snapshot stores Top-5 bull+bear only; absence = not in Top-5 that scan",
                "audit_scope": "rs_live_kavach_audit is written for locked checklist symbols during live 10m recompute",
                "entry_cutoff": "RS checkpoints after 14:30 IST are excluded",
                "promotion_note": (
                    "If classification is 'RS confirmed but not promoted / direction flip', "
                    "Top-5 presence flipped vs 09:25 side but daily_snapshot never gained the symbol — "
                    "likely a promotion/re-lock gap for opposite-direction candidates."
                ),
            },
        }
    finally:
        db.close()


def _upstox_ohlc_fallback(db, symbol: str, session_date: str) -> Dict[str, Any]:
    """Pull 5m OHLC for the day from Upstox — clearly labeled reconstructed."""
    try:
        from backend.config import settings
        from backend.services.relative_strength_scanner import _sorted_candles
        from backend.services.upstox_service import UpstoxService

        row = db.execute(
            text(
                """
                SELECT currmth_future_instrument_key
                FROM arbitrage_master
                WHERE UPPER(stock) = :sym
                  AND currmth_future_instrument_key IS NOT NULL
                LIMIT 1
                """
            ),
            {"sym": symbol},
        ).fetchone()
        if not row or not row.currmth_future_instrument_key:
            return {
                "available": False,
                "label": "reconstructed from Upstox, not original Kavach output",
                "error": "No futures instrument_key in arbitrage_master",
            }
        ikey = str(row.currmth_future_instrument_key).strip()
        end_d = datetime.strptime(session_date, "%Y-%m-%d").date()
        ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        ux.reload_token_from_storage()
        raw = ux.get_historical_candles_by_instrument_key(
            ikey,
            interval="minutes/5",
            days_back=3,
            range_end_date=end_d,
        )
        candles = _sorted_candles(raw or [])
        day_bars = []
        for c in candles:
            ts = str(c.get("timestamp") or "")
            if ts[:10] != session_date:
                continue
            day_bars.append(
                {
                    "timestamp": ts,
                    "open": _f(c.get("open")),
                    "high": _f(c.get("high")),
                    "low": _f(c.get("low")),
                    "close": _f(c.get("close")),
                    "volume": _f(c.get("volume")),
                }
            )
        if not day_bars:
            return {
                "available": False,
                "label": "reconstructed from Upstox, not original Kavach output",
                "instrument_key": ikey,
                "error": "No 5m candles for that IST date",
            }
        opens = day_bars[0]["open"]
        closes = day_bars[-1]["close"]
        highs = [b["high"] for b in day_bars if b["high"] is not None]
        lows = [b["low"] for b in day_bars if b["low"] is not None]
        vol = sum(b["volume"] or 0 for b in day_bars)
        move_pct = None
        if opens and closes and opens > 0:
            move_pct = round((closes - opens) / opens * 100.0, 3)
        return {
            "available": True,
            "label": "reconstructed from Upstox, not original Kavach output",
            "instrument_key": ikey,
            "interval": "minutes/5",
            "bar_count": len(day_bars),
            "session_ohlc": {
                "open": opens,
                "high": max(highs) if highs else None,
                "low": min(lows) if lows else None,
                "close": closes,
                "volume": vol,
                "move_pct": move_pct,
            },
            "bars": day_bars,
        }
    except Exception as exc:
        logger.debug("upstox fallback failed for %s %s: %s", symbol, session_date, exc)
        return {
            "available": False,
            "label": "reconstructed from Upstox, not original Kavach output",
            "error": str(exc),
        }
