"""Kavach Open Trades — checklist Take Trade / manage / EXIT NOW alarm.

Single table ``kavach_checklist_trades`` (OPEN / CLOSED) preserves id for audit
continuity. Edits go to ``kavach_checklist_trade_edits``.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.daily_checklist_trade_state import MAX_INR_RISK, RR_LOW, _f, _lot_for_symbol

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

STATE_TRAILING = "TRAILING"
STATE_PROFIT_LOCKED = "PROFIT_LOCKED"
STATE_PLAN_EXIT = "PLAN_EXIT"
STATE_EXIT_NOW = "EXIT_NOW"

# R1 signal_path values persisted on kavach_r1_early_warning_log
R1_PATH_PLAN_EXIT = "plan_exit"
R1_PATH_EXIT_NOW_BETWEEN = "exit_now_ema10_between_price_vwap"
R1_PATH_EXIT_NOW_CROSSED = "exit_now_ema10_already_crossed"
R1_PATH_PLAN_THEN_EMA10 = "plan_exit_then_exit_now"

STATUS_OPEN = "OPEN"
STATUS_CLOSED = "CLOSED"

EXIT_REASONS = (
    "EMA10 reverse close (rule)",
    "EMA5 reverse close (profit protection)",
    "Risk cap exceeded",
    "Lock removed via R1",
    "Lock removed via R2",
    "Discretionary early exit",
    "15:15 square-off",
    "Session loss cap hit",
)


def format_lock_removal_exit_reason(rule: str, removed_at: Optional[datetime] = None) -> str:
    """Panel EXIT NOW label when lock membership is revoked via R1/R2."""
    tag = (rule or "").strip().upper()
    if tag not in ("R1", "R2"):
        tag = "R2"
    when = removed_at or _now()
    if when.tzinfo is None:
        when = IST.localize(when)
    else:
        when = when.astimezone(IST)
    return f"Lock removed via {tag} at {when.strftime('%H:%M')} — setup no longer qualified"


def format_r1_plan_exit_reason(removed_at: Optional[datetime] = None) -> str:
    """Informational PLAN EXIT reason (R1 VWAP early warning — not an exit trigger)."""
    when = removed_at or _now()
    if when.tzinfo is None:
        when = IST.localize(when)
    else:
        when = when.astimezone(IST)
    return (
        f"PLAN EXIT — R1 VWAP confirmed close against position at "
        f"{when.strftime('%H:%M')} (EMA10 not yet crossed)"
    )


def lock_removal_structure_label(
    rule: str,
    *,
    price_closed_beyond_ema10: bool,
    plan_exit: bool = False,
) -> str:
    """Trader-facing distinction: rank drop vs structural EMA10 / VWAP break."""
    tag = (rule or "").strip().upper()
    if tag == "R1":
        if plan_exit and not price_closed_beyond_ema10:
            return (
                "PLAN EXIT — R1 VWAP confirmed close against position "
                "(EMA10 not yet crossed)."
            )
        if price_closed_beyond_ema10:
            return "R1 structure removal — price closed beyond EMA10"
        return (
            "R1 — VWAP confirmed close against position (EMA10 not yet crossed)."
        )
    if price_closed_beyond_ema10:
        return "R2 rank-based removal — price HAS closed beyond EMA10"
    return "R2 rank-based removal — price has NOT closed beyond EMA10"


def classify_r1_signal(
    *,
    close: Optional[float],
    ema10: Optional[float],
    vwap: Optional[float],
    is_long: bool,
) -> Dict[str, Any]:
    """Decide PLAN EXIT vs EXIT NOW for R1 VWAP lock-removal.

    - VWAP adverse + EMA10 not breached → PLAN EXIT (informational).
    - EMA10 already breached, or EMA10 sits between price and VWAP (so a VWAP
      adverse breach implies EMA10 was crossed too) → EXIT NOW directly.
    """
    beyond_ema10 = False
    beyond_vwap = False
    ema10_between = False
    if close is not None and ema10 is not None:
        beyond_ema10 = (close < ema10) if is_long else (close > ema10)
    if close is not None and vwap is not None:
        beyond_vwap = (close < vwap) if is_long else (close > vwap)
    if close is not None and ema10 is not None and vwap is not None:
        lo = min(close, vwap)
        hi = max(close, vwap)
        ema10_between = lo < ema10 < hi

    # Between-price-and-VWAP with adverse VWAP ⇒ EMA10 already on the wrong side
    if beyond_ema10 or (beyond_vwap and ema10_between):
        path = (
            R1_PATH_EXIT_NOW_BETWEEN
            if ema10_between
            else R1_PATH_EXIT_NOW_CROSSED
        )
        return {
            "path": path,
            "state": STATE_EXIT_NOW,
            "plan_exit": False,
            "price_closed_beyond_ema10": True if beyond_ema10 else bool(ema10_between),
            "ema10_between_price_vwap": ema10_between,
            "price_closed_beyond_vwap": beyond_vwap,
        }
    return {
        "path": R1_PATH_PLAN_EXIT,
        "state": STATE_PLAN_EXIT,
        "plan_exit": True,
        "price_closed_beyond_ema10": False,
        "ema10_between_price_vwap": ema10_between,
        "price_closed_beyond_vwap": beyond_vwap,
    }


def _r1_detail_bits(ctx: Dict[str, Any]) -> str:
    bits = []
    if ctx.get("vwap_close_hm"):
        bits.append(f"VWAP@{ctx['vwap_close_hm']}")
    if ctx.get("ema10_distance_pts") is not None:
        bits.append(f"ΔEMA10 {ctx['ema10_distance_pts']}")
    if ctx.get("pnl_at_flag_inr") is not None:
        bits.append(f"P&L ₹{int(ctx['pnl_at_flag_inr'])}")
    return " · ".join(bits)


def _side_from_trade_direction(direction: str) -> str:
    return "BEAR" if (direction or "LONG").upper() == "SHORT" else "BULL"


def _ranking_type_for_side(side: str) -> str:
    return "BEARISH" if (side or "").upper() == "BEAR" else "BULLISH"


def fetch_last_scan_ranks(
    db,
    session_date: str,
    symbol: str,
    *,
    direction: str,
    n: int = 3,
) -> List[Dict[str, Any]]:
    """Last N RS scan ranks for symbol on trade side (newest first)."""
    side = _side_from_trade_direction(direction)
    rt = _ranking_type_for_side(side)
    rows = db.execute(
        text(
            """
            SELECT scan_time, rank_position, relative_strength
            FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date)
              AND UPPER(symbol) = :s
              AND UPPER(ranking_type) = :rt
            ORDER BY scan_time DESC
            LIMIT :n
            """
        ),
        {"d": session_date, "s": (symbol or "").upper(), "rt": rt, "n": n},
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        st = r.scan_time
        hm = None
        if st is not None:
            t = st.astimezone(IST) if getattr(st, "tzinfo", None) else IST.localize(st)
            hm = t.strftime("%H:%M")
        out.append(
            {
                "scan_hm": hm,
                "rank": int(r.rank_position) if r.rank_position is not None else None,
                "rs": _f(r.relative_strength),
            }
        )
    return out


def build_lock_removal_context(
    db,
    session_date: str,
    symbol: str,
    rule: str,
    *,
    direction: str,
    entry_rank: Optional[int] = None,
    removed_at: Optional[datetime] = None,
    trade: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Rank history + price-vs-structure status for EXIT NOW / PLAN EXIT transparency."""
    bar = _confirmed_10m_levels(db, symbol)
    levels = _levels_for_symbol(db, symbol, session_date)
    close = _f(bar.get("close"))
    ema10 = _f(bar.get("ema10")) or _f(levels.get("ema10"))
    vwap = _f(bar.get("vwap")) or _f(levels.get("vwap"))
    live = _live_price(db, symbol)
    if live is None:
        live = close or _f(levels.get("price"))
    is_long = (direction or "LONG").upper() != "SHORT"
    beyond_ema10 = False
    beyond_vwap = False
    if close is not None and ema10 is not None:
        beyond_ema10 = (close < ema10) if is_long else (close > ema10)
    if close is not None and vwap is not None:
        beyond_vwap = (close < vwap) if is_long else (close > vwap)
    ranks = fetch_last_scan_ranks(db, session_date, symbol, direction=direction, n=3)
    removal_rank = ranks[0]["rank"] if ranks else None
    rank_strs = []
    for item in reversed(ranks):  # oldest → newest for arrow trail
        rank_strs.append("out" if item["rank"] is None else str(item["rank"]))
    rank_trail = "→".join(rank_strs) if rank_strs else "—"

    r1_decision: Optional[Dict[str, Any]] = None
    plan_exit = False
    if (rule or "").strip().upper() == "R1":
        r1_decision = classify_r1_signal(
            close=close, ema10=ema10, vwap=vwap, is_long=is_long
        )
        plan_exit = bool(r1_decision.get("plan_exit"))
        # Prefer classifier's beyond flags (between-case may force EXIT NOW)
        beyond_ema10 = bool(r1_decision.get("price_closed_beyond_ema10"))
        beyond_vwap = bool(r1_decision.get("price_closed_beyond_vwap", beyond_vwap))

    label = lock_removal_structure_label(
        rule, price_closed_beyond_ema10=beyond_ema10, plan_exit=plan_exit
    )

    ema10_dist_pts = None
    if live is not None and ema10 is not None:
        ema10_dist_pts = round(abs(live - ema10), 2)
    pnl_at_flag = None
    if trade:
        entry = _f(trade.get("entry_price"))
        qty = int(trade.get("entry_qty") or 0)
        if live is not None and entry is not None and qty:
            pts = (live - entry) if is_long else (entry - live)
            pnl_at_flag = round(pts * qty, 0)

    when = removed_at or _now()
    if isinstance(when, datetime):
        if when.tzinfo is None:
            when = IST.localize(when)
        else:
            when = when.astimezone(IST)
        vwap_close_hm = when.strftime("%H:%M")
    else:
        vwap_close_hm = _bar_hm(bar.get("bar_at"))

    out: Dict[str, Any] = {
        "rule": (rule or "").strip().upper(),
        "direction": (direction or "LONG").upper(),
        "label": label,
        "last_3_ranks": ranks,
        "rank_trail": rank_trail,
        "entry_rank": entry_rank,
        "removal_rank": removal_rank,
        "price_closed_beyond_ema10": beyond_ema10,
        "price_closed_beyond_vwap": beyond_vwap,
        "confirmed_close": close,
        "ema10": ema10,
        "vwap": vwap,
        "live_price": live,
        "ema10_distance_pts": ema10_dist_pts,
        "pnl_at_flag_inr": pnl_at_flag,
        "vwap_close_hm": vwap_close_hm,
        "removed_at": when.isoformat() if isinstance(when, datetime) else str(when),
    }
    if r1_decision:
        out["signal_path"] = r1_decision.get("path")
        out["plan_exit"] = plan_exit
        out["target_state"] = r1_decision.get("state")
        out["ema10_between_price_vwap"] = r1_decision.get("ema10_between_price_vwap")
    return out


def log_r1_early_warning(
    db,
    *,
    trade_id: str,
    symbol: str,
    session_date: str,
    direction: str,
    context: Dict[str, Any],
    exit_trigger_reason: str,
    removed_at: Optional[datetime] = None,
    signal_path: Optional[str] = None,
) -> None:
    """Persist R1 VWAP events (PLAN EXIT or EXIT NOW) for 22-Jul review."""
    ensure_tables()
    if (context.get("rule") or "").upper() != "R1":
        return
    path = signal_path or context.get("signal_path")
    try:
        db.execute(
            text(
                """
                INSERT INTO kavach_r1_early_warning_log (
                    trade_id, symbol, session_date, direction,
                    vwap_close_price, vwap_close_hm,
                    ema10, ema10_distance_pts, live_price,
                    pnl_at_flag_inr, price_closed_beyond_ema10,
                    exit_trigger_reason, label, removed_at, signal_path
                ) VALUES (
                    :tid, :sym, CAST(:d AS date), :dir,
                    :vwap, :hm,
                    :ema10, :dist, :live,
                    :pnl, :beyond,
                    :reason, :label, :rat, :path
                )
                """
            ),
            {
                "tid": trade_id,
                "sym": (symbol or "").upper(),
                "d": session_date,
                "dir": (direction or "LONG").upper(),
                "vwap": context.get("vwap") or context.get("confirmed_close"),
                "hm": context.get("vwap_close_hm"),
                "ema10": context.get("ema10"),
                "dist": context.get("ema10_distance_pts"),
                "live": context.get("live_price"),
                "pnl": context.get("pnl_at_flag_inr"),
                "beyond": bool(context.get("price_closed_beyond_ema10")),
                "reason": exit_trigger_reason,
                "label": context.get("label"),
                "rat": removed_at or _now(),
                "path": path,
            },
        )
    except Exception as exc:
        logger.warning("r1_early_warning_log insert failed %s: %s", symbol, exc)


def log_r2_exit_now(
    db,
    *,
    trade_id: str,
    symbol: str,
    session_date: str,
    direction: str,
    context: Dict[str, Any],
    exit_trigger_reason: str,
    removed_at: Optional[datetime] = None,
) -> None:
    """Persist R2 EXIT NOW events for the 22-Jul Top-10 band review."""
    ensure_tables()
    if (context.get("rule") or "").upper() != "R2":
        return
    try:
        db.execute(
            text(
                """
                INSERT INTO kavach_r2_exit_now_log (
                    trade_id, symbol, session_date, direction,
                    entry_rank, removal_rank, last_3_ranks,
                    price_closed_beyond_ema10, price_closed_beyond_vwap,
                    confirmed_close, ema10, vwap,
                    exit_trigger_reason, label, removed_at
                ) VALUES (
                    :tid, :sym, CAST(:d AS date), :dir,
                    :er, :rr, CAST(:ranks AS jsonb),
                    :pe, :pv,
                    :close, :ema10, :vwap,
                    :reason, :label, :rat
                )
                """
            ),
            {
                "tid": trade_id,
                "sym": (symbol or "").upper(),
                "d": session_date,
                "dir": (direction or "LONG").upper(),
                "er": context.get("entry_rank"),
                "rr": context.get("removal_rank"),
                "ranks": json.dumps(context.get("last_3_ranks") or []),
                "pe": bool(context.get("price_closed_beyond_ema10")),
                "pv": bool(context.get("price_closed_beyond_vwap")),
                "close": context.get("confirmed_close"),
                "ema10": context.get("ema10"),
                "vwap": context.get("vwap"),
                "reason": exit_trigger_reason,
                "label": context.get("label"),
                "rat": removed_at or _now(),
            },
        )
    except Exception as exc:
        logger.warning("r2_exit_now_log insert failed %s: %s", symbol, exc)


def _entry_rank_from_trade(t: Dict[str, Any]) -> Optional[int]:
    prov = t.get("provenance")
    if not isinstance(prov, dict):
        snap = t.get("state_context_snapshot") or {}
        if isinstance(snap, dict):
            prov = snap.get("provenance") or {}
    if not isinstance(prov, dict):
        return None
    r = prov.get("morning_lock_rank")
    try:
        return int(r) if r is not None else None
    except (TypeError, ValueError):
        return None


def canonical_exit_reason(raw: str) -> str:
    """Map state-machine / UI trigger strings onto EXIT_REASONS."""
    if raw in EXIT_REASONS:
        return raw
    mapped = {
        "EMA10 reverse close": "EMA10 reverse close (rule)",
        "EMA5 reverse close after profit protection": "EMA5 reverse close (profit protection)",
        "Risk cap exceeded before 1:2": "Risk cap exceeded",
    }
    if raw in mapped:
        return mapped[raw]
    u = (raw or "").strip().upper()
    if u.startswith("LOCK REMOVED VIA R1"):
        return "Lock removed via R1"
    if u.startswith("LOCK REMOVED VIA R2"):
        return "Lock removed via R2"
    return raw

_ENSURED = False


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def ensure_tables() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_checklist_trades (
                    id VARCHAR(36) PRIMARY KEY,
                    symbol VARCHAR(32) NOT NULL,
                    direction VARCHAR(8) NOT NULL,
                    entry_price NUMERIC(18,4) NOT NULL,
                    entry_time TIMESTAMPTZ NOT NULL,
                    entry_qty INTEGER NOT NULL,
                    session_date DATE NOT NULL,
                    initial_ema10_at_entry NUMERIC(18,4),
                    initial_sl_inr NUMERIC(18,4),
                    state VARCHAR(24) NOT NULL DEFAULT 'TRAILING',
                    current_sl_price NUMERIC(18,4),
                    highest_rr_reached NUMERIC(12,4) DEFAULT 0,
                    alarm_fired_at TIMESTAMPTZ,
                    exit_trigger_reason TEXT,
                    exit_trigger_price NUMERIC(18,4),
                    state_context_snapshot JSONB,
                    status VARCHAR(16) NOT NULL DEFAULT 'OPEN',
                    exit_price NUMERIC(18,4),
                    exit_time TIMESTAMPTZ,
                    exit_reason VARCHAR(64),
                    realized_pnl_points NUMERIC(18,4),
                    realized_pnl_inr NUMERIC(18,4),
                    exit_note TEXT,
                    last_eval_bar_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        # Drop legacy cancel column if present (addendum: no cancel path)
        try:
            conn.execute(text("ALTER TABLE kavach_checklist_trades DROP COLUMN IF EXISTS was_cancelled"))
        except Exception:
            pass
        try:
            conn.execute(
                text(
                    "ALTER TABLE kavach_checklist_trades "
                    "ADD COLUMN IF NOT EXISTS provenance JSONB"
                )
            )
        except Exception:
            pass
        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_kavach_open_sym_day
                ON kavach_checklist_trades (session_date, symbol)
                WHERE status = 'OPEN'
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_kavach_trades_session
                ON kavach_checklist_trades (session_date, status)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_checklist_trade_edits (
                    id SERIAL PRIMARY KEY,
                    trade_id VARCHAR(36) NOT NULL REFERENCES kavach_checklist_trades(id),
                    field_name VARCHAR(32) NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    edited_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_r2_exit_now_log (
                    id SERIAL PRIMARY KEY,
                    trade_id VARCHAR(36),
                    symbol VARCHAR(32) NOT NULL,
                    session_date DATE NOT NULL,
                    direction VARCHAR(8),
                    entry_rank INTEGER,
                    removal_rank INTEGER,
                    last_3_ranks JSONB,
                    price_closed_beyond_ema10 BOOLEAN,
                    price_closed_beyond_vwap BOOLEAN,
                    confirmed_close NUMERIC(18,4),
                    ema10 NUMERIC(18,4),
                    vwap NUMERIC(18,4),
                    exit_trigger_reason TEXT,
                    label TEXT,
                    removed_at TIMESTAMPTZ,
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_kavach_r2_exit_session
                ON kavach_r2_exit_now_log (session_date, symbol)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_r1_early_warning_log (
                    id SERIAL PRIMARY KEY,
                    trade_id VARCHAR(36),
                    symbol VARCHAR(32) NOT NULL,
                    session_date DATE NOT NULL,
                    direction VARCHAR(8),
                    vwap_close_price NUMERIC(18,4),
                    vwap_close_hm VARCHAR(8),
                    ema10 NUMERIC(18,4),
                    ema10_distance_pts NUMERIC(18,4),
                    live_price NUMERIC(18,4),
                    pnl_at_flag_inr NUMERIC(18,2),
                    price_closed_beyond_ema10 BOOLEAN,
                    exit_trigger_reason TEXT,
                    label TEXT,
                    removed_at TIMESTAMPTZ,
                    signal_path TEXT,
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE kavach_r1_early_warning_log
                ADD COLUMN IF NOT EXISTS signal_path TEXT
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_kavach_r1_warn_session
                ON kavach_r1_early_warning_log (session_date, symbol)
                """
            )
        )
    _ENSURED = True


def _now() -> datetime:
    return datetime.now(IST)


def _row_to_dict(r) -> Dict[str, Any]:
    if r is None:
        return {}
    m = dict(r._mapping) if hasattr(r, "_mapping") else dict(r)
    out: Dict[str, Any] = {}
    for k, v in m.items():
        if isinstance(v, datetime):
            out[k] = v.astimezone(IST).isoformat() if v.tzinfo else v.isoformat()
        elif hasattr(v, "isoformat") and not isinstance(v, str):
            out[k] = str(v)
        else:
            out[k] = v
    snap = out.get("state_context_snapshot")
    if isinstance(snap, str):
        try:
            out["state_context_snapshot"] = json.loads(snap)
        except Exception:
            pass
    prov = out.get("provenance")
    if isinstance(prov, str):
        try:
            out["provenance"] = json.loads(prov)
        except Exception:
            pass
    return out


def _live_price(db, symbol: str) -> Optional[float]:
    try:
        row = db.execute(
            text(
                """
                SELECT currmth_future_instrument_key AS ikey
                FROM arbitrage_master WHERE UPPER(stock) = :s LIMIT 1
                """
            ),
            {"s": symbol.upper()},
        ).fetchone()
        ikey = row.ikey if row else None
        if ikey:
            from backend.services import vwap_service

            q = vwap_service.get_market_quote_by_key(ikey) or {}
            px = _f(q.get("last_price") or q.get("ltp"))
            if px:
                return px
    except Exception as exc:
        logger.debug("live price quote failed %s: %s", symbol, exc)
    try:
        row = db.execute(
            text(
                """
                SELECT current_price FROM relative_strength_snapshot
                WHERE UPPER(symbol) = :s
                ORDER BY scan_time DESC LIMIT 1
                """
            ),
            {"s": symbol.upper()},
        ).fetchone()
        return _f(row.current_price) if row else None
    except Exception:
        return None


def _levels_for_symbol(db, symbol: str, session_date: str) -> Dict[str, Any]:
    from backend.services.daily_checklist_trade_state import _load_price_levels

    return (_load_price_levels(db, [symbol], session_date) or {}).get(symbol.upper()) or {}


def _confirmed_10m_levels(db, symbol: str) -> Dict[str, Any]:
    """EMA5 / EMA10 / VWAP / close from last confirmed 10m bar."""
    try:
        from backend.services.daily_checklist_snapshot import _load_candles_for_symbol
        from backend.services.kavach_10m import aggregate_10m_bars, last_closed_10m_pair_end_idx
        from backend.services.relative_strength_scanner import _sorted_candles
        from backend.services.vajra.indicators import cumulative_vwap, ema_series

        candles = _load_candles_for_symbol(db, symbol)
        if not candles or len(candles) < 40:
            return {}
        candles = _sorted_candles(candles)
        pair_end = last_closed_10m_pair_end_idx(candles)
        bars = [b for b in aggregate_10m_bars(candles) if b["end_5m_idx"] <= pair_end]
        if len(bars) < 10:
            return {}
        closes = [float(b["close"]) for b in bars]
        ema5_s = ema_series(closes, 5)
        ema10_s = ema_series(closes, 10)
        last = bars[-1]
        bar_ts = None
        idx = int(last.get("end_5m_idx") or -1)
        if 0 <= idx < len(candles):
            bar_ts = candles[idx].get("timestamp")
        vwap = None
        try:
            end_idx = max(0, min(idx, len(candles) - 1)) if idx >= 0 else len(candles) - 1
            slice_c = candles[: end_idx + 1]
            highs = [float(c.get("high") or 0) for c in slice_c]
            lows = [float(c.get("low") or 0) for c in slice_c]
            cls = [float(c.get("close") or 0) for c in slice_c]
            vols = [float(c.get("volume") or 0) for c in slice_c]
            vw = cumulative_vwap(highs, lows, cls, vols)
            if vw:
                vwap = float(vw[-1])
        except Exception:
            vwap = None
        return {
            "close": closes[-1],
            "ema5": ema5_s[-1] if ema5_s else None,
            "ema10": ema10_s[-1] if ema10_s else None,
            "vwap": vwap,
            "bar_at": bar_ts,
        }
    except Exception as exc:
        logger.debug("10m levels failed %s: %s", symbol, exc)
        return {}


def _bar_hm(bar_at: Any) -> str:
    """Format confirmed-bar timestamp as HH:MM IST."""
    if not bar_at:
        return _now().strftime("%H:%M")
    try:
        if isinstance(bar_at, datetime):
            dt = bar_at
        else:
            dt = datetime.fromisoformat(str(bar_at).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = IST.localize(dt)
        else:
            dt = dt.astimezone(IST)
        return dt.strftime("%H:%M")
    except Exception:
        return _now().strftime("%H:%M")


def classify_click_revalidation(
    *,
    is_long: bool,
    live: Optional[float],
    confirmed_close: Optional[float],
    ema10: Optional[float],
    vwap: Optional[float],
    bar_hm: str,
) -> Dict[str, Any]:
    """Reuse confirmed-close EMA10 rule; borderline = live past level, close not through."""
    out: Dict[str, Any] = {
        "blocked": False,
        "message": None,
        "warning": None,
        "confirmed_beyond_ema10": False,
        "live_beyond_ema10": False,
        "live_beyond_vwap": False,
    }
    if confirmed_close is not None and ema10 is not None:
        beyond = (confirmed_close < ema10) if is_long else (confirmed_close > ema10)
        out["confirmed_beyond_ema10"] = beyond
        if beyond:
            out["blocked"] = True
            out["message"] = (
                f"Setup invalidated since scan — price closed beyond EMA10 at {bar_hm}. "
                "Re-scan required."
            )
            return out
    if live is not None and ema10 is not None:
        out["live_beyond_ema10"] = (live < ema10) if is_long else (live > ema10)
    if live is not None and vwap is not None:
        out["live_beyond_vwap"] = (live < vwap) if is_long else (live > vwap)
    if out["live_beyond_ema10"] or out["live_beyond_vwap"]:
        parts = []
        if out["live_beyond_ema10"]:
            parts.append("EMA10")
        if out["live_beyond_vwap"]:
            parts.append("VWAP")
        out["warning"] = (
            "Borderline — live price past "
            + "/".join(parts)
            + " but confirmed 10m close has not closed through. Proceed with caution."
        )
    return out


def revalidate_setup_at_click(
    db,
    symbol: str,
    direction: str,
    session_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Re-fetch live + confirmed EMA/VWAP before Take Trade submit."""
    sd = session_date or today_ist()
    is_long = (direction or "LONG").upper() != "SHORT"
    live = _live_price(db, symbol)
    bar = _confirmed_10m_levels(db, symbol)
    levels = _levels_for_symbol(db, symbol, sd)
    confirmed_close = _f(bar.get("close"))
    ema5 = _f(bar.get("ema5")) or _f(levels.get("ema5"))
    ema10 = _f(bar.get("ema10")) or _f(levels.get("ema10"))
    vwap = _f(bar.get("vwap")) or _f(levels.get("vwap"))
    if live is None:
        live = confirmed_close or _f(levels.get("price"))
    verdict = classify_click_revalidation(
        is_long=is_long,
        live=live,
        confirmed_close=confirmed_close,
        ema10=ema10,
        vwap=vwap,
        bar_hm=_bar_hm(bar.get("bar_at")),
    )
    verdict["snapshot"] = {
        "live_price": live,
        "confirmed_close": confirmed_close,
        "ema5": ema5,
        "ema10": ema10,
        "vwap": vwap,
        "bar_at": bar.get("bar_at"),
        "bar_hm": _bar_hm(bar.get("bar_at")),
    }
    return verdict


def build_take_trade_provenance(
    db,
    session_date: str,
    symbol: str,
    *,
    direction: str,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Snapshot of checklist / lock / board state at Take Trade click."""
    from backend.services.daily_checklist_zones import morning_locked_symbols

    ctx = dict(context or {})
    sym = (symbol or "").upper()
    lock_map = morning_locked_symbols(db, session_date)
    lock_row = lock_map.get(sym)
    in_morning_lock = lock_row is not None
    # GO board / Fast Watch (best-effort)
    in_go_board = False
    in_fast_watch_top2 = False
    try:
        from backend.services.rs_go_board import get_go_board

        gb = get_go_board(session_date) or {}
        go_syms = {
            str(x.get("symbol") or "").upper()
            for x in (gb.get("symbols") or [])
            if isinstance(x, dict)
        }
        in_go_board = sym in go_syms
    except Exception:
        pass
    try:
        from backend.services.rs_fast_watch import get_fast_watch

        fw = get_fast_watch(session_date) or {}
        featured = fw.get("featured") or {}
        top = []
        for side in ("long", "short"):
            top.extend(featured.get(side) or [])
        top2 = {
            str(x.get("symbol") or "").upper()
            for x in top[:4]
            if isinstance(x, dict)
        }
        in_fast_watch_top2 = sym in top2
    except Exception:
        pass

    badges = ctx.get("gate_badges") or []
    if isinstance(badges, str):
        badges = [badges]

    # Live regime snapshot at click (research — do not enforce).
    regime_at_click: Dict[str, Any] = {}
    try:
        from backend.services.daily_checklist_trade_state import _recent_removals
        from backend.services.daily_checklist_chop_gates import compute_market_regime
        from backend.services.daily_checklist_zones import (
            build_zone1_obs,
            regime_research_snapshot,
        )

        mkt = compute_market_regime(session_date)
        removals = _recent_removals(db, session_date)
        zone1 = build_zone1_obs(rotation_day=None, removals=removals, locked_by=None)
        regime_at_click = regime_research_snapshot(
            market_regime=mkt.get("market_regime"),
            market_regime_label=mkt.get("market_regime_label"),
            imbalance=zone1.get("direction_imbalance"),
            removals=removals,
            direction=direction,
        )
    except Exception as exc:
        logger.debug("take_trade regime snapshot skipped: %s", exc)
        regime_at_click = {
            "market_regime": ctx.get("market_regime"),
            "removals_last_hour": ctx.get("removals_last_hour"),
            "counter_regime": ctx.get("counter_regime"),
        }

    return {
        "captured_at": _now().isoformat(),
        "in_morning_lock": in_morning_lock,
        "morning_lock_rank": (lock_row or {}).get("rank"),
        "morning_lock_direction": (lock_row or {}).get("direction"),
        "in_go_board": in_go_board,
        "in_fast_watch_top2": in_fast_watch_top2,
        "decision_label": ctx.get("decision") or ctx.get("decision_label"),
        "trade_state": ctx.get("trade_state"),
        "downgrade_badges": list(badges),
        "zone_at_click": ctx.get("zone") or ("Zone 3 READY" if ctx.get("trade_state") in ("READY", "READY(RECHECK)") else "Zone 4"),
        "market_regime": regime_at_click.get("market_regime") or ctx.get("market_regime"),
        "confidence": ctx.get("confidence"),
        "rs_pct": ctx.get("rs_pct"),
        "regime_at_click": regime_at_click,
        "regime_unconfirmed": regime_at_click.get("regime_unconfirmed"),
        "regime_lean": regime_at_click.get("regime_lean"),
        "removals_last_hour": regime_at_click.get("removals_last_hour"),
        "counter_regime": regime_at_click.get("counter_regime"),
    }


def take_trade(
    symbol: str,
    *,
    direction: str,
    entry_price: Optional[float] = None,
    entry_time: Optional[str] = None,
    session_date: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ensure_tables()
    sd = session_date or today_ist()
    sym = (symbol or "").strip().upper()
    if not sym:
        raise ValueError("symbol required")
    direction = (direction or "LONG").upper()
    if direction not in ("LONG", "SHORT"):
        raise ValueError("direction must be LONG or SHORT")

    db = SessionLocal()
    try:
        from backend.services.daily_checklist_trade_state import (
            before_entry_window_ist,
            risk_cap_blocks_ready,
            session_rr,
            take_trade_structurally_ok,
            MAX_INR_RISK,
            RR_LOW,
        )

        if before_entry_window_ist():
            raise ValueError("Take Trade disabled before 09:45 IST — waiting for 3 clean 10m bars")

        existing = db.execute(
            text(
                """
                SELECT id FROM kavach_checklist_trades
                WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :s AND status = 'OPEN'
                """
            ),
            {"d": sd, "s": sym},
        ).fetchone()
        if existing:
            raise ValueError("Position already open in Open Trades panel")

        # Same-day re-entry block (panel closed trades with blocking exit reasons)
        from backend.services.daily_checklist_chop_gates import (
            REENTRY_BLOCK_LABEL,
            exit_reason_blocks_reentry,
        )

        prior = db.execute(
            text(
                """
                SELECT exit_reason FROM kavach_checklist_trades
                WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :s
                  AND status = 'CLOSED'
                ORDER BY exit_time DESC NULLS LAST
                """
            ),
            {"d": sd, "s": sym},
        ).fetchall()
        for row in prior:
            if exit_reason_blocks_reentry(row.exit_reason):
                raise ValueError(REENTRY_BLOCK_LABEL)

        lot, _ = _lot_for_symbol(db, sym)
        px = entry_price if entry_price is not None else _live_price(db, sym)
        if px is None:
            levels = _levels_for_symbol(db, sym, sd)
            px = _f(levels.get("price"))
        if px is None:
            raise ValueError("Could not resolve entry price")

        reval = revalidate_setup_at_click(db, sym, direction, sd)
        if reval.get("blocked"):
            raise ValueError(reval.get("message") or "Setup invalidated since scan. Re-scan required.")

        now = _now()
        if entry_time:
            try:
                hh, mm = entry_time.strip().split(":")[:2]
                et = IST.localize(datetime(now.year, now.month, now.day, int(hh), int(mm)))
            except Exception:
                et = now
        else:
            et = now

        levels = _levels_for_symbol(db, sym, sd)
        bar = _confirmed_10m_levels(db, sym)
        ema10 = _f(bar.get("ema10")) or _f(levels.get("ema10"))
        ema5 = _f(bar.get("ema5")) or _f(levels.get("ema5"))
        if ema10 is None:
            raise ValueError("Take Trade disabled — SL (EMA10) not available")
        initial_sl = abs(px - ema10) * lot
        if not take_trade_structurally_ok(entry=px, sl=ema10, risk_inr=initial_sl):
            raise ValueError("Take Trade disabled — SL/Risk not computed")

        from backend.services.daily_checklist_trade_state import (
            _session_hi_lo,
            entry_off_live_ema5,
        )

        hi, lo = _session_hi_lo(db, sym, sd)
        rr = session_rr(
            is_long=(direction == "LONG"),
            entry=px,
            sl=ema10,
            session_hi=hi,
            session_lo=lo,
        )
        if risk_cap_blocks_ready(initial_sl, rr):
            raise ValueError(
                f"Take Trade blocked — risk ₹{int(initial_sl)} > ₹{int(MAX_INR_RISK)} "
                f"and R:R {('1:' + str(rr)) if rr is not None else '—'} < 1:{RR_LOW:g}"
            )
        if ema5 is not None and entry_off_live_ema5(px, ema5):
            logger.warning(
                "take_trade entry off EMA5 %s: entry=%s ema5=%s", sym, px, ema5
            )

        tid = str(uuid.uuid4())
        ctx = dict(context or {})
        ctx.setdefault("confidence", levels.get("confidence_grade"))
        ctx.setdefault("market_regime", levels.get("market_regime"))
        provenance = build_take_trade_provenance(
            db, sd, sym, direction=direction, context=ctx
        )
        provenance["click_revalidation"] = {
            "warning": reval.get("warning"),
            "snapshot": reval.get("snapshot"),
            "live_beyond_ema10": reval.get("live_beyond_ema10"),
            "live_beyond_vwap": reval.get("live_beyond_vwap"),
        }
        ctx["provenance"] = provenance

        db.execute(
            text(
                """
                INSERT INTO kavach_checklist_trades (
                    id, symbol, direction, entry_price, entry_time, entry_qty, session_date,
                    initial_ema10_at_entry, initial_sl_inr, state, current_sl_price,
                    highest_rr_reached, state_context_snapshot, provenance, status
                ) VALUES (
                    :id, :sym, :dir, :px, :et, :qty, CAST(:d AS date),
                    :ema10, :sl_inr, :state, :sl_px,
                    0, CAST(:ctx AS jsonb), CAST(:prov AS jsonb), 'OPEN'
                )
                """
            ),
            {
                "id": tid,
                "sym": sym,
                "dir": direction,
                "px": px,
                "et": et,
                "qty": lot,
                "d": sd,
                "ema10": ema10,
                "sl_inr": initial_sl,
                "state": STATE_TRAILING,
                "sl_px": ema10,
                "ctx": json.dumps(ctx),
                "prov": json.dumps(provenance),
            },
        )
        db.commit()
        trade = get_trade(tid)
        if reval.get("warning"):
            trade["take_warning"] = reval["warning"]
        return trade
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_trade(trade_id: str) -> Dict[str, Any]:
    ensure_tables()
    db = SessionLocal()
    try:
        r = db.execute(
            text("SELECT * FROM kavach_checklist_trades WHERE id = :id"),
            {"id": trade_id},
        ).fetchone()
        if not r:
            raise ValueError("trade not found")
        return enrich_trade_live(_row_to_dict(r), db)
    finally:
        db.close()


def list_session_trades(session_date: Optional[str] = None) -> Dict[str, Any]:
    ensure_tables()
    sd = session_date or today_ist()
    db = SessionLocal()
    try:
        # Evaluate open trades on each poll (candle-close gated inside)
        evaluate_open_trades(db, sd)
        db.commit()

        opens = db.execute(
            text(
                """
                SELECT * FROM kavach_checklist_trades
                WHERE session_date = CAST(:d AS date) AND status = 'OPEN'
                ORDER BY entry_time
                """
            ),
            {"d": sd},
        ).fetchall()
        closed = db.execute(
            text(
                """
                SELECT * FROM kavach_checklist_trades
                WHERE session_date = CAST(:d AS date) AND status = 'CLOSED'
                ORDER BY COALESCE(exit_time, updated_at) DESC
                """
            ),
            {"d": sd},
        ).fetchall()
        open_list = [enrich_trade_live(_row_to_dict(r), db) for r in opens]
        closed_list = [_row_to_dict(r) for r in closed]
        return {
            "session_date": sd,
            "open_trades": open_list,
            "closed_trades": closed_list,
            "exit_now_symbols": [
                t["symbol"] for t in open_list if t.get("state") == STATE_EXIT_NOW
            ],
            "plan_exit_symbols": [
                t["symbol"] for t in open_list if t.get("state") == STATE_PLAN_EXIT
            ],
        }
    finally:
        db.close()


def enrich_trade_live(t: Dict[str, Any], db) -> Dict[str, Any]:
    """Attach live price / PnL / R:R (intrabar) without changing state."""
    sym = t.get("symbol") or ""
    direction = (t.get("direction") or "LONG").upper()
    is_long = direction != "SHORT"
    entry = _f(t.get("entry_price"))
    qty = int(t.get("entry_qty") or 1)
    live = _live_price(db, sym)
    levels = _levels_for_symbol(db, sym, str(t.get("session_date") or today_ist()))
    bar = _confirmed_10m_levels(db, sym)
    if live is None:
        live = _f(bar.get("close")) or _f(levels.get("price"))

    sl = _f(t.get("current_sl_price"))
    state = t.get("state") or STATE_TRAILING
    # Display SL: EMA5 when profit locked, else EMA10 (from last confirmed bar if available)
    if state == STATE_PROFIT_LOCKED:
        disp_sl = _f(bar.get("ema5")) or sl
    elif state == STATE_EXIT_NOW:
        disp_sl = sl
    else:
        disp_sl = _f(bar.get("ema10")) or sl

    pnl_pts = None
    pnl_inr = None
    if live is not None and entry is not None:
        pnl_pts = (live - entry) if is_long else (entry - live)
        pnl_inr = round(pnl_pts * qty, 0)

    dist_pts = abs(live - disp_sl) if live is not None and disp_sl is not None else None
    dist_inr = round(dist_pts * qty, 0) if dist_pts is not None else None

    initial_risk = _f(t.get("initial_sl_inr")) or 0
    rr = None
    if initial_risk and initial_risk > 0 and pnl_inr is not None:
        rr = round(pnl_inr / initial_risk, 2)

    peak = _f(t.get("highest_rr_reached")) or 0
    if rr is not None and rr > peak:
        peak = rr
        # persist peak asynchronously-ish
        try:
            db.execute(
                text(
                    """
                    UPDATE kavach_checklist_trades
                    SET highest_rr_reached = :rr, updated_at = NOW()
                    WHERE id = :id AND highest_rr_reached < :rr
                    """
                ),
                {"id": t["id"], "rr": peak},
            )
            db.commit()
            t["highest_rr_reached"] = peak
        except Exception:
            db.rollback()

    held_min = None
    et = t.get("entry_time")
    try:
        if et:
            edt = datetime.fromisoformat(str(et).replace("Z", "+00:00"))
            if edt.tzinfo is None:
                edt = IST.localize(edt)
            held_min = int((_now() - edt.astimezone(IST)).total_seconds() // 60)
    except Exception:
        pass

    hint = ""
    if state == STATE_TRAILING:
        side = "below" if is_long else "above"
        hint = f"Hold. Exit on 10m close {side} EMA10 (₹{disp_sl:.2f})" if disp_sl else "Hold. Exit on 10m close beyond EMA10"
    elif state == STATE_PROFIT_LOCKED:
        side = "below" if is_long else "above"
        hint = f"R:R hit 1:2 · trail tightened to EMA5 close (₹{disp_sl:.2f})" if disp_sl else "R:R hit 1:2 · trail on EMA5"
    elif state == STATE_PLAN_EXIT:
        side = "below" if is_long else "above"
        hint = (
            "PLAN EXIT — VWAP breached; hold until 10m close "
            f"{side} EMA10 (₹{disp_sl:.2f})" if disp_sl
            else "PLAN EXIT — VWAP breached; hold until confirmed EMA10 close"
        )
        lrc = None
        snap = t.get("state_context_snapshot")
        if isinstance(snap, dict):
            lrc = snap.get("lock_removal_context")
        if isinstance(lrc, dict) and lrc.get("label"):
            hint = f"{hint} · {lrc['label']}"
    elif state == STATE_EXIT_NOW:
        hint = f"EXIT at current candle close · reason: {t.get('exit_trigger_reason') or '—'}"
        lrc = None
        snap = t.get("state_context_snapshot")
        if isinstance(snap, dict):
            lrc = snap.get("lock_removal_context")
        if isinstance(lrc, dict) and lrc.get("label"):
            hint = f"{hint} · {lrc['label']}"
            if lrc.get("rank_trail"):
                hint = f"{hint} · ranks {lrc['rank_trail']}"

    risk_cap_inr = MAX_INR_RISK
    risk_now = dist_inr
    risk_over = bool(risk_now is not None and risk_now > risk_cap_inr) or bool(
        initial_risk and initial_risk > risk_cap_inr
    )
    risk_cap_flag = bool(risk_over and (rr is None or rr < RR_LOW))

    t.update(
        {
            "live_price": live,
            "display_sl": round(disp_sl, 2) if disp_sl is not None else None,
            "distance_sl_pts": round(dist_pts, 2) if dist_pts is not None else None,
            "distance_sl_inr": dist_inr,
            "unrealized_pnl_pts": round(pnl_pts, 2) if pnl_pts is not None else None,
            "unrealized_pnl_inr": pnl_inr,
            "achieved_rr": rr,
            "highest_rr_reached": peak,
            "held_minutes": held_min,
            "action_hint": hint,
            "trade_risk_over": risk_over,
            "trade_risk_cap_flag": risk_cap_flag,
            "trade_risk_cap_inr": int(risk_cap_inr),
        }
    )
    snap = t.get("state_context_snapshot")
    if isinstance(snap, dict) and snap.get("lock_removal_context"):
        t["lock_removal_context"] = snap["lock_removal_context"]
    return t


def edit_trade_field(trade_id: str, field: str, value: Any) -> Dict[str, Any]:
    ensure_tables()
    field = (field or "").strip()
    if field not in ("direction", "entry_price", "entry_time", "entry_qty"):
        raise ValueError("editable fields: direction, entry_price, entry_time, entry_qty")
    db = SessionLocal()
    try:
        r = db.execute(
            text("SELECT * FROM kavach_checklist_trades WHERE id = :id AND status = 'OPEN'"),
            {"id": trade_id},
        ).fetchone()
        if not r:
            raise ValueError("open trade not found")
        t = _row_to_dict(r)
        old = t.get(field)
        new_v = value
        if field == "direction":
            new_v = str(value).upper()
            if new_v not in ("LONG", "SHORT"):
                raise ValueError("direction must be LONG or SHORT")
        elif field == "entry_price":
            new_v = round(float(value), 2)
        elif field == "entry_qty":
            new_v = int(value)
            if new_v <= 0:
                raise ValueError("qty must be positive")
        elif field == "entry_time":
            # accept HH:MM → today's timestamp
            hh, mm = str(value).strip().split(":")[:2]
            now = _now()
            new_v = IST.localize(datetime(now.year, now.month, now.day, int(hh), int(mm)))

        # Recompute initial risk if entry or direction changes
        entry = new_v if field == "entry_price" else _f(t.get("entry_price"))
        qty = new_v if field == "entry_qty" else int(t.get("entry_qty") or 1)
        ema10 = _f(t.get("initial_ema10_at_entry"))
        new_sl_inr = abs(entry - ema10) * qty if entry is not None and ema10 is not None else t.get("initial_sl_inr")

        db.execute(
            text(
                f"""
                UPDATE kavach_checklist_trades
                SET {field} = :v, initial_sl_inr = :sl, updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"v": new_v, "sl": new_sl_inr, "id": trade_id},
        )
        db.execute(
            text(
                """
                INSERT INTO kavach_checklist_trade_edits (trade_id, field_name, old_value, new_value)
                VALUES (:tid, :f, :o, :n)
                """
            ),
            {
                "tid": trade_id,
                "f": field,
                "o": str(old) if old is not None else None,
                "n": str(new_v) if not isinstance(new_v, datetime) else new_v.isoformat(),
            },
        )
        db.commit()
        return get_trade(trade_id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def exit_trade(
    trade_id: str,
    *,
    exit_price: float,
    exit_reason: str,
    exit_note: Optional[str] = None,
    exit_time: Optional[str] = None,
) -> Dict[str, Any]:
    ensure_tables()
    if exit_reason not in EXIT_REASONS:
        exit_reason = canonical_exit_reason(exit_reason)
        if exit_reason not in EXIT_REASONS:
            raise ValueError(f"exit_reason must be one of {EXIT_REASONS}")

    db = SessionLocal()
    try:
        r = db.execute(
            text("SELECT * FROM kavach_checklist_trades WHERE id = :id AND status = 'OPEN'"),
            {"id": trade_id},
        ).fetchone()
        if not r:
            raise ValueError("open trade not found")
        t = _row_to_dict(r)
        entry = _f(t.get("entry_price"))
        qty = int(t.get("entry_qty") or 1)
        is_long = (t.get("direction") or "LONG").upper() != "SHORT"
        px = float(exit_price)
        pts = (px - entry) if is_long else (entry - px)
        inr = round(pts * qty, 0)

        now = _now()
        et = now
        if exit_time:
            try:
                hh, mm = exit_time.strip().split(":")[:2]
                et = IST.localize(datetime(now.year, now.month, now.day, int(hh), int(mm)))
            except Exception:
                et = now

        db.execute(
            text(
                """
                UPDATE kavach_checklist_trades
                SET status = 'CLOSED', exit_price = :px, exit_time = :et,
                    exit_reason = :er, exit_note = :note,
                    realized_pnl_points = :pts, realized_pnl_inr = :inr,
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {
                "id": trade_id,
                "px": px,
                "et": et,
                "er": exit_reason,
                "note": exit_note,
                "pts": pts,
                "inr": inr,
            },
        )
        db.commit()
        return get_trade(trade_id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _parse_ts(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        dt = val
    else:
        try:
            dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def _symbol_on_lock(db, session_date: str, symbol: str) -> bool:
    r = db.execute(
        text(
            """
            SELECT 1 FROM daily_snapshot
            WHERE snapshot_date = CAST(:d AS date) AND UPPER(symbol) = :sym
            LIMIT 1
            """
        ),
        {"d": session_date, "sym": (symbol or "").upper()},
    ).fetchone()
    return r is not None


def latest_r_lock_removal(
    db,
    session_date: str,
    symbol: str,
    *,
    since: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """Latest R1/R2 remove audit row for symbol (optionally after ``since``)."""
    binds: Dict[str, Any] = {"d": session_date, "sym": (symbol or "").upper()}
    since_sql = ""
    if since is not None:
        since_sql = " AND event_at >= :since"
        binds["since"] = since
    try:
        r = db.execute(
            text(
                f"""
                SELECT rule, event_at
                FROM rs_lock_membership_audit
                WHERE session_date = CAST(:d AS date)
                  AND UPPER(symbol) = :sym
                  AND event_type = 'remove'
                  AND rule IN ('R1', 'R2')
                  {since_sql}
                ORDER BY event_at DESC
                LIMIT 1
                """
            ),
            binds,
        ).fetchone()
    except Exception as exc:
        logger.debug("latest_r_lock_removal skipped: %s", exc)
        return None
    if not r:
        return None
    return {"rule": str(r.rule).upper(), "at": _parse_ts(r.event_at)}


def mark_open_trades_exit_on_lock_removal(
    db,
    session_date: str,
    symbol: str,
    rule: str,
    *,
    removed_at: Optional[datetime] = None,
) -> List[str]:
    """Apply lock-removal signal on OPEN panel trades for ``symbol``.

    R2 → always EXIT NOW (+ alarm).
    R1 → PLAN EXIT (informational) when EMA10 not yet breached; EXIT NOW when
    EMA10 already breached or sits between price and VWAP.

    Does not auto-close — trader still confirms EXIT on EXIT NOW.
    Returns ids newly moved to EXIT_NOW or PLAN_EXIT.
    """
    ensure_tables()
    tag = (rule or "").strip().upper()
    now = removed_at or _now()
    if isinstance(now, datetime):
        if now.tzinfo is None:
            now = IST.localize(now)
        else:
            now = now.astimezone(IST)
    rows = db.execute(
        text(
            """
            SELECT id, state, direction, provenance, state_context_snapshot,
                   entry_price, entry_qty
            FROM kavach_checklist_trades
            WHERE session_date = CAST(:d AS date)
              AND status = 'OPEN'
              AND UPPER(symbol) = :sym
            """
        ),
        {"d": session_date, "sym": (symbol or "").upper()},
    ).fetchall()
    newly: List[str] = []
    for row in rows:
        prev_state = row.state or ""
        if prev_state == STATE_EXIT_NOW:
            continue
        t = _row_to_dict(row)
        direction = (t.get("direction") or "LONG").upper()
        ctx = build_lock_removal_context(
            db,
            session_date,
            symbol,
            tag,
            direction=direction,
            entry_rank=_entry_rank_from_trade(t),
            removed_at=now,
            trade=t,
        )

        if tag == "R1":
            target = ctx.get("target_state") or STATE_PLAN_EXIT
            if prev_state == STATE_PLAN_EXIT and target == STATE_PLAN_EXIT:
                continue
            if target == STATE_PLAN_EXIT:
                reason = format_r1_plan_exit_reason(now)
            else:
                reason = format_lock_removal_exit_reason("R1", now)
        else:
            target = STATE_EXIT_NOW
            reason = format_lock_removal_exit_reason(tag, now)

        detail_reason = reason
        if ctx.get("label"):
            detail_reason = f"{reason} | {ctx['label']}"
            if tag == "R2" and ctx.get("rank_trail"):
                detail_reason = f"{detail_reason} | ranks {ctx['rank_trail']}"
            if tag == "R1":
                bits = _r1_detail_bits(ctx)
                if bits:
                    detail_reason = f"{detail_reason} | {bits}"

        snap = t.get("state_context_snapshot") if isinstance(t.get("state_context_snapshot"), dict) else {}
        snap = dict(snap or {})
        snap["lock_removal_context"] = ctx
        alarm_sql = (
            "alarm_fired_at = :alarm"
            if prev_state == STATE_PLAN_EXIT and target == STATE_EXIT_NOW
            else "alarm_fired_at = COALESCE(alarm_fired_at, :alarm)"
        )
        db.execute(
            text(
                f"""
                UPDATE kavach_checklist_trades SET
                    state = :st,
                    exit_trigger_reason = :tr,
                    {alarm_sql},
                    state_context_snapshot = CAST(:snap AS jsonb),
                    updated_at = NOW()
                WHERE id = :id AND state <> :st
                """
            ),
            {
                "st": target,
                "tr": detail_reason,
                "alarm": now,
                "snap": json.dumps(snap),
                "id": row.id,
            },
        )
        log_r2_exit_now(
            db,
            trade_id=str(row.id),
            symbol=symbol,
            session_date=session_date,
            direction=direction,
            context=ctx,
            exit_trigger_reason=detail_reason,
            removed_at=now,
        )
        log_r1_early_warning(
            db,
            trade_id=str(row.id),
            symbol=symbol,
            session_date=session_date,
            direction=direction,
            context=ctx,
            exit_trigger_reason=detail_reason,
            removed_at=now,
            signal_path=ctx.get("signal_path") if tag == "R1" else None,
        )
        newly.append(str(row.id))
        logger.info(
            "open_trades: %s on lock removal symbol=%s rule=%s trade=%s path=%s",
            target,
            symbol,
            tag,
            row.id,
            ctx.get("signal_path"),
        )
    return newly


def evaluate_open_trades(db, session_date: str) -> List[str]:
    """Candle-close state machine + R1/R2 lock-removal signals.

    Returns trade ids that newly entered EXIT_NOW (not PLAN_EXIT).
    """
    newly_exit: List[str] = []
    rows = db.execute(
        text(
            """
            SELECT * FROM kavach_checklist_trades
            WHERE session_date = CAST(:d AS date) AND status = 'OPEN'
            """
        ),
        {"d": session_date},
    ).fetchall()
    for r in rows:
        t = _row_to_dict(r)
        tid = t["id"]
        sym = t["symbol"]
        state = t.get("state") or STATE_TRAILING
        new_state = state
        new_sl = _f(t.get("current_sl_price"))
        trigger_reason = t.get("exit_trigger_reason")
        trigger_px = _f(t.get("exit_trigger_price"))
        alarm_at = t.get("alarm_fired_at")
        peak = _f(t.get("highest_rr_reached")) or 0.0
        last_eval = str(t.get("last_eval_bar_at") or "")
        bar_at = last_eval
        lock_exit = False
        lock_ctx: Optional[Dict[str, Any]] = None
        force_new_alarm = False
        r1_log_path: Optional[str] = None

        if state != STATE_EXIT_NOW:
            since = _parse_ts(t.get("entry_time")) or _parse_ts(t.get("created_at"))
            rem = latest_r_lock_removal(db, session_date, sym, since=since)
            if rem and not _symbol_on_lock(db, session_date, sym):
                rule = (rem["rule"] or "").upper()
                lock_ctx = build_lock_removal_context(
                    db,
                    session_date,
                    sym,
                    rule,
                    direction=t.get("direction") or "LONG",
                    entry_rank=_entry_rank_from_trade(t),
                    removed_at=rem.get("at"),
                    trade=t,
                )
                if rule == "R1":
                    target = lock_ctx.get("target_state") or STATE_PLAN_EXIT
                    if state == STATE_PLAN_EXIT and target == STATE_PLAN_EXIT:
                        lock_exit = False
                    else:
                        new_state = target
                        if target == STATE_PLAN_EXIT:
                            trigger_reason = format_r1_plan_exit_reason(rem.get("at"))
                        else:
                            trigger_reason = format_lock_removal_exit_reason("R1", rem.get("at"))
                        if lock_ctx.get("label"):
                            trigger_reason = f"{trigger_reason} | {lock_ctx['label']}"
                            bits = _r1_detail_bits(lock_ctx)
                            if bits:
                                trigger_reason = f"{trigger_reason} | {bits}"
                        r1_log_path = lock_ctx.get("signal_path")
                        lock_exit = True
                        if state == STATE_PLAN_EXIT and target == STATE_EXIT_NOW:
                            force_new_alarm = True
                else:
                    new_state = STATE_EXIT_NOW
                    trigger_reason = format_lock_removal_exit_reason(rule, rem.get("at"))
                    if lock_ctx.get("label"):
                        trigger_reason = f"{trigger_reason} | {lock_ctx['label']}"
                        if lock_ctx.get("rank_trail"):
                            trigger_reason = f"{trigger_reason} | ranks {lock_ctx['rank_trail']}"
                    lock_exit = True

        bar = _confirmed_10m_levels(db, sym)
        if bar.get("close"):
            bar_at = str(bar.get("bar_at") or "")
            close = float(bar["close"])
            ema5 = _f(bar.get("ema5"))
            ema10 = _f(bar.get("ema10"))
            is_long = (t.get("direction") or "LONG").upper() != "SHORT"
            qty = int(t.get("entry_qty") or 1)
            entry = _f(t.get("entry_price"))
            initial_risk = _f(t.get("initial_sl_inr")) or 0

            pnl_inr = 0.0
            if entry is not None:
                pts = (close - entry) if is_long else (entry - close)
                pnl_inr = pts * qty
            rr = (pnl_inr / initial_risk) if initial_risk > 0 else 0.0
            peak = max(peak, rr)

            if state != STATE_EXIT_NOW and not lock_exit:
                if state == STATE_PLAN_EXIT:
                    new_sl = ema10 if ema10 is not None else new_sl
                    if ema10 is not None:
                        beyond = (close < ema10) if is_long else (close > ema10)
                        if beyond:
                            new_state = STATE_EXIT_NOW
                            trigger_reason = "EMA10 reverse close"
                            trigger_px = close
                            force_new_alarm = True
                            r1_log_path = R1_PATH_PLAN_THEN_EMA10
                elif state == STATE_TRAILING:
                    new_sl = ema10
                    if ema10 is not None and rr < RR_LOW:
                        risk_now = abs(close - ema10) * qty
                        if risk_now > MAX_INR_RISK:
                            new_state = STATE_EXIT_NOW
                            trigger_reason = "Risk cap exceeded before 1:2"
                            trigger_px = close
                    if new_state != STATE_EXIT_NOW and ema10 is not None:
                        beyond = (close < ema10) if is_long else (close > ema10)
                        if beyond:
                            new_state = STATE_EXIT_NOW
                            trigger_reason = "EMA10 reverse close"
                            trigger_px = close
                    if new_state == STATE_TRAILING and rr >= RR_LOW:
                        new_state = STATE_PROFIT_LOCKED
                        new_sl = ema5
                elif state == STATE_PROFIT_LOCKED:
                    new_sl = ema5
                    if ema5 is not None:
                        beyond = (close < ema5) if is_long else (close > ema5)
                        if beyond:
                            new_state = STATE_EXIT_NOW
                            trigger_reason = "EMA5 reverse close after profit protection"
                            trigger_px = close
        elif not lock_exit:
            continue

        entered_exit_now = new_state == STATE_EXIT_NOW and state != STATE_EXIT_NOW
        entered_plan_exit = new_state == STATE_PLAN_EXIT and state != STATE_PLAN_EXIT
        if entered_exit_now:
            if force_new_alarm or not alarm_at:
                alarm_at = _now()
            newly_exit.append(tid)
        elif entered_plan_exit and not alarm_at:
            alarm_at = _now()

        if lock_exit or bar_at != last_eval or new_state != state or (
            new_sl != _f(t.get("current_sl_price"))
        ):
            snap_json = None
            snap = t.get("state_context_snapshot") if isinstance(t.get("state_context_snapshot"), dict) else {}
            snap = dict(snap or {})
            if lock_exit and lock_ctx:
                snap["lock_removal_context"] = lock_ctx
                snap_json = json.dumps(snap)
                if new_state in (STATE_EXIT_NOW, STATE_PLAN_EXIT) and new_state != state:
                    log_r2_exit_now(
                        db,
                        trade_id=str(tid),
                        symbol=sym,
                        session_date=session_date,
                        direction=t.get("direction") or "LONG",
                        context=lock_ctx,
                        exit_trigger_reason=trigger_reason or "",
                        removed_at=lock_ctx.get("removed_at")
                        if isinstance(lock_ctx.get("removed_at"), datetime)
                        else _parse_ts(lock_ctx.get("removed_at")),
                    )
                    log_r1_early_warning(
                        db,
                        trade_id=str(tid),
                        symbol=sym,
                        session_date=session_date,
                        direction=t.get("direction") or "LONG",
                        context=lock_ctx,
                        exit_trigger_reason=trigger_reason or "",
                        removed_at=lock_ctx.get("removed_at")
                        if isinstance(lock_ctx.get("removed_at"), datetime)
                        else _parse_ts(lock_ctx.get("removed_at")),
                        signal_path=r1_log_path or lock_ctx.get("signal_path"),
                    )
            elif entered_exit_now and r1_log_path == R1_PATH_PLAN_THEN_EMA10:
                prior = snap.get("lock_removal_context") if isinstance(snap.get("lock_removal_context"), dict) else {}
                ctx_for_log = dict(prior) if prior else {"rule": "R1", "label": trigger_reason}
                ctx_for_log["rule"] = "R1"
                ctx_for_log["signal_path"] = R1_PATH_PLAN_THEN_EMA10
                snap["lock_removal_context"] = ctx_for_log
                snap_json = json.dumps(snap)
                log_r1_early_warning(
                    db,
                    trade_id=str(tid),
                    symbol=sym,
                    session_date=session_date,
                    direction=t.get("direction") or "LONG",
                    context=ctx_for_log,
                    exit_trigger_reason=trigger_reason or "",
                    removed_at=_now(),
                    signal_path=R1_PATH_PLAN_THEN_EMA10,
                )

            alarm_param = None
            if entered_exit_now or entered_plan_exit:
                alarm_param = alarm_at if isinstance(alarm_at, datetime) else _parse_ts(alarm_at) or _now()

            if force_new_alarm and entered_exit_now:
                alarm_set_sql = "alarm_fired_at = :alarm"
            else:
                alarm_set_sql = "alarm_fired_at = COALESCE(alarm_fired_at, :alarm)"

            params = {
                "st": new_state,
                "sl": new_sl,
                "peak": peak,
                "tr": trigger_reason,
                "tpx": trigger_px,
                "alarm": alarm_param,
                "bar": bar_at or None,
                "id": tid,
            }
            if snap_json is not None:
                params["snap"] = snap_json
                db.execute(
                    text(
                        f"""
                        UPDATE kavach_checklist_trades SET
                            state = :st,
                            current_sl_price = :sl,
                            highest_rr_reached = :peak,
                            exit_trigger_reason = :tr,
                            exit_trigger_price = :tpx,
                            {alarm_set_sql},
                            last_eval_bar_at = COALESCE(:bar, last_eval_bar_at),
                            state_context_snapshot = CAST(:snap AS jsonb),
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    params,
                )
            else:
                db.execute(
                    text(
                        f"""
                        UPDATE kavach_checklist_trades SET
                            state = :st,
                            current_sl_price = :sl,
                            highest_rr_reached = :peak,
                            exit_trigger_reason = :tr,
                            exit_trigger_price = :tpx,
                            {alarm_set_sql},
                            last_eval_bar_at = COALESCE(:bar, last_eval_bar_at),
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    params,
                )
    return newly_exit



def closed_symbols_today(session_date: str) -> Dict[str, Dict[str, Any]]:
    """Feed same-day re-entry block from Panel closed trades (blocking reasons only)."""
    from backend.services.daily_checklist_chop_gates import (
        REENTRY_BLOCK_LABEL,
        exit_reason_blocks_reentry,
    )

    ensure_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(symbol) AS symbol, exit_reason, exit_time, direction,
                       realized_pnl_inr
                FROM kavach_checklist_trades
                WHERE session_date = CAST(:d AS date)
                  AND status = 'CLOSED'
                """
            ),
            {"d": session_date},
        ).fetchall()
        out = {}
        for r in rows:
            if not exit_reason_blocks_reentry(r.exit_reason):
                continue
            sym = str(r.symbol).upper()
            out[sym] = {
                "blocked": True,
                "exit_reason": r.exit_reason,
                "exit_time": str(r.exit_time) if r.exit_time else None,
                "direction": r.direction,
                "label": REENTRY_BLOCK_LABEL,
                "pnl_inr": _f(r.realized_pnl_inr),
                "source": "kavach_checklist_trades",
            }
        return out
    finally:
        db.close()


def open_symbols_today(session_date: str) -> Dict[str, str]:
    ensure_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(symbol) AS symbol, id
                FROM kavach_checklist_trades
                WHERE session_date = CAST(:d AS date) AND status = 'OPEN'
                """
            ),
            {"d": session_date},
        ).fetchall()
        return {str(r.symbol).upper(): r.id for r in rows}
    finally:
        db.close()
