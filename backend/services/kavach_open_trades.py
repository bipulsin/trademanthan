"""Kavach Open Trades — checklist Take Trade / manage / EXIT NOW alarm.

Single table ``kavach_checklist_trades`` (OPEN / CLOSED / CANCELLED) preserves id
for audit continuity. Edits go to ``kavach_checklist_trade_edits``.
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
STATE_EXIT_NOW = "EXIT_NOW"

STATUS_OPEN = "OPEN"
STATUS_CLOSED = "CLOSED"
STATUS_CANCELLED = "CANCELLED"

EXIT_REASONS = (
    "EMA10 reverse close (rule)",
    "EMA5 reverse close (profit protection)",
    "Risk cap exceeded",
    "Discretionary early exit",
    "15:15 square-off",
    "Session loss cap hit",
)

CANCEL_WINDOW_SEC = 120

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
                    was_cancelled BOOLEAN NOT NULL DEFAULT FALSE,
                    last_eval_bar_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
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
    """EMA5 / EMA10 / close from last confirmed 10m bar."""
    try:
        from backend.services.daily_checklist_snapshot import _load_candles_for_symbol
        from backend.services.kavach_10m import aggregate_10m_bars, last_closed_10m_pair_end_idx
        from backend.services.relative_strength_scanner import _sorted_candles
        from backend.services.vajra.indicators import ema_series

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
        return {
            "close": closes[-1],
            "ema5": ema5_s[-1] if ema5_s else None,
            "ema10": ema10_s[-1] if ema10_s else None,
            "bar_at": bar_ts,
        }
    except Exception as exc:
        logger.debug("10m levels failed %s: %s", symbol, exc)
        return {}


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

        # Same-day re-entry block
        blocked = db.execute(
            text(
                """
                SELECT id, exit_reason FROM kavach_checklist_trades
                WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :s
                  AND status = 'CLOSED' AND was_cancelled = FALSE
                ORDER BY exit_time DESC NULLS LAST LIMIT 1
                """
            ),
            {"d": sd, "s": sym},
        ).fetchone()
        if blocked:
            raise ValueError("SL hit earlier today · no re-entry regardless of direction")

        lot, _ = _lot_for_symbol(db, sym)
        px = entry_price if entry_price is not None else _live_price(db, sym)
        if px is None:
            levels = _levels_for_symbol(db, sym, sd)
            px = _f(levels.get("price"))
        if px is None:
            raise ValueError("Could not resolve entry price")

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
        initial_sl = abs(px - ema10) * lot if ema10 is not None else None
        tid = str(uuid.uuid4())
        ctx = dict(context or {})
        ctx.setdefault("confidence", levels.get("confidence_grade"))
        ctx.setdefault("market_regime", levels.get("market_regime"))

        db.execute(
            text(
                """
                INSERT INTO kavach_checklist_trades (
                    id, symbol, direction, entry_price, entry_time, entry_qty, session_date,
                    initial_ema10_at_entry, initial_sl_inr, state, current_sl_price,
                    highest_rr_reached, state_context_snapshot, status
                ) VALUES (
                    :id, :sym, :dir, :px, :et, :qty, CAST(:d AS date),
                    :ema10, :sl_inr, :state, :sl_px,
                    0, CAST(:ctx AS jsonb), 'OPEN'
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
            },
        )
        db.commit()
        return get_trade(tid)
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
                WHERE session_date = CAST(:d AS date) AND status IN ('CLOSED', 'CANCELLED')
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
    elif state == STATE_EXIT_NOW:
        hint = f"EXIT at current candle close · reason: {t.get('exit_trigger_reason') or '—'}"

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
            "can_cancel": _can_cancel(t),
        }
    )
    return t


def _can_cancel(t: Dict[str, Any]) -> bool:
    if t.get("status") != STATUS_OPEN:
        return False
    try:
        ct = t.get("created_at")
        if not ct:
            return False
        cdt = datetime.fromisoformat(str(ct).replace("Z", "+00:00"))
        if cdt.tzinfo is None:
            cdt = IST.localize(cdt)
        return (_now() - cdt.astimezone(IST)).total_seconds() <= CANCEL_WINDOW_SEC
    except Exception:
        return False


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


def cancel_trade(trade_id: str) -> Dict[str, Any]:
    ensure_tables()
    db = SessionLocal()
    try:
        r = db.execute(
            text("SELECT * FROM kavach_checklist_trades WHERE id = :id AND status = 'OPEN'"),
            {"id": trade_id},
        ).fetchone()
        if not r:
            raise ValueError("open trade not found")
        t = _row_to_dict(r)
        if not _can_cancel(t):
            raise ValueError("Cancel only allowed within 2 minutes of Take Trade")
        db.execute(
            text(
                """
                UPDATE kavach_checklist_trades
                SET status = 'CANCELLED', was_cancelled = TRUE,
                    exit_time = NOW(), exit_reason = 'Cancelled — no fill',
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": trade_id},
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
        # allow trigger reasons from state machine mapped to enum
        mapped = {
            "EMA10 reverse close": "EMA10 reverse close (rule)",
            "EMA5 reverse close after profit protection": "EMA5 reverse close (profit protection)",
            "Risk cap exceeded before 1:2": "Risk cap exceeded",
        }
        exit_reason = mapped.get(exit_reason, exit_reason)
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


def evaluate_open_trades(db, session_date: str) -> List[str]:
    """Candle-close state machine. Returns trade ids that newly entered EXIT_NOW."""
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
        bar = _confirmed_10m_levels(db, sym)
        if not bar.get("close"):
            continue
        bar_at = str(bar.get("bar_at") or "")
        last_eval = str(t.get("last_eval_bar_at") or "")
        # Only evaluate once per confirmed bar
        if bar_at and last_eval and bar_at == last_eval:
            # still update display SL from bar when trailing
            pass

        close = float(bar["close"])
        ema5 = _f(bar.get("ema5"))
        ema10 = _f(bar.get("ema10"))
        is_long = (t.get("direction") or "LONG").upper() != "SHORT"
        state = t.get("state") or STATE_TRAILING
        qty = int(t.get("entry_qty") or 1)
        entry = _f(t.get("entry_price"))
        initial_risk = _f(t.get("initial_sl_inr")) or 0

        pnl_inr = 0.0
        if entry is not None:
            pts = (close - entry) if is_long else (entry - close)
            pnl_inr = pts * qty
        rr = (pnl_inr / initial_risk) if initial_risk > 0 else 0.0
        peak = max(_f(t.get("highest_rr_reached")) or 0, rr)

        new_state = state
        new_sl = _f(t.get("current_sl_price"))
        trigger_reason = t.get("exit_trigger_reason")
        trigger_px = _f(t.get("exit_trigger_price"))
        alarm_at = t.get("alarm_fired_at")

        # Skip re-evaluation once EXIT_NOW
        if state != STATE_EXIT_NOW:
            if state == STATE_TRAILING:
                new_sl = ema10
                # Risk cap before 1:2
                if ema10 is not None and rr < RR_LOW:
                    risk_now = abs(close - ema10) * qty
                    if risk_now > MAX_INR_RISK:
                        new_state = STATE_EXIT_NOW
                        trigger_reason = "Risk cap exceeded before 1:2"
                        trigger_px = close
                # EMA10 reverse
                if new_state != STATE_EXIT_NOW and ema10 is not None:
                    beyond = (close < ema10) if is_long else (close > ema10)
                    if beyond:
                        new_state = STATE_EXIT_NOW
                        trigger_reason = "EMA10 reverse close"
                        trigger_px = close
                # Profit lock
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

        if new_state == STATE_EXIT_NOW and state != STATE_EXIT_NOW and not alarm_at:
            alarm_at = _now()
            newly_exit.append(tid)

        # Only write when bar advances or state changes
        if bar_at != last_eval or new_state != state or (new_sl != _f(t.get("current_sl_price"))):
            db.execute(
                text(
                    """
                    UPDATE kavach_checklist_trades SET
                        state = :st,
                        current_sl_price = :sl,
                        highest_rr_reached = :peak,
                        exit_trigger_reason = :tr,
                        exit_trigger_price = :tpx,
                        alarm_fired_at = COALESCE(alarm_fired_at, :alarm),
                        last_eval_bar_at = :bar,
                        updated_at = NOW()
                    WHERE id = :id
                    """
                ),
                {
                    "st": new_state,
                    "sl": new_sl,
                    "peak": peak,
                    "tr": trigger_reason,
                    "tpx": trigger_px,
                    "alarm": alarm_at if new_state == STATE_EXIT_NOW and state != STATE_EXIT_NOW else None,
                    "bar": bar_at or None,
                    "id": tid,
                },
            )
    return newly_exit


def closed_symbols_today(session_date: str) -> Dict[str, Dict[str, Any]]:
    """Feed same-day re-entry block."""
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
                  AND status = 'CLOSED' AND was_cancelled = FALSE
                """
            ),
            {"d": session_date},
        ).fetchall()
        out = {}
        for r in rows:
            sym = str(r.symbol).upper()
            out[sym] = {
                "blocked": True,
                "exit_reason": r.exit_reason,
                "exit_time": str(r.exit_time) if r.exit_time else None,
                "direction": r.direction,
                "label": "SL hit earlier today · no re-entry regardless of direction",
                "pnl_inr": _f(r.realized_pnl_inr),
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
