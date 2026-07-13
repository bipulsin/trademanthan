"""Trade-state columns for Daily RS Checklist (runbook STATE / Entry / SL / Risk / R:R).

Uses existing thresholds only:
  - EMA5 proximity = conviction ``convergence_atr`` (0.35 ATR)
  - Pullback expiry = ``expiry_atr`` (1.5 ATR) from intended entry (EMA5)
  - Max INR risk = ₹3,000 / lot (runbook)
  - ADX ready ≥ 25, recheck 20–25, blocked < 20
  - Confidence ≥ B; regime TREND or TRANSITION
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import bindparam, text

from backend.database import SessionLocal
from backend.services.rs_conviction_config import get_config
from backend.services.smart_futures_picker.position_sizing import (
    get_futures_lot_size_by_instrument_key,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

MAX_INR_RISK = 3000.0
ADX_READY = 25.0
ADX_MIN = 20.0
RR_LOW = 2.0

STATE_READY = "READY"
STATE_READY_RECHECK = "READY(RECHECK)"
STATE_WAIT = "WAIT FOR PULLBACK"
STATE_EXPIRED = "EXPIRED"
STATE_BLOCKED = "BLOCKED"

_STATE_SORT = {
    STATE_READY: 0,
    STATE_READY_RECHECK: 1,
    STATE_WAIT: 2,
    STATE_EXPIRED: 3,
    STATE_BLOCKED: 4,
}

_GRADE_RANK = {"A+": 0, "A": 1, "B": 2, "C": 3, "C*": 3, "D": 4}


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _norm_grade(raw: Optional[str]) -> str:
    g = (raw or "").strip().upper().replace("*", "")
    if g.startswith("A+"):
        return "A+"
    if g.startswith("A"):
        return "A"
    if g.startswith("B"):
        return "B"
    if g.startswith("C"):
        return "C"
    if g.startswith("D"):
        return "D"
    return g or ""


def _grade_ok(grade: str) -> bool:
    return grade in ("A+", "A", "B")


def _regime_ok(regime: Optional[str]) -> bool:
    r = (regime or "").strip().upper()
    return r in ("TREND", "TRANSITION")


def _lot_for_symbol(db, symbol: str) -> Tuple[int, Optional[str]]:
    row = db.execute(
        text(
            """
            SELECT currmth_future_instrument_key AS ikey
            FROM arbitrage_master
            WHERE UPPER(stock) = :s
            LIMIT 1
            """
        ),
        {"s": symbol.upper()},
    ).fetchone()
    ikey = row.ikey if row else None
    if not ikey:
        return 1, None
    lot = get_futures_lot_size_by_instrument_key(ikey)
    return max(int(lot or 1), 1), ikey


def _load_price_levels(db, symbols: List[str], session_date: str) -> Dict[str, Dict[str, Any]]:
    """Prefer latest live audit bar; fall back to latest RS snapshot for the day."""
    out: Dict[str, Dict[str, Any]] = {}
    if not symbols:
        return out
    syms = [s.upper() for s in symbols]
    try:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT ON (UPPER(symbol))
                       UPPER(symbol) AS symbol, price, ema5, ema10, vwap, adx,
                       confidence_grade, market_regime, bar_evaluated_at
                FROM rs_live_kavach_audit
                WHERE session_date = CAST(:d AS date)
                  AND UPPER(symbol) IN :syms
                ORDER BY UPPER(symbol), bar_evaluated_at DESC, id DESC
                """
            ).bindparams(bindparam("syms", expanding=True)),
            {"d": session_date, "syms": syms},
        ).fetchall()
        for r in rows:
            out[str(r.symbol).upper()] = {
                "price": _f(r.price),
                "ema5": _f(r.ema5),
                "ema10": _f(r.ema10),
                "vwap": _f(r.vwap),
                "adx": _f(r.adx),
                "confidence_grade": r.confidence_grade,
                "market_regime": r.market_regime,
                "source": "audit",
            }
    except Exception as exc:
        logger.debug("trade-state audit levels skipped: %s", exc)

    missing = [s for s in syms if s not in out]
    if missing:
        try:
            rows = db.execute(
                text(
                    """
                    SELECT DISTINCT ON (UPPER(s.symbol))
                           UPPER(s.symbol) AS symbol, s.current_price, s.ema5, s.ema10, s.vwap,
                           s.adx, s.confidence_grade, s.market_regime
                    FROM relative_strength_snapshot s
                    WHERE s.scan_time::date = CAST(:d AS date)
                      AND UPPER(s.symbol) IN :syms
                    ORDER BY UPPER(s.symbol), s.scan_time DESC
                    """
                ).bindparams(bindparam("syms", expanding=True)),
                {"d": session_date, "syms": missing},
            ).fetchall()
            for r in rows:
                out[str(r.symbol).upper()] = {
                    "price": _f(r.current_price),
                    "ema5": _f(r.ema5),
                    "ema10": _f(r.ema10),
                    "vwap": _f(r.vwap),
                    "adx": _f(r.adx),
                    "confidence_grade": r.confidence_grade,
                    "market_regime": r.market_regime,
                    "source": "rs_snapshot",
                }
        except Exception as exc:
            logger.debug("trade-state RS levels skipped: %s", exc)
    return out


def _load_atr_map(db, symbols: List[str]) -> Dict[str, float]:
    atr_map: Dict[str, float] = {}
    try:
        from backend.services.rs_conviction_candles import load_instrument_atr_maps

        _, pct_map = load_instrument_atr_maps(db, set(symbols))
        for sym, pct in (pct_map or {}).items():
            atr_map[str(sym).upper()] = float(pct or 0.0)
    except Exception as exc:
        logger.debug("trade-state ATR map skipped: %s", exc)
    return atr_map


def _session_hi_lo(db, symbol: str, session_date: str) -> Tuple[Optional[float], Optional[float]]:
    """Nearest S/R proxy: session high / low from today's candles (via cache/Upstox)."""
    try:
        from backend.services.daily_checklist_snapshot import _load_candles_for_symbol
        from backend.services.relative_strength_scanner import _parse_ist_date, _sorted_candles

        candles = _load_candles_for_symbol(db, symbol)
        if not candles:
            return None, None
        candles = _sorted_candles(candles)
        hi = lo = None
        for c in candles:
            d = _parse_ist_date(c.get("timestamp"))
            if not d or str(d) != session_date:
                continue
            h = _f(c.get("high"))
            l = _f(c.get("low"))
            if h is not None:
                hi = h if hi is None else max(hi, h)
            if l is not None:
                lo = l if lo is None else min(lo, l)
        return hi, lo
    except Exception as exc:
        logger.debug("session hi/lo skipped for %s: %s", symbol, exc)
        return None, None


def _open_positions(db, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Map underlying → open daily_futures_user_trade (any user, order_status=bought)."""
    out: Dict[str, Dict[str, Any]] = {}
    if not symbols:
        return out
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(t.underlying) AS underlying, t.direction_type, t.entry_price,
                       t.lot_size, t.instrument_key, t.entry_time, t.peak_unrealized_pnl_rupees
                FROM daily_futures_user_trade t
                WHERE t.order_status = 'bought'
                  AND UPPER(t.underlying) IN :syms
                ORDER BY t.entry_time DESC NULLS LAST
                """
            ).bindparams(bindparam("syms", expanding=True)),
            {"syms": [s.upper() for s in symbols]},
        ).fetchall()
        for r in rows:
            sym = str(r.underlying).upper()
            if sym in out:
                continue
            out[sym] = {
                "direction": (r.direction_type or "").upper(),
                "entry_price": _f(r.entry_price),
                "lot_size": int(r.lot_size or 1),
                "instrument_key": r.instrument_key,
                "peak_unrealized_pnl_rupees": _f(r.peak_unrealized_pnl_rupees),
            }
    except Exception as exc:
        logger.debug("open positions lookup skipped: %s", exc)
    return out


def _fmt_ist(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    try:
        if getattr(dt, "tzinfo", None):
            return dt.astimezone(IST).isoformat()
        return str(dt)
    except Exception:
        return str(dt)


def _promotion_meta(db, session_date: str, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """promoted_at (latest intraday entry), cycles today, last remove rule."""
    out: Dict[str, Dict[str, Any]] = {
        s.upper(): {"promoted_at": None, "cycles": 0, "last_remove_rule": None}
        for s in symbols
    }
    if not symbols:
        return out
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(symbol) AS symbol, event_type, rule, event_at, direction
                FROM rs_lock_membership_audit
                WHERE session_date = CAST(:d AS date)
                  AND UPPER(symbol) IN :syms
                ORDER BY event_at, id
                """
            ).bindparams(bindparam("syms", expanding=True)),
            {"d": session_date, "syms": [s.upper() for s in symbols]},
        ).fetchall()
        entries: Dict[str, int] = {}
        removes: Dict[str, int] = {}
        for r in rows:
            sym = str(r.symbol).upper()
            meta = out.setdefault(sym, {"promoted_at": None, "cycles": 0, "last_remove_rule": None})
            et = (r.event_type or "").lower()
            rule = (r.rule or "").lower()
            if et == "entry":
                entries[sym] = entries.get(sym, 0) + 1
                # Show latest intraday promote time (incl. re-entry after R1/R2 remove)
                if rule == "intraday_2scan" and r.event_at is not None:
                    meta["promoted_at"] = _fmt_ist(r.event_at)
            elif et == "remove":
                removes[sym] = removes.get(sym, 0) + 1
                meta["last_remove_rule"] = (r.rule or "").upper() or None
        for sym, meta in out.items():
            e = entries.get(sym, 0)
            rm = removes.get(sym, 0)
            # Completed ENTRY→REMOVE cycles; >1 is churn
            meta["cycles"] = min(e, rm)
    except Exception as exc:
        logger.debug("promotion meta skipped: %s", exc)
    return out


def _recent_removals(db, session_date: str, limit: int = 12) -> List[Dict[str, Any]]:
    """Session REMOVE events (R1/R2) for a history strip — not on checklist anymore."""
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(symbol) AS symbol, rule, event_at, direction
                FROM rs_lock_membership_audit
                WHERE session_date = CAST(:d AS date)
                  AND LOWER(event_type) = 'remove'
                ORDER BY event_at DESC, id DESC
                LIMIT :lim
                """
            ),
            {"d": session_date, "lim": limit},
        ).fetchall()
        out = []
        for r in rows:
            rule = (r.rule or "").upper() or "—"
            tag = rule if rule in ("R1", "R2") else rule[:8]
            out.append(
                {
                    "symbol": str(r.symbol).upper(),
                    "rule_tag": tag,
                    "rule": rule,
                    "direction": r.direction,
                    "at": _fmt_ist(r.event_at),
                }
            )
        return out
    except Exception as exc:
        logger.debug("recent removals skipped: %s", exc)
        return []


def compute_trade_state_for_stock(
    stock: Dict[str, Any],
    *,
    levels: Dict[str, Any],
    atr_pct: float,
    lot: int,
    session_hi: Optional[float],
    session_lo: Optional[float],
    open_pos: Optional[Dict[str, Any]],
    promo: Optional[Dict[str, Any]],
    cfg: Optional[Dict[str, Any]] = None,
    market_regime_idx: Optional[str] = None,
    direction_unstable: bool = False,
    unstable_reason: Optional[str] = None,
    whipsaw_count: int = 0,
    pullback_count: int = 0,
    stopped: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = cfg or get_config()
    near_atr = float(cfg.get("convergence_atr") or 0.35)
    expiry_atr = float(cfg.get("expiry_atr") or 1.5)

    direction = (stock.get("direction") or "LONG").upper()
    is_long = direction != "SHORT"

    grade = _norm_grade(
        levels.get("confidence_grade") or stock.get("confidence") or stock.get("dashboard_kavach")
    )
    regime = levels.get("market_regime") or stock.get("market_regime")
    adx = _f(levels.get("adx")) or _f(stock.get("adx_entry")) or _f(stock.get("adx_935"))
    price = _f(levels.get("price"))
    ema5 = _f(levels.get("ema5"))
    ema10 = _f(levels.get("ema10"))
    vwap = _f(levels.get("vwap"))

    atr = None
    if price is not None and atr_pct and atr_pct > 0:
        atr = price * atr_pct / 100.0

    # Intended entry / pullback level
    pullback_level = None
    if ema5 is not None and vwap is not None and price is not None:
        pullback_level = ema5 if abs(price - ema5) <= abs(price - vwap) else vwap
    elif ema5 is not None:
        pullback_level = ema5
    elif vwap is not None:
        pullback_level = vwap

    entry_ready = ema5  # READY uses EMA5 as limit
    sl_price = ema10

    dist_ema5_atr = None
    if price is not None and ema5 is not None and atr and atr > 0:
        dist_ema5_atr = abs(price - ema5) / atr

    dist_entry_atr = None
    intended = entry_ready if entry_ready is not None else pullback_level
    if price is not None and intended is not None and atr and atr > 0:
        dist_entry_atr = abs(price - intended) / atr

    near_ema5 = dist_ema5_atr is not None and dist_ema5_atr <= near_atr
    expired_move = dist_entry_atr is not None and dist_entry_atr > expiry_atr

    risk_pts_ready = None
    risk_inr_ready = None
    if entry_ready is not None and sl_price is not None:
        risk_pts_ready = abs(entry_ready - sl_price)
        risk_inr_ready = round(risk_pts_ready * max(lot, 1), 0)

    risk_pts_pb = None
    risk_inr_pb = None
    if pullback_level is not None and sl_price is not None:
        risk_pts_pb = abs(pullback_level - sl_price)
        risk_inr_pb = round(risk_pts_pb * max(lot, 1), 0)

    block_reasons: List[str] = []
    if not _grade_ok(grade):
        block_reasons.append(f"conf {grade or '—'}")
    if not _regime_ok(regime):
        block_reasons.append(f"regime {(regime or '—')}")
    if adx is not None and adx < ADX_MIN:
        block_reasons.append(f"ADX {adx:.0f}")
    elif adx is None:
        block_reasons.append("ADX —")

    risk_for_block = risk_inr_pb if risk_inr_pb is not None else risk_inr_ready
    if risk_for_block is not None and risk_for_block > MAX_INR_RISK:
        block_reasons.append(f"risk ₹{int(risk_for_block):,}")

    state = STATE_BLOCKED
    blocked_reason = None
    entry_price = None
    display_risk = risk_inr_ready

    if block_reasons:
        state = STATE_BLOCKED
        blocked_reason = "BLOCKED · " + ", ".join(block_reasons)
        entry_price = None
    elif expired_move:
        state = STATE_EXPIRED
        entry_price = None
    elif near_ema5 and risk_inr_ready is not None and risk_inr_ready <= MAX_INR_RISK:
        if adx is not None and ADX_MIN <= adx < ADX_READY:
            state = STATE_READY_RECHECK
        else:
            state = STATE_READY
        entry_price = round(entry_ready, 2) if entry_ready is not None else None
        display_risk = risk_inr_ready
    else:
        state = STATE_WAIT
        entry_price = round(pullback_level, 2) if pullback_level is not None else None
        display_risk = risk_inr_pb if risk_inr_pb is not None else risk_inr_ready

    # Chop / whipsaw / flip / re-entry / pullback gates
    from backend.services.daily_checklist_chop_gates import apply_state_downgrades

    gated_state, gate_reason, gate_badges = apply_state_downgrades(
        state=state,
        market_regime=market_regime_idx or "",
        direction_unstable=direction_unstable,
        unstable_reason=unstable_reason,
        whipsaw_count=whipsaw_count,
        pullback_count=pullback_count,
        stopped=stopped,
    )
    if gated_state != state or gate_reason:
        state = gated_state
        if gate_reason:
            blocked_reason = gate_reason
        if state in (STATE_BLOCKED, STATE_EXPIRED):
            entry_price = None if state == STATE_BLOCKED else entry_price
            if state == STATE_BLOCKED:
                entry_price = None
        elif state == STATE_WAIT and entry_price is None and pullback_level is not None:
            entry_price = round(pullback_level, 2)
            display_risk = risk_inr_pb if risk_inr_pb is not None else risk_inr_ready

    sl_out = round(sl_price, 2) if sl_price is not None else None

    rr = None
    rr_low = False
    if entry_price is not None and sl_out is not None:
        risk = abs(entry_price - sl_out)
        if risk > 0:
            if is_long and session_hi is not None and session_hi > entry_price:
                reward = session_hi - entry_price
                rr = round(reward / risk, 1)
            elif (not is_long) and session_lo is not None and session_lo < entry_price:
                reward = entry_price - session_lo
                rr = round(reward / risk, 1)
            if rr is not None and rr < RR_LOW:
                rr_low = True

    # Position trail + optional PROFIT LOCKED (EMA5 alt exit) — display only
    trail = None
    if open_pos:
        pos_dir = (open_pos.get("direction") or direction).upper()
        pos_long = pos_dir != "SHORT"
        pos_entry = _f(open_pos.get("entry_price"))
        pos_lot = int(open_pos.get("lot_size") or lot)
        open_pnl = None
        if price is not None and pos_entry is not None:
            pts = (price - pos_entry) if pos_long else (pos_entry - price)
            open_pnl = round(pts * pos_lot, 0)
        trail_sl = sl_out
        book = False
        book_reason = None
        if price is not None and sl_out is not None:
            beyond = (price < sl_out) if pos_long else (price > sl_out)
            if beyond:
                book = True
                book_reason = "EMA10 close"
            else:
                cur_risk = abs(price - sl_out) * pos_lot
                if cur_risk > MAX_INR_RISK and (rr is None or rr < RR_LOW):
                    book = True
                    book_reason = f"risk ₹{int(cur_risk):,}"

        entry_risk_inr = None
        if pos_entry is not None and sl_out is not None:
            entry_risk_inr = abs(pos_entry - sl_out) * pos_lot
        peak_pnl = _f(open_pos.get("peak_unrealized_pnl_rupees"))
        fav = max(open_pnl or 0, peak_pnl or 0)
        profit_locked = bool(
            entry_risk_inr and entry_risk_inr > 0 and fav >= RR_LOW * entry_risk_inr
        )
        alt_exit = round(ema5, 2) if profit_locked and ema5 is not None else None

        trail = {
            "trail_state": "BOOK-NOW" if book else ("PROFIT LOCKED" if profit_locked else "HOLD"),
            "trail_reason": book_reason or ("≥1:2 — consider EMA5 reverse close" if profit_locked else None),
            "open_pnl_inr": open_pnl,
            "trail_sl": trail_sl,
            "profit_locked": profit_locked,
            "alt_exit_ema5": alt_exit,
            "entry_risk_inr": int(entry_risk_inr) if entry_risk_inr is not None else None,
        }

    pb_label = None
    if pullback_count >= 3:
        pb_label = f"{pullback_count}th+ pullback"
    elif pullback_count == 1:
        pb_label = "1st pullback"
    elif pullback_count == 2:
        pb_label = "2nd pullback"

    return {
        "trade_state": state,
        "trade_state_reason": blocked_reason,
        "trade_entry": entry_price,
        "trade_sl": sl_out,
        "trade_risk_inr": int(display_risk) if display_risk is not None else None,
        "trade_risk_over": bool(display_risk is not None and display_risk > MAX_INR_RISK),
        "trade_rr": rr,
        "trade_rr_low": rr_low,
        "trade_rr_label": (f"1:{rr}" if rr is not None else None),
        "trade_adx": round(adx, 1) if adx is not None else None,
        "trade_lot": lot,
        "trade_levels_source": levels.get("source"),
        "promoted_at": (promo or {}).get("promoted_at"),
        "lock_cycles": int((promo or {}).get("cycles") or 0),
        "position": trail,
        "whipsaw_count": whipsaw_count,
        "pullback_count": pullback_count,
        "pullback_label": pb_label,
        "direction_unstable": bool(direction_unstable),
        "gate_badges": gate_badges,
        "stopped_out_today": bool(stopped and stopped.get("blocked")),
    }


def enrich_stocks_trade_state(
    stocks: List[Dict[str, Any]],
    session_date: str,
) -> Dict[str, Any]:
    """Mutate stocks in place with trade-state fields; return observation summary."""
    empty_obs = {
        "churn_warning": False,
        "churn_symbols": [],
        "churn_count": 0,
        "recent_removals": [],
        "market_regime": None,
        "market_regime_label": None,
        "exit_rule_reminder": "Exit rule: 10m close beyond EMA10 reverse — not VWAP break",
    }
    if not stocks:
        return empty_obs

    symbols = [s["symbol"] for s in stocks if s.get("symbol")]
    db = SessionLocal()
    try:
        from backend.services.daily_checklist_chop_gates import (
            compute_market_regime,
            count_pullback_attempts,
            count_whipsaw_reversals,
            direction_unstable_flags,
            stopped_out_today,
        )
        from backend.services.daily_checklist_snapshot import _load_candles_for_symbol

        cfg = get_config()
        near_atr = float(cfg.get("convergence_atr") or 0.35)
        levels_map = _load_price_levels(db, symbols, session_date)
        atr_pct_map = _load_atr_map(db, symbols)
        positions = _open_positions(db, symbols)
        promo = _promotion_meta(db, session_date, symbols)
        removals = _recent_removals(db, session_date)
        mkt = compute_market_regime(session_date)
        flips = direction_unstable_flags(
            db,
            session_date,
            symbols,
            current_dirs={s["symbol"]: s.get("direction") for s in stocks if s.get("symbol")},
        )
        stopped_map = stopped_out_today(db, session_date, symbols)

        hi_lo: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
        candle_cache: Dict[str, Any] = {}
        for sym in symbols[:25]:
            hi_lo[sym.upper()] = _session_hi_lo(db, sym, session_date)
            try:
                candle_cache[sym.upper()] = _load_candles_for_symbol(db, sym) or []
            except Exception:
                candle_cache[sym.upper()] = []

        lot_cache: Dict[str, int] = {}
        for s in stocks:
            sym = (s.get("symbol") or "").upper()
            if not sym:
                continue
            if sym not in lot_cache:
                lot_cache[sym], _ = _lot_for_symbol(db, sym)
            hi, lo = hi_lo.get(sym, (None, None))
            price = _f((levels_map.get(sym) or {}).get("price"))
            atr_pct = float(atr_pct_map.get(sym) or 0.0)
            atr = (price * atr_pct / 100.0) if price and atr_pct > 0 else None
            is_long = (s.get("direction") or "LONG").upper() != "SHORT"
            candles = candle_cache.get(sym) or []
            whip = count_whipsaw_reversals(
                candles, session_date=session_date, is_long=is_long, near_atr=near_atr, atr=atr
            ) if candles else 0
            pb = count_pullback_attempts(
                candles, session_date=session_date, is_long=is_long, near_atr=near_atr, atr=atr
            ) if candles else 0
            flip = flips.get(sym) or {}
            ts = compute_trade_state_for_stock(
                s,
                levels=levels_map.get(sym) or {},
                atr_pct=atr_pct,
                lot=lot_cache[sym],
                session_hi=hi,
                session_lo=lo,
                open_pos=positions.get(sym),
                promo=promo.get(sym),
                cfg=cfg,
                market_regime_idx=mkt.get("market_regime"),
                direction_unstable=bool(flip.get("unstable")),
                unstable_reason=flip.get("reason"),
                whipsaw_count=whip,
                pullback_count=pb,
                stopped=stopped_map.get(sym),
            )
            s.update(ts)

        churn_syms = [s["symbol"] for s in stocks if int(s.get("lock_cycles") or 0) > 1]
        return {
            "churn_warning": len(churn_syms) >= 3,
            "churn_symbols": churn_syms,
            "churn_count": len(churn_syms),
            "recent_removals": removals,
            **mkt,
        }
    finally:
        db.close()


def sort_stocks_by_trade_state(
    stocks: List[Dict[str, Any]],
    rank_map: Optional[Dict[str, Tuple[int, int]]] = None,
) -> List[Dict[str, Any]]:
    rank_map = rank_map or {}

    def key(s: Dict[str, Any]) -> Tuple:
        st = s.get("trade_state") or STATE_BLOCKED
        state_i = _STATE_SORT.get(st, 9)
        grade = _norm_grade(s.get("confidence") or s.get("dashboard_kavach"))
        grade_i = _GRADE_RANK.get(grade, 9)
        sym = s.get("symbol") or ""
        rs_rank = rank_map.get(sym, (0, 99))[1]
        return (state_i, grade_i, rs_rank, sym)

    return sorted(stocks, key=key)
