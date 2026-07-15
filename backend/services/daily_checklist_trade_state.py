"""Trade-state columns for Daily RS Checklist (runbook STATE / Entry / SL / Risk / R:R).

Uses existing thresholds only:
  - EMA5 proximity = conviction ``convergence_atr`` (0.35 ATR)
  - Pullback expiry = ``expiry_atr`` (1.5 ATR) from intended entry (EMA5)
  - Max INR risk = ₹3,000 / lot (runbook)
  - ADX ready ≥ 25, recheck 20–25, blocked < 20
  - Confidence ≥ B; regime TREND or TRANSITION

READY NOW note (2026-07-14 / 2026-07-15):
  ``compute_trade_state_for_stock`` historically set READY from live levels /
  ADX / proximity alone — it did **not** check ``daily_snapshot`` lock
  membership. Display list is filtered to locked symbols in ``get_state``, but
  that is a separate source of truth from the badge math. Consistency is logged
  every refresh; VWAP-quality weighting of READY is behind
  ``READY_VWAP_QUALITY_GATE`` (default off / shadow).

  2026-07-15 gates:
  - No READY / Take Trade before 09:45 IST (SCANNING until then).
  - Entry outside today's session high/low → EXPIRED (stale/gap).
  - Entry must be live EMA5 (±ENTRY_EMA5_TOL_PCT); no VWAP-blend limit.
  - Missing SL / Risk → not READY; Take Trade disabled.
  - Risk > ₹3k with R:R < 1:2 → BLOCKED (hard). R:R ≥ 1:2 → cap waived label.
  - Live Trend/Supertrend/MACD ≥2-of-3 oppose lock direction → WAIT (DIR CONFLICT).
    Root cause: sticky daily_snapshot direction (no swap after f11e1b7) vs live
    10m Kavach fields; READY historically ignored that disagreement.
  - ATR consumed (from open + from opening range) logged for research only —
    same atr14 used for 1.5× expiry; never gates READY.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import bindparam, text

from backend.database import SessionLocal, engine
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
# READY entry must sit within this % of live EMA5 (stale / wrong-anchor guard).
ENTRY_EMA5_TOL_PCT = 0.5

STATE_READY = "READY"
STATE_READY_RECHECK = "READY(RECHECK)"
STATE_WAIT = "WAIT FOR PULLBACK"
STATE_SCANNING = "SCANNING"
STATE_EXPIRED = "EXPIRED"
STATE_BLOCKED = "BLOCKED"

# Hard entry window (IST): READY NOW / Take Trade only from 09:45.
ENTRY_START_MIN = 9 * 60 + 45
ENTRY_END_MIN = 14 * 60 + 30

_READY_LOG_ENSURED = False


def entry_window_open_ist(now: Optional[datetime] = None) -> bool:
    """True when READY NOW / Take Trade are allowed (09:45–14:30 IST)."""
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    m = now.hour * 60 + now.minute
    return ENTRY_START_MIN <= m <= ENTRY_END_MIN


def before_entry_window_ist(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    return (now.hour * 60 + now.minute) < ENTRY_START_MIN


def entry_outside_session_range(
    *,
    is_long: bool,
    entry: Optional[float],
    session_hi: Optional[float],
    session_lo: Optional[float],
) -> bool:
    """True when limit entry is outside today's traded range (untouchable / stale).

    LONG: entry below today's low · SHORT: entry above today's high.
    """
    if entry is None:
        return False
    if is_long and session_lo is not None and entry < session_lo:
        return True
    if (not is_long) and session_hi is not None and entry > session_hi:
        return True
    return False


def entry_off_live_ema5(
    entry: Optional[float],
    ema5: Optional[float],
    *,
    tol_pct: float = ENTRY_EMA5_TOL_PCT,
) -> bool:
    """True when entry is missing EMA5 or outside ±tol_pct of live EMA5."""
    if entry is None or ema5 is None:
        return True
    if abs(ema5) < 1e-9:
        return True
    return abs(entry - ema5) / abs(ema5) * 100.0 > float(tol_pct)


def session_rr(
    *,
    is_long: bool,
    entry: Optional[float],
    sl: Optional[float],
    session_hi: Optional[float],
    session_lo: Optional[float],
) -> Optional[float]:
    """R:R to session extreme from entry/SL (same definition used on cards)."""
    if entry is None or sl is None:
        return None
    risk = abs(entry - sl)
    if risk <= 0:
        return None
    if is_long and session_hi is not None and session_hi > entry:
        return round((session_hi - entry) / risk, 1)
    if (not is_long) and session_lo is not None and session_lo < entry:
        return round((entry - session_lo) / risk, 1)
    return None


def risk_cap_blocks_ready(
    risk_inr: Optional[float],
    rr: Optional[float],
) -> bool:
    """Hard block when INR risk > cap and R:R is missing or below 1:2."""
    if risk_inr is None or risk_inr <= MAX_INR_RISK:
        return False
    return rr is None or rr < RR_LOW


def take_trade_structurally_ok(
    *,
    entry: Optional[float],
    sl: Optional[float],
    risk_inr: Optional[float],
) -> bool:
    """Take Trade needs a computable entry, stop, and INR risk."""
    return entry is not None and sl is not None and risk_inr is not None


def live_momentum_sides(
    *,
    ema_vs_vwap: Optional[str] = None,
    supertrend: Optional[str] = None,
    macd: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Map checklist live labels → BULL / BEAR / None (neutral/unknown).

    Same three fields shown on the TradingView Kavach panel (Trend≈EMA5 vs VWAP,
    Supertrend, MACD). Crossing / At VWAP count as neutral, not a vote.
    """
    ema = (ema_vs_vwap or "").strip().lower()
    st = (supertrend or "").strip().lower()
    md = (macd or "").strip().lower()

    ema_side = None
    if ema == "above":
        ema_side = "BULL"
    elif ema == "below":
        ema_side = "BEAR"

    st_side = None
    if st == "bullish":
        st_side = "BULL"
    elif st == "bearish":
        st_side = "BEAR"

    macd_side = None
    if md == "bullish":
        macd_side = "BULL"
    elif md == "bearish":
        macd_side = "BEAR"
    # "crossing" → neutral

    return {"ema_vs_vwap": ema_side, "supertrend": st_side, "macd": macd_side}


def overlay_live_momentum_from_candles(
    stock: Dict[str, Any],
    candles: Optional[List[Any]],
    *,
    nifty_pct: float = 0.0,
) -> Dict[str, Optional[str]]:
    """Refresh Trend/ST/MACD labels from closed 10m candles (every enrich poll).

    Checklist DB rows can lag the live recompute path; conflict checks must not
    depend on sticky morning labels. Mutates stock in place when metrics resolve.
    Returns the labels used (overlay or prior).
    """
    prior = {
        "ema_vs_vwap": stock.get("ema_vs_vwap"),
        "supertrend": stock.get("supertrend"),
        "macd": stock.get("macd"),
    }
    if not candles:
        return prior
    try:
        from backend.services.daily_checklist import (
            _ema_vs_vwap_label,
            _macd_label,
            _supertrend_label,
        )
        from backend.services.kavach_10m import metrics_from_10m_candles
        from backend.services.relative_strength_scanner import RANKING_BEARISH, RANKING_BULLISH

        direction = (stock.get("direction") or "LONG").upper()
        ranking = RANKING_BEARISH if direction == "SHORT" else RANKING_BULLISH
        metrics = metrics_from_10m_candles(
            candles, ranking_type=ranking, nifty_pct=float(nifty_pct or 0.0)
        )
        if not metrics:
            return prior
        ema_lbl = _ema_vs_vwap_label(_f(metrics.get("ema5")), _f(metrics.get("vwap")))
        st_lbl = _supertrend_label(_f(metrics.get("supertrend")))
        macd_lbl = _macd_label(
            _f(metrics.get("macd")),
            _f(metrics.get("macd_signal")),
            _f(metrics.get("macd_histogram")),
        )
        if ema_lbl is not None:
            stock["ema_vs_vwap"] = ema_lbl
        if st_lbl is not None:
            stock["supertrend"] = st_lbl
        if macd_lbl is not None:
            stock["macd"] = macd_lbl
        if metrics.get("kavach_state"):
            stock["dashboard_kavach_live"] = metrics.get("kavach_state")
        return {
            "ema_vs_vwap": stock.get("ema_vs_vwap"),
            "supertrend": stock.get("supertrend"),
            "macd": stock.get("macd"),
        }
    except Exception as exc:
        logger.debug("live momentum overlay skipped for %s: %s", stock.get("symbol"), exc)
        return prior


def direction_live_conflict(
    *,
    direction: str,
    ema_vs_vwap: Optional[str] = None,
    supertrend: Optional[str] = None,
    macd: Optional[str] = None,
) -> Dict[str, Any]:
    """Compare sticky checklist lock direction vs live Trend/ST/MACD.

    Returns conflict count (0–3), opposing field names, and live lean.
    ≥2 opposing votes → suppress READY (caller). Does not flip lock direction.
    """
    is_long = (direction or "LONG").upper() != "SHORT"
    expect = "BULL" if is_long else "BEAR"
    sides = live_momentum_sides(
        ema_vs_vwap=ema_vs_vwap, supertrend=supertrend, macd=macd
    )
    opposing: List[str] = []
    agreeing: List[str] = []
    for name, side in sides.items():
        if side is None:
            continue
        if side != expect:
            opposing.append(name)
        else:
            agreeing.append(name)

    bull_votes = sum(1 for s in sides.values() if s == "BULL")
    bear_votes = sum(1 for s in sides.values() if s == "BEAR")
    live_lean = None
    if bull_votes >= 2 and bull_votes > bear_votes:
        live_lean = "Bullish"
    elif bear_votes >= 2 and bear_votes > bull_votes:
        live_lean = "Bearish"

    n = len(opposing)
    checklist_dir = "LONG" if is_long else "SHORT"
    reason = None
    if n >= 2:
        fields = "+".join(
            {
                "ema_vs_vwap": "Trend",
                "supertrend": "Supertrend",
                "macd": "MACD",
            }.get(f, f)
            for f in opposing
        )
        reason = (
            f"direction conflict: checklist {checklist_dir} vs live "
            f"{live_lean or 'opposing'} {fields}"
        )
    return {
        "conflict_count": n,
        "opposing_fields": opposing,
        "agreeing_fields": agreeing,
        "live_lean": live_lean,
        "checklist_direction": checklist_dir,
        "suppress_ready": n >= 2,
        "reason": reason,
        "sides": sides,
    }


def ensure_ready_consistency_log() -> None:
    global _READY_LOG_ENSURED
    if _READY_LOG_ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_ready_consistency_log (
                    id SERIAL PRIMARY KEY,
                    session_date DATE NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    direction VARCHAR(8),
                    rendered_state VARCHAR(32),
                    pre_gate_state VARCHAR(32),
                    in_lock BOOLEAN,
                    lock_rank INTEGER,
                    lock_direction VARCHAR(16),
                    lock_mismatch BOOLEAN,
                    vwap_slope_score NUMERIC(12,4),
                    steep_ok BOOLEAN,
                    flip_flop BOOLEAN,
                    whipsaw_crosses INTEGER,
                    quality_pass BOOLEAN,
                    vwap_gate_enabled BOOLEAN,
                    vwap_would_block BOOLEAN,
                    vwap_gate_applied BOOLEAN,
                    inputs JSONB,
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_ready_consistency_session
                ON kavach_ready_consistency_log (session_date, symbol)
                """
            )
        )
    _READY_LOG_ENSURED = True


def log_ready_consistency(db, rows: List[Dict[str, Any]]) -> None:
    """Background diagnostic — not user-facing. Best-effort per refresh."""
    if not rows:
        return
    try:
        ensure_ready_consistency_log()
        for r in rows:
            db.execute(
                text(
                    """
                    INSERT INTO kavach_ready_consistency_log (
                        session_date, symbol, direction,
                        rendered_state, pre_gate_state,
                        in_lock, lock_rank, lock_direction, lock_mismatch,
                        vwap_slope_score, steep_ok, flip_flop, whipsaw_crosses,
                        quality_pass, vwap_gate_enabled, vwap_would_block,
                        vwap_gate_applied, inputs
                    ) VALUES (
                        CAST(:d AS date), :sym, :dir,
                        :rst, :pst,
                        :il, :lr, :ld, :mm,
                        :vs, :so, :ff, :wc,
                        :qp, :vge, :vwb,
                        :vga, CAST(:inp AS jsonb)
                    )
                    """
                ),
                {
                    "d": r.get("session_date"),
                    "sym": r.get("symbol"),
                    "dir": r.get("direction"),
                    "rst": r.get("rendered_state"),
                    "pst": r.get("pre_gate_state"),
                    "il": r.get("in_lock"),
                    "lr": r.get("lock_rank"),
                    "ld": r.get("lock_direction"),
                    "mm": r.get("lock_mismatch"),
                    "vs": r.get("vwap_slope_score"),
                    "so": r.get("steep_ok"),
                    "ff": r.get("flip_flop"),
                    "wc": r.get("whipsaw_crosses"),
                    "qp": r.get("quality_pass"),
                    "vge": r.get("vwap_gate_enabled"),
                    "vwb": r.get("vwap_would_block"),
                    "vga": r.get("vwap_gate_applied"),
                    "inp": json.dumps(r.get("inputs") or {}),
                },
            )
        db.commit()
    except Exception as exc:
        logger.debug("ready consistency log failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass


_STATE_SORT = {
    STATE_READY: 0,
    STATE_READY_RECHECK: 1,
    STATE_WAIT: 2,
    STATE_SCANNING: 2,
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
    meta = _session_day_levels(db, symbol, session_date)
    return meta.get("session_hi"), meta.get("session_lo")


def session_day_levels_from_candles(
    candles: Optional[List[Any]],
    session_date: str,
) -> Dict[str, Optional[float]]:
    """Session H/L/open + opening-candle H/L from sorted intraday candles."""
    out: Dict[str, Optional[float]] = {
        "session_hi": None,
        "session_lo": None,
        "session_open": None,
        "opening_candle_high": None,
        "opening_candle_low": None,
    }
    if not candles:
        return out
    try:
        from backend.services.relative_strength_scanner import _parse_ist_date, _sorted_candles

        sorted_c = _sorted_candles(candles)
        first = True
        for c in sorted_c:
            d = _parse_ist_date(c.get("timestamp"))
            if not d or str(d) != session_date:
                continue
            o = _f(c.get("open"))
            h = _f(c.get("high"))
            l = _f(c.get("low"))
            if first:
                out["session_open"] = o
                out["opening_candle_high"] = h
                out["opening_candle_low"] = l
                first = False
            if h is not None:
                out["session_hi"] = h if out["session_hi"] is None else max(out["session_hi"], h)
            if l is not None:
                out["session_lo"] = l if out["session_lo"] is None else min(out["session_lo"], l)
    except Exception as exc:
        logger.debug("session day levels from candles skipped: %s", exc)
    return out


def _session_day_levels(db, symbol: str, session_date: str) -> Dict[str, Optional[float]]:
    try:
        from backend.services.daily_checklist_snapshot import _load_candles_for_symbol

        candles = _load_candles_for_symbol(db, symbol)
        return session_day_levels_from_candles(candles, session_date)
    except Exception as exc:
        logger.debug("session day levels skipped for %s: %s", symbol, exc)
        return {
            "session_hi": None,
            "session_lo": None,
            "session_open": None,
            "opening_candle_high": None,
            "opening_candle_low": None,
        }


def compute_atr_consumed_metrics(
    *,
    price: Optional[float],
    atr: Optional[float],
    atr_pct: Optional[float] = None,
    session_open: Optional[float] = None,
    opening_candle_high: Optional[float] = None,
    opening_candle_low: Optional[float] = None,
    is_long: bool = True,
) -> Dict[str, Any]:
    """Research metrics: share of daily ATR already consumed (no gating).

    Uses the same 14-day ATR convention as pullback expiry (price × atr14_pct / 100).
    """
    empty = {
        "daily_atr": round(atr, 4) if atr is not None else None,
        "atr_pct": float(atr_pct) if atr_pct is not None else None,
        "session_open": session_open,
        "opening_candle_high": opening_candle_high,
        "opening_candle_low": opening_candle_low,
        "move_from_open": None,
        "move_from_opening_range": None,
        "atr_consumed_pct_from_open": None,
        "atr_consumed_pct_from_opening_range": None,
        "opening_range_ref": None,
    }
    if price is None or atr is None or atr <= 0:
        return empty

    move_open = None
    pct_open = None
    if session_open is not None:
        move_open = abs(price - session_open)
        pct_open = round(move_open / atr * 100.0, 1)

    # Upside extension → opening high; downside → opening low.
    ref = opening_candle_high if is_long else opening_candle_low
    move_or = None
    pct_or = None
    if ref is not None:
        move_or = abs(price - ref)
        pct_or = round(move_or / atr * 100.0, 1)

    return {
        "daily_atr": round(float(atr), 4),
        "atr_pct": float(atr_pct) if atr_pct is not None else None,
        "session_open": session_open,
        "opening_candle_high": opening_candle_high,
        "opening_candle_low": opening_candle_low,
        "move_from_open": round(move_open, 4) if move_open is not None else None,
        "move_from_opening_range": round(move_or, 4) if move_or is not None else None,
        "atr_consumed_pct_from_open": pct_open,
        "atr_consumed_pct_from_opening_range": pct_or,
        "opening_range_ref": "opening_high" if is_long else "opening_low",
    }

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
    now: Optional[datetime] = None,
    session_open: Optional[float] = None,
    opening_candle_high: Optional[float] = None,
    opening_candle_low: Optional[float] = None,
) -> Dict[str, Any]:
    cfg = cfg or get_config()
    near_atr = float(cfg.get("convergence_atr") or 0.35)
    expiry_atr = float(cfg.get("expiry_atr") or 1.5)
    clock = now or datetime.now(IST)

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

    # Research-only ATR consumption (same ATR as 1.5× expiry span). No gating.
    atr_consumed = compute_atr_consumed_metrics(
        price=price,
        atr=atr,
        atr_pct=atr_pct,
        session_open=session_open,
        opening_candle_high=opening_candle_high,
        opening_candle_low=opening_candle_low,
        is_long=is_long,
    )

    # Pullback / READY entry is always live EMA5 (never VWAP blend).
    pullback_level = ema5
    entry_ready = ema5
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

    # Pullback expiry boundary: entry ± 1.5 ATR away from the setup (invalidation, not SL).
    expiry_price = None
    if intended is not None and atr and atr > 0:
        span = float(expiry_atr) * atr
        expiry_price = round(intended + span, 2) if is_long else round(intended - span, 2)

    state = STATE_BLOCKED
    blocked_reason = None
    entry_price = None
    display_risk = risk_inr_ready
    gate_badges: List[str] = []
    risk_cap_waived = False

    if block_reasons:
        state = STATE_BLOCKED
        blocked_reason = "BLOCKED · " + ", ".join(block_reasons)
        entry_price = None
    elif expired_move:
        state = STATE_EXPIRED
        # Keep intended entry visible so the card can show what expired.
        entry_price = round(intended, 2) if intended is not None else None
        display_risk = risk_inr_ready if risk_inr_ready is not None else risk_inr_pb
    elif near_ema5 and entry_ready is not None:
        if adx is not None and ADX_MIN <= adx < ADX_READY:
            state = STATE_READY_RECHECK
        else:
            state = STATE_READY
        entry_price = round(float(entry_ready), 2)
        display_risk = risk_inr_ready
    else:
        state = STATE_WAIT
        entry_price = round(pullback_level, 2) if pullback_level is not None else None
        display_risk = risk_inr_pb if risk_inr_pb is not None else risk_inr_ready

    # EMA5 anchor sanity: READY entry must match live EMA5 within tolerance.
    if state in (STATE_READY, STATE_READY_RECHECK):
        if ema5 is None or entry_off_live_ema5(entry_price, ema5):
            logger.warning(
                "READY entry off live EMA5 %s: entry=%s ema5=%s source=%s",
                stock.get("symbol"),
                entry_price,
                ema5,
                levels.get("source"),
            )
            state = STATE_WAIT
            blocked_reason = (
                f"WAIT · entry not anchored to EMA5 "
                f"(entry {entry_price}, EMA5 {ema5})"
            )
            entry_price = round(ema5, 2) if ema5 is not None else entry_price
            display_risk = risk_inr_pb if risk_inr_pb is not None else risk_inr_ready
        else:
            # Re-assert exact live EMA5 (no blend / stale override).
            entry_price = round(float(ema5), 2)
            display_risk = risk_inr_ready

    # Missing SL or INR risk → never READY / Take Trade.
    if state in (STATE_READY, STATE_READY_RECHECK) and not take_trade_structurally_ok(
        entry=entry_price, sl=sl_price, risk_inr=display_risk
    ):
        state = STATE_WAIT
        blocked_reason = "WAIT · SL/Risk not computed — Take Trade disabled"
        display_risk = risk_inr_ready if risk_inr_ready is not None else risk_inr_pb

    # Sticky lock direction vs live Trend/Supertrend/MACD (Kavach panel fields).
    # ≥2 of 3 opposing → cannot be READY (post-f11e1b7 locks no longer flip side).
    dir_conflict = direction_live_conflict(
        direction=direction,
        ema_vs_vwap=stock.get("ema_vs_vwap") or levels.get("ema_vs_vwap"),
        supertrend=stock.get("supertrend") or levels.get("supertrend"),
        macd=stock.get("macd") or levels.get("macd"),
    )
    if state in (STATE_READY, STATE_READY_RECHECK) and dir_conflict.get("suppress_ready"):
        state = STATE_WAIT
        blocked_reason = "WAIT · " + (
            dir_conflict.get("reason")
            or "direction conflict vs live Trend/Supertrend/MACD"
        )
        logger.warning(
            "DIR CONFLICT suppress READY %s: %s (sides=%s labels ema=%s st=%s macd=%s)",
            stock.get("symbol"),
            blocked_reason,
            dir_conflict.get("sides"),
            stock.get("ema_vs_vwap") or levels.get("ema_vs_vwap"),
            stock.get("supertrend") or levels.get("supertrend"),
            stock.get("macd") or levels.get("macd"),
        )
    elif int(dir_conflict.get("conflict_count") or 0) >= 1:
        logger.info(
            "DIR CONFLICT flag %s count=%s opposing=%s labels ema=%s st=%s macd=%s state=%s",
            stock.get("symbol"),
            dir_conflict.get("conflict_count"),
            dir_conflict.get("opposing_fields"),
            stock.get("ema_vs_vwap") or levels.get("ema_vs_vwap"),
            stock.get("supertrend") or levels.get("supertrend"),
            stock.get("macd") or levels.get("macd"),
            state,
        )

    # Stale / gapped entry: level sits outside today's traded range → EXPIRED
    # (same bucket as >1.5 ATR pullback expiry). Re-checked every refresh.
    if state in (STATE_READY, STATE_READY_RECHECK, STATE_WAIT) and entry_outside_session_range(
        is_long=is_long,
        entry=entry_price if entry_price is not None else (
            round(entry_ready, 2) if entry_ready is not None else None
        ),
        session_hi=session_hi,
        session_lo=session_lo,
    ):
        state = STATE_EXPIRED
        check_entry = entry_price if entry_price is not None else (
            round(entry_ready, 2) if entry_ready is not None else None
        )
        entry_price = check_entry
        display_risk = risk_inr_ready if risk_inr_ready is not None else risk_inr_pb
        if is_long:
            blocked_reason = (
                f"EXPIRED · entry {check_entry} below today's low {session_lo}"
            )
        else:
            blocked_reason = (
                f"EXPIRED · entry {check_entry} above today's high {session_hi}"
            )

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

    # Re-check range after gate mutations (entry may have been reset to pullback).
    if state in (STATE_READY, STATE_READY_RECHECK, STATE_WAIT) and entry_outside_session_range(
        is_long=is_long,
        entry=entry_price,
        session_hi=session_hi,
        session_lo=session_lo,
    ):
        state = STATE_EXPIRED
        if is_long:
            blocked_reason = f"EXPIRED · entry {entry_price} below today's low {session_lo}"
        else:
            blocked_reason = f"EXPIRED · entry {entry_price} above today's high {session_hi}"

    # Hard gate: no READY NOW before 09:45 IST (need 3 clean 10m closes from 09:15).
    if before_entry_window_ist(clock) and state in (STATE_READY, STATE_READY_RECHECK):
        state = STATE_SCANNING
        blocked_reason = "SCANNING · before 09:45 — waiting for 3 clean 10m bars"
        badges = list(gate_badges or [])
        if "PRE-09:45" not in badges:
            badges.append("PRE-09:45")
        gate_badges = badges

    # Visibility: DIR CONFLICT badge whenever live momentum opposes lock (≥1 field),
    # including WAIT after ≥2 suppress and soft 1-of-3 on READY.
    if (
        int(dir_conflict.get("conflict_count") or 0) >= 1
        and state in (STATE_READY, STATE_READY_RECHECK, STATE_WAIT, STATE_SCANNING)
    ):
        badges = list(gate_badges or [])
        if "DIR CONFLICT" not in badges:
            badges.append("DIR CONFLICT")
        gate_badges = badges

    # Informational only: ATR consumed from open (no READY impact).
    pct_open = atr_consumed.get("atr_consumed_pct_from_open")
    if pct_open is not None and state in (
        STATE_READY,
        STATE_READY_RECHECK,
        STATE_WAIT,
        STATE_SCANNING,
    ):
        atr_chip = f"ATR {int(round(float(pct_open)))}%"
        badges = list(gate_badges or [])
        badges = [b for b in badges if not str(b).startswith("ATR ")]
        badges.append(atr_chip)
        gate_badges = badges

    sl_out = round(sl_price, 2) if sl_price is not None else None

    rr = session_rr(
        is_long=is_long,
        entry=entry_price,
        sl=sl_out,
        session_hi=session_hi,
        session_lo=session_lo,
    )
    rr_low = bool(rr is not None and rr < RR_LOW)

    # Hard ₹3k risk gate (not display-only): over cap + weak/missing R:R → BLOCKED.
    if state in (STATE_READY, STATE_READY_RECHECK) and risk_cap_blocks_ready(display_risk, rr):
        state = STATE_BLOCKED
        blocked_reason = (
            f"BLOCKED · risk ₹{int(display_risk)} > ₹{int(MAX_INR_RISK)} "
            f"and R:R {('1:' + str(rr)) if rr is not None else '—'} < 1:{RR_LOW:g}"
        )
        risk_cap_waived = False
    elif (
        state in (STATE_READY, STATE_READY_RECHECK)
        and display_risk is not None
        and display_risk > MAX_INR_RISK
        and rr is not None
        and rr >= RR_LOW
    ):
        risk_cap_waived = True
        badges = list(gate_badges or [])
        if "CAP WAIVED" not in badges:
            badges.append("CAP WAIVED")
        gate_badges = badges

    # Final structural Take Trade check after all mutations.
    if state in (STATE_READY, STATE_READY_RECHECK) and not take_trade_structurally_ok(
        entry=entry_price, sl=sl_out, risk_inr=display_risk
    ):
        state = STATE_WAIT
        blocked_reason = "WAIT · SL/Risk not computed — Take Trade disabled"

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
        pb_label = f"{pullback_count}+ pullback"
    elif pullback_count == 1:
        pb_label = "1st pullback"
    elif pullback_count == 2:
        pb_label = "2nd pullback"

    risk_over = bool(display_risk is not None and display_risk > MAX_INR_RISK)
    # Visual flag when over cap without an explicit R:R waiver.
    risk_cap_flag = bool(risk_over and not risk_cap_waived)
    take_ok = bool(
        state in (STATE_READY, STATE_READY_RECHECK)
        and entry_window_open_ist(clock)
        and take_trade_structurally_ok(entry=entry_price, sl=sl_out, risk_inr=display_risk)
        and not risk_cap_blocks_ready(display_risk, rr)
    )
    waiver_label = None
    if risk_cap_waived and rr is not None:
        waiver_label = f"cap waived — R:R 1:{rr}"

    return {
        "trade_state": state,
        "trade_state_reason": blocked_reason,
        "trade_entry": entry_price,
        "trade_sl": sl_out,
        "trade_risk_inr": int(display_risk) if display_risk is not None else None,
        "trade_risk_over": risk_over,
        "trade_risk_cap_flag": risk_cap_flag,
        "trade_risk_cap_waived": bool(risk_cap_waived),
        "trade_risk_cap_waiver_label": waiver_label,
        "trade_risk_cap_inr": int(MAX_INR_RISK),
        "trade_expiry_price": expiry_price,
        "trade_expiry_atr": float(expiry_atr),
        "trade_expiry_crossed": bool(state == STATE_EXPIRED or expired_move),
        "trade_rr": rr,
        "trade_rr_low": rr_low,
        "trade_rr_label": (f"1:{rr}" if rr is not None else None),
        "trade_adx": round(adx, 1) if adx is not None else None,
        "trade_lot": lot,
        "trade_levels_source": levels.get("source"),
        "trade_entry_window_open": entry_window_open_ist(clock),
        "trade_take_enabled": take_ok,
        "promoted_at": (promo or {}).get("promoted_at"),
        "lock_cycles": int((promo or {}).get("cycles") or 0),
        "position": trail,
        "whipsaw_count": whipsaw_count,
        "pullback_count": pullback_count,
        "pullback_label": pb_label,
        "direction_unstable": bool(direction_unstable),
        "gate_badges": gate_badges,
        "dir_conflict": {
            "conflict_count": int(dir_conflict.get("conflict_count") or 0),
            "opposing_fields": list(dir_conflict.get("opposing_fields") or []),
            "live_lean": dir_conflict.get("live_lean"),
            "suppress_ready": bool(dir_conflict.get("suppress_ready")),
            "reason": dir_conflict.get("reason"),
        },
        "atr_consumed": atr_consumed,
        "stopped_out_today": bool(stopped and stopped.get("blocked")),
    }


def enrich_stocks_trade_state(
    stocks: List[Dict[str, Any]],
    session_date: str,
    *,
    locked_by: Optional[str] = None,
    rotation_day: Optional[Dict[str, Any]] = None,
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
        "rotation_chip": None,
        "direction_imbalance": None,
        "compromised_lock": None,
        "session_window_text": "Entry 09:45–14:30 · Square-off 15:15",
        "ready_vwap_gate_enabled": False,
        "ready_consistency_logged": 0,
    }
    if not stocks:
        from backend.services.daily_checklist_zones import build_zone1_obs

        empty_obs.update(
            build_zone1_obs(
                rotation_day=rotation_day,
                removals=[],
                locked_by=locked_by,
            )
        )
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
        from backend.services.daily_checklist_zones import (
            build_zone1_obs,
            regime_research_snapshot,
        )

        zone1_early = build_zone1_obs(
            rotation_day=rotation_day,
            removals=removals,
            locked_by=locked_by,
        )
        flips = direction_unstable_flags(
            db,
            session_date,
            symbols,
            current_dirs={s["symbol"]: s.get("direction") for s in stocks if s.get("symbol")},
        )
        stopped_map = stopped_out_today(db, session_date, symbols)

        hi_lo: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
        session_meta: Dict[str, Dict[str, Optional[float]]] = {}
        candle_cache: Dict[str, Any] = {}
        for sym in symbols[:25]:
            su = sym.upper()
            try:
                candle_cache[su] = _load_candles_for_symbol(db, sym) or []
            except Exception:
                candle_cache[su] = []
            meta = session_day_levels_from_candles(candle_cache[su], session_date)
            session_meta[su] = meta
            hi_lo[su] = (meta.get("session_hi"), meta.get("session_lo"))
            if hi_lo[su] == (None, None):
                # Fallback when candle parse yields nothing.
                hi_lo[su] = _session_hi_lo(db, sym, session_date)
                fb = _session_day_levels(db, sym, session_date)
                session_meta[su] = fb

        lot_cache: Dict[str, int] = {}
        consistency_rows: List[Dict[str, Any]] = []
        from backend.services.daily_checklist_zones import morning_locked_symbols
        from backend.services.rs_vwap_quality import (
            ready_vwap_quality_gate_enabled,
            score_vwap_quality,
        )

        lock_map = morning_locked_symbols(db, session_date)
        vwap_gate_on = ready_vwap_quality_gate_enabled()

        nifty_pct = 0.0
        try:
            from backend.services.daily_checklist_live import _latest_nifty_pct

            nifty_pct = float(_latest_nifty_pct(db) or 0.0)
        except Exception:
            nifty_pct = 0.0

        for s in stocks:
            sym = (s.get("symbol") or "").upper()
            if not sym:
                continue
            if sym not in lot_cache:
                lot_cache[sym], _ = _lot_for_symbol(db, sym)
            hi, lo = hi_lo.get(sym, (None, None))
            smeta = session_meta.get(sym) or {}
            price = _f((levels_map.get(sym) or {}).get("price"))
            atr_pct = float(atr_pct_map.get(sym) or 0.0)
            atr = (price * atr_pct / 100.0) if price and atr_pct > 0 else None
            is_long = (s.get("direction") or "LONG").upper() != "SHORT"
            candles = candle_cache.get(sym) or []
            # Every poll: refresh Trend/ST/MACD from the same 10m candle path as
            # live Kavach so DIR CONFLICT is not stuck on sticky checklist labels.
            overlay_live_momentum_from_candles(s, candles, nifty_pct=nifty_pct)
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
                session_open=smeta.get("session_open"),
                opening_candle_high=smeta.get("opening_candle_high"),
                opening_candle_low=smeta.get("opening_candle_low"),
            )
            s.update(ts)

            lock_row = lock_map.get(sym)
            in_lock = lock_row is not None
            s["in_lock"] = in_lock
            s["lock_rank"] = (lock_row or {}).get("rank")
            s["lock_direction"] = (lock_row or {}).get("direction")

            pre_gate = s.get("trade_state")
            is_ready_pre = pre_gate in (STATE_READY, STATE_READY_RECHECK)

            # VWAP-quality in same enrich pass (candles already loaded — no extra latency path).
            qualify_since = (promo.get(sym) or {}).get("promoted_at")
            vq = score_vwap_quality(
                candles,
                side=s.get("direction") or "LONG",
                atr_daily_pct=atr_pct if atr_pct > 0 else 1.0,
                cfg=cfg,
                since=qualify_since,
            )
            s["vwap_quality"] = vq
            would_block = is_ready_pre and not bool(vq.get("quality_pass"))
            gate_applied = False
            if is_ready_pre and vwap_gate_on and not vq.get("quality_pass"):
                # Prefer slope over raw RS-rank: unstable / flat VWAP cannot stay READY.
                s["trade_state"] = STATE_WAIT
                reason = "VWAP quality"
                if vq.get("flip_flop"):
                    reason = "VWAP flip-flop since qualify"
                elif not vq.get("steep_ok"):
                    reason = "VWAP slope not steep"
                elif vq.get("unstable"):
                    reason = "VWAP unstable"
                s["trade_state_reason"] = reason
                s["zone_downgrade"] = "vwap_quality"
                gate_applied = True

            # Final 09:45 gate after VWAP (never unlock READY early).
            if before_entry_window_ist() and s.get("trade_state") in (
                STATE_READY,
                STATE_READY_RECHECK,
            ):
                s["trade_state"] = STATE_SCANNING
                s["trade_state_reason"] = "SCANNING · before 09:45 — waiting for 3 clean 10m bars"
                badges = list(s.get("gate_badges") or [])
                if "PRE-09:45" not in badges:
                    badges.append("PRE-09:45")
                s["gate_badges"] = badges
                s["trade_take_enabled"] = False
            else:
                s["trade_take_enabled"] = bool(
                    s.get("trade_state") in (STATE_READY, STATE_READY_RECHECK)
                    and entry_window_open_ist()
                    and take_trade_structurally_ok(
                        entry=s.get("trade_entry"),
                        sl=s.get("trade_sl"),
                        risk_inr=s.get("trade_risk_inr"),
                    )
                    and not risk_cap_blocks_ready(s.get("trade_risk_inr"), s.get("trade_rr"))
                )
            s["trade_entry_window_open"] = entry_window_open_ist()

            rendered = s.get("trade_state")
            lock_mismatch = bool(is_ready_pre and not in_lock)
            # Log every pre-gate READY (incl. shadow near-misses) and any lock mismatch.
            if is_ready_pre or lock_mismatch or gate_applied:
                regime_snap = regime_research_snapshot(
                    market_regime=mkt.get("market_regime"),
                    market_regime_label=mkt.get("market_regime_label"),
                    imbalance=zone1_early.get("direction_imbalance"),
                    removals=removals,
                    direction=s.get("direction"),
                )
                consistency_rows.append(
                    {
                        "session_date": session_date,
                        "symbol": sym,
                        "direction": s.get("direction"),
                        "rendered_state": rendered,
                        "pre_gate_state": pre_gate,
                        "in_lock": in_lock,
                        "lock_rank": s.get("lock_rank"),
                        "lock_direction": s.get("lock_direction"),
                        "lock_mismatch": lock_mismatch,
                        "vwap_slope_score": vq.get("slope_score"),
                        "steep_ok": vq.get("steep_ok"),
                        "flip_flop": vq.get("flip_flop"),
                        "whipsaw_crosses": vq.get("whipsaw_crosses"),
                        "quality_pass": vq.get("quality_pass"),
                        "vwap_gate_enabled": vwap_gate_on,
                        "vwap_would_block": would_block,
                        "vwap_gate_applied": gate_applied,
                        "inputs": {
                            "adverse_closes": vq.get("adverse_closes"),
                            "first_adverse_hm": vq.get("first_adverse_hm"),
                            "signed_slope_atr": vq.get("signed_slope_atr"),
                            "promoted_at": str(qualify_since) if qualify_since else None,
                            "atr_pct": atr_pct,
                            # Research: regime at READY signal (no enforcement).
                            "regime": regime_snap.get("market_regime"),
                            "regime_label": regime_snap.get("market_regime_label"),
                            "regime_unconfirmed": regime_snap.get("regime_unconfirmed"),
                            "regime_lean": regime_snap.get("regime_lean"),
                            "removals_last_hour": regime_snap.get("removals_last_hour"),
                            "counter_regime": regime_snap.get("counter_regime"),
                            "dir_conflict_count": (s.get("dir_conflict") or {}).get(
                                "conflict_count"
                            ),
                            "dir_conflict_live_lean": (s.get("dir_conflict") or {}).get(
                                "live_lean"
                            ),
                            "dir_conflict_reason": (s.get("dir_conflict") or {}).get(
                                "reason"
                            ),
                            "dir_conflict_suppress": (s.get("dir_conflict") or {}).get(
                                "suppress_ready"
                            ),
                            # Research: ATR consumed at READY signal time (no enforcement).
                            "atr_consumed": s.get("atr_consumed"),
                            "atr_consumed_pct_from_open": (s.get("atr_consumed") or {}).get(
                                "atr_consumed_pct_from_open"
                            ),
                            "atr_consumed_pct_from_opening_range": (
                                s.get("atr_consumed") or {}
                            ).get("atr_consumed_pct_from_opening_range"),
                            "confidence": s.get("confidence") or s.get("dashboard_kavach"),
                            "trade_entry": s.get("trade_entry"),
                            "trade_sl": s.get("trade_sl"),
                        },
                    }
                )
                if lock_mismatch:
                    logger.warning(
                        "READY lock mismatch symbol=%s state=%s in_lock=%s (logged for 22-Jul)",
                        sym,
                        pre_gate,
                        in_lock,
                    )

        if consistency_rows:
            log_ready_consistency(db, consistency_rows)

        churn_syms = [s["symbol"] for s in stocks if int(s.get("lock_cycles") or 0) > 1]
        from backend.services.daily_checklist_zones import (
            annotate_regime_context,
            apply_zone_downgrades,
        )

        apply_zone_downgrades(
            stocks,
            imbalance=zone1_early.get("direction_imbalance"),
            compromised=zone1_early.get("compromised_lock"),
        )
        # Visibility only — never changes trade_state / Take Trade.
        annotate_regime_context(
            stocks,
            market_regime=mkt.get("market_regime"),
            market_regime_label=mkt.get("market_regime_label"),
            imbalance=zone1_early.get("direction_imbalance"),
            removals=removals,
        )
        return {
            "churn_warning": len(churn_syms) >= 3,
            "churn_symbols": churn_syms,
            "churn_count": len(churn_syms),
            "recent_removals": removals,
            "ready_vwap_gate_enabled": vwap_gate_on,
            "ready_consistency_logged": len(consistency_rows),
            **mkt,
            **zone1_early,
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
        # When VWAP quality available, prefer steep/clean over raw RS rank.
        vq = s.get("vwap_quality") or {}
        slope = float(vq.get("slope_score") or 0)
        # Higher slope first among same state/grade (negate for ascending sort).
        slope_key = -slope if vq else 0
        sym = s.get("symbol") or ""
        rs_rank = rank_map.get(sym, (0, 99))[1]
        return (state_i, grade_i, slope_key, rs_rank, sym)

    return sorted(stocks, key=key)
