"""Shadow-only: Hypothesis D dual EMA10+VWAP intrabar breach on open trades.

Observational only — never mutates live exit state / gating / Pine.

Detection cadence matches existing ``evaluate_open_trades`` confirmed-10m
bar path (same inputs as C1/C2/C3 exit-candidate shadow): bar high/low vs
EMA10 and session VWAP already computed by ``_confirmed_10m_levels``.

This is **not** tick-level "as the candle forms" mid-bar exit pricing.
True forming-candle dual-breach would need new real-time LTP-vs-levels
logic and is out of scope for this module (see module docstring / report).

Simulated Hyp D exit = confirmed dual-breach candle **close** (matches the
historical Hyp D backtest methodology).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

TABLE = "kavach_dual_breach_exit_shadow"
_ENSURED = False


def _ist():
    import pytz

    return pytz.timezone("Asia/Kolkata")


def _f(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def evaluate_dual_breach(
    *,
    is_long: bool,
    entry: float,
    risk_pts: float,
    qty: int,
    bar_high: float,
    bar_low: float,
    bar_close: float,
    ema10: Optional[float],
    vwap: Optional[float],
    peak_r: float = 0.0,
) -> Dict[str, Any]:
    """Pure check: adverse extreme vs EMA10 and VWAP on one confirmed bar."""
    e10 = _f(ema10)
    vw = _f(vwap)
    hi = float(bar_high)
    lo = float(bar_low)
    close = float(bar_close)
    risk = float(risk_pts) if risk_pts and risk_pts > 0 else 0.0

    if is_long:
        ema10_breached = e10 is not None and lo < e10
        vwap_breached = vw is not None and lo < vw
        adverse_extreme = lo
    else:
        ema10_breached = e10 is not None and hi > e10
        vwap_breached = vw is not None and hi > vw
        adverse_extreme = hi

    dual = bool(ema10_breached and vwap_breached)

    def _r(px: float) -> Optional[float]:
        if risk <= 0:
            return None
        pts = (px - entry) if is_long else (entry - px)
        return round(pts / risk, 4)

    close_r = _r(close)
    extreme_r = _r(adverse_extreme)
    sim_pnl = None
    if dual and qty:
        pts = (close - entry) if is_long else (entry - close)
        sim_pnl = round(pts * int(qty), 2)

    return {
        "ema10_breached": bool(ema10_breached),
        "vwap_breached": bool(vwap_breached),
        "dual_breach": dual,
        "hyp_d_would_exit": dual,
        "hyp_d_sim_exit_price": round(close, 4) if dual else None,
        "hyp_d_sim_exit_r": close_r if dual else None,
        "hyp_d_sim_exit_pnl_inr": sim_pnl,
        "unrealized_r_at_close": close_r,
        "unrealized_r_at_adverse_extreme": extreme_r,
        "peak_r": round(float(peak_r or 0), 4),
        "ema10": e10,
        "vwap": vw,
        "bar_high": hi,
        "bar_low": lo,
        "bar_close": close,
        "detection_mode": "confirmed_10m_bar_hl_vs_ema10_vwap",
    }


def ensure_dual_breach_exit_shadow() -> None:
    global _ENSURED
    if _ENSURED:
        return
    try:
        from sqlalchemy import text

        from backend.database import engine

        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {TABLE} (
                        id SERIAL PRIMARY KEY,
                        session_date DATE NOT NULL,
                        trade_id VARCHAR(64),
                        symbol VARCHAR(32) NOT NULL,
                        direction VARCHAR(8),
                        logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        bar_at TIMESTAMPTZ,
                        entry_price NUMERIC(16,4),
                        risk_pts NUMERIC(16,6),
                        qty INTEGER,
                        unrealized_r NUMERIC(12,4),
                        unrealized_r_at_extreme NUMERIC(12,4),
                        peak_r NUMERIC(12,4),
                        bar_high NUMERIC(16,4),
                        bar_low NUMERIC(16,4),
                        bar_close NUMERIC(16,4),
                        ema10 NUMERIC(16,4),
                        vwap NUMERIC(16,4),
                        ema10_breached BOOLEAN NOT NULL DEFAULT FALSE,
                        vwap_breached BOOLEAN NOT NULL DEFAULT FALSE,
                        dual_breach BOOLEAN NOT NULL DEFAULT FALSE,
                        hyp_d_would_exit BOOLEAN NOT NULL DEFAULT FALSE,
                        hyp_d_sim_exit_price NUMERIC(16,4),
                        hyp_d_sim_exit_r NUMERIC(12,4),
                        hyp_d_sim_exit_pnl_inr NUMERIC(16,2),
                        live_state VARCHAR(32),
                        actual_exit_price NUMERIC(16,4),
                        actual_exit_time TIMESTAMPTZ,
                        actual_exit_r NUMERIC(12,4),
                        actual_exit_pnl_inr NUMERIC(16,2),
                        actual_outcome_backfilled_at TIMESTAMPTZ,
                        payload JSONB,
                        shadow_mode BOOLEAN NOT NULL DEFAULT TRUE,
                        detection_mode VARCHAR(64) DEFAULT 'confirmed_10m_bar_hl_vs_ema10_vwap'
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_dual_breach_shadow_session
                    ON {TABLE} (session_date, symbol, logged_at)
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_dual_breach_shadow_trade
                    ON {TABLE} (trade_id)
                    """
                )
            )
        _ENSURED = True
    except Exception as exc:
        logger.debug("dual breach shadow ensure failed: %s", exc)


def log_dual_breach_exit_shadow(
    db,
    *,
    session_date: str,
    symbol: str,
    snapshot: Dict[str, Any],
    trade_id: Optional[str] = None,
    direction: Optional[str] = None,
    entry_price: Optional[float] = None,
    risk_pts: Optional[float] = None,
    qty: Optional[int] = None,
    live_state: Optional[str] = None,
    bar_at: Optional[datetime] = None,
    logged_at: Optional[datetime] = None,
) -> None:
    """Insert only when dual_breach fires. Never raises into live eval."""
    if not snapshot or not snapshot.get("dual_breach"):
        return
    try:
        from sqlalchemy import text

        ensure_dual_breach_exit_shadow()
        tz = _ist()
        now = logged_at or datetime.now(tz)
        if isinstance(now, datetime) and now.tzinfo is None:
            now = tz.localize(now)
        bat = bar_at or now
        if isinstance(bat, datetime) and bat.tzinfo is None:
            bat = tz.localize(bat)

        db.execute(
            text(
                f"""
                INSERT INTO {TABLE} (
                    session_date, trade_id, symbol, direction, logged_at, bar_at,
                    entry_price, risk_pts, qty,
                    unrealized_r, unrealized_r_at_extreme, peak_r,
                    bar_high, bar_low, bar_close, ema10, vwap,
                    ema10_breached, vwap_breached, dual_breach, hyp_d_would_exit,
                    hyp_d_sim_exit_price, hyp_d_sim_exit_r, hyp_d_sim_exit_pnl_inr,
                    live_state, payload, shadow_mode, detection_mode
                ) VALUES (
                    CAST(:session_date AS date), :trade_id, :symbol, :direction,
                    :logged_at, :bar_at,
                    :entry_price, :risk_pts, :qty,
                    :unrealized_r, :unrealized_r_at_extreme, :peak_r,
                    :bar_high, :bar_low, :bar_close, :ema10, :vwap,
                    :ema10_breached, :vwap_breached, :dual_breach, :hyp_d_would_exit,
                    :hyp_d_sim_exit_price, :hyp_d_sim_exit_r, :hyp_d_sim_exit_pnl_inr,
                    :live_state, CAST(:payload AS jsonb), TRUE, :detection_mode
                )
                """
            ),
            {
                "session_date": session_date,
                "trade_id": trade_id,
                "symbol": symbol,
                "direction": direction,
                "logged_at": now,
                "bar_at": bat,
                "entry_price": entry_price,
                "risk_pts": risk_pts,
                "qty": qty,
                "unrealized_r": snapshot.get("unrealized_r_at_close"),
                "unrealized_r_at_extreme": snapshot.get("unrealized_r_at_adverse_extreme"),
                "peak_r": snapshot.get("peak_r"),
                "bar_high": snapshot.get("bar_high"),
                "bar_low": snapshot.get("bar_low"),
                "bar_close": snapshot.get("bar_close"),
                "ema10": snapshot.get("ema10"),
                "vwap": snapshot.get("vwap"),
                "ema10_breached": bool(snapshot.get("ema10_breached")),
                "vwap_breached": bool(snapshot.get("vwap_breached")),
                "dual_breach": True,
                "hyp_d_would_exit": True,
                "hyp_d_sim_exit_price": snapshot.get("hyp_d_sim_exit_price"),
                "hyp_d_sim_exit_r": snapshot.get("hyp_d_sim_exit_r"),
                "hyp_d_sim_exit_pnl_inr": snapshot.get("hyp_d_sim_exit_pnl_inr"),
                "live_state": live_state,
                "payload": json.dumps(snapshot),
                "detection_mode": snapshot.get("detection_mode")
                or "confirmed_10m_bar_hl_vs_ema10_vwap",
            },
        )
    except Exception as exc:
        logger.debug("dual breach shadow log failed %s: %s", symbol, exc)


def backfill_dual_breach_actual_outcome(
    db,
    *,
    trade_id: str,
    actual_exit_price: float,
    actual_exit_time: Optional[datetime],
    actual_exit_r: Optional[float],
    actual_exit_pnl_inr: Optional[float],
) -> None:
    """Fill actual outcome on prior shadow rows once the live trade closes."""
    if not trade_id:
        return
    try:
        from sqlalchemy import text

        ensure_dual_breach_exit_shadow()
        tz = _ist()
        now = datetime.now(tz)
        db.execute(
            text(
                f"""
                UPDATE {TABLE}
                SET actual_exit_price = :px,
                    actual_exit_time = :et,
                    actual_exit_r = :ar,
                    actual_exit_pnl_inr = :ap,
                    actual_outcome_backfilled_at = :now
                WHERE trade_id = :tid
                  AND actual_outcome_backfilled_at IS NULL
                """
            ),
            {
                "tid": str(trade_id),
                "px": actual_exit_price,
                "et": actual_exit_time,
                "ar": actual_exit_r,
                "ap": actual_exit_pnl_inr,
                "now": now,
            },
        )
    except Exception as exc:
        logger.debug("dual breach outcome backfill failed %s: %s", trade_id, exc)
