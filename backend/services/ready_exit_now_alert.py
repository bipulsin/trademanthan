"""Informational EXIT NOW alert on live READY cards (VWAP or EMA10 side break).

Does **not** demote READY or change take-enablement — that remains the VWAP-side
gate / FSM. This only stamps ``exit_now_alert`` on the stock for UI + audio.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import engine
from backend.services.relative_strength_scanner import _f, _sorted_candles
from backend.services.rs_conviction_signals import ema10_10min
from backend.services.vwap_side_gate import (
    closed_10m_session_bars,
    last_closed_close_and_session_vwap,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

LOG_REASON = "exit_now_alert_fired"
_TABLE_OK = False


def ensure_ready_exit_now_alert_log() -> None:
    global _TABLE_OK
    if _TABLE_OK:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS ready_exit_now_alert_log (
                    id BIGSERIAL PRIMARY KEY,
                    session_date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    direction TEXT,
                    bar_end TIMESTAMPTZ,
                    trigger_reason TEXT NOT NULL,
                    close DOUBLE PRECISION,
                    vwap DOUBLE PRECISION,
                    ema10 DOUBLE PRECISION,
                    fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (session_date, symbol, bar_end)
                )
                """
            )
        )
    _TABLE_OK = True


def _norm_side(direction: Optional[str]) -> str:
    side = (direction or "LONG").upper()
    if side in ("BEAR", "BEARISH", "SHORT"):
        return "SHORT"
    return "LONG"


def last_closed_close_vwap_ema10(
    candles: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[Dict[str, Any]]]:
    """Last closed 10m close, session VWAP, EMA10 through that bar, and bar dict."""
    close, vwap, _n = last_closed_close_and_session_vwap(candles, now=now)
    closed = closed_10m_session_bars(candles, now=now)
    if not closed or close is None:
        return None, None, None, None
    last = closed[-1]
    end_idx = last.get("end_5m_idx")
    sorted_c = _sorted_candles(candles or [])
    subset = sorted_c[: int(end_idx) + 1] if end_idx is not None else sorted_c
    ema10 = ema10_10min(subset)
    return close, vwap, ema10 if ema10 is not None else None, last


def evaluate_exit_now_alert(
    direction: Optional[str],
    candles: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """LONG: close < VWAP OR close < EMA10. SHORT: close > VWAP OR close > EMA10."""
    side = _norm_side(direction)
    close, vwap, ema10, last = last_closed_close_vwap_ema10(candles, now=now)
    detail = {
        "close": close,
        "vwap": round(vwap, 4) if vwap is not None else None,
        "ema10": round(ema10, 4) if ema10 is not None else None,
        "direction": side,
        "bar_end": str(last.get("bar_end")) if last and last.get("bar_end") else None,
        "bar_open": last.get("timestamp") if last else None,
    }
    if close is None:
        return {"active": False, "reason": None, "detail": {**detail, "fail": "no_close"}}

    vwap_viol = False
    ema_viol = False
    if vwap is not None and vwap > 0:
        vwap_viol = (close < vwap) if side == "LONG" else (close > vwap)
    if ema10 is not None and ema10 > 0:
        ema_viol = (close < ema10) if side == "LONG" else (close > ema10)

    if not vwap_viol and not ema_viol:
        return {"active": False, "reason": None, "detail": detail}

    if vwap_viol and ema_viol:
        reason = "both"
        label = "VWAP + EMA10"
    elif vwap_viol:
        reason = "vwap"
        label = "VWAP"
    else:
        reason = "ema10"
        label = "EMA10"

    return {
        "active": True,
        "reason": reason,
        "trigger_label": label,
        "banner": f"EXIT NOW · {label}",
        "detail": detail,
    }


def _log_fire(
    db,
    *,
    session_date: str,
    symbol: str,
    direction: str,
    alert: Dict[str, Any],
) -> bool:
    """Insert once per (session, symbol, bar_end). Returns True if newly logged."""
    ensure_ready_exit_now_alert_log()
    d = alert.get("detail") or {}
    bar_end = d.get("bar_end")
    try:
        row = db.execute(
            text(
                """
                INSERT INTO ready_exit_now_alert_log (
                    session_date, symbol, direction, bar_end, trigger_reason,
                    close, vwap, ema10
                ) VALUES (
                    CAST(:d AS date), :sym, :dir, CAST(:be AS timestamptz), :reason,
                    :close, :vwap, :ema10
                )
                ON CONFLICT (session_date, symbol, bar_end) DO NOTHING
                RETURNING id
                """
            ),
            {
                "d": session_date,
                "sym": symbol.upper(),
                "dir": direction,
                "be": bar_end,
                "reason": alert.get("reason") or "unknown",
                "close": d.get("close"),
                "vwap": d.get("vwap"),
                "ema10": d.get("ema10"),
            },
        ).first()
        if row:
            logger.info(
                "%s symbol=%s direction=%s reason=%s close=%s vwap=%s ema10=%s bar_end=%s",
                LOG_REASON,
                symbol.upper(),
                direction,
                alert.get("reason"),
                d.get("close"),
                d.get("vwap"),
                d.get("ema10"),
                bar_end,
            )
            return True
    except Exception as exc:
        logger.warning("exit_now_alert log failed %s: %s", symbol, exc)
        # Still emit structured log for ops even if table insert fails.
        logger.info(
            "%s symbol=%s direction=%s reason=%s close=%s vwap=%s ema10=%s",
            LOG_REASON,
            symbol.upper(),
            direction,
            alert.get("reason"),
            d.get("close"),
            d.get("vwap"),
            d.get("ema10"),
        )
    return False


def apply_ready_exit_now_alerts(
    stocks: List[Dict[str, Any]],
    *,
    db,
    session_date: str,
    candle_cache: Dict[str, Any],
    now: Optional[datetime] = None,
) -> Dict[str, int]:
    """Stamp exit_now_alert on currently-READY stocks; log first fire per bar."""
    stats = {"checked": 0, "active": 0, "logged": 0}
    ready_states = ("READY", "READY(RECHECK)")
    for s in stocks:
        state = str(s.get("trade_state") or "").upper()
        if state not in ready_states:
            s.pop("exit_now_alert", None)
            continue
        sym = (s.get("symbol") or "").upper()
        if not sym:
            continue
        stats["checked"] += 1
        candles = candle_cache.get(sym) or []
        alert = evaluate_exit_now_alert(s.get("direction"), candles, now=now)
        alert["informational_only"] = True
        s["exit_now_alert"] = alert
        if not alert.get("active"):
            continue
        stats["active"] += 1
        if _log_fire(
            db,
            session_date=session_date,
            symbol=sym,
            direction=_norm_side(s.get("direction")),
            alert=alert,
        ):
            stats["logged"] += 1
            alert["logged_this_cycle"] = True
    return stats
