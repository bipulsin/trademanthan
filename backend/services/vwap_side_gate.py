"""Hard VWAP-side gate for READY NOW (organic + SQ).

LONG requires last *closed* 10m bar close > session VWAP.
SHORT requires last *closed* 10m bar close < session VWAP.

10m bars are session-paired 5m (09:15+09:20, 09:25+09:30, …) via
``aggregate_10m_bars``; a bar is closed when its ``bar_end`` ≤ now.

Env ``READY_VWAP_SIDE_GATE`` (default on). Distinct from slope-based
``READY_VWAP_QUALITY_GATE``.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.kavach_10m import aggregate_10m_bars
from backend.services.kavach_volume import _parse_ist
from backend.services.relative_strength_scanner import _f

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

REJECT_REASON = "vwap_side_gate_reject"


def vwap_side_gate_enabled() -> bool:
    """Hard close-vs-VWAP READY gate. Default ON (Day-1 wrong-side fix)."""
    return os.environ.get("READY_VWAP_SIDE_GATE", "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _to_ist(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        dt = ts
    else:
        dt = _parse_ist(ts)
    if dt is None:
        return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def closed_10m_session_bars(
    candles: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Today's fully closed 10m bars (bar_end ≤ now), in order."""
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    out: List[Dict[str, Any]] = []
    for b in aggregate_10m_bars(candles or []):
        be = _to_ist(b.get("bar_end"))
        if be is None or be.date() != now.date():
            continue
        if be <= now:
            out.append(b)
    return out


def last_closed_close_and_session_vwap(
    candles: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
) -> Tuple[Optional[float], Optional[float], Optional[int]]:
    """Return (close, session_vwap, n_closed) for the last closed 10m bar today."""
    closed = closed_10m_session_bars(candles, now=now)
    if not closed:
        return None, None, None
    pv = 0.0
    vv = 0.0
    for b in closed:
        h = _f(b.get("high"))
        l = _f(b.get("low"))
        c = _f(b.get("close"))
        v = _f(b.get("volume")) or 1.0
        tp = (h + l + c) / 3.0
        pv += tp * v
        vv += v
    vwap = pv / vv if vv > 0 else None
    close = _f(closed[-1].get("close"))
    if close is None or vwap is None or vwap <= 0:
        return None, None, None
    return float(close), float(vwap), len(closed)


def vwap_side_ok(
    direction: Optional[str],
    candles: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Binary VWAP-side check for assigned direction on last closed 10m bar."""
    side = (direction or "LONG").upper()
    if side in ("BEAR", "BEARISH"):
        side = "SHORT"
    elif side in ("BULL", "BULLISH"):
        side = "LONG"
    close, vwap, n_closed = last_closed_close_and_session_vwap(candles, now=now)
    detail = {
        "close": close,
        "vwap": round(vwap, 4) if vwap is not None else None,
        "direction": side,
        "n_closed_10m": n_closed,
        "close_minus_vwap": (
            round(close - vwap, 4) if close is not None and vwap is not None else None
        ),
    }
    if close is None or vwap is None:
        return {
            "ok": False,
            "reason": REJECT_REASON,
            "detail": {**detail, "fail": "missing_close_or_vwap"},
        }
    ok = (close < vwap) if side == "SHORT" else (close > vwap)
    return {
        "ok": bool(ok),
        "reason": None if ok else REJECT_REASON,
        "detail": detail,
    }


def apply_vwap_side_gate(
    stock: Dict[str, Any],
    candles: List[Dict[str, Any]],
    *,
    ready_states: Tuple[str, ...] = ("READY", "READY(RECHECK)"),
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """If READY and wrong-side of VWAP, demote to WAIT. Mutates stock."""
    result = vwap_side_ok(stock.get("direction"), candles, now=now)
    stock["vwap_side_gate"] = result
    state = str(stock.get("trade_state") or "")
    if not vwap_side_gate_enabled():
        result = {**result, "enabled": False, "demoted": False}
        stock["vwap_side_gate"] = result
        return result
    result["enabled"] = True
    result["demoted"] = False
    if state not in ready_states:
        return result
    if result.get("ok"):
        return result

    d = result.get("detail") or {}
    close = d.get("close")
    vwap = d.get("vwap")
    side = d.get("direction") or stock.get("direction")
    stock["trade_state"] = "WAIT FOR PULLBACK"
    stock["trade_state_reason"] = (
        f"WAIT · {REJECT_REASON}: {side} but close {close} vs VWAP {vwap}"
    )
    stock["trade_take_enabled"] = False
    stock["zone_downgrade"] = REJECT_REASON
    badges = list(stock.get("gate_badges") or [])
    if "VWAP SIDE" not in badges:
        badges.append("VWAP SIDE")
    stock["gate_badges"] = badges
    result["demoted"] = True
    logger.info(
        "%s symbol=%s direction=%s close=%s vwap=%s",
        REJECT_REASON,
        (stock.get("symbol") or "").upper(),
        side,
        close,
        vwap,
    )
    return result


def opposite_direction(direction: Optional[str]) -> str:
    side = (direction or "LONG").upper()
    if side in ("SHORT", "BEAR", "BEARISH"):
        return "LONG"
    return "SHORT"
