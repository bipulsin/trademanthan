"""2-candle VWAP acceleration rule for directional side flips.

A side may flip LONG↔SHORT only when:
  - Candle N closes on the opposite side of session VWAP from the prior side, AND
  - Candle N+1 closes even further from VWAP on that new side (acceleration).

Also used to *force* a confirmed flip when price accelerates opposite the prior
side even if a raw vote has not yet flipped (upstream Fix-3 anchor).

Separate from SQ's VW sub-score (composite persistence), which does not change
Garuda ``side`` or RS ``ranking_type``.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from backend.database import engine

logger = logging.getLogger(__name__)

LOG_PREFIX = "vwap_2candle_side"
_TABLE_OK = False


def vwap_2candle_side_enabled() -> bool:
    return os.environ.get("VWAP_2CANDLE_SIDE_GATE", "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def bars_with_session_vwap(
    candles: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Closed 10m bars with cumulative session VWAP attached (for flip confirm)."""
    from backend.services.vwap_side_gate import closed_10m_session_bars

    closed = closed_10m_session_bars(candles or [], now=now)
    out: List[Dict[str, Any]] = []
    pv = vv = 0.0
    for b in closed:
        try:
            h = float(b.get("high"))
            l = float(b.get("low"))
            c = float(b.get("close"))
            v = float(b.get("volume") or 1.0)
        except (TypeError, ValueError):
            continue
        tp = (h + l + c) / 3.0
        pv += tp * v
        vv += v
        row = dict(b)
        row["vwap"] = (pv / vv) if vv > 0 else None
        out.append(row)
    return out


def ensure_directional_side_flip_log() -> None:
    global _TABLE_OK
    if _TABLE_OK:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS directional_side_flip_log (
                    id BIGSERIAL PRIMARY KEY,
                    session_date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    source TEXT NOT NULL,
                    bar_end TIMESTAMPTZ,
                    prev_side TEXT,
                    raw_side TEXT,
                    resolved_side TEXT NOT NULL,
                    action TEXT NOT NULL,
                    flip_close DOUBLE PRECISION,
                    flip_vwap DOUBLE PRECISION,
                    flip_delta DOUBLE PRECISION,
                    confirm_close DOUBLE PRECISION,
                    confirm_vwap DOUBLE PRECISION,
                    confirm_delta DOUBLE PRECISION,
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS directional_side_flip_log_sess_sym_idx
                ON directional_side_flip_log (session_date, symbol)
                """
            )
        )
    _TABLE_OK = True


def _norm_side(side: Optional[str]) -> Optional[str]:
    if side is None:
        return None
    s = str(side).upper().strip()
    if s in ("SHORT", "BEAR", "BEARISH"):
        return "SHORT"
    if s in ("LONG", "BULL", "BULLISH"):
        return "LONG"
    if s in ("NEUTRAL", "", "NONE"):
        return "NEUTRAL"
    return s


def _bar_close_vwap(bar: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    try:
        close = float(bar.get("close"))
    except (TypeError, ValueError):
        return None, None
    vwap = bar.get("vwap")
    if vwap is None:
        return close, None
    try:
        return close, float(vwap)
    except (TypeError, ValueError):
        return close, None


def evaluate_2candle_vwap_confirm(
    prev_side: Optional[str],
    bars: List[Dict[str, Any]],
    idx: int,
) -> Dict[str, Any]:
    """At confirm bar ``idx``, test whether (idx-1, idx) confirm a flip from prev_side."""
    prev = _norm_side(prev_side)
    out: Dict[str, Any] = {
        "confirmed": False,
        "new_side": None,
        "reason": None,
        "flip": None,
        "confirm": None,
    }
    if prev not in ("LONG", "SHORT") or idx < 1 or idx >= len(bars):
        out["reason"] = "need_prev_side_and_two_bars"
        return out
    c0, v0 = _bar_close_vwap(bars[idx - 1])
    c1, v1 = _bar_close_vwap(bars[idx])
    if c0 is None or v0 is None or v0 <= 0 or c1 is None or v1 is None or v1 <= 0:
        out["reason"] = "missing_close_or_vwap"
        return out
    d0 = c0 - v0
    d1 = c1 - v1
    flip = {
        "close": c0,
        "vwap": round(v0, 4),
        "delta": round(d0, 4),
        "bar_end": str(bars[idx - 1].get("bar_end") or bars[idx - 1].get("timestamp")),
    }
    confirm = {
        "close": c1,
        "vwap": round(v1, 4),
        "delta": round(d1, 4),
        "bar_end": str(bars[idx].get("bar_end") or bars[idx].get("timestamp")),
    }
    out["flip"] = flip
    out["confirm"] = confirm
    if prev == "LONG":
        # Opposite = SHORT: both below VWAP, confirm further below
        if d0 < 0 and d1 < 0 and d1 < d0:
            out["confirmed"] = True
            out["new_side"] = "SHORT"
            out["reason"] = "short_extension_confirmed"
            return out
        if d0 < 0 and d1 < 0:
            out["reason"] = "flip_candle_ok_confirm_not_extended"
            return out
        out["reason"] = "no_flip_candle_below_vwap"
        return out
    # prev SHORT → confirm LONG
    if d0 > 0 and d1 > 0 and d1 > d0:
        out["confirmed"] = True
        out["new_side"] = "LONG"
        out["reason"] = "long_extension_confirmed"
        return out
    if d0 > 0 and d1 > 0:
        out["reason"] = "flip_candle_ok_confirm_not_extended"
        return out
    out["reason"] = "no_flip_candle_above_vwap"
    return out


def resolve_directional_side(
    prev_side: Optional[str],
    raw_side: Optional[str],
    bars: List[Dict[str, Any]],
    idx: int,
) -> Dict[str, Any]:
    """Resolve published side: 2-candle confirm forces flip; unconfirmed raw flips rejected."""
    prev = _norm_side(prev_side)
    raw = _norm_side(raw_side) or "NEUTRAL"
    detail = evaluate_2candle_vwap_confirm(prev, bars, idx)

    if detail.get("confirmed") and detail.get("new_side"):
        return {
            "side": detail["new_side"],
            "action": "confirmed_flip",
            "prev_side": prev,
            "raw_side": raw,
            "confirm_detail": detail,
        }

    if prev in ("LONG", "SHORT") and raw in ("LONG", "SHORT") and raw != prev:
        return {
            "side": prev,
            "action": "flip_rejected_no_confirm",
            "prev_side": prev,
            "raw_side": raw,
            "confirm_detail": detail,
        }

    if raw == "NEUTRAL" and prev in ("LONG", "SHORT"):
        return {
            "side": prev,
            "action": "hold_prev_on_neutral",
            "prev_side": prev,
            "raw_side": raw,
            "confirm_detail": detail,
        }

    side = raw if raw in ("LONG", "SHORT", "NEUTRAL") else (prev or "NEUTRAL")
    return {
        "side": side,
        "action": "raw",
        "prev_side": prev,
        "raw_side": raw,
        "confirm_detail": detail,
    }


def ranking_type_from_side(side: Optional[str]) -> Optional[str]:
    s = _norm_side(side)
    if s == "LONG":
        return "BULLISH"
    if s == "SHORT":
        return "BEARISH"
    if s == "NEUTRAL":
        return "NEUTRAL"
    return None


def side_from_ranking_type(ranking_type: Optional[str]) -> Optional[str]:
    return _norm_side(ranking_type)


def log_side_resolution(
    db,
    *,
    session_date: str,
    symbol: str,
    source: str,
    bar_end: Any,
    resolved: Dict[str, Any],
) -> None:
    """Persist flip evaluations (confirmed, rejected, or notable holds)."""
    action = resolved.get("action")
    if action not in (
        "confirmed_flip",
        "flip_rejected_no_confirm",
    ):
        # Only log decisions that matter for audit; skip quiet raw holds.
        return
    ensure_directional_side_flip_log()
    detail = resolved.get("confirm_detail") or {}
    flip = detail.get("flip") or {}
    confirm = detail.get("confirm") or {}
    try:
        db.execute(
            text(
                """
                INSERT INTO directional_side_flip_log (
                    session_date, symbol, source, bar_end,
                    prev_side, raw_side, resolved_side, action,
                    flip_close, flip_vwap, flip_delta,
                    confirm_close, confirm_vwap, confirm_delta
                ) VALUES (
                    CAST(:d AS date), :sym, :src, :be,
                    :prev, :raw, :resolved, :action,
                    :fc, :fv, :fd, :cc, :cv, :cd
                )
                """
            ),
            {
                "d": session_date,
                "sym": (symbol or "").upper(),
                "src": source,
                "be": bar_end,
                "prev": resolved.get("prev_side"),
                "raw": resolved.get("raw_side"),
                "resolved": resolved.get("side"),
                "action": action,
                "fc": flip.get("close"),
                "fv": flip.get("vwap"),
                "fd": flip.get("delta"),
                "cc": confirm.get("close"),
                "cv": confirm.get("vwap"),
                "cd": confirm.get("delta"),
            },
        )
    except Exception as exc:
        logger.warning("directional_side_flip_log insert failed %s: %s", symbol, exc)
    logger.info(
        "%s source=%s symbol=%s action=%s prev=%s raw=%s resolved=%s reason=%s",
        LOG_PREFIX,
        source,
        (symbol or "").upper(),
        action,
        resolved.get("prev_side"),
        resolved.get("raw_side"),
        resolved.get("side"),
        (detail.get("reason") if isinstance(detail, dict) else None),
    )
