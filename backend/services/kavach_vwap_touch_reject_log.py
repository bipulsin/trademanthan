"""Shadow log: VWAP intrabar touch-and-reject on 10m bars (research only).

Flag definition (per closed 10m bar, lock direction):
  LONG:  low <= vwap and close > vwap
  SHORT: high >= vwap and close < vwap

Never used to gate READY / entries. Forward-logged from live Kavach audit path.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
TABLE = "kavach_vwap_touch_reject_log"

_CREATE = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    id BIGSERIAL PRIMARY KEY,
    session_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    lock_direction TEXT NOT NULL,
    bar_evaluated_at TIMESTAMPTZ NOT NULL,
    bar_open DOUBLE PRECISION,
    bar_high DOUBLE PRECISION,
    bar_low DOUBLE PRECISION,
    bar_close DOUBLE PRECISION,
    vwap DOUBLE PRECISION,
    vwap_touch_reject BOOLEAN NOT NULL DEFAULT FALSE,
    vwap_wick_through_pts DOUBLE PRECISION,
    source TEXT DEFAULT 'live',
    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (session_date, symbol, lock_direction, bar_evaluated_at)
)
"""

_INSERT = text(
    f"""
    INSERT INTO {TABLE} (
        session_date, symbol, lock_direction, bar_evaluated_at,
        bar_open, bar_high, bar_low, bar_close, vwap,
        vwap_touch_reject, vwap_wick_through_pts, source
    ) VALUES (
        CAST(:session_date AS date), :symbol, :lock_direction, :bar_evaluated_at,
        :bar_open, :bar_high, :bar_low, :bar_close, :vwap,
        :vwap_touch_reject, :vwap_wick_through_pts, :source
    )
    ON CONFLICT (session_date, symbol, lock_direction, bar_evaluated_at) DO UPDATE SET
        bar_open = EXCLUDED.bar_open,
        bar_high = EXCLUDED.bar_high,
        bar_low = EXCLUDED.bar_low,
        bar_close = EXCLUDED.bar_close,
        vwap = EXCLUDED.vwap,
        vwap_touch_reject = EXCLUDED.vwap_touch_reject,
        vwap_wick_through_pts = EXCLUDED.vwap_wick_through_pts,
        source = EXCLUDED.source,
        logged_at = NOW()
    """
)


def ensure_vwap_touch_reject_table() -> None:
    with engine.begin() as conn:
        conn.execute(text(_CREATE))
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_session "
                f"ON {TABLE} (session_date DESC, symbol)"
            )
        )


def compute_touch_reject(
    *,
    direction: str,
    high: Optional[float],
    low: Optional[float],
    close: Optional[float],
    vwap: Optional[float],
) -> Dict[str, Any]:
    d = (direction or "").upper()
    try:
        h, l, c, v = float(high), float(low), float(close), float(vwap)
    except (TypeError, ValueError):
        return {"vwap_touch_reject": False, "vwap_wick_through_pts": None}
    if d == "LONG":
        ok = l <= v < c or (l <= v and c > v)
        # standard: low touches/crosses VWAP, close back above
        ok = l <= v and c > v
        wick = max(0.0, v - l) if ok else 0.0
    elif d == "SHORT":
        ok = h >= v and c < v
        wick = max(0.0, h - v) if ok else 0.0
    else:
        return {"vwap_touch_reject": False, "vwap_wick_through_pts": None}
    return {"vwap_touch_reject": bool(ok), "vwap_wick_through_pts": round(wick, 4) if ok else 0.0}


def persist_vwap_touch_reject(
    db,
    *,
    symbol: str,
    lock_direction: str,
    metrics: Dict[str, Any],
    source: str = "live",
) -> None:
    """Shadow-only write. Safe no-op if OHLC/VWAP missing."""
    bar_at = metrics.get("bar_evaluated_at")
    vwap = metrics.get("vwap")
    high = metrics.get("bar_high")
    low = metrics.get("bar_low")
    close = metrics.get("price") or metrics.get("bar_close")
    if bar_at is None or vwap is None or high is None or low is None or close is None:
        return
    if isinstance(bar_at, str):
        bar_at = datetime.fromisoformat(bar_at.replace("Z", "+00:00"))
    if bar_at.tzinfo is None:
        bar_at = IST.localize(bar_at)
    else:
        bar_at = bar_at.astimezone(IST)
    flags = compute_touch_reject(
        direction=lock_direction, high=high, low=low, close=close, vwap=vwap
    )
    # Log all lock bars (True and False) so absence ≠ not computed; filter True offline.
    # Do NOT call ensure_vwap_touch_reject_table() here: DDL (CREATE INDEX) while
    # ``db`` holds an open transaction deadlocks against itself on multi-bar writes.
    sd = bar_at.strftime("%Y-%m-%d")
    try:
        db.execute(
            _INSERT,
            {
                "session_date": sd,
                "symbol": symbol.upper(),
                "lock_direction": (lock_direction or "").upper(),
                "bar_evaluated_at": bar_at,
                "bar_open": metrics.get("bar_open"),
                "bar_high": high,
                "bar_low": low,
                "bar_close": close,
                "vwap": vwap,
                "vwap_touch_reject": flags["vwap_touch_reject"],
                "vwap_wick_through_pts": flags["vwap_wick_through_pts"],
                "source": source,
            },
        )
    except Exception:
        logger.exception("persist_vwap_touch_reject failed %s", symbol)
