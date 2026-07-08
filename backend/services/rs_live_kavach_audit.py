"""Persist live 10m Kavach recompute audit rows for locked checklist symbols."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
RETENTION_TRADING_DAYS = 10

_INSERT = text(
    """
    INSERT INTO rs_live_kavach_audit (
        session_date, computed_at, symbol, lock_direction,
        bar_evaluated_at, kavach_state, prev_kavach_state,
        trade_score, confidence_grade, volume_label,
        vwap_purity_pct, market_regime, adx,
        ema5, ema10, vwap, price, timeframe
    ) VALUES (
        :session_date, :computed_at, :symbol, :lock_direction,
        :bar_evaluated_at, :kavach_state, :prev_kavach_state,
        :trade_score, :confidence_grade, :volume_label,
        :vwap_purity_pct, :market_regime, :adx,
        :ema5, :ema10, :vwap, :price, :timeframe
    )
    """
)


def _session_date_from_bar(bar_evaluated_at: datetime) -> str:
    dt = bar_evaluated_at.astimezone(IST) if bar_evaluated_at.tzinfo else IST.localize(bar_evaluated_at)
    return dt.strftime("%Y-%m-%d")


def last_audit_state(db, session_date: str, symbol: str) -> Optional[str]:
    _, state, _ = latest_audit_pair(db, session_date, symbol)
    return state


def latest_audit_pair(db, session_date: str, symbol: str) -> tuple:
    row = db.execute(
        text(
            """
            SELECT prev_kavach_state, kavach_state, trade_score, confidence_grade, price
            FROM rs_live_kavach_audit
            WHERE session_date = CAST(:d AS date) AND symbol = :sym
            ORDER BY bar_evaluated_at DESC, id DESC
            LIMIT 1
            """
        ),
        {"d": session_date, "sym": symbol.upper()},
    ).fetchone()
    if not row:
        return None, None, {}
    return row.prev_kavach_state, row.kavach_state, {
        "trade_score": row.trade_score,
        "confidence_grade": row.confidence_grade,
        "price": row.price,
    }


def persist_live_kavach_audit(
    db,
    *,
    symbol: str,
    lock_direction: str,
    metrics: Dict[str, Any],
    prev_kavach_state: Optional[str] = None,
) -> None:
    """Write one audit row for this recompute cycle."""
    bar_at = metrics.get("bar_evaluated_at")
    if bar_at is None:
        return
    if isinstance(bar_at, str):
        bar_at = datetime.fromisoformat(bar_at.replace("Z", "+00:00"))
    if bar_at.tzinfo is None:
        bar_at = IST.localize(bar_at)
    sd = _session_date_from_bar(bar_at)
    now = datetime.now(IST)
    params = {
        "session_date": sd,
        "computed_at": now,
        "symbol": symbol.upper(),
        "lock_direction": lock_direction,
        "bar_evaluated_at": bar_at,
        "kavach_state": metrics.get("kavach_state"),
        "prev_kavach_state": prev_kavach_state,
        "trade_score": metrics.get("trade_score"),
        "confidence_grade": metrics.get("confidence_grade"),
        "volume_label": metrics.get("volume_label"),
        "vwap_purity_pct": metrics.get("vwap_purity_pct"),
        "market_regime": metrics.get("market_regime"),
        "adx": metrics.get("adx"),
        "ema5": metrics.get("ema5"),
        "ema10": metrics.get("ema10_10m"),
        "vwap": metrics.get("vwap"),
        "price": metrics.get("price"),
        "timeframe": metrics.get("timeframe") or "10m",
    }
    db.execute(_INSERT, params)


def prune_old_audit_rows(db, *, keep_days: int = RETENTION_TRADING_DAYS) -> int:
    cutoff = (datetime.now(IST) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    res = db.execute(
        text("DELETE FROM rs_live_kavach_audit WHERE session_date < CAST(:c AS date)"),
        {"c": cutoff},
    )
    return res.rowcount or 0
