"""Live Executed option LTPs from the shared Upstox v3 websocket feed.

Reuses ``upstox_market_feed`` (no second connection). Sidecar provider
``stock_option_executed`` is merged into every subscribe so Rocket / market_data
replace-mode restarts do not drop option keys.

Breakfast exclusivity: ``set_feed_provider_keys`` / ``ensure_market_feed_running``
skip non-owner starts during 09:10–lock/09:25. Scheduler only runs WS sync from
**09:30 IST** (after Breakfast) through the cash session. The 2h
``refresh_executed_ltps`` REST path remains the fallback.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.ist_datetime import naive_ist
from backend.services.stock_option_signals import (
    INVALIDATE_REMARKS,
    STATUS_EXECUTED,
    ensure_stock_option_tables,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
PROVIDER_NAME = "stock_option_executed"
# Cap DB write rate per (row_id, leg); UI polls workspace ~60s.
_PERSIST_MIN_INTERVAL_SEC = 5.0

_LOCK = threading.RLock()
# instrument_key (normalized :) -> [(row_id, "sell"|"buy"), ...]
_IK_TO_LEGS: Dict[str, List[Tuple[int, str]]] = {}
# (row_id, leg) -> (monotonic_ts, last_ltp)
_LAST_PERSIST: Dict[Tuple[int, str], Tuple[float, float]] = {}


def _normalize_ik(key: str) -> str:
    return (key or "").strip().replace(":", "|")


def _now_naive(now: Optional[datetime] = None) -> datetime:
    if now is None:
        return naive_ist(datetime.now(IST))
    return naive_ist(now)


def list_executed_option_ltp_rows() -> List[Dict[str, Any]]:
    """Executed rows with both legs filled (same filter as ``refresh_executed_ltps``).

    Includes auto-expired rows that the user later filled with costs/strikes.
    Excludes only explicit invalidate remarks (\"Trade not executed…\").
    """
    ensure_stock_option_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol, side,
                       sell_strike, buy_strike,
                       user_sell_strike, user_buy_strike,
                       sell_instrument_key, buy_instrument_key
                FROM stock_option_signals
                WHERE status = :executed
                  AND date_traded IS NOT NULL
                  AND sell_cost IS NOT NULL
                  AND buy_cost IS NOT NULL
                  AND (
                    COALESCE(user_sell_strike, sell_strike) IS NOT NULL
                    AND COALESCE(user_buy_strike, buy_strike) IS NOT NULL
                  )
                  AND remarks IS DISTINCT FROM :remarks
                """
            ),
            {
                "executed": STATUS_EXECUTED,
                "remarks": INVALIDATE_REMARKS,
            },
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


def executed_option_instrument_keys(rows: Optional[List[Dict[str, Any]]] = None) -> List[str]:
    src = rows if rows is not None else list_executed_option_ltp_rows()
    keys: List[str] = []
    seen = set()
    for r in src:
        for col in ("sell_instrument_key", "buy_instrument_key"):
            raw = str(r.get(col) or "").strip()
            if not raw:
                continue
            ik = _normalize_ik(raw)
            if ik in seen:
                continue
            seen.add(ik)
            keys.append(raw)
    return keys


def _rebuild_ik_map(rows: List[Dict[str, Any]]) -> Dict[str, List[Tuple[int, str]]]:
    mapping: Dict[str, List[Tuple[int, str]]] = {}
    for r in rows:
        rid = int(r["id"])
        for col, leg in (
            ("sell_instrument_key", "sell"),
            ("buy_instrument_key", "buy"),
        ):
            raw = str(r.get(col) or "").strip()
            if not raw:
                continue
            ik = _normalize_ik(raw)
            mapping.setdefault(ik, []).append((rid, leg))
    return mapping


def sync_executed_option_subscriptions(*, force: bool = False) -> Dict[str, Any]:
    """
    Refresh in-memory ik→row map and register/clear the feed provider.

    Call on a short interval and after submit/exit so incomplete/exited rows unsubscribe.
    """
    from backend.config import settings

    if not getattr(settings, "UPSTOX_MARKET_FEED_ENABLED", True):
        return {"ok": True, "skipped": "feed_disabled"}

    try:
        from backend.services.breakfast_upstox_gate import defer_job_for_breakfast_exclusivity

        if defer_job_for_breakfast_exclusivity("stock_option_ws_ltp_sync"):
            return {"ok": True, "skipped": "breakfast_exclusivity"}
    except Exception as e:
        logger.exception("breakfast_exclusivity: check_failed error=%s", e)

    try:
        from backend.services.stock_option_signals import ensure_executed_option_instrument_keys

        ensure_executed_option_instrument_keys()
    except Exception as e:
        logger.info("stock_option_ws_ltp: key ensure failed: %s", e)

    try:
        rows = list_executed_option_ltp_rows()
    except Exception as e:
        logger.info("stock_option_ws_ltp: list rows failed: %s", e)
        return {"ok": False, "error": str(e)[:200]}

    mapping = _rebuild_ik_map(rows)
    keys = executed_option_instrument_keys(rows)

    with _LOCK:
        _IK_TO_LEGS.clear()
        _IK_TO_LEGS.update(mapping)
        # Drop persist throttle for rows no longer tracked.
        live_ids = {int(r["id"]) for r in rows}
        for key in list(_LAST_PERSIST.keys()):
            if key[0] not in live_ids:
                _LAST_PERSIST.pop(key, None)

    try:
        from backend.services.upstox_market_feed import set_feed_provider_keys

        set_feed_provider_keys(PROVIDER_NAME, keys)
    except Exception as e:
        logger.info("stock_option_ws_ltp: set_feed_provider_keys failed: %s", e)
        return {"ok": False, "error": str(e)[:200], "rows": len(rows), "keys": len(keys)}

    out = {
        "ok": True,
        "rows": len(rows),
        "keys": len(keys),
        "force": bool(force),
    }
    logger.info("stock_option_ws_ltp: synced subscriptions %s", out)
    return out


def on_upstox_option_tick(
    instrument_key: str,
    *,
    ltp: float,
    now: Optional[datetime] = None,
) -> None:
    """Hot-path callback from ``upstox_market_feed`` ingest (throttled DB writes)."""
    ik = _normalize_ik(instrument_key)
    try:
        price = float(ltp)
    except (TypeError, ValueError):
        return
    if price <= 0:
        return

    with _LOCK:
        legs = list(_IK_TO_LEGS.get(ik) or ())
    if not legs:
        return

    mono = time.monotonic()
    ts = _now_naive(now)
    for row_id, leg in legs:
        throttle_key = (row_id, leg)
        with _LOCK:
            prev = _LAST_PERSIST.get(throttle_key)
            if prev is not None:
                last_ts, last_px = prev
                if (mono - last_ts) < _PERSIST_MIN_INTERVAL_SEC and abs(last_px - price) < 1e-9:
                    continue
            _LAST_PERSIST[throttle_key] = (mono, price)
        _persist_leg_ltp(row_id, leg, price, ts)


def _persist_leg_ltp(row_id: int, leg: str, ltp: float, ts: datetime) -> None:
    col = "sell_ltp" if leg == "sell" else "buy_ltp"
    if col not in ("sell_ltp", "buy_ltp"):
        return
    db = SessionLocal()
    try:
        db.execute(
            text(
                f"""
                UPDATE stock_option_signals
                SET {col} = :ltp,
                    ltp_updated_at = :ts,
                    updated_at = :ts
                WHERE id = :id
                  AND status = :executed
                """
            ),
            {
                "ltp": ltp,
                "ts": ts,
                "id": row_id,
                "executed": STATUS_EXECUTED,
            },
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.debug("stock_option_ws_ltp: persist failed id=%s leg=%s", row_id, leg, exc_info=True)
    finally:
        db.close()


def subscription_status() -> Dict[str, Any]:
    with _LOCK:
        return {
            "provider": PROVIDER_NAME,
            "tracked_keys": len(_IK_TO_LEGS),
            "tracked_legs": sum(len(v) for v in _IK_TO_LEGS.values()),
            "persist_throttle_sec": _PERSIST_MIN_INTERVAL_SEC,
        }
