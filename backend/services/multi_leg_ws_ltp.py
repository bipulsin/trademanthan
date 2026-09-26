"""Live LTP for open multi-leg journal legs via the shared Upstox v3 feed.

Sidecar provider ``multi_leg_options`` is merged into every subscribe, same pattern
as Stock Options and Commodities Div. Replacing the provider key set unsubscribes
legs that were closed or deleted (the feed manager has no per-key refcount; an
empty list removes this provider).
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
PROVIDER_NAME = "multi_leg_options"
_PERSIST_MIN_INTERVAL_SEC = 3.0

_LOCK = threading.RLock()
# instrument_key (normalized |) -> [leg_id, ...]
_IK_TO_LEGS: Dict[str, List[str]] = {}
_LAST_PERSIST: Dict[str, tuple] = {}


def _normalize_ik(key: str) -> str:
    return (key or "").strip().replace(":", "|")


def sync_subscriptions(*, force: bool = False) -> Dict[str, Any]:
    """Register open-leg keys, or clear the provider when none remain."""
    from backend.config import settings

    if not getattr(settings, "UPSTOX_MARKET_FEED_ENABLED", True):
        return {"ok": True, "skipped": "feed_disabled"}

    try:
        from backend.services.breakfast_upstox_gate import defer_job_for_breakfast_exclusivity

        if defer_job_for_breakfast_exclusivity("multi_leg_ws_ltp_sync"):
            return {"ok": True, "skipped": "breakfast_exclusivity"}
    except Exception as exc:
        logger.exception("breakfast_exclusivity: check_failed error=%s", exc)

    try:
        from backend.services.multi_leg_options import active_open_leg_keys

        rows = active_open_leg_keys()
    except Exception as exc:
        logger.info("multi_leg_ws_ltp: list rows failed: %s", exc)
        return {"ok": False, "error": str(exc)[:200]}

    mapping: Dict[str, List[str]] = {}
    keys: List[str] = []
    seen = set()
    for row in rows:
        raw = str(row.get("instrument_key") or "").strip()
        if not raw:
            continue
        ik = _normalize_ik(raw)
        mapping.setdefault(ik, []).append(str(row["leg_id"]))
        if ik not in seen:
            seen.add(ik)
            keys.append(raw)

    with _LOCK:
        _IK_TO_LEGS.clear()
        _IK_TO_LEGS.update(mapping)
        live = {str(r["leg_id"]) for r in rows}
        for leg_id in list(_LAST_PERSIST.keys()):
            if leg_id not in live:
                _LAST_PERSIST.pop(leg_id, None)

    try:
        from backend.services.upstox_market_feed import set_feed_provider_keys

        set_feed_provider_keys(PROVIDER_NAME, keys)
    except Exception as exc:
        logger.info("multi_leg_ws_ltp: set_feed_provider_keys failed: %s", exc)
        return {"ok": False, "error": str(exc)[:200], "rows": len(rows), "keys": len(keys)}

    out = {"ok": True, "rows": len(rows), "keys": len(keys), "force": bool(force)}
    logger.info("multi_leg_ws_ltp: synced subscriptions %s", out)
    return out


def on_upstox_multi_leg_tick(
    instrument_key: str,
    *,
    ltp: float,
    now: Optional[datetime] = None,
) -> None:
    """Hot-path callback from upstox_market_feed. Persists LTP only (delta is REST greek)."""
    ik = _normalize_ik(instrument_key)
    try:
        price = float(ltp)
    except (TypeError, ValueError):
        return
    if price <= 0:
        return
    with _LOCK:
        leg_ids = list(_IK_TO_LEGS.get(ik) or ())
    if not leg_ids:
        return
    mono = time.monotonic()
    stamp = now or datetime.now(IST)
    if stamp.tzinfo is None:
        stamp = IST.localize(stamp)
    else:
        stamp = stamp.astimezone(IST)
    for leg_id in leg_ids:
        with _LOCK:
            prev = _LAST_PERSIST.get(leg_id)
            if prev is not None:
                last_ts, last_px = prev
                if (mono - last_ts) < _PERSIST_MIN_INTERVAL_SEC and abs(last_px - price) < 1e-9:
                    continue
            _LAST_PERSIST[leg_id] = (mono, price)
        _persist_leg_ltp(leg_id, price, stamp)


def _persist_leg_ltp(leg_id: str, ltp: float, ts: datetime) -> None:
    """Update LTP and leg PnL. direction_sign is +1 BUY / -1 SELL (see multi_leg_options)."""
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE multi_leg_trade_legs SET
                    ltp = :ltp,
                    leg_pnl = (:ltp - entry_price)
                        * CASE WHEN side = 'BUY' THEN 1 ELSE -1 END
                        * lot_size,
                    updated_at = :ts
                WHERE id = CAST(:id AS uuid)
                  AND exit_time IS NULL
                """
            ),
            {"id": leg_id, "ltp": ltp, "ts": ts},
        )
        db.execute(
            text(
                """
                UPDATE multi_leg_trades t SET
                    total_pnl = (
                        SELECT SUM(leg_pnl) FROM multi_leg_trade_legs WHERE trade_id = t.id
                    ),
                    updated_at = :ts
                WHERE t.id = (
                    SELECT trade_id FROM multi_leg_trade_legs WHERE id = CAST(:id AS uuid)
                )
                AND t.status = 'ACTIVE'
                """
            ),
            {"id": leg_id, "ts": ts},
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.debug("multi_leg_ws_ltp persist failed for %s", leg_id)
    finally:
        db.close()
