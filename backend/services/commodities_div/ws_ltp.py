"""Live In-Trade LTPs for Commodities Div via shared Upstox v3 websocket feed.

Reuses ``upstox_market_feed`` (no second connection). Sidecar provider
``commodities_div_in_trade`` is merged into every subscribe so Rocket / market_data
replace-mode restarts do not drop commodity keys.

Breakfast exclusivity: ``set_feed_provider_keys`` / sync skip non-owner starts during
09:10–lock/09:25. REST ``refresh_in_trade_ltp`` remains the fallback.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, time as dt_time
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.commodities_div.schema import ensure_commodities_div_tables
from backend.services.ist_datetime import naive_ist

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
PROVIDER_NAME = "commodities_div_in_trade"
# Cap DB write rate per signal_id; UI polls workspace ~15s.
_PERSIST_MIN_INTERVAL_SEC = 3.0
# MCX metals / energy roughly 09:00–23:55 IST weekdays (broader than NSE cash).
_SESSION_START = dt_time(9, 0)
_SESSION_END = dt_time(23, 55)

_LOCK = threading.RLock()
# instrument_key (normalized |) -> [signal_id, ...]
_IK_TO_IDS: Dict[str, List[int]] = {}
# signal_id -> (monotonic_ts, last_ltp)
_LAST_PERSIST: Dict[int, tuple] = {}


def _normalize_ik(key: str) -> str:
    return (key or "").strip().replace(":", "|")


def _now_naive(now: Optional[datetime] = None) -> datetime:
    if now is None:
        return naive_ist(datetime.now(IST))
    return naive_ist(now)


def within_mcx_ltp_session(now: Optional[datetime] = None) -> bool:
    ts = _now_naive(now)
    if ts.weekday() >= 5:
        return False
    t = ts.time()
    return _SESSION_START <= t <= _SESSION_END


def list_in_trade_ltp_rows() -> List[Dict[str, Any]]:
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol_raw, symbol_mapped, instrument_key, contract, lot_size
                FROM commodities_div_signals
                WHERE status = 'In-Trade'
                ORDER BY id DESC
                """
            )
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


def ensure_in_trade_instrument_keys(rows: Optional[List[Dict[str, Any]]] = None) -> int:
    """Resolve missing instrument_key via mapping (front-month FUT)."""
    from backend.services.commodities_div.mapping import attach_instrument_fields

    src = rows if rows is not None else list_in_trade_ltp_rows()
    fixed = 0
    db = SessionLocal()
    try:
        for r in src:
            if (r.get("instrument_key") or "").strip():
                continue
            inst = attach_instrument_fields(str(r.get("symbol_raw") or ""))
            ik = (inst.get("instrument_key") or "").strip()
            if not ik:
                continue
            db.execute(
                text(
                    """
                    UPDATE commodities_div_signals SET
                        instrument_key = :ik,
                        contract = COALESCE(:contract, contract),
                        lot_size = COALESCE(:lot, lot_size),
                        updated_at = NOW()
                    WHERE id = :id AND status = 'In-Trade'
                    """
                ),
                {
                    "ik": ik,
                    "contract": inst.get("contract"),
                    "lot": inst.get("lot_size"),
                    "id": int(r["id"]),
                },
            )
            r["instrument_key"] = ik
            fixed += 1
        if fixed:
            db.commit()
    except Exception:
        db.rollback()
        logger.exception("commodities_div_ws_ltp: ensure keys failed")
    finally:
        db.close()
    return fixed


def in_trade_instrument_keys(rows: Optional[List[Dict[str, Any]]] = None) -> List[str]:
    src = rows if rows is not None else list_in_trade_ltp_rows()
    keys: List[str] = []
    seen = set()
    for r in src:
        raw = str(r.get("instrument_key") or "").strip()
        if not raw:
            continue
        ik = _normalize_ik(raw)
        if ik in seen:
            continue
        seen.add(ik)
        keys.append(raw)
    return keys


def _rebuild_ik_map(rows: List[Dict[str, Any]]) -> Dict[str, List[int]]:
    mapping: Dict[str, List[int]] = {}
    for r in rows:
        raw = str(r.get("instrument_key") or "").strip()
        if not raw:
            continue
        ik = _normalize_ik(raw)
        mapping.setdefault(ik, []).append(int(r["id"]))
    return mapping


def sync_in_trade_subscriptions(*, force: bool = False) -> Dict[str, Any]:
    """
    Refresh in-memory ik→signal map and register/clear the feed provider.

    Call on interval and after take/exit/delete so closed rows unsubscribe.
    """
    from backend.config import settings

    if not getattr(settings, "UPSTOX_MARKET_FEED_ENABLED", True):
        return {"ok": True, "skipped": "feed_disabled"}

    try:
        from backend.services.breakfast_upstox_gate import defer_job_for_breakfast_exclusivity

        if defer_job_for_breakfast_exclusivity("commodities_div_ws_ltp_sync"):
            return {"ok": True, "skipped": "breakfast_exclusivity"}
    except Exception as e:
        logger.exception("breakfast_exclusivity: check_failed error=%s", e)

    try:
        rows = list_in_trade_ltp_rows()
        ensure_in_trade_instrument_keys(rows)
        rows = list_in_trade_ltp_rows()
    except Exception as e:
        logger.info("commodities_div_ws_ltp: list rows failed: %s", e)
        return {"ok": False, "error": str(e)[:200]}

    mapping = _rebuild_ik_map(rows)
    keys = in_trade_instrument_keys(rows)

    with _LOCK:
        _IK_TO_IDS.clear()
        _IK_TO_IDS.update(mapping)
        live_ids = {int(r["id"]) for r in rows}
        for sid in list(_LAST_PERSIST.keys()):
            if sid not in live_ids:
                _LAST_PERSIST.pop(sid, None)

    try:
        from backend.services.upstox_market_feed import set_feed_provider_keys

        set_feed_provider_keys(PROVIDER_NAME, keys)
    except Exception as e:
        logger.info("commodities_div_ws_ltp: set_feed_provider_keys failed: %s", e)
        return {"ok": False, "error": str(e)[:200], "rows": len(rows), "keys": len(keys)}

    out = {
        "ok": True,
        "rows": len(rows),
        "keys": len(keys),
        "force": bool(force),
        "provider": PROVIDER_NAME,
    }
    logger.info("commodities_div_ws_ltp: synced subscriptions %s", out)
    return out


def on_upstox_commodities_div_tick(
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
        ids = list(_IK_TO_IDS.get(ik) or ())
    if not ids:
        return

    mono = time.monotonic()
    ts = _now_naive(now)
    for signal_id in ids:
        with _LOCK:
            prev = _LAST_PERSIST.get(signal_id)
            if prev is not None:
                last_ts, last_px = prev
                if (mono - last_ts) < _PERSIST_MIN_INTERVAL_SEC and abs(last_px - price) < 1e-9:
                    continue
            _LAST_PERSIST[signal_id] = (mono, price)
        _persist_ltp(signal_id, price, ts)


def _persist_ltp(signal_id: int, ltp: float, ts: datetime) -> None:
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE commodities_div_signals
                SET ltp = :ltp,
                    ltp_updated_at = :ts,
                    updated_at = NOW()
                WHERE id = :id
                  AND status = 'In-Trade'
                """
            ),
            {"ltp": ltp, "ts": ts, "id": int(signal_id)},
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.debug("commodities_div_ws_ltp: persist failed id=%s", signal_id, exc_info=True)
    finally:
        db.close()


def subscription_status() -> Dict[str, Any]:
    with _LOCK:
        return {
            "provider": PROVIDER_NAME,
            "tracked_keys": len(_IK_TO_IDS),
            "tracked_rows": sum(len(v) for v in _IK_TO_IDS.values()),
            "persist_throttle_sec": _PERSIST_MIN_INTERVAL_SEC,
        }
