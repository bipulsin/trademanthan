"""Post-scan entry monitoring — every 5 minutes until market close."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pytz

from backend.config import settings
from backend.database import SessionLocal
from backend.services.market_data.reads import get_row_market_snapshot, ltp_map_with_fallback
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.upstox_service import UpstoxService
from backend.services.volume_mismatch.candles import (
    batch_fetch_candles,
    clear_candle_cache,
    last_two_completed_5m_bars,
)
from sqlalchemy import text

from backend.services.volume_mismatch.repository import (
    fetch_signals_for_date,
    update_signal_monitor_fields,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
MARKET_CLOSE_MINUTES = 15 * 60 + 30  # 15:30 IST


def _ist_minutes(now: datetime) -> int:
    t = now.astimezone(IST) if now.tzinfo else IST.localize(now)
    return t.hour * 60 + t.minute


def is_monitor_window(now: Optional[datetime] = None) -> bool:
    t = now or datetime.now(IST)
    if t.weekday() >= 5:
        return False
    m = _ist_minutes(t)
    return (9 * 60 + 35) <= m <= MARKET_CLOSE_MINUTES


def _entry_ready_long(
    price: float,
    vwap: float,
    first_high: float,
    cur_5m_vol: float,
    prev_5m_vol: float,
) -> bool:
    if price <= 0 or vwap <= 0 or first_high <= 0:
        return False
    if price <= first_high or price <= vwap:
        return False
    if prev_5m_vol <= 0:
        return cur_5m_vol > 0
    return cur_5m_vol > prev_5m_vol


def _entry_ready_short(
    price: float,
    vwap: float,
    first_low: float,
    cur_5m_vol: float,
    prev_5m_vol: float,
) -> bool:
    if price <= 0 or vwap <= 0 or first_low <= 0:
        return False
    if price >= first_low or price >= vwap:
        return False
    if prev_5m_vol <= 0:
        return cur_5m_vol > 0
    return cur_5m_vol > prev_5m_vol


def run_volume_mismatch_entry_monitor(
    *,
    trade_date: Optional[date] = None,
) -> Dict[str, Any]:
    now = datetime.now(IST)
    if not is_monitor_window(now):
        return {"success": True, "skipped": "outside_monitor_window"}

    sd = trade_date or effective_session_date_ist_for_trend()
    db = SessionLocal()
    try:
        rows = fetch_signals_for_date(db, sd)
        waiting = [r for r in rows if str(r.get("entry_status") or "").upper() == "WAITING"]
        if not waiting:
            return {"success": True, "trade_date": str(sd), "checked": 0, "ready": 0}

        keys = [str(r.get("instrument_token") or "").strip() for r in waiting]
        keys = [k for k in keys if k]
        ltp_map = ltp_map_with_fallback(keys, allow_broker_fallback=True, allow_stale=True)

        clear_candle_cache()
        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        candles_5m = batch_fetch_candles(upstox, keys, "minutes/5", days_back=2)

        ready_count = 0
        status_changes: List[Dict[str, Any]] = []
        for r in waiting:
            sid = int(r["id"])
            ik = str(r.get("instrument_token") or "").strip()
            sym = str(r.get("symbol") or "").strip()
            direction = str(r.get("direction") or "").upper()
            first_high = float(r.get("first_15m_high") or 0)
            first_low = float(r.get("first_15m_low") or 0)

            price = ltp_map.get(ik)
            if price is None:
                snap = get_row_market_snapshot(sym)
                if snap:
                    try:
                        price = float(snap.get("currmth_future_ltp") or 0) or None
                    except (TypeError, ValueError):
                        price = None

            vwap: Optional[float] = None
            snap = get_row_market_snapshot(sym)
            if snap:
                try:
                    vwap = float(snap.get("currmth_future_vwap") or 0) or None
                except (TypeError, ValueError):
                    vwap = None

            cur_5m, prev_5m = last_two_completed_5m_bars(candles_5m.get(ik) or [], now=now)
            cur_vol = float((cur_5m or {}).get("volume") or 0)
            prev_vol = float((prev_5m or {}).get("volume") or 0)

            ready = False
            if price and vwap:
                if direction == "LONG":
                    ready = _entry_ready_long(price, vwap, first_high, cur_vol, prev_vol)
                elif direction == "SHORT":
                    ready = _entry_ready_short(price, vwap, first_low, cur_vol, prev_vol)

            new_status = "READY" if ready else "WAITING"
            update_signal_monitor_fields(
                db,
                sid,
                current_price=price,
                vwap=vwap,
                entry_status=new_status,
            )
            if ready:
                ready_count += 1
                status_changes.append(
                    {
                        "id": sid,
                        "symbol": sym,
                        "direction": direction,
                        "previous_status": "WAITING",
                        "entry_status": "READY",
                        "preferred_entry": r.get("preferred_entry"),
                        "stop_loss": r.get("stop_loss"),
                        "target1": r.get("target1"),
                        "target2": r.get("target2"),
                    }
                )

        db.commit()
        if ready_count:
            logger.info("Volume Mismatch monitor %s: %s READY of %s WAITING", sd, ready_count, len(waiting))
        return {
            "success": True,
            "trade_date": str(sd),
            "checked": len(waiting),
            "ready": ready_count,
            "status_changes": status_changes,
        }
    except Exception as e:
        db.rollback()
        logger.error("VM entry monitor failed: %s", e, exc_info=True)
        raise
    finally:
        db.close()


def expire_stale_signals(trade_date: Optional[date] = None) -> int:
    """Mark WAITING signals EXPIRED after market close."""
    sd = trade_date or effective_session_date_ist_for_trend()
    db = SessionLocal()
    try:
        res = db.execute(
            text(
                """
                UPDATE volume_mismatch_signals
                SET entry_status = 'EXPIRED', updated_at = NOW()
                WHERE trade_date = :trade_date
                  AND UPPER(entry_status) = 'WAITING'
                """
            ),
            {"trade_date": sd},
        )
        db.commit()
        return int(res.rowcount or 0)
    finally:
        db.close()
