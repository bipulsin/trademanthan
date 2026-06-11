"""Post-scan entry monitoring — every 5 minutes until market close."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pytz

from backend.config import settings
from backend.database import SessionLocal
from backend.services.market_data.reads import ltp_map_with_fallback
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.upstox_service import UpstoxService
from backend.services.volume_mismatch.candles import (
    batch_fetch_candles,
    clear_candle_cache,
    last_two_completed_5m_bars,
)
from sqlalchemy import text

from backend.services.volume_mismatch.fresh_market import (
    refresh_stale_arbitrage_master,
    resolve_fut_price_and_indicators,
)
from backend.services.volume_mismatch.momentum_discovery import (
    discover_intraday_momentum_signals,
    is_momentum_discovery_window,
)
from backend.services.volume_mismatch.repository import (
    fetch_signals_for_date,
    update_signal_monitor_fields,
)
from backend.services.volume_mismatch.session_trend import (
    assess_session_trend,
    flipped_direction,
)
from backend.services.volume_mismatch.signal_engine import trade_levels_for_direction

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

        discovered: List[Dict[str, Any]] = []
        if is_momentum_discovery_window(now):
            try:
                discovered = discover_intraday_momentum_signals(db, sd, now=now)
                if discovered:
                    rows = fetch_signals_for_date(db, sd)
                    waiting = [r for r in rows if str(r.get("entry_status") or "").upper() == "WAITING"]
            except Exception as disc_err:
                logger.warning("VM momentum discovery skipped: %s", disc_err)

        if not waiting:
            return {
                "success": True,
                "trade_date": str(sd),
                "checked": 0,
                "ready": 0,
                "discovered": len(discovered),
            }

        symbols = [str(r.get("symbol") or "").strip().upper() for r in waiting]
        refresh_stale_arbitrage_master(symbols)

        keys = [str(r.get("instrument_token") or "").strip() for r in waiting]
        keys = [k for k in keys if k]
        ltp_map = ltp_map_with_fallback(
            keys,
            allow_broker_fallback=True,
            allow_stale=False,
        )

        clear_candle_cache()
        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        candles_5m = batch_fetch_candles(upstox, keys, "minutes/5", days_back=2)

        ready_count = 0
        flip_count = 0
        status_changes: List[Dict[str, Any]] = []
        for r in waiting:
            sid = int(r["id"])
            ik = str(r.get("instrument_token") or "").strip()
            sym = str(r.get("symbol") or "").strip()
            direction = str(r.get("direction") or "").upper()
            first_high = float(r.get("first_15m_high") or 0)
            first_low = float(r.get("first_15m_low") or 0)

            bars = candles_5m.get(ik) or []
            resolved = resolve_fut_price_and_indicators(
                instrument_key=ik,
                symbol=sym,
                ltp_map=ltp_map,
                candles_5m=bars,
            )
            price = resolved.get("price")
            vwap = resolved.get("vwap")
            ema5 = resolved.get("ema5")

            cur_5m, prev_5m = last_two_completed_5m_bars(bars, now=now)
            cur_vol = float((cur_5m or {}).get("volume") or 0)
            prev_vol = float((prev_5m or {}).get("volume") or 0)

            flip_fields: Dict[str, Any] = {}
            if price and vwap and ema5 and cur_5m:
                trend = assess_session_trend(price, vwap, ema5, cur_5m, prev_5m)
                new_dir = flipped_direction(direction, trend)
                if new_dir:
                    levels = trade_levels_for_direction(new_dir, first_high, first_low)
                    direction = new_dir
                    flip_fields = {
                        "direction": new_dir,
                        **levels,
                    }
                    flip_count += 1
                    logger.info(
                        "VM direction flip %s %s -> %s (trend=%s price=%.2f vwap=%.2f ema5=%.2f)",
                        sd,
                        sym,
                        new_dir,
                        trend,
                        price,
                        vwap,
                        ema5,
                    )

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
                **flip_fields,
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
                        "preferred_entry": flip_fields.get("preferred_entry") or r.get("preferred_entry"),
                        "stop_loss": flip_fields.get("stop_loss") or r.get("stop_loss"),
                        "target1": flip_fields.get("target1") or r.get("target1"),
                        "target2": flip_fields.get("target2") or r.get("target2"),
                        "direction_flipped": bool(flip_fields),
                    }
                )

        db.commit()
        if ready_count or flip_count:
            logger.info(
                "Volume Mismatch monitor %s: %s READY of %s WAITING, %s direction flips",
                sd,
                ready_count,
                len(waiting),
                flip_count,
            )
        return {
            "success": True,
            "trade_date": str(sd),
            "checked": len(waiting),
            "ready": ready_count,
            "direction_flips": flip_count,
            "discovered": len(discovered),
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
