"""Stage 2 — 5m EMA refresh for Ready-to-Trade futures."""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz

from backend.services.momentx import state as mx_state
from backend.services.momentx.candles import batch_fetch, drop_forming_candle, splits_ohlcv
from backend.services.momentx.config import CONFIG
from backend.services.momentx.indicators import ema_last, is_ema_trigger, order_emas_by_value

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def refresh_emas(*, force: bool = False) -> Dict[str, Any]:
    from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

    now = datetime.now(IST)
    if not force and should_skip_scheduled_market_jobs_ist():
        return {"ok": True, "skipped": "holiday"}

    cards = mx_state.ready_cards()
    if not cards:
        mx_state.update_ema_fields({}, ema_at=now.isoformat())
        return {"ok": True, "updated": 0}

    mx_state.set_ema_running(True)
    started = time.monotonic()
    try:
        keys = [c["future_instrument_key"] for c in cards if c.get("future_instrument_key")]
        candles_by_ik = batch_fetch(
            keys,
            "minutes/5",
            CONFIG.minutes5_days_back,
            max_workers=CONFIG.candle_workers,
        )
        updates: Dict[str, Dict[str, Any]] = {}
        for card in cards:
            stock = str(card.get("stock") or "")
            ik = str(card.get("future_instrument_key") or "")
            try:
                raw = candles_by_ik.get(ik) or []
                closed = drop_forming_candle(raw, 5)
                if len(closed) < CONFIG.minutes5_min_bars:
                    logger.info("momentx ema skip %s: insufficient 5m (%s)", stock, len(closed))
                    continue
                _, _, _, closes, _ = splits_ohlcv(closed)
                e5 = ema_last(closes, CONFIG.ema_fast)
                e15 = ema_last(closes, CONFIG.ema_mid)
                e50 = ema_last(closes, CONFIG.ema_slow)
                trigger = is_ema_trigger(e5, e15, e50)
                prev_trigger = bool(card.get("trigger"))
                patch: Dict[str, Any] = {
                    "ema5": e5,
                    "ema15": e15,
                    "ema50": e50,
                    "ema_ordered": [
                        {"label": lab, "value": val}
                        for lab, val in order_emas_by_value(
                            e5, e15, e50,
                            fast=CONFIG.ema_fast,
                            mid=CONFIG.ema_mid,
                            slow=CONFIG.ema_slow,
                        )
                    ],
                    "ema_at": now.isoformat(),
                    "trigger": trigger,
                }
                if trigger and not prev_trigger:
                    patch["trigger_since"] = now.isoformat()
                    patch["trigger_event"] = True
                elif not trigger:
                    patch["trigger_since"] = None
                    patch["trigger_event"] = False
                else:
                    patch["trigger_event"] = False
                # Seed LTP from last closed 5m close when WS has not ticked yet.
                if card.get("ltp") is None and closes:
                    patch["ltp"] = float(closes[-1])
                    patch["ltp_at"] = now.isoformat()
                    patch["ltp_dir"] = "flat"
                updates[stock] = patch
            except Exception as e:
                logger.info("momentx ema error %s: %s", stock, e, exc_info=True)

        mx_state.update_ema_fields(updates, ema_at=now.isoformat())
        duration = time.monotonic() - started
        summary = {"ok": True, "updated": len(updates), "duration_sec": round(duration, 2)}
        logger.info("momentx ema refresh %s", summary)
        return summary
    finally:
        mx_state.set_ema_running(False)
