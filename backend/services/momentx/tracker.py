"""Stage 2 — 5m EMA refresh for Ready-to-Trade futures."""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

import pytz

from backend.services.momentx import state as mx_state
from backend.services.momentx.candles import batch_fetch, drop_forming_candle, splits_ohlcv
from backend.services.momentx.config import CONFIG
from backend.services.momentx.indicators import (
    ema_cross_above_from_below,
    ema_last,
    ema_series,
    is_ema_trigger,
    order_emas_by_value,
)
from backend.services.momentx.status_rules import apply_status_to_card, past_scan_cutoff

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def refresh_emas(*, force: bool = False) -> Dict[str, Any]:
    from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

    now = datetime.now(IST)
    if not force and should_skip_scheduled_market_jobs_ist():
        return {"ok": True, "skipped": "holiday"}
    if not force and past_scan_cutoff(now):
        return {"ok": True, "skipped": "scan_cutoff"}

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
                e5_series = ema_series(closes, CONFIG.ema_fast)
                e10_series = ema_series(closes, CONFIG.ema_cross)
                e5 = e5_series[-1] if e5_series else None
                e10 = e10_series[-1] if e10_series else None
                e15 = ema_last(closes, CONFIG.ema_mid)
                e50 = ema_last(closes, CONFIG.ema_slow)
                trigger = is_ema_trigger(e5, e15, e50)
                prev_trigger = bool(card.get("trigger"))
                crossed = ema_cross_above_from_below(e5_series, e10_series)
                ordered = [
                    {"label": lab, "value": val}
                    for lab, val in order_emas_by_value(
                        e5, e15, e50,
                        fast=CONFIG.ema_fast,
                        mid=CONFIG.ema_mid,
                        slow=CONFIG.ema_slow,
                    )
                ]
                if e10 is not None:
                    ordered.append({"label": f"EMA({CONFIG.ema_cross})", "value": float(e10)})
                    ordered.sort(key=lambda r: float(r["value"]), reverse=True)
                patch: Dict[str, Any] = {
                    "ema5": e5,
                    "ema10": e10,
                    "ema15": e15,
                    "ema50": e50,
                    "ema_ordered": ordered,
                    "ema_at": now.isoformat(),
                    "trigger": trigger,
                    "ema5_cross_ema10": crossed,
                }
                if crossed:
                    # Apply Active reactivation onto a copy then copy status fields into patch.
                    tmp = dict(card)
                    tmp.update(patch)
                    apply_status_to_card(tmp, now=now, ema_cross_activate=True)
                    patch["status"] = tmp.get("status")
                    patch["active"] = tmp.get("active")
                    patch["active_since"] = tmp.get("active_since")
                    patch["first_active_at"] = tmp.get("first_active_at")
                    patch["filters_ok"] = tmp.get("filters_ok", True)
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
        # Re-apply Active→Locked timeouts after EMA patches.
        from backend.services.momentx.status_rules import refresh_board_statuses

        refreshed = refresh_board_statuses(mx_state.ready_cards(), now=now)
        mx_state.set_ready_cards(refreshed)
        duration = time.monotonic() - started
        summary = {
            "ok": True,
            "updated": len(updates),
            "cross_activations": sum(1 for u in updates.values() if u.get("ema5_cross_ema10")),
            "duration_sec": round(duration, 2),
        }
        logger.info("momentx ema refresh %s", summary)
        return summary
    finally:
        mx_state.set_ema_running(False)
