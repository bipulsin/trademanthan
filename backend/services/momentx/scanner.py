"""Stage 1 — 15-minute MomentX equity scan over arbitrage_master."""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz

from backend.services.momentx import state as mx_state
from backend.services.momentx.candles import batch_fetch, fetch_daily_cached
from backend.services.momentx.config import CONFIG
from backend.services.momentx.filter import evaluate_stage1
from backend.services.momentx.universe import load_momentx_universe

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _session_open(now: Optional[datetime] = None) -> bool:
    t = now or datetime.now(IST)
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    if t.weekday() >= 5:
        return False
    open_t = CONFIG.market_open_hhmm
    close_t = CONFIG.market_close_hhmm
    minutes = t.hour * 60 + t.minute
    return (open_t[0] * 60 + open_t[1]) <= minutes <= (close_t[0] * 60 + close_t[1])


def run_stage1_scan(*, force: bool = False) -> Dict[str, Any]:
    """Scan the universe; replace Ready-to-Trade cards (preserving EMA fields when possible)."""
    from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

    now = datetime.now(IST)
    if not force and should_skip_scheduled_market_jobs_ist():
        mx_state.set_market_status("holiday")
        return {"ok": True, "skipped": "holiday"}
    if not force and not _session_open(now):
        mx_state.set_market_status("closed")
        return {"ok": True, "skipped": "market_closed"}

    mx_state.set_market_status("open" if _session_open(now) else "closed")
    mx_state.set_scan_running(True)
    started = time.monotonic()
    skipped: List[str] = []
    qualified: List[Dict[str, Any]] = []

    try:
        universe = load_momentx_universe()
        equity_keys = [u["equity_instrument_key"] for u in universe]
        logger.info("momentx scan start universe=%s", len(universe))

        # Daily candles (process cache inside fetch_daily_cached) then 15m bars.
        daily_by_ik: Dict[str, List[Dict[str, Any]]] = {}
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=CONFIG.candle_workers) as pool:
            futs = {pool.submit(fetch_daily_cached, ik): ik for ik in equity_keys}
            for fut in as_completed(futs):
                ik = futs[fut]
                try:
                    daily_by_ik[ik] = fut.result() or []
                except Exception as e:
                    logger.info("momentx daily fetch failed %s: %s", ik, e)
                    daily_by_ik[ik] = []

        m15_by_ik = batch_fetch(
            equity_keys,
            "minutes/15",
            CONFIG.minutes15_days_back,
            max_workers=CONFIG.candle_workers,
        )
        daily_ok = sum(1 for v in daily_by_ik.values() if len(v) >= CONFIG.sma_period)
        m15_ok = sum(1 for v in m15_by_ik.values() if len(v) >= CONFIG.minutes15_min_bars)
        logger.info(
            "momentx candles ready daily_ok=%s/%s m15_ok=%s/%s",
            daily_ok,
            len(equity_keys),
            m15_ok,
            len(equity_keys),
        )

        prev_by_stock = {
            str(c.get("stock") or "").upper(): c for c in mx_state.ready_cards()
        }

        for row in universe:
            stock = row["stock"]
            eq = row["equity_instrument_key"]
            try:
                daily = daily_by_ik.get(eq) or []
                m15 = m15_by_ik.get(eq) or []
                ev = evaluate_stage1(daily=daily, minutes15=m15, now=now)
                if ev.get("skip_reason"):
                    skipped.append(f"{stock}:{ev['skip_reason']}")
                    continue
                if not ev.get("qualified"):
                    continue
                prev = prev_by_stock.get(stock.upper()) or {}
                card = {
                    "stock": stock,
                    "equity_instrument_key": eq,
                    "future_symbol": row["future_symbol"],
                    "future_instrument_key": row["future_instrument_key"],
                    "stage1": {
                        "price": ev.get("price"),
                        "sma200": ev.get("sma200"),
                        "prev_close": ev.get("prev_close"),
                        "pct_change": ev.get("pct_change"),
                        "rsi": ev.get("rsi"),
                        "williams_r": ev.get("williams_r"),
                        "adx": ev.get("adx"),
                        "today_volume": ev.get("today_volume"),
                        "volume_sma5": ev.get("volume_sma5"),
                        "conditions": ev.get("conditions") or {},
                    },
                    # Preserve Stage-2 fields across rescans when still Ready.
                    "ltp": prev.get("ltp"),
                    "ltp_at": prev.get("ltp_at"),
                    "ltp_dir": prev.get("ltp_dir") or "flat",
                    "ema5": prev.get("ema5"),
                    "ema15": prev.get("ema15"),
                    "ema50": prev.get("ema50"),
                    "ema_ordered": prev.get("ema_ordered") or [],
                    "ema_at": prev.get("ema_at"),
                    "trigger": bool(prev.get("trigger")),
                    "trigger_since": prev.get("trigger_since"),
                }
                qualified.append(card)
            except Exception as e:
                skipped.append(f"{stock}:error")
                logger.info("momentx scan symbol error %s: %s", stock, e, exc_info=True)

        duration = time.monotonic() - started
        mx_state.replace_ready(
            qualified,
            scan_at=now.isoformat(),
            universe_size=len(universe),
            skipped=skipped[:50],
            duration_sec=round(duration, 2),
            error=None,
        )
        # Refresh futures WS subscriptions for the new Ready set.
        try:
            from backend.services.momentx.ws_ltp import sync_momentx_subscriptions

            sync_momentx_subscriptions()
        except Exception as e:
            logger.info("momentx scan: ws sync failed: %s", e)

        # Kick an immediate EMA refresh for Ready cards.
        try:
            from backend.services.momentx.tracker import refresh_emas

            refresh_emas(force=True)
        except Exception as e:
            logger.info("momentx scan: ema refresh failed: %s", e)

        summary = {
            "ok": True,
            "universe": len(universe),
            "qualified": len(qualified),
            "skipped": len(skipped),
            "duration_sec": round(duration, 2),
        }
        logger.info("momentx scan done %s", summary)
        return summary
    except Exception as e:
        logger.exception("momentx scan failed")
        mx_state.replace_ready(
            mx_state.ready_cards(),
            universe_size=mx_state.snapshot().get("universe_size") or 0,
            error=str(e)[:300],
        )
        return {"ok": False, "error": str(e)[:300]}
    finally:
        mx_state.set_scan_running(False)


def ensure_scan_if_needed() -> None:
    """On API/page load: kick a background scan if market is open and none yet.

    Runs off-thread so ``GET /ready`` stays fast (~3 min full-universe candle fetch).
    """
    import threading

    from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

    snap = mx_state.snapshot()
    if snap.get("scan_at") or snap.get("scan_running"):
        return
    if should_skip_scheduled_market_jobs_ist():
        mx_state.set_market_status("holiday")
        return
    if not _session_open():
        mx_state.set_market_status("closed")
        return

    def _bg() -> None:
        try:
            run_stage1_scan(force=False)
        except Exception:
            logger.exception("momentx background ensure scan failed")

    mx_state.set_scan_running(True)
    threading.Thread(target=_bg, name="momentx-ensure-scan", daemon=True).start()
