"""
Higher-timeframe (HTF) 4-way OI for dashboard heatmap.

Once per day after the session (16:05 IST, after the 16:00 OI freeze snapshot).
Compares last completed Upstox ``days/1`` FUT bar vs the bar 10 trading sessions earlier
(close and OI). Does not run on the 30-min session heatmap path.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# Latest completed daily bar vs the daily bar 10 sessions earlier (11 bars inclusive).
HTF_OI_SESSIONS_BACK = 10
# Calendar span so 11+ NSE sessions are present (weekends/holidays).
HTF_DAILY_DAYS_BACK = 25

_lock = threading.Lock()
_by_instrument: Dict[str, Dict[str, Any]] = {}
_computed_at_iso: Optional[str] = None


def daily_bars_with_oi(candles: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Ascending daily bars that include close + OI (Upstox candle[6] mapped to ``oi``)."""
    out: List[Dict[str, Any]] = []
    for c in candles or []:
        if c.get("oi") is None or c.get("close") is None:
            continue
        ts = str(c.get("timestamp") or "").strip()
        if not ts:
            continue
        try:
            close = float(c["close"])
            oi = float(c["oi"])
        except (TypeError, ValueError):
            continue
        out.append({"timestamp": ts, "close": close, "oi": oi})
    out.sort(key=lambda x: x["timestamp"])
    return out


def htf_oi_from_daily_bars(bars: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Window: ``bars[-1]`` = last completed daily bar; ``bars[-(10+1)]`` = 10 sessions earlier.
    Same 4-way rules as session OI (price Δ vs OI Δ).
    """
    need = HTF_OI_SESSIONS_BACK + 1
    if len(bars) < need:
        return None
    latest = bars[-1]
    prior = bars[-need]
    dp = float(latest["close"]) - float(prior["close"])
    doi = float(latest["oi"]) - float(prior["oi"])
    from backend.services.oi_heatmap import _normalize_signal
    from backend.services.oi_integration import interpret_oi_signal

    sig = _normalize_signal(interpret_oi_signal(dp, doi))
    return {
        "htf_oi_signal": sig,
        "price_from": float(prior["close"]),
        "price_to": float(latest["close"]),
        "oi_from": int(prior["oi"]),
        "oi_to": int(latest["oi"]),
        "bar_latest": latest["timestamp"],
        "bar_prior": prior["timestamp"],
        "sessions_back": HTF_OI_SESSIONS_BACK,
    }


def _persist(rows: List[Dict[str, Any]], computed_at: datetime) -> None:
    db = SessionLocal()
    try:
        db.execute(text("DELETE FROM oi_heatmap_htf_latest"))
        for r in rows:
            db.execute(
                text(
                    """
                    INSERT INTO oi_heatmap_htf_latest (
                        instrument_key, underlying_symbol, htf_oi_signal,
                        price_from, price_to, oi_from, oi_to,
                        bar_latest, bar_prior, computed_at
                    ) VALUES (
                        :instrument_key, :underlying_symbol, :htf_oi_signal,
                        :price_from, :price_to, :oi_from, :oi_to,
                        :bar_latest, :bar_prior, :computed_at
                    )
                    """
                ),
                {
                    "instrument_key": r["instrument_key"],
                    "underlying_symbol": r.get("underlying_symbol"),
                    "htf_oi_signal": r.get("htf_oi_signal"),
                    "price_from": r.get("price_from"),
                    "price_to": r.get("price_to"),
                    "oi_from": r.get("oi_from"),
                    "oi_to": r.get("oi_to"),
                    "bar_latest": r.get("bar_latest"),
                    "bar_prior": r.get("bar_prior"),
                    "computed_at": computed_at,
                },
            )
        db.commit()
    except Exception as e:
        db.rollback()
        logger.warning("oi_heatmap_htf: persist skipped: %s", e)
        raise
    finally:
        db.close()


def load_htf_from_db() -> Tuple[Dict[str, Dict[str, Any]], Optional[str]]:
    db = SessionLocal()
    by_ik: Dict[str, Dict[str, Any]] = {}
    computed: Optional[str] = None
    try:
        rows = db.execute(
            text(
                """
                SELECT instrument_key, underlying_symbol, htf_oi_signal,
                       price_from, price_to, oi_from, oi_to,
                       bar_latest, bar_prior, computed_at
                FROM oi_heatmap_htf_latest
                """
            )
        ).mappings()
        for row in rows:
            d = dict(row)
            ik = str(d.get("instrument_key") or "").strip()
            if not ik:
                continue
            cat = d.get("computed_at")
            if computed is None and cat is not None:
                computed = cat.isoformat() if hasattr(cat, "isoformat") else str(cat)
            by_ik[ik] = d
    except Exception as e:
        logger.debug("oi_heatmap_htf: load failed: %s", e)
    finally:
        db.close()
    return by_ik, computed


def get_htf_cache() -> Tuple[Dict[str, Dict[str, Any]], Optional[str]]:
    global _by_instrument, _computed_at_iso
    with _lock:
        if _by_instrument:
            return dict(_by_instrument), _computed_at_iso
    by_ik, ts = load_htf_from_db()
    with _lock:
        _by_instrument = by_ik
        _computed_at_iso = ts
        return dict(_by_instrument), _computed_at_iso


def attach_htf_oi(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_ik, _ = get_htf_cache()
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        d = dict(r)
        ik = str(d.get("instrument_key") or "").strip()
        rec = by_ik.get(ik) or {}
        d["htf_oi_signal"] = rec.get("htf_oi_signal")
        d["htf_bar_latest"] = rec.get("bar_latest")
        d["htf_bar_prior"] = rec.get("bar_prior")
        out.append(d)
    return out


def htf_window_payload() -> Dict[str, Any]:
    _, ts = get_htf_cache()
    return {
        "sessions_back": HTF_OI_SESSIONS_BACK,
        "latest_bar": "last completed daily FUT bar from Upstox days/1 (close + oi)",
        "prior_bar": "daily FUT bar 10 trading sessions earlier in the same series",
        "computed_at": ts,
    }


def refresh_oi_heatmap_htf(*, force: bool = False) -> Dict[str, Any]:
    """
    Bulk ``days/1`` (OI field) for arbitrage_master currmth FUT. Cached via shared candle cache.
    """
    global _by_instrument, _computed_at_iso

    if not getattr(settings, "OI_HEATMAP_LIVE_ENABLED", True):
        return {"success": False, "skipped": "OI_HEATMAP_LIVE_ENABLED false"}

    if not force:
        _, ts = get_htf_cache()
        if ts:
            try:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                if dt.astimezone(IST).date() == datetime.now(IST).date():
                    return {"success": True, "skipped": "already_computed_today", "computed_at": ts}
            except Exception:
                pass

    from backend.services.oi_heatmap import (
        build_universe_instrument_keys_from_arbitrage_master,
        load_nse_instruments_json,
    )
    from backend.services.upstox_service import UpstoxService

    keys = build_universe_instrument_keys_from_arbitrage_master()
    if not keys:
        return {"success": False, "error": "empty_universe"}

    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    raw = load_nse_instruments_json()
    ik_meta = {((r.get("instrument_key") or "").strip()): r for r in raw if isinstance(r, dict)}
    pause_s = float(getattr(settings, "OI_BATCH_PAUSE_SEC", 0.35))

    rows: List[Dict[str, Any]] = []
    skipped_no_oi = 0
    skipped_short = 0
    for i, ik in enumerate(keys):
        if i > 0 and pause_s > 0:
            time.sleep(pause_s)
        try:
            candles = ux.get_historical_candles_by_instrument_key(
                ik, interval="days/1", days_back=HTF_DAILY_DAYS_BACK
            )
        except Exception as e:
            logger.debug("oi_heatmap_htf: candles %s: %s", ik, e)
            skipped_no_oi += 1
            continue
        bars = daily_bars_with_oi(candles)
        if not bars:
            skipped_no_oi += 1
            continue
        parsed = htf_oi_from_daily_bars(bars)
        if not parsed:
            skipped_short += 1
            continue
        meta = ik_meta.get(ik) or {}
        und = (meta.get("underlying_symbol") or meta.get("name") or "").strip().upper()
        rec = dict(parsed)
        rec["instrument_key"] = ik
        rec["underlying_symbol"] = und
        rows.append(rec)

    now = datetime.now(IST)
    try:
        _persist(rows, now)
    except Exception as e:
        return {"success": False, "error": str(e), "count": len(rows)}

    by_ik = {r["instrument_key"]: r for r in rows}
    iso = now.isoformat()
    with _lock:
        _by_instrument = by_ik
        _computed_at_iso = iso

    counts: Dict[str, int] = {}
    for r in rows:
        s = str(r.get("htf_oi_signal") or "NONE")
        counts[s] = counts.get(s, 0) + 1

    logger.info(
        "oi_heatmap_htf: computed %s rows skipped_no_oi=%s skipped_short=%s counts=%s",
        len(rows),
        skipped_no_oi,
        skipped_short,
        counts,
    )
    return {
        "success": True,
        "count": len(rows),
        "universe": len(keys),
        "skipped_no_oi": skipped_no_oi,
        "skipped_short_series": skipped_short,
        "signal_counts": counts,
        "computed_at": iso,
        "window": htf_window_payload(),
    }
