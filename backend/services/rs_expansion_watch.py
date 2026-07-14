"""Full-universe one-sided expansion detector (EXPANSION WATCH tier).

Scans the F&O universe for VWAP-slope steepening + EMA5/EMA10 alignment,
independent of RS Top-5/Top-10 rank. Surfaced as a distinct alert tier —
never merged into the RS-ranked checklist.

Live alerts stay OFF until ``scripts/analyze_expansion_watch_backtest.py``
clears a credible_positive Wilson / baseline-lift gate
(``EXPANSION_WATCH_LIVE=1`` only after that review).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.kavach_10m import aggregate_10m_bars, last_closed_10m_pair_end_idx
from backend.services.kavach_momentum_ignition_validate import THRESHOLD_VWAP_SLOPE
from backend.services.relative_strength_scanner import _f, _sorted_candles
from backend.services.rs_conviction_config import get_config
from backend.services.rs_conviction_signals import normalized_vwap_slope
from backend.services.vajra.indicators import ema_series

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

ALERT_TIER = "EXPANSION WATCH — not RS-ranked."
EMA_ALIGN_BARS = 2
DEFAULT_ATR_EXT_MAX = 1.5


def live_enabled() -> bool:
    return os.environ.get("EXPANSION_WATCH_LIVE", "0").strip().lower() in ("1", "true", "yes")


def atr_extension_max() -> float:
    raw = os.environ.get("EXPANSION_WATCH_ATR_MAX")
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return float(get_config().get("expansion_watch_atr_max") or DEFAULT_ATR_EXT_MAX)


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def fno_universe(db) -> List[str]:
    rows = db.execute(
        text(
            """
            SELECT DISTINCT UPPER(stock) AS symbol
            FROM arbitrage_master
            WHERE stock IS NOT NULL
              AND currmth_future_instrument_key IS NOT NULL
            ORDER BY 1
            """
        )
    ).fetchall()
    return [str(r.symbol).upper() for r in rows if r.symbol]


def _signed_slope_ok(score: float, signed: float, side: str) -> bool:
    if score < THRESHOLD_VWAP_SLOPE:
        return False
    if side == "SHORT":
        return signed < 0
    return signed > 0


def _signed_vwap_slope_atr(candles: List[Dict], atr_daily_pct: float) -> float:
    from backend.services.kavach_volume import last_closed_bar_index
    from backend.services.rs_conviction_signals import _today_slice, _vwap_series_today

    today, first_today = _today_slice(candles)
    vwap_s = _vwap_series_today(candles)
    closed = last_closed_bar_index(candles)
    if closed < first_today or len(vwap_s) < 7:
        return 0.0
    idx_now = min(len(vwap_s) - 1, closed - first_today)
    idx_prev = max(0, idx_now - 6)
    if idx_now <= idx_prev:
        return 0.0
    price = _f(candles[closed].get("close"), 1.0)
    atr = price * max(atr_daily_pct, 0.001) / 100.0
    if atr <= 0:
        return 0.0
    return (vwap_s[idx_now] - vwap_s[idx_prev]) / atr


def evaluate_candles_for_expansion(
    candles: List[Dict[str, Any]],
    *,
    side: str,
    atr_daily_pct: float,
    atr_ext_max: Optional[float] = None,
    cfg: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Return candidate dict if expansion criteria met on last confirmed 10m close."""
    if not candles or len(candles) < 40:
        return None
    cfg = cfg or get_config()
    atr_ext_max = atr_ext_max if atr_ext_max is not None else atr_extension_max()
    candles = _sorted_candles(candles)
    pair_end = last_closed_10m_pair_end_idx(candles)
    bars = [b for b in aggregate_10m_bars(candles) if b["end_5m_idx"] <= pair_end]
    if len(bars) < 12:
        return None

    closes = [float(b["close"]) for b in bars]
    ema5_s = ema_series(closes, 5)
    ema10_s = ema_series(closes, 10)
    if not ema5_s or not ema10_s or len(ema5_s) < EMA_ALIGN_BARS:
        return None

    is_long = (side or "LONG").upper() != "SHORT"
    aligned = 0
    for i in range(1, EMA_ALIGN_BARS + 1):
        e5 = ema5_s[-i]
        e10 = ema10_s[-i]
        if e5 is None or e10 is None:
            return None
        ok = (e5 > e10) if is_long else (e5 < e10)
        if not ok:
            return None
        aligned += 1
    if aligned < EMA_ALIGN_BARS:
        return None

    score = normalized_vwap_slope(candles, atr_daily_pct, cfg)
    signed = _signed_vwap_slope_atr(candles, atr_daily_pct)
    if not _signed_slope_ok(score, signed, "LONG" if is_long else "SHORT"):
        return None

    # Breakout reference = close of the oldest bar in the EMA alignment window
    breakout_close = closes[-EMA_ALIGN_BARS]
    last_close = closes[-1]
    price = last_close
    atr = price * max(atr_daily_pct, 0.001) / 100.0
    if atr <= 0:
        return None
    extension = abs(last_close - breakout_close) / atr
    if extension > atr_ext_max:
        return None

    last = bars[-1]
    bar_ts = None
    idx = int(last.get("end_5m_idx") or -1)
    if 0 <= idx < len(candles):
        bar_ts = candles[idx].get("timestamp")

    return {
        "tier": ALERT_TIER,
        "direction": "LONG" if is_long else "SHORT",
        "vwap_slope_score": round(score, 2),
        "signed_slope_atr": round(signed, 4),
        "ema_align_bars": aligned,
        "extension_atr": round(extension, 3),
        "atr_ext_max": atr_ext_max,
        "breakout_close": breakout_close,
        "confirmed_close": last_close,
        "ema5": ema5_s[-1],
        "ema10": ema10_s[-1],
        "bar_at": bar_ts,
        "live_enabled": live_enabled(),
    }


def scan_expansion_candidates(
    db,
    session_date: str,
    *,
    symbols: Optional[List[str]] = None,
    atr_by_symbol: Optional[Dict[str, float]] = None,
) -> List[Dict[str, Any]]:
    """Evaluate F&O universe (or given symbols). Empty when live flag is off."""
    if not live_enabled():
        return []

    from backend.services.daily_checklist_snapshot import _load_candles_for_symbol

    syms = symbols or fno_universe(db)
    atr_map = atr_by_symbol or {}
    cfg = get_config()
    out: List[Dict[str, Any]] = []
    for sym in syms:
        try:
            candles = _load_candles_for_symbol(db, sym)
            if not candles:
                continue
            atr_pct = float(atr_map.get(sym) or 1.0)
            for side in ("LONG", "SHORT"):
                hit = evaluate_candles_for_expansion(
                    candles, side=side, atr_daily_pct=atr_pct, cfg=cfg
                )
                if hit:
                    hit["symbol"] = sym
                    hit["session_date"] = session_date
                    out.append(hit)
        except Exception as exc:
            logger.debug("expansion watch skip %s: %s", sym, exc)
    return out


def get_expansion_watch(session_date: Optional[str] = None) -> Dict[str, Any]:
    """API-facing payload. Live candidates only when EXPANSION_WATCH_LIVE=1."""
    sd = session_date or today_ist()
    enabled = live_enabled()
    payload: Dict[str, Any] = {
        "session_date": sd,
        "tier_label": ALERT_TIER,
        "live_enabled": enabled,
        "candidates": [],
        "note": (
            None
            if enabled
            else "Backtest gate required before live alerts (EXPANSION_WATCH_LIVE=0)."
        ),
    }
    if not enabled:
        return payload
    db = SessionLocal()
    try:
        payload["candidates"] = scan_expansion_candidates(db, sd)
    finally:
        db.close()
    return payload
