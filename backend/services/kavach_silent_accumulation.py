"""
Standalone Silent Accumulation — flat price + rising OI + adequate volume.

Distinct from rs_conviction_signals.accumulation_signal() (2-of-3 OBV/efficiency/volume
without explicit OI). This signal uses WS 1m OI series + price band vs ATR.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.rs_conviction_candles import load_instrument_atr_maps

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _f(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def detect_silent_accumulation_1m(
    bars: List[Dict[str, Any]],
    atr_daily_pct: float,
    *,
    window: int = 15,
    price_band_atr_mult: float = 0.3,
    min_oi_rise_pct: float = 1.5,
    min_vol_ratio: float = 1.0,
) -> Dict[str, Any]:
    """
    Detect silent accumulation on 1-minute WS bars.

    Conditions (all required):
    - Price range over window <= price_band_atr_mult × daily ATR (rupees)
    - OI rose >= min_oi_rise_pct from window start open to window end close
    - Mean window volume >= min_vol_ratio × session mean volume per bar
    """
    if len(bars) < window:
        return {"active": False, "reason": "insufficient_bars", "window": window, "bars": len(bars)}

    win = bars[-window:]
    closes = [_f(b.get("close")) for b in win]
    highs = [_f(b.get("high")) for b in win]
    lows = [_f(b.get("low")) for b in win]
    vols = [_f(b.get("volume")) for b in win]
    oi_opens = [int(b.get("oi_open") or b.get("oi_close") or 0) for b in win]
    oi_closes = [int(b.get("oi_close") or 0) for b in win]

    price = closes[-1] or closes[0] or 1.0
    atr_rupees = price * max(atr_daily_pct, 0.1) / 100.0
    band_limit = atr_rupees * price_band_atr_mult
    price_range = max(highs) - min(lows)

    oi_start = oi_opens[0] or oi_closes[0]
    oi_end = oi_closes[-1]
    oi_rise_pct = ((oi_end - oi_start) / max(oi_start, 1)) * 100.0 if oi_start > 0 else 0.0

    session_vols = [_f(b.get("volume")) for b in bars]
    session_avg = sum(session_vols) / max(len(session_vols), 1)
    win_avg_vol = sum(vols) / max(len(vols), 1)
    vol_all_zero = session_avg <= 0
    vol_ratio = win_avg_vol / session_avg if session_avg > 0 else 1.0

    price_quiet = price_range <= band_limit
    oi_rising = oi_rise_pct >= min_oi_rise_pct
    vol_ok = vol_ratio >= min_vol_ratio if not vol_all_zero else True
    low_conf_volume = vol_all_zero

    active = price_quiet and oi_rising and vol_ok
    score = 0.0
    if active:
        score = min(
            100.0,
            40.0
            + min(30.0, oi_rise_pct * 5.0)
            + min(20.0, max(0.0, (band_limit - price_range) / max(band_limit, 1e-9) * 20.0))
            + min(10.0, (vol_ratio - 1.0) * 10.0),
        )

    return {
        "active": active,
        "score": round(score, 2),
        "price_range": round(price_range, 4),
        "band_limit": round(band_limit, 4),
        "oi_rise_pct": round(oi_rise_pct, 3),
        "vol_ratio": round(vol_ratio, 3),
        "window": window,
        "checks": {
            "price_quiet": price_quiet,
            "oi_rising": oi_rising,
            "volume_ok": vol_ok,
            "volume_bootstrap": low_conf_volume,
        },
        "low_confidence_volume": low_conf_volume,
    }


def load_ws_1m_bars(
    instrument_key: str,
    session_date: str,
    *,
    db=None,
    max_bars: int = 400,
) -> List[Dict[str, Any]]:
    own_db = db is None
    if own_db:
        db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT candle_time, open, high, low, close,
                       oi_open, oi_high, oi_low, oi_close, volume,
                       bid_depth_qty, ask_depth_qty, tbq, tsq
                FROM upstox_ws_intraday_1m
                WHERE instrument_key = :ik
                  AND candle_time::date = CAST(:d AS date)
                ORDER BY candle_time ASC
                LIMIT :lim
                """
            ),
            {"ik": instrument_key, "d": session_date, "lim": max_bars},
        ).fetchall()
        return [
            {
                "candle_time": r.candle_time.isoformat() if r.candle_time else None,
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "oi_open": r.oi_open,
                "oi_high": r.oi_high,
                "oi_low": r.oi_low,
                "oi_close": r.oi_close,
                "volume": r.volume,
                "bid_depth_qty": r.bid_depth_qty,
                "ask_depth_qty": r.ask_depth_qty,
                "tbq": r.tbq,
                "tsq": r.tsq,
            }
            for r in rows
        ]
    finally:
        if own_db and db is not None:
            db.close()


def evaluate_symbol(
    symbol: str,
    side: str,
    instrument_key: str,
    atr_pct: float,
    session_date: str,
    *,
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    from backend.services.rs_conviction_config import get_config

    cfg = cfg or get_config()
    bars = load_ws_1m_bars(instrument_key, session_date)
    window = int(cfg.get("silent_accum_window_1m") or 15)
    return detect_silent_accumulation_1m(
        bars,
        atr_pct,
        window=window,
        price_band_atr_mult=float(cfg.get("silent_accum_price_band_atr") or 0.3),
        min_oi_rise_pct=float(cfg.get("silent_accum_min_oi_rise_pct") or 1.5),
        min_vol_ratio=float(cfg.get("silent_accum_min_vol_ratio") or 1.0),
    )


def run_silent_accumulation_cycle(
    symbols_sides: List[Tuple[str, str]],
    session_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Evaluate and log standalone silent accumulation (no composite gate)."""
    if not symbols_sides:
        return {"ok": True, "logged": 0}
    from backend.services.rs_conviction_config import get_config

    cfg = get_config()
    if not cfg.get("silent_accumulation_enabled", True):
        return {"ok": True, "logged": 0, "skipped": "disabled"}

    sd = session_date or datetime.now(IST).strftime("%Y-%m-%d")
    now = datetime.now(IST)
    sym_set = {s for s, _ in symbols_sides}
    db = SessionLocal()
    logged = 0
    results: Dict[str, Dict[str, Any]] = {}
    try:
        ikey_map, atr_map = load_instrument_atr_maps(db, sym_set)
        for sym, side in symbols_sides:
            ik = ikey_map.get(sym, "")
            if not ik:
                continue
            payload = evaluate_symbol(sym, side, ik, atr_map.get(sym, 1.0), sd, cfg=cfg)
            results[sym] = payload
            if not payload.get("active"):
                continue
            db.execute(
                text(
                    """
                    INSERT INTO rs_silent_accumulation_log (
                        session_date, computed_at, symbol, side,
                        accum_score, active, detail_json
                    ) VALUES (:d, :t, :sym, :side, :score, TRUE, :detail)
                    """
                ),
                {
                    "d": sd,
                    "t": now,
                    "sym": sym,
                    "side": side,
                    "score": payload.get("score"),
                    "detail": json.dumps(payload),
                },
            )
            logged += 1
        db.commit()
    finally:
        db.close()
    return {"ok": True, "logged": logged, "results": results}


def walk_forward_first_fire(
    bars: List[Dict[str, Any]],
    atr_daily_pct: float,
    *,
    window: int = 15,
    price_band_atr_mult: float = 0.3,
    min_oi_rise_pct: float = 1.5,
    min_vol_ratio: float = 1.0,
) -> Optional[Dict[str, Any]]:
    """Return first bar index/time where silent accumulation becomes active."""
    if len(bars) < window:
        return None
    for end in range(window, len(bars) + 1):
        slice_bars = bars[:end]
        res = detect_silent_accumulation_1m(
            slice_bars,
            atr_daily_pct,
            window=window,
            price_band_atr_mult=price_band_atr_mult,
            min_oi_rise_pct=min_oi_rise_pct,
            min_vol_ratio=min_vol_ratio,
        )
        if res.get("active"):
            return {
                "candle_time": slice_bars[-1].get("candle_time"),
                "end_index": end - 1,
                **res,
            }
    return None
