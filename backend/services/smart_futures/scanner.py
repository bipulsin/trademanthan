"""Layer 1 pre-filter + Layer 2 Renko structure + scoring."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pytz

from backend.services.smart_futures import data_service, repository
from backend.services.smart_futures.indicators import atr_wilder
from backend.services.smart_futures.renko_engine import (
    RenkoBrick,
    build_traditional_renko,
    entry_pullback_long,
    entry_pullback_short,
    renko_structure_filter_long,
    renko_structure_filter_short,
)
from backend.services.smart_futures.renko_engine import count_alternations
from backend.services.upstox_service import upstox_service

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def get_prev_close_daily(instrument_key: str) -> Optional[float]:
    c = upstox_service.get_historical_candles_by_instrument_key(
        instrument_key, interval="days/1", days_back=10
    )
    if not c or len(c) < 2:
        return None
    c = sorted(c, key=lambda x: x.get("timestamp") or "")
    try:
        return float(c[-2]["close"])
    except (KeyError, TypeError, ValueError, IndexError):
        return None


def compute_score(
    direction: str,
    bricks: list[RenkoBrick],
    vol_spike: bool,
    momentum_ok: bool,
) -> int:
    """Score 0–6 per spec."""
    if not bricks:
        return 0
    want = "GREEN" if direction == "LONG" else "RED"
    s = 0
    last5 = [b.color for b in bricks[-5:]]
    if len(last5) == 5 and all(c == want for c in last5):
        s += 2
    last6 = [b.color for b in bricks[-6:]] if len(bricks) >= 6 else last5
    if len(last6) >= 4 and count_alternations(last6) <= 1:
        s += 2
    if momentum_ok:
        s += 1
    if vol_spike:
        s += 1
    return min(6, s)


def scan_symbol(
    symbol: str,
    instrument_key: str,
    now_ist: datetime,
    config: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Full scan for one future symbol. Returns candidate dict or None if hard fail.
    Brick size: admin override, else ATR(brick_atr_period) on 1h (default period 10).
    """
    cfg = config if config is not None else repository.get_config()
    brick_main = data_service.resolve_main_brick_size(instrument_key, cfg)
    if brick_main is None or brick_main <= 0:
        logger.debug("smart_futures: no brick size %s", symbol)
        return None

    c5 = data_service.get_5m_candles(instrument_key, days_back=6)
    c15 = data_service.get_15m_candles(instrument_key, days_back=10)
    if not c5 or len(c5) < 20:
        return None

    prev_close = get_prev_close_daily(instrument_key)
    if prev_close is None or prev_close <= 0:
        pc, _ = data_service.quote_prev_close_and_open(instrument_key)
        prev_close = pc
    if prev_close is None or prev_close <= 0:
        return None

    meta: Dict[str, Any] = {}
    hm = now_ist.hour * 60 + now_ist.minute
    # Layer 1 must pass before Renko (bull or bear branch in prefilter_gap_volume_atr).
    pre_ok, pre_reason, meta = data_service.prefilter_gap_volume_atr(
        instrument_key, c5, c15 or [], prev_close
    )
    if not pre_ok:
        logger.debug("smart_futures prefilter skip %s: %s", symbol, pre_reason)
        return None
    layer1_side = str(meta.get("layer1_side") or "").strip().lower()

    closes = data_service.closes_from_candles(c5)
    bricks = build_traditional_renko(closes, brick_main)

    ltp_map = upstox_service.get_market_quotes_batch_by_keys([instrument_key])
    ltp = ltp_map.get(instrument_key)

    # Momentum: current range vs ATR 15m
    momentum_ok = False
    if c15 and len(c15) >= 12:
        c15s = sorted(c15, key=lambda x: x.get("timestamp") or "")
        highs = [float(x["high"]) for x in c15s]
        lows = [float(x["low"]) for x in c15s]
        closes15 = [float(x["close"]) for x in c15s]
        atr10_15 = atr_wilder(highs, lows, closes15, period=10)
        if atr10_15 and atr10_15 > 0 and closes:
            momentum_ok = abs(closes[-1] - closes[-5]) >= 0.3 * atr10_15

    long_struct, _ = renko_structure_filter_long(bricks)
    short_struct, _ = renko_structure_filter_short(bricks)

    direction = "NONE"
    structure_pass = False
    if long_struct and not short_struct:
        direction = "LONG"
        structure_pass = True
    elif short_struct and not long_struct:
        direction = "SHORT"
        structure_pass = True
    elif long_struct and short_struct:
        direction = "LONG" if bricks[-1].color == "GREEN" else "SHORT"
        structure_pass = True

    last_color = bricks[-1].color if bricks else None

    vfa = meta.get("vol_first15")
    vpv = meta.get("vol_prev_avg")
    vol_spike = bool(vpv and vfa and float(vfa) > float(vpv))

    score = 0
    if direction in ("LONG", "SHORT"):
        score = compute_score(direction, bricks, vol_spike, momentum_ok)

    # Layer 2: Renko direction must match Layer 1 side (bull → LONG, bear → SHORT).
    if structure_pass and direction in ("LONG", "SHORT") and layer1_side in ("bull", "bear"):
        if layer1_side == "bull" and direction != "LONG":
            return None
        if layer1_side == "bear" and direction != "SHORT":
            return None

    # Entry signal (layer 3): 9:30–13:45 IST, score ≥ 4, pullback — only if Layer 2 gave a trade direction.
    entry_signal = False
    entry_win = (9 * 60 + 30) <= hm <= (13 * 60 + 45)
    if (
        entry_win
        and structure_pass
        and direction in ("LONG", "SHORT")
        and score >= 4
    ):
        if direction == "LONG":
            ok_e, _ = entry_pullback_long(bricks)
            entry_signal = ok_e
        else:
            ok_e, _ = entry_pullback_short(bricks)
            entry_signal = ok_e

    return {
        "symbol": symbol,
        "instrument_key": instrument_key,
        "score": score,
        "direction": direction if structure_pass else "NONE",
        "last_brick_color": last_color,
        "entry_signal": entry_signal,
        "exit_ready": False,
        "main_brick_size": brick_main,
        "ltp": ltp,
        "prefilter_pass": pre_ok,
        "structure_pass": structure_pass,
        "prefilter_reason": pre_reason,
        "layer1_side": layer1_side or None,
        "meta": meta,
    }
