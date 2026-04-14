"""
Smart Futures: index alignment (NIFTY + BANKNIFTY) and exit rules (divergence / VWAP / regime).

Used by the picker context and by /daily exit hints for open positions.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, List, Optional, Sequence, Tuple

import pytz

from backend.services.smart_futures_config import (
    ADX_LENGTH,
    TRAIL_LOCK_ATR_MULT,
    TRAIL_STAGE1_ATR_MULT,
    TRAIL_STAGE2_ATR_MULT,
    TRAILING_STOP_ENABLED,
)
from backend.services.smart_futures_picker.indicators import (
    adx_last_two,
    divergence_bundle,
    session_vwap,
    wilder_atr,
    wilder_atr_14,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
NIFTY50_KEY = "NSE_INDEX|Nifty 50"
BANKNIFTY_KEY = "NSE_INDEX|Nifty Bank"


def _ema_last(series: Sequence[float], span: int) -> Optional[float]:
    if not series:
        return None
    k = 2.0 / (float(span) + 1.0)
    e = float(series[0])
    for v in series[1:]:
        e = float(v) * k + e * (1.0 - k)
    return float(e)


def _supertrend_dir_last_two(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 10,
    multiplier: float = 3.0,
) -> Tuple[Optional[int], Optional[int]]:
    """
    Returns (current_dir, previous_dir) where:
    +1 = green/uptrend, -1 = red/downtrend.
    """
    n = len(closes)
    if n < max(20, period + 3):
        return None, None
    fub: List[float] = [0.0] * n
    flb: List[float] = [0.0] * n
    st: List[float] = [0.0] * n
    direction: List[int] = [1] * n
    for i in range(n):
        atr_i = wilder_atr(highs[: i + 1], lows[: i + 1], closes[: i + 1], period)
        if atr_i is None:
            continue
        hl2 = (float(highs[i]) + float(lows[i])) / 2.0
        bub = hl2 + float(multiplier) * float(atr_i)
        blb = hl2 - float(multiplier) * float(atr_i)
        if i == 0:
            fub[i], flb[i], st[i], direction[i] = bub, blb, blb, 1
            continue
        fub[i] = bub if (bub < fub[i - 1] or float(closes[i - 1]) > fub[i - 1]) else fub[i - 1]
        flb[i] = blb if (blb > flb[i - 1] or float(closes[i - 1]) < flb[i - 1]) else flb[i - 1]
        if st[i - 1] == fub[i - 1]:
            st[i] = fub[i] if float(closes[i]) <= fub[i] else flb[i]
        else:
            st[i] = flb[i] if float(closes[i]) >= flb[i] else fub[i]
        direction[i] = 1 if float(closes[i]) >= st[i] else -1
    return int(direction[-1]), int(direction[-2])


def _sort_candles(candles: Optional[List[dict]]) -> List[dict]:
    if not candles:
        return []
    return sorted(candles, key=lambda c: str(c.get("timestamp") or ""))


def _ist_date_from_ts(ts: str) -> Optional[date]:
    if not ts or len(ts) < 10:
        return None
    try:
        dt = datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S")
        return dt.replace(tzinfo=IST).date()
    except ValueError:
        try:
            return datetime.strptime(ts[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def m5_bars_for_session(candles: Optional[List[dict]], session_date: date) -> List[dict]:
    s = _sort_candles(candles)
    return [b for b in s if _ist_date_from_ts(str(b.get("timestamp") or "")) == session_date]


def session_last_close_and_vwap(m5: List[dict]) -> Tuple[Optional[float], Optional[float]]:
    if len(m5) < 10:
        return None, None
    highs = [float(b["high"]) for b in m5]
    lows = [float(b["low"]) for b in m5]
    closes = [float(b["close"]) for b in m5]
    vols = [float(b.get("volume") or 0) for b in m5]
    lc = closes[-1]
    vw = session_vwap(highs, lows, closes, vols)
    return float(lc), float(vw)


def index_session_long_short_flags(
    upstox: Any,
    session_date: date,
    *,
    range_end_date: Optional[date] = None,
    days_back: int = 5,
) -> Tuple[bool, bool]:
    """Fetch NIFTY50 + BANKNIFTY session 5m and return (supports_long, supports_short). Fail-closed."""
    end_d = range_end_date or session_date
    try:
        nm = upstox.get_historical_candles_by_instrument_key(
            NIFTY50_KEY, interval="minutes/5", days_back=days_back, range_end_date=end_d
        )
        bm = upstox.get_historical_candles_by_instrument_key(
            BANKNIFTY_KEY, interval="minutes/5", days_back=days_back, range_end_date=end_d
        )
        n_today = m5_bars_for_session(nm, session_date)
        b_today = m5_bars_for_session(bm, session_date)
        ok, lg, sh = index_alignment_supports(n_today, b_today)
        if not ok:
            return False, False
        return lg, sh
    except Exception as e:
        logger.warning("index_session_long_short_flags: %s", e)
        return False, False


def index_alignment_supports(
    nifty_m5: List[dict],
    bank_m5: List[dict],
) -> Tuple[bool, bool, bool]:
    """
    Returns ``(data_ok, supports_long, supports_short)`` using last close vs session VWAP on each index.
    Long: both closes above VWAP. Short: both below. Strict inequalities.
    """
    nc, nv = session_last_close_and_vwap(nifty_m5)
    bc, bv = session_last_close_and_vwap(bank_m5)
    if nc is None or nv is None or bc is None or bv is None:
        return False, False, False
    sup_long = nc > nv and bc > bv
    sup_short = nc < nv and bc < bv
    return True, sup_long, sup_short


def should_exit_position(
    side: str,
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    vwap: float,
    adx_curr: Optional[float],
    adx_prev: Optional[float],
    atr5: Optional[float],
    atr14: Optional[float],
    md: float,
    rd: float,
    sd: float,
    entry_price: Optional[float] = None,
) -> Tuple[bool, str]:
    """
    Multi-factor exit confirmation:
    - Hard exits: VWAP adverse break, supertrend reversal, hard stop-loss.
    - Soft warnings (ATR contraction/divergence) require confirmation.
    """
    sd_u = str(side or "").strip().upper()
    lc = float(closes[-1]) if closes else 0.0
    ll = float(lows[-1]) if lows else lc
    hh = float(highs[-1]) if highs else lc
    div_sum = float(md) + float(rd) + float(sd)
    ema9 = _ema_last(closes, 9)
    st_curr, st_prev = _supertrend_dir_last_two(highs, lows, closes)

    # HARD EXIT 1: price adverse to VWAP
    if sd_u == "LONG" and lc < float(vwap):
        return True, "Exit: VWAP Breakdown"
    if sd_u == "SHORT" and lc > float(vwap):
        return True, "Exit: VWAP Breakout Against Short"

    # HARD EXIT 2: supertrend flip
    if sd_u == "LONG" and st_prev == 1 and st_curr == -1:
        return True, "Exit: Supertrend Reversal"
    if sd_u == "SHORT" and st_prev == -1 and st_curr == 1:
        return True, "Exit: Supertrend Reversal"

    # HARD EXIT 3: hard stop loss hit
    if entry_price is not None and atr14 is not None and atr14 > 0:
        stop_dist = 1.2 * float(atr14)
        if sd_u == "LONG" and ll <= float(entry_price) - stop_dist:
            return True, "Exit: Hard Stop Loss Hit"
        if sd_u == "SHORT" and hh >= float(entry_price) + stop_dist:
            return True, "Exit: Hard Stop Loss Hit"

    atr_contracting = bool(atr5 is not None and atr14 is not None and atr14 > 0 and atr5 < atr14)

    if sd_u == "LONG":
        # SOFT EXIT A: momentum weakening needs VWAP+supertrend confirmation
        if atr_contracting and lc < float(vwap) and st_curr == -1:
            return True, "Exit: Momentum Weakening + VWAP Breakdown"
        # SOFT EXIT B: bearish divergence needs VWAP or EMA9 breach
        if div_sum <= -0.5 and (lc < float(vwap) or (ema9 is not None and lc < float(ema9))):
            if lc < float(vwap):
                return True, "Exit: Bearish Divergence + VWAP Breakdown"
            return True, "Exit: Bearish Divergence + EMA 9 Breach"
    elif sd_u == "SHORT":
        if atr_contracting and lc > float(vwap) and st_curr == 1:
            return True, "Exit: Momentum Weakening + VWAP Breakout"
        if div_sum >= 0.5 and (lc > float(vwap) or (ema9 is not None and lc > float(ema9))):
            if lc > float(vwap):
                return True, "Exit: Bullish Divergence + VWAP Breakout"
            return True, "Exit: Bullish Divergence + EMA 9 Breach"
    return False, ""


def exit_evaluation_from_m5_dicts(
    side: str,
    m5_today: List[dict],
    entry_price: Optional[float] = None,
) -> Tuple[bool, str]:
    """Build OHLC series from session 5m bars and run ``should_exit_position``."""
    if len(m5_today) < 15:
        return False, ""
    highs = [float(b["high"]) for b in m5_today]
    lows = [float(b["low"]) for b in m5_today]
    closes = [float(b["close"]) for b in m5_today]
    vols = [float(b.get("volume") or 0) for b in m5_today]
    vwap = session_vwap(highs, lows, closes, vols)
    atr14 = wilder_atr_14(highs, lows, closes)
    atr5 = wilder_atr(highs, lows, closes, 5)
    adx_c, adx_p = adx_last_two(highs, lows, closes, ADX_LENGTH)
    md, rd, sd = divergence_bundle(highs, lows, closes)
    return should_exit_position(
        side, highs, lows, closes, vwap, adx_c, adx_p, atr5, atr14, md, rd, sd, entry_price=entry_price
    )


def compute_trailing_stop_levels(
    side: str,
    entry_price: float,
    last_close: float,
    atr14: float,
    lot_size: int,
    *,
    current_stop_price: Optional[float] = None,
    stop_stage: Optional[str] = None,
) -> Tuple[float, str]:
    """
    Stage 1: PnL >= TRAIL_STAGE1_ATR_MULT * ATR * lot → stop at entry (breakeven).
    Stage 2: PnL >= TRAIL_STAGE2_ATR_MULT * ATR * lot → trail 1 ATR from entry (favorable side).
    Never loosen stop (only move in favor).
    """
    if not TRAILING_STOP_ENABLED or atr14 <= 0 or lot_size <= 0 or entry_price <= 0:
        return (
            float(current_stop_price or entry_price),
            str(stop_stage or "INITIAL"),
        )
    sd = str(side or "").strip().upper()
    pnl_r = (last_close - entry_price) * lot_size if sd == "LONG" else (entry_price - last_close) * lot_size
    s1 = float(TRAIL_STAGE1_ATR_MULT) * float(atr14) * float(lot_size)
    s2 = float(TRAIL_STAGE2_ATR_MULT) * float(atr14) * float(lot_size)
    lock = float(TRAIL_LOCK_ATR_MULT) * float(atr14)

    cur = float(current_stop_price) if current_stop_price is not None else (
        entry_price - 1.2 * atr14 if sd == "LONG" else entry_price + 1.2 * atr14
    )
    stage = str(stop_stage or "INITIAL")

    if pnl_r >= s2:
        if sd == "LONG":
            new_stop = max(cur, entry_price + lock, entry_price)
        else:
            new_stop = min(cur, entry_price - lock, entry_price)
        return new_stop, "TRAILING"
    if pnl_r >= s1:
        new_stop = entry_price
        if sd == "LONG":
            new_stop = max(cur, entry_price)
        else:
            new_stop = min(cur, entry_price)
        return new_stop, "BREAKEVEN"
    return cur, stage
