"""Trade simulation for Open-Low 15m strategy."""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.open_low_15m.config import (
    ATR_LEN,
    FORCE_EXIT_TIME,
    OPEN_LOW_TOL_PCT,
    RANGE_ATR_MULT,
    RISK_INR_MAX,
    RISK_INR_MIN,
    TP_R_LEVELS,
    TRAIL_MOVE_R,
    TRAIL_STEP_R,
)
from backend.services.open_low_15m.indicators import bar_indicators, daily_ema10_as_of, signal_exit_long
from backend.services.volume_mismatch.candles import _parse_ts, first_15m_bar_for_session

IST = pytz.timezone("Asia/Kolkata")
POST_1245 = time(12, 45)


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _open_equals_low(o: float, l: float) -> bool:
    if o <= 0:
        return False
    return abs(o - l) <= o * (OPEN_LOW_TOL_PCT / 100.0)


def _session_bars_sorted(candles: List[Dict[str, Any]], session_date: date) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for c in candles:
        ts = _parse_ts(c.get("timestamp"))
        if ts is None:
            continue
        t = ts.astimezone(IST)
        if t.date() != session_date:
            continue
        if t.time() < time(9, 15) or t.time() > time(15, 30):
            continue
        out.append({**c, "_dt": t})
    out.sort(key=lambda x: x["_dt"])
    return out


def _trail_sl_price(entry: float, r_dist: float, mfe_price: float) -> float:
    if r_dist <= 0:
        return entry - r_dist
    mfe_r = (mfe_price - entry) / r_dist
    steps = int(mfe_r // TRAIL_STEP_R)
    if steps <= 0:
        return entry - r_dist
    locked_r = (steps - 1) * TRAIL_MOVE_R * TRAIL_STEP_R / TRAIL_MOVE_R
    # At 1.5R MFE -> breakeven; at 3R -> +1.5R locked
    locked_r = max(0.0, (steps - 1) * TRAIL_STEP_R)
    return entry + locked_r * r_dist


def _fib_mid_30m(bars: List[Dict[str, Any]], idx: int) -> Optional[float]:
    """Midpoint (high+low)/2 of the 30-minute bucket containing bar idx."""
    if idx < 0 or idx >= len(bars):
        return None
    dt: datetime = bars[idx]["_dt"]
    if dt.minute < 30:
        bucket_start = dt.replace(minute=0, second=0, microsecond=0)
        bucket_end = bucket_start.replace(minute=30)
    else:
        bucket_start = dt.replace(minute=30, second=0, microsecond=0)
        bucket_end = bucket_start.replace(hour=bucket_start.hour + 1, minute=0)
    seg = [b for b in bars if bucket_start <= b["_dt"] < bucket_end]
    if not seg:
        return None
    hi = max(_f(b.get("high")) for b in seg)
    lo = min(_f(b.get("low")) for b in seg)
    return (hi + lo) / 2.0


def detect_setup(
    *,
    symbol: str,
    future_symbol: str,
    instrument_key: str,
    session_date: date,
    candles_15m: List[Dict[str, Any]],
    prev_close: float,
    daily_closes_before: List[float],
    lot_size: int,
    max_gap_pct: float = 2.0,
) -> Optional[Dict[str, Any]]:
    bars = _session_bars_sorted(candles_15m, session_date)
    first = first_15m_bar_for_session(candles_15m, session_date)
    if first is None and bars:
        first = bars[0]
    if first is None:
        return None
    o, h, l, close_px = _f(first.get("open")), _f(first.get("high")), _f(first.get("low")), _f(first.get("close"))
    if o <= 0 or not _open_equals_low(o, l):
        return None
    if prev_close > 0:
        gap_pct = abs(o - prev_close) / prev_close * 100.0
        if gap_pct > max_gap_pct:
            return None

    # Indicator warmup: all 15m bars through end of first session bar
    warmup: List[Dict[str, Any]] = []
    for candle in sorted(candles_15m, key=lambda x: str(x.get("timestamp") or "")):
        ts = _parse_ts(candle.get("timestamp"))
        if ts is None:
            continue
        t = ts.astimezone(IST)
        if t.date() > session_date:
            continue
        if t.date() == session_date and t.time() > time(9, 15):
            break
        warmup.append(candle)
    if first not in warmup:
        warmup.append(first)
    hist_h = [_f(b.get("high")) for b in warmup]
    hist_l = [_f(b.get("low")) for b in warmup]
    hist_c = [_f(b.get("close")) for b in warmup]
    hist_v = [_f(b.get("volume")) for b in warmup]
    ind0 = bar_indicators(hist_h, hist_l, hist_c, hist_v)
    ema10_daily = daily_ema10_as_of(daily_closes_before)
    if ema10_daily is not None and close_px <= ema10_daily:
        return None
    if close_px <= ind0["vwap"]:
        return None
    if ind0["supertrend_dir"] != 1:
        return None

    rng = h - l
    atr14 = ind0.get("atr14")
    use_alt_sl = atr14 is not None and rng > RANGE_ATR_MULT * float(atr14)
    sl_primary = l
    sl_alt = l + 0.5 * rng
    entry_px = h
    sl_used = sl_alt if use_alt_sl else sl_primary
    sl_type = "alternative" if use_alt_sl else "primary"
    r_dist = entry_px - sl_used
    if r_dist <= 0:
        return None
    risk_inr = r_dist * lot_size
    if risk_inr < RISK_INR_MIN or risk_inr > RISK_INR_MAX:
        return None

    gain_pct = (close_px - o) / o * 100.0 if o > 0 else 0.0
    return {
        "symbol": symbol,
        "future_symbol": future_symbol,
        "instrument_key": instrument_key,
        "session_date": session_date.isoformat(),
        "setup_open": o,
        "setup_high": h,
        "setup_low": l,
        "setup_close": close_px,
        "setup_range": rng,
        "first_15m_gain_pct": round(gain_pct, 4),
        "prev_close": prev_close,
        "entry_trigger": entry_px,
        "sl_type": sl_type,
        "sl_price": sl_used,
        "sl_primary": sl_primary,
        "sl_alternative": sl_alt,
        "r_distance": r_dist,
        "lot_size": lot_size,
        "risk_inr": round(risk_inr, 2),
        "atr14_at_setup": atr14,
        "use_alt_sl": use_alt_sl,
    }


def simulate_trade(
    setup: Dict[str, Any],
    candles_15m: List[Dict[str, Any]],
    tp_variant: str,
) -> Optional[Dict[str, Any]]:
    session_date = date.fromisoformat(setup["session_date"])
    bars = _session_bars_sorted(candles_15m, session_date)
    if len(bars) < 2:
        return None

    entry_px = _f(setup["entry_trigger"])
    sl0 = _f(setup["sl_price"])
    r_dist = _f(setup["r_distance"])
    tp_r = TP_R_LEVELS.get(tp_variant, 1.0)
    tp_px = entry_px + tp_r * r_dist

    entry_idx: Optional[int] = None
    entry_time: Optional[datetime] = None
    for i, b in enumerate(bars[1:], start=1):
        if _f(b.get("high")) >= entry_px:
            bt: datetime = b["_dt"]
            if bt.time() >= POST_1245:
                fib_mid = _fib_mid_30m(bars, i)
                if fib_mid is not None:
                    lo = _f(b.get("low"))
                    cl = _f(b.get("close"))
                    if not (lo < fib_mid and cl > fib_mid):
                        continue
            entry_idx = i
            entry_time = bt
            break
    if entry_idx is None or entry_time is None:
        return None

    sl = sl0
    mfe = entry_px
    exit_px = entry_px
    exit_time: Optional[datetime] = None
    exit_reason = "open"
    tp_hit = False
    trail_hit = False

    highs: List[float] = [_f(b.get("high")) for b in bars[: entry_idx + 1]]
    lows: List[float] = [_f(b.get("low")) for b in bars[: entry_idx + 1]]
    closes: List[float] = [_f(b.get("close")) for b in bars[: entry_idx + 1]]
    vols: List[float] = [_f(b.get("volume")) for b in bars[: entry_idx + 1]]
    prev_ind: Optional[dict] = bar_indicators(highs, lows, closes, vols)

    for j in range(entry_idx, len(bars)):
        b = bars[j]
        hi, lo, cl = _f(b.get("high")), _f(b.get("low")), _f(b.get("close"))
        bt: datetime = b["_dt"]
        mfe = max(mfe, hi)
        trail_sl = _trail_sl_price(entry_px, r_dist, mfe)
        sl = max(sl0, trail_sl)

        if j > entry_idx:
            highs.append(hi)
            lows.append(lo)
            closes.append(cl)
            vols.append(_f(b.get("volume")))
        ind = bar_indicators(highs, lows, closes, vols)

        if lo <= sl:
            exit_px = sl
            exit_time = bt
            exit_reason = "trail_stop" if trail_sl > sl0 + 1e-9 else "stop_loss"
            trail_hit = trail_sl > sl0 + 1e-9
            break
        if hi >= tp_px:
            exit_px = tp_px
            exit_time = bt
            exit_reason = f"take_profit_{tp_variant}"
            tp_hit = True
            break
        if prev_ind is not None and len(closes) >= 2:
            sig, sig_reason = signal_exit_long(
                prev_close=closes[-2],
                close=cl,
                prev_vwap=prev_ind["vwap"],
                vwap=ind["vwap"],
                prev_ema5=prev_ind["ema5"],
                ema5=ind["ema5"],
                prev_ema10=prev_ind["ema10"],
                ema10=ind["ema10"],
                st_prev=prev_ind["supertrend_dir"],
                st_cur=ind["supertrend_dir"],
                atr5=ind["atr5"],
                atr14=ind["atr14"],
            )
            if sig:
                exit_px = cl
                exit_time = bt
                exit_reason = f"signal_{sig_reason}"
                break
        prev_ind = ind
        if bt.time() >= FORCE_EXIT_TIME:
            exit_px = cl
            exit_time = bt
            exit_reason = "time_1515"
            break

    if exit_time is None:
        last = bars[-1]
        exit_px = _f(last.get("close"))
        exit_time = last["_dt"]
        exit_reason = "time_eod"

    r_realized = (exit_px - entry_px) / r_dist if r_dist > 0 else 0.0
    pnl = (exit_px - entry_px) * int(setup.get("lot_size") or 1)
    hold_min = max(0, int((exit_time - entry_time).total_seconds() // 60))

    exit_ind = bar_indicators(highs, lows, closes, vols) if closes else {}

    row = dict(setup)
    row.update(
        {
            "tp_variant": tp_variant,
            "tp_r": tp_r,
            "tp_price": tp_px,
            "entry_time": entry_time.isoformat(),
            "entry_price": entry_px,
            "exit_time": exit_time.isoformat(),
            "exit_price": round(exit_px, 4),
            "exit_reason": exit_reason,
            "r_realized": round(r_realized, 4),
            "pnl_inr": round(pnl, 2),
            "holding_minutes": hold_min,
            "tp_hit": tp_hit,
            "trail_stop_used": trail_hit,
            "exit_vwap": exit_ind.get("vwap"),
            "exit_ema5": exit_ind.get("ema5"),
            "exit_ema10": exit_ind.get("ema10"),
            "exit_supertrend_dir": exit_ind.get("supertrend_dir"),
            "win": r_realized > 0,
        }
    )
    return row
