"""Trade simulation for Open-Low 15m strategy."""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

import pytz

from backend.services.open_low_15m.config import (
    FORCE_EXIT_TIME,
    TP_R_LEVELS,
    TRAIL_MOVE_R,
    TRAIL_STEP_R,
)
from backend.services.open_low_15m.indicators import bar_indicators, signal_exit_long
from backend.services.open_low_15m.signals import evaluate_setup
from backend.services.volume_mismatch.candles import _parse_ts

IST = pytz.timezone("Asia/Kolkata")
POST_1245 = time(12, 45)


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


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
    daily_closes_before: List[float] | None = None,
    lot_size: int,
    max_gap_pct: float = 2.0,
) -> Optional[Dict[str, Any]]:
    """Backward-compatible wrapper; ignores daily_closes_before (uses intraday EMA10)."""
    setup, _reason = evaluate_setup(
        symbol=symbol,
        future_symbol=future_symbol,
        instrument_key=instrument_key,
        session_date=session_date,
        candles_15m=candles_15m,
        prev_close=prev_close,
        lot_size=lot_size,
        max_gap_pct=max_gap_pct,
    )
    return setup


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
        hi, lo = _f(b.get("high")), _f(b.get("low"))
        if hi < entry_px:
            continue
        bt: datetime = b["_dt"]
        if bt.time() >= POST_1245:
            fib_mid = _fib_mid_30m(bars, i)
            if fib_mid is not None:
                cl = _f(b.get("close"))
                if not (lo < fib_mid and cl > fib_mid):
                    continue
        # Conservative: if SL touched on entry bar before/at trigger, skip entry on this bar.
        if lo <= sl0:
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

        # Touch-based SL / TP (intrabar), before close-based signal exits.
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

        if j > entry_idx:
            highs.append(hi)
            lows.append(lo)
            closes.append(cl)
            vols.append(_f(b.get("volume")))
        ind = bar_indicators(highs, lows, closes, vols)

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
