"""One stock-day row. Features use only the 09:45 close and earlier bars.

Labels use the session after 09:45. A later bar never changes a feature.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.services.ema_scan.calendar import candle_start, holidays, in_cash_session, previous_trading_day
from backend.services.ema_scan.indicators import adx_di_series, ema_series, wilder_atr_series
from backend.services.mtf_swing.bars import ensure_ist, parse_ts

THRESHOLDS = (2, 3, 4)
SESSION_BARS = 75
REFERENCE_CANDLE = 6  # 09:40–09:45, reference is its close

FEATURE_META = [
    ("prev_range_atr", "Prev day range / ATR", "compression", "num"),
    ("nr7", "NR7", "compression", "bin"),
    ("range_5_20", "5d range / 20d range", "compression", "num"),
    ("atr5_atr50", "ATR(5) / ATR(50)", "compression", "num"),
    ("bb_width_pct", "BB bandwidth percentile", "compression", "num"),
    ("rvol_30", "RVOL first 30m", "volume", "num"),
    ("rvol_15", "RVOL first 15m", "volume", "num"),
    ("oi_change_pct", "OI change %", "oi", "num"),
    ("oi_state", "OI state", "oi", "cat"),
    ("gap_atr", "Gap (ATR)", "gap", "num"),
    ("gap_pct", "Gap %", "gap", "num"),
    ("gap_location", "Gap vs prior day", "gap", "cat"),
    ("gap_held", "Gap held at 09:45", "gap", "bin"),
    ("or15_atr", "First 15m range / ATR", "open", "num"),
    ("or_break", "Candles 4–6 beyond opening range", "open", "bin"),
    ("close_pos_30", "09:45 close in 30m range", "open", "num"),
    ("move_open_atr", "Move from open (ATR)", "open", "num"),
    ("rs_nifty", "RS vs Nifty at 09:45", "rs", "num"),
    ("rs_sector", "RS vs sector at 09:45", "rs", "num"),
    ("rs_20d", "20-day RS vs Nifty", "rs", "num"),
    ("dist_ema20_atr", "09:45 vs EMA20 (ATR)", "trend", "num"),
    ("dist_ema50_atr", "09:45 vs EMA50 (ATR)", "trend", "num"),
    ("ema20_slope_atr", "EMA20 slope (ATR)", "trend", "num"),
    ("above_20h", "Above 20-day high", "trend", "bin"),
    ("below_20l", "Below 20-day low", "trend", "bin"),
    ("dist_20_extreme_atr", "Nearest 20d extreme (ATR)", "trend", "num"),
    ("dist_50_extreme_atr", "Nearest 50d extreme (ATR)", "trend", "num"),
    ("adx_daily", "Daily ADX", "strength", "num"),
    ("adx_5m", "5m ADX at 09:45", "strength", "num"),
    ("di_spread", "5m +DI minus −DI", "strength", "num"),
    ("ema30_dist_pct", "09:45 vs 5m EMA30 %", "strength", "num"),
    ("extreme_gap", "Extreme gap (>2 ATR)", "catalyst", "bin"),
    ("extreme_rvol", "Extreme RVOL (>3x)", "catalyst", "bin"),
]


def _key(ts: datetime) -> str:
    return ensure_ist(ts).strftime("%Y-%m-%dT%H:%M")


def _f(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:
        return None
    return out


def _prepare_5m(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    hols = holidays()
    by_key: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        ts = parse_ts(row.get("timestamp"))
        if ts is None:
            continue
        ts = ensure_ist(ts)
        if not in_cash_session(ts, 5):
            continue
        if ts.date().weekday() >= 5 or ts.date() in hols:
            continue
        try:
            bar = {
                "timestamp": ts,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume") or 0.0),
            }
        except (KeyError, TypeError, ValueError):
            continue
        oi = _f(row.get("oi"))
        if oi is not None:
            bar["oi"] = oi
        by_key[_key(ts)] = bar
    return by_key


def _prepare_daily(rows: Sequence[Dict[str, Any]]) -> Dict[date, Dict[str, Any]]:
    out: Dict[date, Dict[str, Any]] = {}
    for row in rows or []:
        ts = parse_ts(row.get("timestamp"))
        if ts is None:
            raw = str(row.get("timestamp") or "")[:10]
            try:
                day = date.fromisoformat(raw)
            except ValueError:
                continue
        else:
            day = ensure_ist(ts).date()
        try:
            bar = {
                "date": day,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            }
        except (KeyError, TypeError, ValueError):
            continue
        oi = _f(row.get("oi"))
        if oi is not None:
            bar["oi"] = oi
        out[day] = bar
    return out


def _ordered(by_key: Dict[str, Dict[str, Any]], end_key: str) -> List[Dict[str, Any]]:
    return [by_key[k] for k in sorted(by_key) if k <= end_key]


def _bb_width_pct(closes: Sequence[float], period: int = 20, lookback: int = 120) -> Optional[float]:
    if len(closes) < period + 5:
        return None
    widths: List[float] = []
    for i in range(period - 1, len(closes)):
        window = [float(x) for x in closes[i - period + 1 : i + 1]]
        mean = sum(window) / float(period)
        var = sum((x - mean) ** 2 for x in window) / float(period)
        if mean == 0:
            widths.append(0.0)
        else:
            widths.append((4.0 * (var ** 0.5)) / abs(mean))
    hist = widths[-lookback:]
    if len(hist) < 10:
        return None
    current = hist[-1]
    return 100.0 * sum(1 for w in hist if w < current) / float(len(hist))


def _rvol(by_key: Dict[str, Dict[str, Any]], day: date, prior_days: Sequence[date], candles: int) -> Optional[float]:
    def window_volume(session: date) -> Optional[float]:
        total = 0.0
        for n in range(1, candles + 1):
            bar = by_key.get(_key(candle_start(session, n)))
            if bar is None:
                return None
            total += float(bar["volume"])
        return total

    today = window_volume(day)
    if today is None:
        return None
    hist = []
    for session in prior_days[-20:]:
        vol = window_volume(session)
        if vol is not None and vol > 0:
            hist.append(vol)
    if len(hist) < 10:
        return None
    avg = sum(hist) / float(len(hist))
    if avg <= 0:
        return None
    return today / avg


def _pct_change(now: Optional[float], was: Optional[float]) -> Optional[float]:
    if now is None or was is None or was == 0:
        return None
    return (now / was - 1.0) * 100.0


def _index_close(by_key: Optional[Dict[str, Dict[str, Any]]], day: date) -> Optional[float]:
    if not by_key:
        return None
    bar = by_key.get(_key(candle_start(day, REFERENCE_CANDLE)))
    if bar is None:
        return None
    return float(bar["close"])


def _oi_state(price_change: float, oi_change: float) -> str:
    up = price_change > 0
    down = price_change < 0
    oi_up = oi_change > 0
    oi_down = oi_change < 0
    if up and oi_up:
        return "long_buildup"
    if down and oi_up:
        return "short_buildup"
    if up and oi_down:
        return "short_covering"
    if down and oi_down:
        return "long_unwinding"
    return "neutral"


def _first_hit(bars: Sequence[Dict[str, Any]], up_px: float, down_px: float) -> Optional[str]:
    """Which price is touched first. A bullish bar is treated as open → low → high."""
    for bar in bars:
        hit_up = float(bar["high"]) >= up_px
        hit_down = float(bar["low"]) <= down_px
        if not hit_up and not hit_down:
            continue
        if hit_up and not hit_down:
            return "up"
        if hit_down and not hit_up:
            return "down"
        if float(bar["close"]) >= float(bar["open"]):
            return "down"
        return "up"
    return None


def _labels(post: Sequence[Dict[str, Any]], ref: float, atr: float) -> Dict[str, Any]:
    highs = [float(b["high"]) for b in post]
    lows = [float(b["low"]) for b in post]
    max_up = (max(highs) - ref) / atr
    max_down = (ref - min(lows)) / atr
    close_move = (float(post[-1]["close"]) - ref) / atr
    out: Dict[str, Any] = {
        "max_up_atr": max_up,
        "max_down_atr": max_down,
        "close_move_atr": close_move,
    }
    for threshold in THRESHOLDS:
        tag = str(threshold)
        out[f"big_up_{tag}"] = 1 if max_up >= threshold else 0
        out[f"big_down_{tag}"] = 1 if max_down >= threshold else 0
        out[f"big_either_{tag}"] = 1 if max_up >= threshold or max_down >= threshold else 0
        up_first = _first_hit(post, ref + threshold * atr, ref - 1.0 * atr)
        down_first = _first_hit(post, ref + 1.0 * atr, ref - threshold * atr)
        out[f"clean_up_{tag}"] = 1 if up_first == "up" else 0
        out[f"clean_down_{tag}"] = 1 if down_first == "down" else 0
    return out


def build_stock_day(
    *,
    symbol: str,
    contract: str,
    instrument_key: str,
    day: date,
    bars_5m: Sequence[Dict[str, Any]],
    bars_daily: Sequence[Dict[str, Any]],
    nifty_5m: Optional[Sequence[Dict[str, Any]]] = None,
    nifty_daily: Optional[Sequence[Dict[str, Any]]] = None,
    sector_5m: Optional[Sequence[Dict[str, Any]]] = None,
    sector_daily: Optional[Sequence[Dict[str, Any]]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Return ``(row, None)`` or ``(None, skip_reason)``."""
    intraday = _prepare_5m(bars_5m)
    daily = _prepare_daily(bars_daily)
    opening = [_key(candle_start(day, n)) for n in range(1, REFERENCE_CANDLE + 1)]
    if any(k not in intraday for k in opening):
        return None, "missing one of the first 6 five-minute candles"
    forward = [_key(candle_start(day, n)) for n in range(REFERENCE_CANDLE + 1, SESSION_BARS + 1)]
    if any(k not in intraday for k in forward):
        return None, "session after 09:45 has missing 5-minute candles"

    hols = holidays()
    prev = previous_trading_day(day, hols)
    if prev is None or prev not in daily:
        return None, "daily ATR(14) not available — missing the previous session"
    daily_days = [d for d in sorted(daily) if d < day]
    if not daily_days or daily_days[-1] != prev:
        return None, "daily ATR(14) not available — missing the previous session"
    d_high = [daily[d]["high"] for d in daily_days]
    d_low = [daily[d]["low"] for d in daily_days]
    d_close = [daily[d]["close"] for d in daily_days]
    atr_series = wilder_atr_series(d_high, d_low, d_close, 14)
    atr = atr_series[-1]
    if atr is None or atr <= 0:
        return None, "daily ATR(14) not stable as of the previous close"

    ref_bar = intraday[opening[-1]]
    ref = float(ref_bar["close"])
    first = intraday[opening[0]]
    post = [intraday[k] for k in forward]
    labels = _labels(post, ref, float(atr))

    # 5-minute indicators stop at the reference candle.
    ref_key = opening[-1]
    series = _ordered(intraday, ref_key)
    closes = [b["close"] for b in series]
    ema30 = ema_series(closes, 30)
    adx5, pdi, mdi = adx_di_series(
        [b["high"] for b in series],
        [b["low"] for b in series],
        closes,
        14,
    )
    ref_i = len(series) - 1

    prev_bar = daily[prev]
    prev_close = float(prev_bar["close"])
    prev_range = float(prev_bar["high"]) - float(prev_bar["low"])
    ranges = [float(daily[d]["high"]) - float(daily[d]["low"]) for d in daily_days]
    nr7 = None
    if len(ranges) >= 7:
        nr7 = 1 if ranges[-1] < min(ranges[-7:-1]) else 0
    range_5_20 = None
    if len(ranges) >= 20:
        r5 = sum(ranges[-5:]) / 5.0
        r20 = sum(ranges[-20:]) / 20.0
        if r20 > 0:
            range_5_20 = r5 / r20
    atr5 = wilder_atr_series(d_high, d_low, d_close, 5)
    atr50 = wilder_atr_series(d_high, d_low, d_close, 50)
    atr_ratio = None
    if atr5[-1] and atr50[-1] and atr50[-1] > 0:
        atr_ratio = float(atr5[-1]) / float(atr50[-1])

    prior_sessions = [d for d in sorted({ensure_ist(b["timestamp"]).date() for b in intraday.values()}) if d < day]
    gap = float(first["open"]) - prev_close
    gap_atr = gap / float(atr)
    gap_pct = (float(first["open"]) / prev_close - 1.0) * 100.0 if prev_close else None
    if float(first["open"]) > float(prev_bar["high"]):
        gap_location = "above_high"
    elif float(first["open"]) < float(prev_bar["low"]):
        gap_location = "below_low"
    else:
        gap_location = "inside"
    gap_held = None
    if float(first["open"]) > prev_close:
        gap_held = 1 if min(float(intraday[k]["low"]) for k in opening) > prev_close else 0
    elif float(first["open"]) < prev_close:
        gap_held = 1 if max(float(intraday[k]["high"]) for k in opening) < prev_close else 0

    or_high = max(float(intraday[opening[i]]["high"]) for i in range(3))
    or_low = min(float(intraday[opening[i]]["low"]) for i in range(3))
    or_break = 0
    for i in range(3, 6):
        close = float(intraday[opening[i]]["close"])
        if close > or_high or close < or_low:
            or_break = 1
            break
    hi30 = max(float(intraday[k]["high"]) for k in opening)
    lo30 = min(float(intraday[k]["low"]) for k in opening)
    span = hi30 - lo30
    close_pos = (ref - lo30) / span if span > 0 else 0.5

    ema20 = ema_series(d_close, 20)
    ema50 = ema_series(d_close, 50)
    dist20 = (ref - ema20[-1]) / float(atr) if len(d_close) >= 20 else None
    dist50 = (ref - ema50[-1]) / float(atr) if len(d_close) >= 50 else None
    slope = None
    if len(d_close) >= 25:
        slope = (ema20[-1] - ema20[-6]) / float(atr)
    above_20h = None
    below_20l = None
    dist20x = None
    if len(daily_days) >= 20:
        hi = max(d_high[-20:])
        lo = min(d_low[-20:])
        above_20h = 1 if ref > hi else 0
        below_20l = 1 if ref < lo else 0
        dist20x = min(abs(ref - hi), abs(ref - lo)) / float(atr)
    dist50x = None
    if len(daily_days) >= 50:
        hi = max(d_high[-50:])
        lo = min(d_low[-50:])
        dist50x = min(abs(ref - hi), abs(ref - lo)) / float(atr)
    adx_d, _, _ = adx_di_series(d_high, d_low, d_close, 14)

    nifty_map = _prepare_5m(nifty_5m or [])
    nifty_day = _prepare_daily(nifty_daily or [])
    nifty_ref = _index_close(nifty_map, day)
    nifty_prev = float(nifty_day[prev]["close"]) if prev in nifty_day else None
    stock_pct = _pct_change(ref, prev_close)
    nifty_pct = _pct_change(nifty_ref, nifty_prev)
    rs_nifty = None if stock_pct is None or nifty_pct is None else stock_pct - nifty_pct
    rs_20d = None
    if len(daily_days) >= 21 and nifty_prev is not None:
        ago = daily_days[-21]
        nifty_days = [d for d in sorted(nifty_day) if d < day]
        if ago in daily and len(nifty_days) >= 21:
            stock_ret = _pct_change(prev_close, float(daily[ago]["close"]))
            nifty_ret = _pct_change(nifty_prev, float(nifty_day[nifty_days[-21]]["close"]))
            if stock_ret is not None and nifty_ret is not None:
                rs_20d = stock_ret - nifty_ret

    sector_map = _prepare_5m(sector_5m or [])
    sector_day = _prepare_daily(sector_daily or [])
    sector_ref = _index_close(sector_map, day)
    sector_prev = float(sector_day[prev]["close"]) if prev in sector_day else None
    sector_pct = _pct_change(sector_ref, sector_prev)
    rs_sector = None if stock_pct is None or sector_pct is None else stock_pct - sector_pct

    def _oi(value: Any) -> Optional[float]:
        number = _f(value)
        if number is None or number <= 0:
            return None
        return number

    oi_now = _oi(ref_bar.get("oi"))
    oi_prev = _oi(prev_bar.get("oi"))
    oi_basis = ""
    oi_change = None
    if oi_now is not None and oi_prev is not None:
        oi_change = (oi_now / oi_prev - 1.0) * 100.0
        oi_basis = "intraday_0945"
    elif len(daily_days) >= 2:
        older = daily[daily_days[-2]]
        oi_a = _oi(prev_bar.get("oi"))
        oi_b = _oi(older.get("oi"))
        if oi_a is not None and oi_b is not None:
            oi_change = (oi_a / oi_b - 1.0) * 100.0
            oi_basis = "previous_session"
    oi_state = ""
    if oi_change is not None and stock_pct is not None:
        oi_state = _oi_state(stock_pct, oi_change)

    rvol30 = _rvol(intraday, day, prior_sessions, 6)
    rvol15 = _rvol(intraday, day, prior_sessions, 3)
    extreme_gap = 1 if abs(gap_atr) >= 2 else 0
    extreme_rvol = 1 if rvol30 is not None and rvol30 >= 3 else 0

    ema30_dist = None
    if ema30 and ema30[-1]:
        ema30_dist = (ref / float(ema30[-1]) - 1.0) * 100.0
    di_spread = None
    if pdi[ref_i] is not None and mdi[ref_i] is not None:
        di_spread = float(pdi[ref_i]) - float(mdi[ref_i])

    row: Dict[str, Any] = {
        "date": day.isoformat(),
        "symbol": symbol,
        "contract": contract,
        "instrument_key": instrument_key,
        "reference": ref,
        "atr": float(atr),
        "prev_close": prev_close,
        "prev_range_atr": prev_range / float(atr),
        "nr7": nr7,
        "range_5_20": range_5_20,
        "atr5_atr50": atr_ratio,
        "bb_width_pct": _bb_width_pct(d_close),
        "rvol_30": rvol30,
        "rvol_15": rvol15,
        "oi_change_pct": oi_change,
        "oi_state": oi_state,
        "oi_basis": oi_basis,
        "gap_atr": gap_atr,
        "gap_pct": gap_pct,
        "gap_location": gap_location,
        "gap_held": gap_held,
        "or15_atr": (or_high - or_low) / float(atr),
        "or_break": or_break,
        "close_pos_30": close_pos,
        "move_open_atr": (ref - float(first["open"])) / float(atr),
        "rs_nifty": rs_nifty,
        "rs_sector": rs_sector,
        "rs_20d": rs_20d,
        "dist_ema20_atr": dist20,
        "dist_ema50_atr": dist50,
        "ema20_slope_atr": slope,
        "above_20h": above_20h,
        "below_20l": below_20l,
        "dist_20_extreme_atr": dist20x,
        "dist_50_extreme_atr": dist50x,
        "adx_daily": adx_d[-1] if adx_d else None,
        "adx_5m": adx5[ref_i],
        "di_spread": di_spread,
        "ema30_dist_pct": ema30_dist,
        "extreme_gap": extreme_gap,
        "extreme_rvol": extreme_rvol,
    }
    row.update(labels)
    return row, None
