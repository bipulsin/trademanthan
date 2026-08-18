"""Pine Layer 10f Rocket/Crash scoring (research backtest only).

Replicates the Pine session + S1–S4 / BS1–BS4 rules. This module is not used by
live websocket Rocket/Crash (`rocket_pre_ignition` / `rocket_ws_live`).
"""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Sequence, Tuple
from uuid import uuid4

import pytz

from backend.services.smart_futures_picker.indicators import (
    _adx_column_name,
    true_range,
)

IST = pytz.timezone("Asia/Kolkata")
SESSION_OPEN = time(9, 15)
SESSION_END = time(15, 30)
DEFAULT_TICK = 0.05
ADX_LENGTH = 14
LOOKBACK_CAP = 20
SCORE_MIN = 2


def parse_ist(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return IST.localize(ts)
        return ts.astimezone(IST)
    s = str(ts).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
    except ValueError:
        return None


def in_session(dt: datetime) -> bool:
    t = dt.time()
    return SESSION_OPEN <= t < SESSION_END


def session_phase(sess_bar: int) -> str:
    if sess_bar <= 5:
        return "early"
    if sess_bar <= 15:
        return "mid"
    return "late"


def adx_bucket(adx: Optional[float]) -> Optional[str]:
    if adx is None:
        return None
    if adx < 20:
        return "lt20"
    if adx <= 30:
        return "20to30"
    return "gt30"


def _ema_series(values: Sequence[float], period: int) -> List[Optional[float]]:
    n = len(values)
    out: List[Optional[float]] = [None] * n
    p = max(1, int(period))
    if n < p:
        return out
    k = 2.0 / (p + 1.0)
    sma = sum(float(values[i]) for i in range(p)) / float(p)
    out[p - 1] = sma
    prev = sma
    for i in range(p, n):
        prev = float(values[i]) * k + prev * (1.0 - k)
        out[i] = prev
    return out


def _wilder_atr_series(
    highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int
) -> List[Optional[float]]:
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    p = max(1, int(period))
    if n < p + 1:
        return out
    trs = [0.0] * n
    for i in range(1, n):
        trs[i] = true_range(float(highs[i]), float(lows[i]), float(closes[i - 1]))
    atr = sum(trs[1 : p + 1]) / float(p)
    out[p] = atr
    pm1 = float(p - 1)
    for i in range(p + 1, n):
        atr = (atr * pm1 + trs[i]) / float(p)
        out[i] = atr
    return out


def _adx_series(
    highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], length: int = ADX_LENGTH
) -> List[Optional[float]]:
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    need = max(30, length + 20)
    if n < need:
        return out
    try:
        import pandas as pd
        import pandas_ta as ta
    except Exception:
        return out
    df = pd.DataFrame(
        {"high": list(highs), "low": list(lows), "close": list(closes)}
    )
    adx_df = ta.adx(df["high"], df["low"], df["close"], length=int(length), lensig=int(length))
    if adx_df is None or adx_df.empty:
        return out
    col = _adx_column_name(adx_df, int(length))
    if not col:
        return out
    for i, v in enumerate(adx_df[col].tolist()):
        try:
            if v is None or v != v:
                continue
            out[i] = float(v)
        except (TypeError, ValueError):
            continue
    return out


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _bar_ohlcv(c: Dict[str, Any]) -> Tuple[float, float, float, float, float]:
    return (
        _f(c.get("open")),
        _f(c.get("high")),
        _f(c.get("low")),
        _f(c.get("close")),
        max(0.0, _f(c.get("volume"))),
    )


def score_bars(
    candles: Sequence[Dict[str, Any]],
    *,
    tick_size: float = DEFAULT_TICK,
) -> List[Dict[str, Any]]:
    """Score every in-session 10m bar. Does not filter by score threshold."""
    rows: List[Dict[str, Any]] = []
    parsed: List[Tuple[datetime, Dict[str, Any]]] = []
    for c in candles:
        dt = parse_ist(c.get("timestamp") or c.get("candle_start"))
        if dt is None or not in_session(dt):
            continue
        parsed.append((dt, c))
    parsed.sort(key=lambda x: x[0])
    if not parsed:
        return rows

    n = len(parsed)
    opens = [0.0] * n
    highs = [0.0] * n
    lows = [0.0] * n
    closes = [0.0] * n
    volumes = [0.0] * n
    times: List[datetime] = []
    for i, (dt, c) in enumerate(parsed):
        o, h, lo, cl, v = _bar_ohlcv(c)
        opens[i], highs[i], lows[i], closes[i], volumes[i] = o, h, lo, cl, v
        times.append(dt)

    ema5_s = _ema_series(closes, 5)
    atr5_s = _wilder_atr_series(highs, lows, closes, 5)
    atr10_s = _wilder_atr_series(highs, lows, closes, 10)
    atr14_s = _wilder_atr_series(highs, lows, closes, 14)
    adx_s = _adx_series(highs, lows, closes, ADX_LENGTH)

    tick = max(1e-9, float(tick_size))
    sess_bar = 0
    cum_delta = 0.0
    session_cum: List[float] = []
    prev_date: Optional[date] = None

    for i in range(n):
        d = times[i].date()
        if prev_date != d:
            sess_bar = 0
            cum_delta = 0.0
            session_cum = []
            prev_date = d
        sess_bar += 1
        lookback = min(sess_bar, LOOKBACK_CAP)

        o, h, lo, cl, vol = opens[i], highs[i], lows[i], closes[i], volumes[i]
        bar_range = max(h - lo, tick)
        close_pos = (cl - lo) / bar_range if bar_range > 0 else 0.5
        delta_bar = ((close_pos - 0.5) * 2.0) * vol
        cum_delta += delta_bar
        session_cum.append(cum_delta)

        if sess_bar >= 4:
            delta_slope = session_cum[-1] - session_cum[-4]
        else:
            delta_slope = None

        atr5 = atr5_s[i]
        atr14 = atr14_s[i]
        atr10 = atr10_s[i]
        ema5 = ema5_s[i]
        squeeze = bool(atr5 is not None and atr14 is not None and atr5 < atr14)

        prior_quiet = False
        prior3_avg = None
        if i >= 3:
            prior3_avg = (volumes[i - 1] + volumes[i - 2] + volumes[i - 3]) / 3.0
            prior_quiet = volumes[i - 1] < prior3_avg

        lb_start = i - lookback + 1
        window = closes[lb_start : i + 1]
        price_h_adapt = max(window) if window else cl
        price_l_adapt = min(window) if window else cl

        over_extended = bool(ema5 is not None and atr10 is not None and cl > ema5 + 2.0 * atr10)
        under_extended = bool(ema5 is not None and atr10 is not None and cl < ema5 - 2.0 * atr10)

        prev_c = closes[i - 1] if i >= 1 else None
        prev_lo = lows[i - 1] if i >= 1 else None
        prev_hi = highs[i - 1] if i >= 1 else None

        s1 = bool(prev_c is not None and cl < o and cl >= prev_c and close_pos >= 0.55)
        s2 = bool(
            delta_slope is not None
            and delta_slope > 0
            and cl < 0.98 * price_h_adapt
        )
        s3 = bool(
            squeeze
            and sess_bar >= 4
            and prev_lo is not None
            and lo >= prev_lo
            and cl >= o
        )
        s4 = bool(
            prior_quiet
            and prior3_avg is not None
            and vol >= 1.2 * prior3_avg
            and cl > o
            and close_pos >= 0.6
        )
        score_long = (int(s1) + int(s2) + int(s3) + int(s4)) if not over_extended else 0

        bs1 = bool(prev_c is not None and cl > o and cl <= prev_c and close_pos <= 0.45)
        bs2 = bool(
            delta_slope is not None
            and delta_slope < 0
            and cl > (2.0 - 0.98) * price_l_adapt
        )
        bs3 = bool(
            squeeze
            and sess_bar >= 4
            and prev_hi is not None
            and h <= prev_hi
            and cl <= o
        )
        bs4 = bool(
            prior_quiet
            and prior3_avg is not None
            and vol >= 1.2 * prior3_avg
            and cl < o
            and close_pos <= 0.4
        )
        score_short = (int(bs1) + int(bs2) + int(bs3) + int(bs4)) if not under_extended else 0

        if score_long >= SCORE_MIN and score_short >= SCORE_MIN:
            dominant = "both"
        elif score_long >= SCORE_MIN:
            dominant = "long"
        elif score_short >= SCORE_MIN:
            dominant = "short"
        else:
            dominant = "none"

        rows.append(
            {
                "i": i,
                "symbol": None,
                "session_date": d,
                "candle_time": times[i],
                "sess_bar_number": sess_bar,
                "score_long": score_long,
                "score_short": score_short,
                "dominant_side": dominant,
                "s1": s1,
                "s2": s2,
                "s3": s3,
                "s4": s4,
                "bs1": bs1,
                "bs2": bs2,
                "bs3": bs3,
                "bs4": bs4,
                "close": cl,
                "open": o,
                "high": h,
                "low": lo,
                "volume": vol,
                "cum_delta_session": cum_delta,
                "delta_slope": delta_slope,
                "atr5": atr5,
                "atr14": atr14,
                "atr10": atr10,
                "ema5": ema5,
                "squeeze": squeeze,
                "prior_quiet": prior_quiet,
                "over_extended": over_extended,
                "under_extended": under_extended,
                "adx_at_signal": adx_s[i],
                "session_phase": session_phase(sess_bar),
                "close_pos": close_pos,
            }
        )
    return rows


def _fwd_ret(closes: Sequence[float], i: int, n_bars: int, last_same_session: int) -> Optional[float]:
    j = i + n_bars
    if j > last_same_session:
        return None
    c0 = float(closes[i])
    if c0 == 0:
        return None
    return (float(closes[j]) - c0) / c0


def _mfe_mae(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    i: int,
    last_same_session: int,
    side: str,
    horizon: int = 5,
) -> Tuple[Optional[float], Optional[float]]:
    end = min(i + horizon, last_same_session)
    if end <= i:
        return None, None
    c0 = float(closes[i])
    if c0 == 0:
        return None, None
    fav = 0.0
    adv = 0.0
    for j in range(i + 1, end + 1):
        up = (float(highs[j]) - c0) / c0
        dn = (c0 - float(lows[j])) / c0
        if side == "short":
            fav = max(fav, dn)
            adv = max(adv, up)
        else:
            fav = max(fav, up)
            adv = max(adv, dn)
    return fav, adv


def _direction_correct(ret: Optional[float], side: str) -> Optional[bool]:
    if ret is None:
        return None
    if side == "short":
        return ret < 0
    return ret > 0


def attach_forward_outcomes(rows: List[Dict[str, Any]]) -> None:
    """Mutate rows with same-session forward returns / MFE / MAE / direction flags."""
    if not rows:
        return
    closes = [float(r["close"]) for r in rows]
    highs = [float(r["high"]) for r in rows]
    lows = [float(r["low"]) for r in rows]
    dates = [r["session_date"] for r in rows]
    last_of_date: Dict[date, int] = {}
    for i, d in enumerate(dates):
        last_of_date[d] = i

    for i, r in enumerate(rows):
        last = last_of_date[r["session_date"]]
        r["fwd_ret_1bar"] = _fwd_ret(closes, i, 1, last)
        r["fwd_ret_3bar"] = _fwd_ret(closes, i, 3, last)
        r["fwd_ret_5bar"] = _fwd_ret(closes, i, 5, last)
        r["fwd_ret_10bar"] = _fwd_ret(closes, i, 10, last)
        side = r["dominant_side"]
        mfe_side = "short" if side == "short" else "long"
        mfe, mae = _mfe_mae(highs, lows, closes, i, last, mfe_side, 5)
        r["fwd_mfe_5bar"] = mfe
        r["fwd_mae_5bar"] = mae
        r["fwd_direction_correct_1bar"] = _direction_correct(r["fwd_ret_1bar"], mfe_side)
        r["fwd_direction_correct_3bar"] = _direction_correct(r["fwd_ret_3bar"], mfe_side)


def events_from_scored(rows: List[Dict[str, Any]], symbol: str) -> List[Dict[str, Any]]:
    """Keep bars where long or short score >= 2 and assign event_id."""
    out: List[Dict[str, Any]] = []
    for r in rows:
        if int(r["score_long"]) < SCORE_MIN and int(r["score_short"]) < SCORE_MIN:
            continue
        ev = dict(r)
        ev.pop("i", None)
        ev.pop("close_pos", None)
        ev["event_id"] = str(uuid4())
        ev["symbol"] = symbol
        out.append(ev)
    return out
