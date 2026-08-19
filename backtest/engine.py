"""HA Momentum indicators, signals, and candle-by-candle trade simulation."""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

IST = "Asia/Kolkata"
WARMUP = 70
SLIPPAGE_PCT = 0.001
MAX_SL_RS = 5000.0
RR_T1 = 2.0
RR_T2 = 3.0
LARGE_CANDLE_THRESHOLD_PCT = 0.3
SIGNAL_START = time(9, 30)
SIGNAL_END = time(14, 45)
FORCED_EXIT = time(15, 0)


def _series_ema(close: pd.Series, length: int) -> pd.Series:
    return close.ewm(span=length, adjust=False).mean()


def _wilder_adx(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    prev_high = high.shift(1)
    prev_low = low.shift(1)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    up = high - prev_high
    down = prev_low - low
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    atr = tr.ewm(alpha=1.0 / length, adjust=False).mean()
    plus_di = 100.0 * plus_dm.ewm(alpha=1.0 / length, adjust=False).mean() / atr.replace(0, pd.NA)
    minus_di = 100.0 * minus_dm.ewm(alpha=1.0 / length, adjust=False).mean() / atr.replace(0, pd.NA)
    di_sum = (plus_di + minus_di).replace(0, pd.NA)
    dx = (100.0 * (plus_di - minus_di).abs() / di_sum)
    dx = pd.to_numeric(dx, errors="coerce").fillna(0.0)
    return dx.ewm(alpha=1.0 / length, adjust=False).mean()


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = out["close"].astype(float)
    high = out["high"].astype(float)
    low = out["low"].astype(float)
    out["ema5"] = _series_ema(close, 5)
    out["ema15"] = _series_ema(close, 15)
    out["ema50"] = _series_ema(close, 50)
    fast = _series_ema(close, 24)
    slow = _series_ema(close, 52)
    macd_line = fast - slow
    signal = macd_line.ewm(span=18, adjust=False).mean()
    out["macd_hist"] = macd_line - signal
    used_pta = False
    try:
        import pandas_ta as ta

        macd = ta.macd(close, fast=24, slow=52, signal=18)
        if macd is not None and not macd.empty:
            hist_col = [c for c in macd.columns if str(c).startswith("MACDh")]
            if hist_col:
                out["macd_hist"] = macd[hist_col[0]].values
                used_pta = True
        adx_df = ta.adx(high, low, close, length=14)
        if adx_df is not None and not adx_df.empty:
            adx_col = [c for c in adx_df.columns if str(c).upper().startswith("ADX")]
            if adx_col:
                out["adx14"] = adx_df[adx_col[0]].values
                used_pta = True
    except Exception:
        used_pta = False
    if "adx14" not in out.columns:
        out["adx14"] = _wilder_adx(high, low, close, 14)
    out["adx14_prev"] = out["adx14"].shift(1)
    out.attrs["pandas_ta"] = used_pta
    return out


def _in_signal_window(ts: datetime) -> bool:
    t = ts.time().replace(microsecond=0)
    return SIGNAL_START <= t <= SIGNAL_END


def long_signal(row: pd.Series, prev: pd.Series) -> bool:
    return bool(
        row["ema5"] > row["ema15"]
        and prev["ema5"] <= prev["ema15"]
        and row["ema15"] > row["ema50"]
        and row["ema5"] > row["ema50"]
        and row["macd_hist"] > 0
        and row["adx14"] > 20
        and row["adx14"] > row["adx14_prev"]
    )


def short_signal(row: pd.Series, prev: pd.Series) -> bool:
    return bool(
        row["ema5"] < row["ema15"]
        and prev["ema5"] >= prev["ema15"]
        and row["ema15"] < row["ema50"]
        and row["ema5"] < row["ema50"]
        and row["macd_hist"] < 0
        and row["adx14"] > 20
        and row["adx14"] > row["adx14_prev"]
    )


def _pnl(direction: str, entry: float, exit_px: float, qty: int) -> float:
    if direction == "LONG":
        return round((exit_px - entry) * qty, 2)
    return round((entry - exit_px) * qty, 2)


def simulate_trade(
    df: pd.DataFrame,
    i: int,
    direction: str,
    lot_qty: int,
) -> Dict[str, Any]:
    row = df.iloc[i]
    prev = df.iloc[i - 1]
    close = float(row["close"])
    high = float(row["high"])
    low = float(row["low"])
    if direction == "LONG":
        entry = close * (1.0 + SLIPPAGE_PCT)
    else:
        entry = close * (1.0 - SLIPPAGE_PCT)
    candle_pct = abs(high - low) / low * 100.0 if low else 0.0
    use_prev = candle_pct > LARGE_CANDLE_THRESHOLD_PCT
    if direction == "LONG":
        sl = float(prev["low"]) if use_prev else low
    else:
        sl = float(prev["high"]) if use_prev else high
    risk = abs(entry - sl)
    sl_rs = risk * lot_qty
    if direction == "LONG":
        t1 = entry + RR_T1 * risk
        t2 = entry + RR_T2 * risk
        mfe = high
    else:
        t1 = entry - RR_T1 * risk
        t2 = entry - RR_T2 * risk
        mfe = low
    ts: datetime = row["ts"].to_pydatetime() if hasattr(row["ts"], "to_pydatetime") else row["ts"]
    out: Dict[str, Any] = {
        "direction": direction,
        "signal_time": ts,
        "entry_price": round(entry, 2),
        "sl_price": round(sl, 2),
        "sl_distance": round(risk, 4),
        "sl_rs": round(sl_rs, 2),
        "lot_qty": int(lot_qty),
        "t1_price": round(t1, 2),
        "t2_price": round(t2, 2),
        "entry_candle_size_pct": round(candle_pct, 4),
        "sl_used_prev_candle": bool(use_prev),
        "skipped": sl_rs > MAX_SL_RS,
        "reason": "SL_EXCEEDS_5K" if sl_rs > MAX_SL_RS else None,
    }
    if out["skipped"] or risk <= 0 or lot_qty <= 0:
        return out

    t1_hit = t2_hit = sl_hit = False
    first_reason: Optional[str] = None
    first_px: Optional[float] = None
    exit_px = None
    exit_ts = None
    exit_reason = None
    entry_day = ts.date()

    for j in range(i + 1, len(df)):
        bar = df.iloc[j]
        bts: datetime = bar["ts"].to_pydatetime() if hasattr(bar["ts"], "to_pydatetime") else bar["ts"]
        if bts.date() != entry_day:
            if first_reason is None:
                first_reason, first_px = "TIME_EXIT", float(bar["open"])
            exit_reason = "TIME_EXIT"
            exit_px = float(bar["open"])
            exit_ts = bts
            break
        if bts.time().replace(microsecond=0) >= FORCED_EXIT:
            if first_reason is None:
                first_reason, first_px = "TIME_EXIT", float(bar["open"])
            exit_reason = "TIME_EXIT"
            exit_px = float(bar["open"])
            exit_ts = bts
            break

        bh, bl, bo = float(bar["high"]), float(bar["low"]), float(bar["open"])
        if direction == "LONG":
            mfe = max(mfe, bh)
            if bl <= sl:
                sl_hit = True
                if first_reason is None:
                    first_reason, first_px = "SL_HIT", sl
                exit_reason, exit_px, exit_ts = "SL_HIT", sl, bts
                break
            if bh >= t1:
                t1_hit = True
                if first_reason is None:
                    first_reason, first_px = "T1", t1
            if bh >= t2:
                t2_hit = True
                if first_reason is None:
                    first_reason, first_px = "T2", t2
        else:
            mfe = min(mfe, bl)
            if bh >= sl:
                sl_hit = True
                if first_reason is None:
                    first_reason, first_px = "SL_HIT", sl
                exit_reason, exit_px, exit_ts = "SL_HIT", sl, bts
                break
            if bl <= t1:
                t1_hit = True
                if first_reason is None:
                    first_reason, first_px = "T1", t1
            if bl <= t2:
                t2_hit = True
                if first_reason is None:
                    first_reason, first_px = "T2", t2

    if exit_reason is None:
        last = df.iloc[-1]
        last_ts = last["ts"].to_pydatetime() if hasattr(last["ts"], "to_pydatetime") else last["ts"]
        exit_reason, exit_px, exit_ts = "TIME_EXIT", float(last["close"]), last_ts
        if first_reason is None:
            first_reason, first_px = "TIME_EXIT", exit_px

    actual_px = first_px if first_px is not None else exit_px
    actual_reason = first_reason or exit_reason
    t1_exit = t1 if t1_hit else actual_px
    t2_exit = t2 if t2_hit else actual_px
    hold = 0
    if exit_ts is not None:
        hold = int((exit_ts - ts).total_seconds() // 60)

    out.update(
        {
            "t1_hit": t1_hit,
            "t2_hit": t2_hit,
            "sl_hit": sl_hit,
            "t1_exit_price": round(float(t1_exit), 2),
            "t2_exit_price": round(float(t2_exit), 2),
            "actual_exit_price": round(float(actual_px), 2),
            "actual_exit_time": exit_ts,
            "exit_reason": actual_reason,
            "pnl_t1_rs": _pnl(direction, entry, float(t1_exit), lot_qty),
            "pnl_t2_rs": _pnl(direction, entry, float(t2_exit), lot_qty),
            "actual_pnl_rs": _pnl(direction, entry, float(actual_px), lot_qty),
            "max_favorable": round(float(mfe), 2),
            "holding_min": hold,
        }
    )
    return out


def run_symbol(
    candles: List[Dict[str, Any]],
    *,
    symbol: str,
    instrument_key: str,
    lot_qty: int,
    from_d: date,
    to_d: date,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not candles:
        return [], []
    df = pd.DataFrame(candles)
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"]).sort_values("ts").drop_duplicates("ts")
    df["ts"] = df["ts"].dt.tz_convert(IST)
    df = add_indicators(df).reset_index(drop=True)
    trades: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    used_days = set()
    for i in range(max(WARMUP, 1), len(df)):
        row = df.iloc[i]
        prev = df.iloc[i - 1]
        ts = row["ts"].to_pydatetime()
        d = ts.date()
        if d < from_d or d > to_d:
            continue
        if not _in_signal_window(ts):
            continue
        if d in used_days:
            continue
        if pd.isna(row["ema50"]) or pd.isna(row["adx14"]) or pd.isna(row["adx14_prev"]):
            continue
        direction = None
        if long_signal(row, prev):
            direction = "LONG"
        elif short_signal(row, prev):
            direction = "SHORT"
        if not direction:
            continue
        used_days.add(d)
        sim = simulate_trade(df, i, direction, lot_qty)
        sim["symbol"] = symbol
        sim["instrument_key"] = instrument_key
        if sim.get("skipped"):
            skipped.append(sim)
        else:
            trades.append(sim)
    return trades, skipped
