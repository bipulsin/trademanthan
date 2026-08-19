"""HA Momentum v2 simulation (parameterized SL, RR, cutoff, Nifty VWAP)."""
from __future__ import annotations

import logging
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from backtest.engine import (
    FORCED_EXIT,
    IST,
    LARGE_CANDLE_THRESHOLD_PCT,
    SIGNAL_START,
    WARMUP,
    add_indicators,
    long_signal,
    short_signal,
    _pnl,
)

SLIPPAGE_PCT = 0.001
SIGNAL_SCAN_END = time(14, 45)
logger = logging.getLogger("ha_engine_v2")


def candles_to_df(candles: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(candles)
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"]).sort_values("ts").drop_duplicates("ts")
    df["ts"] = df["ts"].dt.tz_convert(IST)
    return add_indicators(df).reset_index(drop=True)


def nifty_session_vwap(candles: List[Dict[str, Any]]) -> Tuple[Dict[pd.Timestamp, float], Dict[pd.Timestamp, float], bool]:
    """Per-day VWAP from 09:15. Returns close and vwap maps keyed by tz-aware ts. used_volume flag."""
    if not candles:
        return {}, {}, False
    df = pd.DataFrame(candles)
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"]).sort_values("ts").drop_duplicates("ts")
    df["ts"] = df["ts"].dt.tz_convert(IST)
    df["tp"] = (df["high"].astype(float) + df["low"].astype(float) + df["close"].astype(float)) / 3.0
    df["vol"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0.0)
    used_vol = bool((df["vol"] > 0).any())
    df["day"] = df["ts"].dt.date
    closes: Dict[pd.Timestamp, float] = {}
    vwaps: Dict[pd.Timestamp, float] = {}
    for _, g in df.groupby("day", sort=True):
        g = g.sort_values("ts")
        if used_vol:
            pv = (g["tp"] * g["vol"]).cumsum()
            vv = g["vol"].cumsum().replace(0, pd.NA)
            vw = pv / vv
            vw = vw.ffill()
            if vw.isna().all():
                vw = g["tp"].expanding().mean()
                used_vol = False
        else:
            vw = g["tp"].expanding().mean()
        for ts, c, v in zip(g["ts"], g["close"].astype(float), vw):
            closes[ts] = float(c)
            if pd.notna(v):
                vwaps[ts] = float(v)
    return closes, vwaps, used_vol


def _lookup_nifty(ts: datetime, closes: Dict, vwaps: Dict) -> Tuple[Optional[float], Optional[float]]:
    if not closes:
        return None, None
    key = pd.Timestamp(ts)
    if key.tzinfo is None:
        key = key.tz_localize(IST)
    if key in closes:
        return closes[key], vwaps.get(key)
    logger.warning("Nifty VWAP missing at %s — forward-filling last known", key)
    earlier = [k for k in closes if k <= key and k.date() == key.date()]
    if not earlier:
        earlier = [k for k in closes if k <= key]
    if not earlier:
        return None, None
    last = max(earlier)
    return closes[last], vwaps.get(last)


def simulate_trade(
    df: pd.DataFrame,
    i: int,
    direction: str,
    lot_qty: int,
    *,
    rr_t1: float,
    rr_t2: float,
    use_fixed_sl: bool,
    fixed_sl_pct: float,
    sl_cap: float,
) -> Dict[str, Any]:
    row = df.iloc[i]
    prev = df.iloc[i - 1]
    close = float(row["close"])
    high = float(row["high"])
    low = float(row["low"])
    entry = close * (1.0 + SLIPPAGE_PCT) if direction == "LONG" else close * (1.0 - SLIPPAGE_PCT)
    candle_pct = abs(high - low) / low * 100.0 if low else 0.0
    use_prev = candle_pct > LARGE_CANDLE_THRESHOLD_PCT
    if use_fixed_sl:
        sl = entry * (1.0 - fixed_sl_pct) if direction == "LONG" else entry * (1.0 + fixed_sl_pct)
        sl_logic = "FIXED_PCT"
    else:
        if direction == "LONG":
            sl = float(prev["low"]) if use_prev else low
        else:
            sl = float(prev["high"]) if use_prev else high
        sl_logic = "CANDLE_LOW"
    risk = abs(entry - sl)
    sl_rs = risk * lot_qty
    if direction == "LONG":
        t1 = entry + rr_t1 * risk
        t2 = entry + rr_t2 * risk
        mfe = high
        mae = low
    else:
        t1 = entry - rr_t1 * risk
        t2 = entry - rr_t2 * risk
        mfe = low
        mae = high
    ts = row["ts"].to_pydatetime() if hasattr(row["ts"], "to_pydatetime") else row["ts"]
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
        "sl_used_prev_candle": bool(use_prev) and not use_fixed_sl,
        "sl_logic_used": sl_logic,
        "skipped": (not use_fixed_sl) and sl_rs > sl_cap,
        "reason": "SL_EXCEEDS_5K" if (not use_fixed_sl and sl_rs > sl_cap) else None,
    }
    if out["skipped"] or risk <= 0 or lot_qty <= 0:
        return out

    t1_hit = t2_hit = sl_hit = False
    exit_px = None
    exit_ts = None
    exit_reason = None
    entry_day = ts.date()

    for j in range(i + 1, len(df)):
        bar = df.iloc[j]
        bts = bar["ts"].to_pydatetime() if hasattr(bar["ts"], "to_pydatetime") else bar["ts"]
        if bts.date() != entry_day or bts.time().replace(microsecond=0) >= FORCED_EXIT:
            exit_reason = "TIME_EXIT"
            exit_px = float(bar["open"])
            exit_ts = bts
            break
        bh, bl = float(bar["high"]), float(bar["low"])
        if direction == "LONG":
            mfe = max(mfe, bh)
            mae = min(mae, bl)
            if bl <= sl:
                sl_hit = True
                exit_reason = "T1_THEN_SL" if t1_hit else "SL_HIT"
                exit_px, exit_ts = sl, bts
                break
            if bh >= t1:
                t1_hit = True
            if bh >= t2:
                t2_hit = True
                exit_reason = "T1_THEN_T2" if t1_hit else "T2_HIT"
                exit_px, exit_ts = t2, bts
                break
        else:
            mfe = min(mfe, bl)
            mae = max(mae, bh)
            if bh >= sl:
                sl_hit = True
                exit_reason = "T1_THEN_SL" if t1_hit else "SL_HIT"
                exit_px, exit_ts = sl, bts
                break
            if bl <= t1:
                t1_hit = True
            if bl <= t2:
                t2_hit = True
                exit_reason = "T1_THEN_T2" if t1_hit else "T2_HIT"
                exit_px, exit_ts = t2, bts
                break

    if exit_reason is None:
        last = df.iloc[-1]
        exit_ts = last["ts"].to_pydatetime() if hasattr(last["ts"], "to_pydatetime") else last["ts"]
        exit_reason, exit_px = "TIME_EXIT", float(last["close"])

    t1_exit = t1 if t1_hit else exit_px
    t2_exit = t2 if t2_hit else exit_px
    hold = int((exit_ts - ts).total_seconds() // 60) if exit_ts is not None else 0
    out.update(
        {
            "t1_hit": t1_hit,
            "t2_hit": t2_hit,
            "sl_hit": sl_hit,
            "t1_exit_price": round(float(t1_exit), 2),
            "t2_exit_price": round(float(t2_exit), 2),
            "actual_exit_price": round(float(exit_px), 2),
            "actual_exit_time": exit_ts,
            "exit_reason": exit_reason,
            "pnl_t1_rs": _pnl(direction, entry, float(t1_exit), lot_qty),
            "pnl_t2_rs": _pnl(direction, entry, float(t2_exit), lot_qty),
            "actual_pnl_rs": _pnl(direction, entry, float(exit_px), lot_qty),
            "max_favorable": round(float(mfe), 2),
            "max_adverse": round(float(mae), 2),
            "holding_min": hold,
        }
    )
    return out


def run_prepared(
    df: pd.DataFrame,
    *,
    symbol: str,
    instrument_key: str,
    lot_qty: int,
    from_d: date,
    to_d: date,
    variant: Dict[str, Any],
    nifty_closes: Dict,
    nifty_vwaps: Dict,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    cutoff = datetime.strptime(str(variant.get("cutoff") or "14:45"), "%H:%M").time()
    rr_t1 = float(variant["rr_t1"])
    rr_t2 = float(variant["rr_t2"])
    use_fixed = bool(variant.get("use_fixed_sl"))
    fixed_pct = float(variant.get("fixed_sl_pct") or 0.004)
    sl_cap = float(variant.get("sl_cap") or 5000)
    nifty_filter = bool(variant.get("nifty_filter"))
    short_only = bool(variant.get("short_only"))
    vname = str(variant["name"])
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
        tclock = ts.time().replace(microsecond=0)
        if tclock < SIGNAL_START or tclock > SIGNAL_SCAN_END:
            continue
        if d in used_days:
            continue
        if pd.isna(row["ema50"]) or pd.isna(row["adx14"]) or pd.isna(row["adx14_prev"]):
            continue
        direction = None
        if (not short_only) and long_signal(row, prev):
            direction = "LONG"
        elif short_signal(row, prev):
            direction = "SHORT"
        if not direction:
            continue
        n_close, n_vwap = _lookup_nifty(ts, nifty_closes, nifty_vwaps)
        above = None
        if n_close is not None and n_vwap is not None:
            above = n_close > n_vwap

        def stamp(sim: Dict[str, Any]) -> Dict[str, Any]:
            sim["symbol"] = symbol
            sim["instrument_key"] = instrument_key
            sim["variant"] = vname
            sim["nifty_close_signal"] = round(n_close, 2) if n_close is not None else None
            sim["nifty_vwap_signal"] = round(n_vwap, 2) if n_vwap is not None else None
            sim["nifty_above_vwap"] = above
            return sim

        if tclock > cutoff:
            used_days.add(d)
            skipped.append(
                stamp(
                    {
                        "direction": direction,
                        "signal_time": ts,
                        "entry_price": None,
                        "sl_price": None,
                        "sl_distance": None,
                        "sl_rs": None,
                        "lot_qty": lot_qty,
                        "skipped": True,
                        "reason": "ENTRY_AFTER_1330_CUTOFF",
                    }
                )
            )
            continue
        if nifty_filter and direction == "LONG" and above is False:
            used_days.add(d)
            skipped.append(
                stamp(
                    {
                        "direction": direction,
                        "signal_time": ts,
                        "entry_price": None,
                        "sl_price": None,
                        "sl_distance": None,
                        "sl_rs": None,
                        "lot_qty": lot_qty,
                        "skipped": True,
                        "reason": "NIFTY_BELOW_VWAP_LONG_FILTERED",
                    }
                )
            )
            continue
        used_days.add(d)
        sim = simulate_trade(
            df,
            i,
            direction,
            lot_qty,
            rr_t1=rr_t1,
            rr_t2=rr_t2,
            use_fixed_sl=use_fixed,
            fixed_sl_pct=fixed_pct,
            sl_cap=sl_cap,
        )
        sim = stamp(sim)
        if sim.get("skipped"):
            skipped.append(sim)
        else:
            trades.append(sim)
    return trades, skipped
