"""Sambhav features v1 — causal feature set for Phase 2A (no look-ahead).

Volume/VWAP are omitted when index volume is unavailable (all-zero NIFTY).
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

from backend.services.sambhav.candles import to_ist
from backend.services.sambhav.config import SESSION_END, SESSION_START, TF_MINUTES

# Ordered feature columns persisted in sambhav_features_v1 (excludes volume/VWAP).
FEATURE_NAMES_V1: tuple[str, ...] = (
    # price / candle structure
    "close",
    "candle_return",
    "oc_return",
    "hl_range_pct",
    "body_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "close_loc",
    # returns
    "ret_1",
    "ret_2",
    "ret_3",
    "ret_6",
    "ret_9",
    "ret_18",
    # trend
    "ema9",
    "ema21",
    "ema50",
    "ema9_minus_ema21",
    "ema21_minus_ema50",
    "close_vs_ema9",
    "close_vs_ema21",
    "close_vs_ema50",
    "ema9_slope",
    "ema21_slope",
    # momentum
    "rsi9",
    "rsi14",
    "macd",
    "macd_signal",
    "macd_hist",
    "adx14",
    # volatility
    "atr14",
    "atr14_pct",
    "realized_vol3",
    "realized_vol6",
    "realized_vol18",
    "range_vs_avg",
    # time / session
    "minutes_since_open",
    "hour",
    "minute",
    "day_of_week",
    "sin_time",
    "cos_time",
    "session_progress",
)

# Warm-up dominated by EMA50 / ADX / MACD (~50 bars).
FEATURE_WARMUP_BARS_V1 = 50


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False, min_periods=span).mean()


def _rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def _macd_components(close: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
    ema12 = _ema(close, 12)
    ema26 = _ema(close, 26)
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False, min_periods=9).mean()
    hist = macd - signal
    return macd, signal, hist


def _adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    up = high.diff()
    down = -low.diff()
    plus_dm = pd.Series(
        np.where((up > down) & (up > 0), up, 0.0),
        index=high.index,
        dtype=float,
    )
    minus_dm = pd.Series(
        np.where((down > up) & (down > 0), down, 0.0),
        index=high.index,
        dtype=float,
    )
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    plus_di = 100.0 * (
        plus_dm.ewm(alpha=1 / period, min_periods=period, adjust=False).mean() / atr.replace(0, np.nan)
    )
    minus_di = 100.0 * (
        minus_dm.ewm(alpha=1 / period, min_periods=period, adjust=False).mean() / atr.replace(0, np.nan)
    )
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def bars_to_dataframe(bars: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame(columns=["candle_start", "open", "high", "low", "close", "volume"])
    rows = []
    for b in bars:
        rows.append(
            {
                "candle_start": to_ist(b["candle_start"]),
                "open": float(b["open"]),
                "high": float(b["high"]),
                "low": float(b["low"]),
                "close": float(b["close"]),
                "volume": float(b.get("volume") or 0),
            }
        )
    return pd.DataFrame(rows).sort_values("candle_start").reset_index(drop=True)


def assess_volume_availability(df: pd.DataFrame) -> Dict[str, Any]:
    """NIFTY index volume from Upstox is typically zero — do not invent features."""
    if df is None or df.empty or "volume" not in df.columns:
        return {"volume_available": False, "nonzero_fraction": 0.0, "reason": "no_volume_column"}
    v = df["volume"].fillna(0.0)
    nonzero = float((v > 0).mean()) if len(v) else 0.0
    available = nonzero >= 0.05  # require at least 5% nonzero bars
    return {
        "volume_available": bool(available),
        "nonzero_fraction": nonzero,
        "vmin": float(v.min()) if len(v) else None,
        "vmax": float(v.max()) if len(v) else None,
        "reason": None if available else "index_volume_all_zero_or_sparse",
    }


def compute_features_v1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Causal features at bar T using only T and earlier.

    Does not forward-fill. Warm-up rows remain NaN.
    Does not include volume/VWAP features (caller records volume_available).
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["candle_start", *FEATURE_NAMES_V1])

    d = df.sort_values("candle_start").reset_index(drop=True).copy()
    o, h, l, c = d["open"], d["high"], d["low"], d["close"]
    eps = 1e-12
    rng = (h - l).replace(0, np.nan)

    candle_return = c.pct_change(1)
    oc_return = (c - o) / (o + eps)
    hl_range_pct = (h - l) / (c + eps)
    body = (c - o).abs()
    body_pct = body / rng
    upper_wick_pct = (h - pd.concat([o, c], axis=1).max(axis=1)) / rng
    lower_wick_pct = (pd.concat([o, c], axis=1).min(axis=1) - l) / rng
    close_loc = (c - l) / rng

    ret_1 = c.pct_change(1)
    ret_2 = c.pct_change(2)
    ret_3 = c.pct_change(3)
    ret_6 = c.pct_change(6)
    ret_9 = c.pct_change(9)
    ret_18 = c.pct_change(18)

    ema9 = _ema(c, 9)
    ema21 = _ema(c, 21)
    ema50 = _ema(c, 50)
    ema9_minus_ema21 = (ema9 - ema21) / (c + eps)
    ema21_minus_ema50 = (ema21 - ema50) / (c + eps)
    close_vs_ema9 = (c - ema9) / (c + eps)
    close_vs_ema21 = (c - ema21) / (c + eps)
    close_vs_ema50 = (c - ema50) / (c + eps)
    ema9_slope = ema9.pct_change(1)
    ema21_slope = ema21.pct_change(1)

    rsi9 = _rsi(c, 9)
    rsi14 = _rsi(c, 14)
    macd, macd_signal, macd_hist = _macd_components(c)
    adx14 = _adx(h, l, c, 14)

    atr14 = _atr(h, l, c, 14)
    atr14_pct = atr14 / (c + eps)
    log_ret = np.log((c + eps) / (c.shift(1) + eps))
    realized_vol3 = log_ret.rolling(3, min_periods=3).std()
    realized_vol6 = log_ret.rolling(6, min_periods=6).std()
    realized_vol18 = log_ret.rolling(18, min_periods=18).std()
    avg_range = hl_range_pct.rolling(18, min_periods=6).mean()
    range_vs_avg = hl_range_pct / (avg_range + eps)

    open_mins = SESSION_START.hour * 60 + SESSION_START.minute
    end_mins = SESSION_END.hour * 60 + SESSION_END.minute
    session_len = max(end_mins - open_mins, 1)
    minutes_since_open = d["candle_start"].apply(
        lambda ts: (ts.hour * 60 + ts.minute) - open_mins
    ).astype(float)
    hour = d["candle_start"].dt.hour.astype(float)
    minute = d["candle_start"].dt.minute.astype(float)
    day_of_week = d["candle_start"].dt.weekday.astype(float)
    ang = 2.0 * math.pi * (minutes_since_open / float(session_len))
    sin_time = np.sin(ang)
    cos_time = np.cos(ang)
    session_progress = minutes_since_open / float(session_len)

    return pd.DataFrame(
        {
            "candle_start": d["candle_start"],
            "close": c.astype(float),
            "candle_return": candle_return,
            "oc_return": oc_return,
            "hl_range_pct": hl_range_pct,
            "body_pct": body_pct,
            "upper_wick_pct": upper_wick_pct,
            "lower_wick_pct": lower_wick_pct,
            "close_loc": close_loc,
            "ret_1": ret_1,
            "ret_2": ret_2,
            "ret_3": ret_3,
            "ret_6": ret_6,
            "ret_9": ret_9,
            "ret_18": ret_18,
            "ema9": ema9,
            "ema21": ema21,
            "ema50": ema50,
            "ema9_minus_ema21": ema9_minus_ema21,
            "ema21_minus_ema50": ema21_minus_ema50,
            "close_vs_ema9": close_vs_ema9,
            "close_vs_ema21": close_vs_ema21,
            "close_vs_ema50": close_vs_ema50,
            "ema9_slope": ema9_slope,
            "ema21_slope": ema21_slope,
            "rsi9": rsi9,
            "rsi14": rsi14,
            "macd": macd,
            "macd_signal": macd_signal,
            "macd_hist": macd_hist,
            "adx14": adx14,
            "atr14": atr14,
            "atr14_pct": atr14_pct,
            "realized_vol3": realized_vol3,
            "realized_vol6": realized_vol6,
            "realized_vol18": realized_vol18,
            "range_vs_avg": range_vs_avg,
            "minutes_since_open": minutes_since_open,
            "hour": hour,
            "minute": minute,
            "day_of_week": day_of_week,
            "sin_time": sin_time,
            "cos_time": cos_time,
            "session_progress": session_progress,
        }
    )


def assert_no_lookahead_features_v1(bars: Sequence[Dict[str, Any]]) -> None:
    df = bars_to_dataframe(bars)
    if len(df) < 60:
        raise AssertionError("need >= 60 bars for v1 leakage test")
    full = compute_features_v1(df)
    mid = len(df) // 2
    truncated = compute_features_v1(df.iloc[: mid + 1].copy())
    cols = list(FEATURE_NAMES_V1)
    a = full.loc[mid, cols].to_numpy(dtype=float)
    b = truncated.loc[mid, cols].to_numpy(dtype=float)
    if not np.allclose(a, b, equal_nan=True):
        raise AssertionError("look-ahead leakage detected in features_v1")


def feature_completeness_mask(feat_df: pd.DataFrame) -> pd.Series:
    cols = list(FEATURE_NAMES_V1)
    return feat_df[cols].notna().all(axis=1)
