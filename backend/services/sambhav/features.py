"""Feature engine for 10m bars — strict causal (no look-ahead)."""

from __future__ import annotations

import math
from typing import Any, Dict, List, Sequence

import numpy as np
import pandas as pd

from backend.services.sambhav.candles import to_ist
from backend.services.sambhav.config import FEATURE_NAMES, SESSION_END, SESSION_START, TF_MINUTES


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False, min_periods=span).mean()


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def _macd_hist(close: pd.Series) -> pd.Series:
    ema12 = _ema(close, 12)
    ema26 = _ema(close, 26)
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False, min_periods=9).mean()
    return macd - signal


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


def bars_to_dataframe(bars: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame(
            columns=["candle_start", "open", "high", "low", "close", "volume"]
        )
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
    df = pd.DataFrame(rows).sort_values("candle_start").reset_index(drop=True)
    return df


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute FEATURE_NAMES columns. All shifts are past-only (positive shift).
    Rows with insufficient history remain NaN and should be dropped before train.
    """
    if df is None or df.empty:
        out = pd.DataFrame(columns=["candle_start", *FEATURE_NAMES])
        return out

    d = df.sort_values("candle_start").reset_index(drop=True).copy()
    o, h, l, c, v = d["open"], d["high"], d["low"], d["close"], d["volume"]
    eps = 1e-12

    ret_1 = c.pct_change(1)
    ret_3 = c.pct_change(3)
    ret_6 = c.pct_change(6)

    log_range = np.log((h + eps) / (l + eps))
    body = (c - o).abs()
    rng = (h - l).replace(0, np.nan)
    body_pct = body / rng
    upper_wick_pct = (h - pd.concat([o, c], axis=1).max(axis=1)) / rng
    lower_wick_pct = (pd.concat([o, c], axis=1).min(axis=1) - l) / rng
    close_loc = (c - l) / rng
    gap_from_prev = (o - c.shift(1)) / (c.shift(1) + eps)

    ema9 = _ema(c, 9)
    ema21 = _ema(c, 21)
    ema9_slope = ema9.pct_change(1)
    ema21_slope = ema21.pct_change(1)
    ema9_vs_ema21 = (ema9 - ema21) / (c + eps)
    close_vs_ema21 = (c - ema21) / (c + eps)
    ema_stack = ((ema9 > ema21) & (c > ema9)).astype(float) - (
        (ema9 < ema21) & (c < ema9)
    ).astype(float)

    rsi14 = _rsi(c, 14)
    roc3 = c.pct_change(3)
    roc6 = c.pct_change(6)
    macd_hist = _macd_hist(c)

    atr14 = _atr(h, l, c, 14)
    atr14_pct = atr14 / (c + eps)
    log_ret = np.log((c + eps) / (c.shift(1) + eps))
    realized_vol6 = log_ret.rolling(6, min_periods=6).std()
    high_low_pct = (h - l) / (c + eps)
    range_expand = high_low_pct / (high_low_pct.rolling(6, min_periods=6).mean() + eps)

    vol_mean20 = v.rolling(20, min_periods=10).mean()
    vol_std20 = v.rolling(20, min_periods=10).std()
    vol_z20 = (v - vol_mean20) / (vol_std20 + eps)
    vol_ratio5 = v / (v.rolling(5, min_periods=3).mean() + eps)
    dollar_vol_proxy = np.log1p(v * c)

    # Time-of-day (known at bar close; no future info)
    mins_from_open = d["candle_start"].apply(
        lambda ts: (ts.hour * 60 + ts.minute) - (SESSION_START.hour * 60 + SESSION_START.minute)
    )
    session_len = (SESSION_END.hour * 60 + SESSION_END.minute) - (
        SESSION_START.hour * 60 + SESSION_START.minute
    )
    mins_to_close = session_len - mins_from_open - TF_MINUTES
    # Cyclic encoding over session length
    ang = 2.0 * math.pi * (mins_from_open.astype(float) / max(session_len, 1))
    tod_sin = np.sin(ang)
    tod_cos = np.cos(ang)
    is_open_bucket = (mins_from_open <= 20).astype(float)
    is_close_bucket = (mins_to_close <= 30).astype(float)

    feat = pd.DataFrame(
        {
            "candle_start": d["candle_start"],
            "ret_1": ret_1,
            "ret_3": ret_3,
            "ret_6": ret_6,
            "log_range": log_range,
            "body_pct": body_pct,
            "upper_wick_pct": upper_wick_pct,
            "lower_wick_pct": lower_wick_pct,
            "close_loc": close_loc,
            "gap_from_prev": gap_from_prev,
            "ema9_slope": ema9_slope,
            "ema21_slope": ema21_slope,
            "ema9_vs_ema21": ema9_vs_ema21,
            "close_vs_ema21": close_vs_ema21,
            "ema_stack": ema_stack,
            "rsi14": rsi14,
            "roc3": roc3,
            "roc6": roc6,
            "macd_hist": macd_hist,
            "atr14_pct": atr14_pct,
            "realized_vol6": realized_vol6,
            "high_low_pct": high_low_pct,
            "range_expand": range_expand,
            "vol_z20": vol_z20,
            "vol_ratio5": vol_ratio5,
            "dollar_vol_proxy": dollar_vol_proxy,
            "tod_sin": tod_sin,
            "tod_cos": tod_cos,
            "mins_from_open": mins_from_open.astype(float),
            "mins_to_close": mins_to_close.astype(float),
            "is_open_bucket": is_open_bucket,
            "is_close_bucket": is_close_bucket,
        }
    )
    return feat


def feature_matrix(
    feat_df: pd.DataFrame,
    *,
    dropna: bool = True,
) -> tuple[pd.DataFrame, np.ndarray]:
    """Return (aligned frame with candle_start, X ndarray)."""
    cols = list(FEATURE_NAMES)
    frame = feat_df[["candle_start", *cols]].copy()
    if dropna:
        frame = frame.dropna(subset=cols).reset_index(drop=True)
    X = frame[cols].to_numpy(dtype=float)
    return frame, X


def assert_no_lookahead_features(bars: Sequence[Dict[str, Any]]) -> None:
    """
    Leakage test helper: features at index i must be unchanged when future bars
    after i are truncated.
    """
    df = bars_to_dataframe(bars)
    if len(df) < 40:
        raise AssertionError("need >= 40 bars for leakage test")
    full = compute_features(df)
    mid = len(df) // 2
    truncated = compute_features(df.iloc[: mid + 1].copy())
    cols = list(FEATURE_NAMES)
    a = full.loc[mid, cols].to_numpy(dtype=float)
    b = truncated.loc[mid, cols].to_numpy(dtype=float)
    if not np.allclose(a, b, equal_nan=True):
        raise AssertionError("look-ahead leakage detected in features")
