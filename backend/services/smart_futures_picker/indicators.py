"""
Indicators for Smart Futures CMS v2: OBV slope, Wilder ATR(5/14), configurable ADX length, VWAP,
volume surge, Renko momentum, HA trend, EMA spread slope, and oscillator divergences
(divergences used for exit / alerts only — not in CMS sum).
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

from backend.services.smart_futures_config import ADX_SLOW_LENGTH, cms_weights_active

# --- OBV slope (exact user spec, linregress slope only) ---


def _linregress_slope(xs: Sequence[float], ys: Sequence[float]) -> float:
    """Least-squares slope; same as scipy.stats.linregress(...).slope."""
    n = len(xs)
    if n < 2 or len(ys) != n:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    den = sum((xs[i] - mx) ** 2 for i in range(n))
    return (num / den) if den else 0.0


def compute_obv_slope_daily(closes: Sequence[float], volumes: Sequence[float]) -> float:
    """
    closes, volumes: length 10, oldest → newest.
    Returns OBV_slope in [-1, 1] per spec.
    """
    if len(closes) != 10 or len(volumes) != 10:
        return 0.0
    obv = [0.0] * 10
    for i in range(1, 10):
        if closes[i] > closes[i - 1]:
            obv[i] = obv[i - 1] + volumes[i]
        elif closes[i] < closes[i - 1]:
            obv[i] = obv[i - 1] - volumes[i]
        else:
            obv[i] = obv[i - 1]
    avg_daily_vol = sum(volumes) / 10.0
    if avg_daily_vol <= 0:
        return 0.0
    slope7 = _linregress_slope(list(range(7)), obv[-7:])
    slope3 = _linregress_slope(list(range(3)), obv[-3:])
    raw_slope = (slope7 + slope3) / 2.0
    obv_slope = raw_slope / avg_daily_vol
    return max(-1.0, min(1.0, obv_slope))


# --- 5-min: TR, Wilder ATR(14), session VWAP ---


def true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def wilder_atr(
    highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int
) -> Optional[float]:
    """
    Wilder / RMA ATR at the latest bar. Returns None if insufficient history.
    """
    if period < 1:
        return None
    n = len(closes)
    if n < period + 1:
        return None
    trs: List[float] = []
    for i in range(1, n):
        trs.append(true_range(highs[i], lows[i], closes[i - 1]))
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / float(period)
    pm1 = float(period - 1)
    for j in range(period, len(trs)):
        atr = (atr * pm1 + trs[j]) / float(period)
    return float(atr)


def wilder_atr_14(
    highs: Sequence[float], lows: Sequence[float], closes: Sequence[float]
) -> Optional[float]:
    """
    Standard ATR(14) on last bars; returns ATR at latest bar or None if insufficient.
    """
    return wilder_atr(highs, lows, closes, 14)


def session_vwap(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], volumes: Sequence[float]) -> float:
    """Typical-price VWAP over all bars in window."""
    num = 0.0
    den = 0.0
    for i in range(len(closes)):
        tp = (highs[i] + lows[i] + closes[i]) / 3.0
        v = max(0.0, volumes[i])
        num += tp * v
        den += v
    return (num / den) if den > 0 else float(closes[-1])


def volume_surge_ratio(
    m5_volumes: Sequence[float], avg_daily_vol: float, session_elapsed_fraction: float
) -> float:
    """
    Session cumulative volume vs expected volume to this point in session.
    session_elapsed_fraction in (0, 1] — fraction of full session elapsed (e.g. minutes/375).
    """
    if avg_daily_vol <= 0:
        return 1.0
    cum = sum(max(0.0, v) for v in m5_volumes)
    exp = avg_daily_vol * max(0.05, min(1.0, session_elapsed_fraction))
    return cum / max(1e-6, exp)


# --- ADX via pandas_ta (Wilder-compatible); length from config ---


def _adx_column_name(adx_df, length: int) -> Optional[str]:
    for candidate in (f"ADX_{length}", f"ADX_{int(length)}"):
        if candidate in adx_df.columns:
            return candidate
    return next((c for c in adx_df.columns if str(c).upper().startswith("ADX")), None)


def adx_value(
    highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], length: int
) -> Optional[float]:
    try:
        import pandas as pd
        import pandas_ta as ta

        need = max(30, length + 20)
        if len(closes) < need:
            return None
        df = pd.DataFrame({"high": list(highs), "low": list(lows), "close": list(closes)})
        adx_df = ta.adx(df["high"], df["low"], df["close"], length=int(length))
        if adx_df is None or adx_df.empty:
            return None
        col = _adx_column_name(adx_df, int(length))
        if not col:
            return None
        v = adx_df[col].iloc[-1]
        try:
            if v is None or v != v:  # NaN
                return None
        except Exception:
            return None
        return float(v)
    except Exception:
        return None


def adx_14_value(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float]) -> Optional[float]:
    """Backward-compatible: ADX at slow reference length (default 14)."""
    return adx_value(highs, lows, closes, ADX_SLOW_LENGTH)


# --- Renko momentum (brick = ATR/10 on 5m closes), HA trend, div proxies ---


def renko_momentum_score(closes: Sequence[float], brick: float) -> float:
    """Price move over recent window in units of Renko brick size, clamped to [-1, 1]."""
    if brick <= 0 or len(closes) < 6:
        return 0.0
    span = min(24, len(closes))
    delta = closes[-1] - closes[-span]
    units = delta / brick
    return max(-1.0, min(1.0, units / max(4.0, float(span) / 2.0)))


def ha_trend_score(opens: List[float], highs: List[float], lows: List[float], closes: List[float]) -> float:
    """Heikin-Ashi closes: +1 / -1 / 0 from last 3 HA closes."""
    n = len(closes)
    if n < 6:
        return 0.0
    ha_o = (opens[0] + closes[0]) / 2.0
    ha_c = (opens[0] + highs[0] + lows[0] + closes[0]) / 4.0
    ha_closes: List[float] = [ha_c]
    for i in range(1, n):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        ha_c_new = (o + h + l + c) / 4.0
        ha_o = (ha_o + ha_c) / 2.0
        ha_c = ha_c_new
        ha_closes.append(ha_c)
    if len(ha_closes) < 3:
        return 0.0
    a, b, c0 = ha_closes[-3], ha_closes[-2], ha_closes[-1]
    if c0 > b > a:
        return 1.0
    if c0 < b < a:
        return -1.0
    return 0.0


def _simple_divergence(price: Sequence[float], osc: Sequence[float], lookback: int = 8) -> float:
    """+1 bullish div, -1 bearish div, 0 none (heuristic on last lookback bars)."""
    n = min(len(price), len(osc), lookback)
    if n < 5:
        return 0.0
    p = price[-n:]
    o = osc[-n:]
    p_low_i = min(range(n), key=lambda i: p[i])
    p_high_i = max(range(n), key=lambda i: p[i])
    o_low_i = min(range(n), key=lambda i: o[i])
    o_high_i = max(range(n), key=lambda i: o[i])
    # Bullish: price lower low at end vs start, osc higher low
    if p_low_i == n - 1 and p[0] > p[-1] and o[-1] > o[0]:
        return 1.0
    # Bearish: price higher high at end, osc lower high
    if p_high_i == n - 1 and p[-1] > p[0] and o[-1] < o[0]:
        return -1.0
    return 0.0


def macd_line(closes: Sequence[float], fast: int = 12, slow: int = 26, signal: int = 9) -> List[float]:
    if len(closes) < slow + signal:
        return [0.0] * len(closes)

    def ema(series: Sequence[float], span: int) -> List[float]:
        k = 2.0 / (span + 1)
        out = [series[0]]
        for i in range(1, len(series)):
            out.append(series[i] * k + out[-1] * (1 - k))
        return out

    ef = ema(list(closes), fast)
    es = ema(list(closes), slow)
    macd = [ef[i] - es[i] for i in range(len(closes))]
    return macd


def rsi_14(closes: Sequence[float]) -> List[float]:
    n = len(closes)
    if n < 15:
        return [50.0] * n
    gains = [0.0]
    losses = [0.0]
    for i in range(1, n):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    rsis = [50.0] * n
    avg_g = sum(gains[1:15]) / 14.0
    avg_l = sum(losses[1:15]) / 14.0
    rs = avg_g / avg_l if avg_l > 0 else 99.0
    rsis[14] = 100.0 - (100.0 / (1.0 + rs))
    ag, al = avg_g, avg_l
    for i in range(15, n):
        ag = (ag * 13.0 + gains[i]) / 14.0
        al = (al * 13.0 + losses[i]) / 14.0
        rs = ag / al if al > 0 else 99.0
        rsis[i] = 100.0 - (100.0 / (1.0 + rs))
    return rsis


def stoch_k(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], k: int = 14) -> List[float]:
    out = [50.0] * len(closes)
    for i in range(k - 1, len(closes)):
        hh = max(highs[i - k + 1 : i + 1])
        ll = min(lows[i - k + 1 : i + 1])
        if hh == ll:
            out[i] = 50.0
        else:
            out[i] = 100.0 * (closes[i] - ll) / (hh - ll)
    return out


def divergence_bundle(
    highs: Sequence[float], lows: Sequence[float], closes: Sequence[float]
) -> Tuple[float, float, float]:
    m = macd_line(closes)
    r = rsi_14(closes)
    s = stoch_k(highs, lows, closes)
    md = _simple_divergence(closes, m)
    rd = _simple_divergence(closes, r)
    sd = _simple_divergence(closes, s)
    return md, rd, sd


def adx_last_two(
    highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], length: int
) -> Tuple[Optional[float], Optional[float]]:
    """Latest ADX(length) and previous bar ADX, or (None, None) if unavailable."""
    try:
        import pandas as pd
        import pandas_ta as ta

        need = max(32, int(length) + 20)
        if len(closes) < need:
            return None, None
        df = pd.DataFrame({"high": list(highs), "low": list(lows), "close": list(closes)})
        adx_df = ta.adx(df["high"], df["low"], df["close"], length=int(length))
        if adx_df is None or adx_df.empty:
            return None, None
        col = _adx_column_name(adx_df, int(length))
        if not col:
            return None, None
        series = adx_df[col].dropna()
        if len(series) < 2:
            return None, None
        return float(series.iloc[-1]), float(series.iloc[-2])
    except Exception:
        return None, None


def adx_14_last_two(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float]) -> Tuple[Optional[float], Optional[float]]:
    """Backward-compatible: last two ADX values at ADX_SLOW_LENGTH (14)."""
    return adx_last_two(highs, lows, closes, ADX_SLOW_LENGTH)


def market_regime_ok(
    atr5: float,
    atr14: float,
    adx: float,
    adx_prev: Optional[float],
    *,
    adx_threshold: float,
) -> bool:
    """Layer 1: trending regime (mandatory for new CMS pipeline)."""
    if adx_prev is None:
        return False
    return atr5 > atr14 and adx > float(adx_threshold) and adx > adx_prev


def volume_surge_norm_01(volume_surge_ratio_val: float) -> float:
    """Map session volume surge ratio to [0, 1] (linear past entry floor ~1.5)."""
    return max(0.0, min(1.0, (float(volume_surge_ratio_val) - 1.5) / 1.5))


def vwap_deviation_atr_norm(close: float, vwap: float, atr14: float) -> float:
    """(close − VWAP) / ATR(14), clipped to [-2, +2]."""
    if atr14 <= 0:
        return 0.0
    return max(-2.0, min(2.0, (float(close) - float(vwap)) / float(atr14)))


def ema_spread_series(closes: Sequence[float], fast: int = 5, slow: int = 13) -> List[float]:
    """EMA(fast) − EMA(slow) at each bar (simple sequential EMA on full series)."""
    n = len(closes)
    out = [0.0] * n
    if n < slow + 2:
        return out

    def ema_series(series: Sequence[float], span: int) -> List[float]:
        k = 2.0 / (float(span) + 1.0)
        acc = [float(series[0])]
        for i in range(1, len(series)):
            acc.append(float(series[i]) * k + acc[-1] * (1.0 - k))
        return acc

    ef = ema_series(closes, fast)
    es = ema_series(closes, slow)
    for i in range(n):
        out[i] = ef[i] - es[i]
    return out


def ema_slope_norm_m5(closes: Sequence[float], norm_window: int = 50) -> float:
    """
    EMA_spread = EMA(5) − EMA(13); normalize latest spread to [-1, 1] using rolling min/max
    over the last ``norm_window`` spreads (same bar count as closes).
    """
    if len(closes) < 20:
        return 0.0
    spreads = ema_spread_series(closes, 5, 13)
    w = min(norm_window, len(spreads))
    seg = spreads[-w:]
    lo = min(seg)
    hi = max(seg)
    cur = seg[-1]
    if hi - lo <= 1e-12:
        return 0.0
    u = 2.0 * (cur - lo) / (hi - lo) - 1.0
    return max(-1.0, min(1.0, float(u)))


def breakout_volume_spike(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], vs: float) -> bool:
    """Optional +0.1 CMS boost: breakout of recent range with elevated surge."""
    if len(closes) < 10 or float(vs) < 2.0:
        return False
    prior_high = max(highs[-10:-1])
    prior_low = min(lows[-10:-1])
    lc = float(closes[-1])
    return lc > prior_high or lc < prior_low


def compute_cms_score_weighted(
    obv_slope_norm: float,
    volume_surge_norm: float,
    vwap_deviation: float,
    renko_momentum_norm: float,
    ha_trend_norm: float,
    ema_slope_norm: float,
    breakout_boost: bool,
    *,
    oi_score_norm: float = 0.0,
    weights: Optional[Dict[str, float]] = None,
) -> float:
    """
    Layer 2: weighted normalized CMS (no divergences). Weights from ``cms_weights_active()`` by default.
    """
    w = weights if weights is not None else cms_weights_active()
    s = (
        float(w.get("obv_slope", 0.0)) * float(obv_slope_norm)
        + float(w.get("volume_surge", 0.0)) * float(volume_surge_norm)
        + float(w.get("vwap_dev", 0.0)) * float(vwap_deviation)
        + float(w.get("renko_mom", 0.0)) * float(renko_momentum_norm)
        + float(w.get("ha_trend", 0.0)) * float(ha_trend_norm)
        + float(w.get("ema_slope", 0.0)) * float(ema_slope_norm)
    )
    oi_w = float(w.get("oi_score", 0.0))
    if oi_w > 0.0:
        s += oi_w * float(oi_score_norm)
    if breakout_boost:
        s += 0.1
    return float(s)


def compute_cms_final_multiplier(cms_score: float, atr5: float, atr14: float, adx: float) -> float:
    """Layer 2→3: volatility and trend weighting on the combined score."""
    if atr14 <= 0 or adx <= 0:
        return 0.0
    return float(cms_score) * (float(atr5) / float(atr14)) * (float(adx) / 25.0)


def compute_cms_core(
    obv_slope: float,
    volume_surge: float,
    close_vwap_atr: float,
    renko_momentum: float,
    ha_trend: float,
    ema_slope: float,
    *,
    breakout_boost: bool = False,
    oi_score_norm: float = 0.0,
    weights: Optional[Dict[str, float]] = None,
) -> float:
    """
    Normalized CMS **score** (Layer 2). Divergences are not inputs — use ``divergence_bundle`` only in exit logic.

    ``close_vwap_atr`` may be a wider clip from the caller; VWAP term is re-clipped to [-2, 2] internally
    when used as deviation — callers should pass (close−VWAP)/ATR.
    """
    obv_n = max(-1.0, min(1.0, float(obv_slope)))
    vs_n = volume_surge_norm_01(volume_surge)
    vd = max(-2.0, min(2.0, float(close_vwap_atr)))
    rm_n = max(-1.0, min(1.0, float(renko_momentum)))
    ha_n = max(-1.0, min(1.0, float(ha_trend)))
    ema_n = max(-1.0, min(1.0, float(ema_slope)))
    return compute_cms_score_weighted(
        obv_n,
        vs_n,
        vd,
        rm_n,
        ha_n,
        ema_n,
        breakout_boost,
        oi_score_norm=oi_score_norm,
        weights=weights,
    )
