"""Pure Fear & Greed math: indicators, percentile normalize, zones, combine.

No I/O. Safe to unit-test with synthetic candle series.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.services.momentx.indicators import sma, wilder_atr, wilder_rsi

# Equal defaults; missing indicators are dropped and weights rebalanced.
DEFAULT_WEIGHTS: Dict[str, float] = {
    "momentum": 1.0,
    "volatility": 1.0,
    "volume": 1.0,
    "open_interest": 1.0,
    "futures_premium": 1.0,
    "relative_strength": 1.0,
}

ZONE_LABELS = (
    (0, 24, "Extreme Fear"),
    (25, 49, "Fear"),
    (50, 74, "Greed"),
    (75, 100, "Extreme Greed"),
)

MA_PERIOD = 50
RSI_PERIOD = 14
ATR_PERIOD = 14
VOLUME_AVG_PERIOD = 20
RV_WINDOW = 20
RS_LOOKBACK = 20
TRADING_DAYS_PER_YEAR = 252


def map_zone(score: int) -> str:
    """Map a clamped 0–100 integer score to a zone label."""
    s = int(score)
    if s <= 24:
        return "Extreme Fear"
    if s <= 49:
        return "Fear"
    if s <= 74:
        return "Greed"
    return "Extreme Greed"


def percentile_rank(value: float, history: Sequence[float]) -> Optional[float]:
    """Percentile rank of ``value`` within ``history`` (0–100).

    Uses the fraction of historical points strictly less than value, plus half
    the ties — a standard mid-rank percentile.
    """
    if not history:
        return None
    vals = [float(x) for x in history]
    n = len(vals)
    if n == 0:
        return None
    v = float(value)
    below = sum(1 for x in vals if x < v)
    equal = sum(1 for x in vals if x == v)
    return 100.0 * (below + 0.5 * equal) / float(n)


def normalize_series(
    series: Sequence[float],
    *,
    invert: bool = False,
) -> Optional[float]:
    """Percentile-rank the last point of ``series`` over the full series.

    When ``invert`` is True (fear-leaning metrics such as volatility), the
    score is flipped so 0 = extreme fear and 100 = extreme greed.
    """
    if not series:
        return None
    vals = [float(x) for x in series]
    rank = percentile_rank(vals[-1], vals)
    if rank is None:
        return None
    if invert:
        rank = 100.0 - rank
    return max(0.0, min(100.0, rank))


def rebalance_weights(
    available: Sequence[str],
    weights: Optional[Dict[str, float]] = None,
) -> Dict[str, float]:
    """Keep only available keys and renormalize so weights sum to 1.0."""
    base = dict(weights or DEFAULT_WEIGHTS)
    picked: Dict[str, float] = {}
    for key in available:
        w = float(base.get(key, 0.0))
        if w > 0:
            picked[key] = w
    total = sum(picked.values())
    if total <= 0:
        n = len(available)
        if n == 0:
            return {}
        eq = 1.0 / float(n)
        return {k: eq for k in available}
    return {k: v / total for k, v in picked.items()}


def combine_scores(
    indicator_scores: Dict[str, float],
    weights: Optional[Dict[str, float]] = None,
) -> Tuple[int, Dict[str, float]]:
    """Weighted average → clamped integer 0–100, plus the effective weights used."""
    avail = [k for k, v in indicator_scores.items() if v is not None]
    effective = rebalance_weights(avail, weights)
    if not effective:
        return 50, {}
    total = 0.0
    for k, w in effective.items():
        total += float(indicator_scores[k]) * w
    score = int(round(max(0.0, min(100.0, total))))
    return score, effective


def interpret_oi(price_change: float, oi_change: float) -> Dict[str, Any]:
    """Map price Δ + OI Δ into buildup/covering semantics and a greed lean score.

    Continuous raw score in [-1, 1]:
      +1 long buildup, +0.4 short covering, -0.4 long unwinding, -1 short buildup.
    """
    px_up = price_change > 0
    px_down = price_change < 0
    oi_up = oi_change > 0
    oi_down = oi_change < 0

    if px_up and oi_up:
        label = "long_buildup"
        note = "Price up + OI up — long buildup (greed)"
        raw = 1.0
    elif px_down and oi_up:
        label = "short_buildup"
        note = "Price down + OI up — short buildup (fear)"
        raw = -1.0
    elif px_up and oi_down:
        label = "short_covering"
        note = "Price up + OI down — short covering (mild greed)"
        raw = 0.4
    elif px_down and oi_down:
        label = "long_unwinding"
        note = "Price down + OI down — long unwinding (mild fear)"
        raw = -0.4
    else:
        label = "neutral"
        note = "No clear OI/price direction"
        raw = 0.0

    # Map [-1, 1] → [0, 100] for a single-observation normalized lean.
    normalized = round(50.0 + 50.0 * raw, 2)
    return {
        "label": label,
        "note": note,
        "raw": raw,
        "normalized": max(0.0, min(100.0, normalized)),
    }


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except (TypeError, ValueError):
        return None


def _closes(candles: Sequence[Dict[str, Any]]) -> List[float]:
    out: List[float] = []
    for c in candles:
        v = _safe_float(c.get("close"))
        if v is not None and v > 0:
            out.append(v)
    return out


def _series_field(candles: Sequence[Dict[str, Any]], field: str) -> List[float]:
    out: List[float] = []
    for c in candles:
        v = _safe_float(c.get(field))
        if v is None:
            out.append(0.0)
        else:
            out.append(v)
    return out


def _realized_vol_pct(closes: Sequence[float], window: int = RV_WINDOW) -> Optional[float]:
    """Annualized realized vol (%) from trailing log returns."""
    import math

    if len(closes) < window + 1:
        return None
    rets: List[float] = []
    for i in range(len(closes) - window, len(closes)):
        a, b = float(closes[i - 1]), float(closes[i])
        if a <= 0 or b <= 0:
            continue
        rets.append(math.log(b / a))
    if len(rets) < max(5, window // 2):
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, len(rets) - 1)
    return (var ** 0.5) * (TRADING_DAYS_PER_YEAR ** 0.5) * 100.0


def _momentum_raw_series(closes: Sequence[float]) -> List[float]:
    """Blend of % distance from MA50 and RSI, per bar where both exist."""
    out: List[float] = []
    for i in range(len(closes)):
        window = closes[: i + 1]
        if len(window) < MA_PERIOD:
            continue
        ma = sma(window, MA_PERIOD)
        rsi = wilder_rsi(window, RSI_PERIOD)
        if ma is None or ma <= 0 or rsi is None:
            continue
        dist_pct = (window[-1] - ma) / ma * 100.0
        # Map dist roughly into RSI-like scale then average.
        dist_as_0_100 = max(0.0, min(100.0, 50.0 + dist_pct * 5.0))
        out.append(0.5 * dist_as_0_100 + 0.5 * float(rsi))
    return out


def _atr_pct_series(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
) -> List[float]:
    out: List[float] = []
    for i in range(len(closes)):
        h = highs[: i + 1]
        l = lows[: i + 1]
        c = closes[: i + 1]
        atr = wilder_atr(h, l, c, ATR_PERIOD)
        if atr is None or c[-1] <= 0:
            continue
        out.append(100.0 * float(atr) / float(c[-1]))
    return out


def _volume_signed_series(
    closes: Sequence[float],
    volumes: Sequence[float],
) -> List[float]:
    out: List[float] = []
    for i in range(len(closes)):
        if i < VOLUME_AVG_PERIOD:
            continue
        avg = sum(volumes[i - VOLUME_AVG_PERIOD + 1 : i + 1]) / float(VOLUME_AVG_PERIOD)
        if avg <= 0:
            continue
        ratio = volumes[i] / avg
        px_chg = closes[i] - closes[i - 1] if i > 0 else 0.0
        direction = 1.0 if px_chg > 0 else (-1.0 if px_chg < 0 else 0.0)
        out.append(ratio * direction)
    return out


def _oi_raw_series(
    closes: Sequence[float],
    ois: Sequence[float],
) -> List[float]:
    out: List[float] = []
    for i in range(1, len(closes)):
        if ois[i] <= 0 and ois[i - 1] <= 0:
            continue
        px = closes[i] - closes[i - 1]
        oi = ois[i] - ois[i - 1]
        out.append(float(interpret_oi(px, oi)["raw"]))
    return out


def _premium_annualized_series(
    spot_closes: Sequence[float],
    fut_closes: Sequence[float],
    days_to_expiry: float = 30.0,
) -> List[float]:
    """Annualized futures premium/discount (%) vs spot."""
    n = min(len(spot_closes), len(fut_closes))
    if n == 0:
        return []
    out: List[float] = []
    dte = max(1.0, float(days_to_expiry))
    for i in range(n):
        s, f = float(spot_closes[i]), float(fut_closes[i])
        if s <= 0 or f <= 0:
            continue
        prem = (f - s) / s
        ann = prem * (TRADING_DAYS_PER_YEAR / dte) * 100.0
        out.append(ann)
    return out


def _rs_series(
    instrument_closes: Sequence[float],
    benchmark_closes: Sequence[float],
    lookback: int = RS_LOOKBACK,
) -> List[float]:
    n = min(len(instrument_closes), len(benchmark_closes))
    out: List[float] = []
    for i in range(lookback, n):
        i0 = instrument_closes[i - lookback]
        b0 = benchmark_closes[i - lookback]
        i1 = instrument_closes[i]
        b1 = benchmark_closes[i]
        if i0 <= 0 or b0 <= 0:
            continue
        inst_ret = (i1 - i0) / i0 * 100.0
        bench_ret = (b1 - b0) / b0 * 100.0
        out.append(inst_ret - bench_ret)
    return out


def compute_fear_greed_from_candles(
    candles: Sequence[Dict[str, Any]],
    *,
    spot_candles: Optional[Sequence[Dict[str, Any]]] = None,
    fut_candles: Optional[Sequence[Dict[str, Any]]] = None,
    benchmark_candles: Optional[Sequence[Dict[str, Any]]] = None,
    weights: Optional[Dict[str, float]] = None,
    days_to_expiry: float = 30.0,
) -> Dict[str, Any]:
    """Compute Fear & Greed from candle dicts (close/high/low/volume/oi).

    Returns structured payload with score, zone, per-indicator detail, skipped.
    """
    indicators: Dict[str, Dict[str, Any]] = {}
    skipped: Dict[str, str] = {}
    scores: Dict[str, float] = {}

    closes = _closes(candles)
    highs = _series_field(candles, "high") if candles else []
    lows = _series_field(candles, "low") if candles else []
    volumes = _series_field(candles, "volume") if candles else []
    ois = _series_field(candles, "oi") if candles else []

    # Align OHLC lengths to closes when some closes were dropped — rebuild from
    # candles that have valid close only.
    aligned: List[Dict[str, Any]] = []
    for c in candles or []:
        if _safe_float(c.get("close")) and float(c["close"]) > 0:
            aligned.append(c)
    if aligned:
        highs = [_safe_float(c.get("high")) or float(c["close"]) for c in aligned]
        lows = [_safe_float(c.get("low")) or float(c["close"]) for c in aligned]
        volumes = [_safe_float(c.get("volume")) or 0.0 for c in aligned]
        ois = [_safe_float(c.get("oi")) or 0.0 for c in aligned]
        closes = [float(c["close"]) for c in aligned]

    # --- momentum ---
    # Raw blend is already on a 0–100 absolute scale (RSI + MA distance).
    # Prefer absolute level so a sustained uptrend does not collapse to ~50
    # under percentile-rank saturation; still publish history percentile as context.
    mom_series = _momentum_raw_series(closes)
    if len(mom_series) >= 10:
        mom_abs = max(0.0, min(100.0, float(mom_series[-1])))
        mom_pct = normalize_series(mom_series, invert=False)
        mom_norm = 0.7 * mom_abs + 0.3 * (mom_pct if mom_pct is not None else mom_abs)
        last_rsi = wilder_rsi(closes, RSI_PERIOD)
        ma50 = sma(closes, MA_PERIOD)
        dist = None
        if ma50 and ma50 > 0:
            dist = (closes[-1] - ma50) / ma50 * 100.0
        indicators["momentum"] = {
            "raw": round(mom_series[-1], 4),
            "normalized": round(mom_norm, 2),
            "note": f"MA50 dist {dist:.2f}%, RSI {last_rsi:.1f}" if dist is not None and last_rsi is not None else "Price vs MA50 + RSI",
        }
        scores["momentum"] = mom_norm
    else:
        skipped["momentum"] = "insufficient history for MA50/RSI"

    # --- volatility (invert: high vol = fear) ---
    atr_series = _atr_pct_series(highs, lows, closes) if highs and lows else []
    if len(atr_series) < 10:
        # Fallback: realized vol series built expanding
        rv_pts: List[float] = []
        for i in range(RV_WINDOW + 1, len(closes) + 1):
            rv = _realized_vol_pct(closes[:i], RV_WINDOW)
            if rv is not None:
                rv_pts.append(rv)
        atr_series = rv_pts
    if len(atr_series) >= 10:
        vol_norm = normalize_series(atr_series, invert=True)
        indicators["volatility"] = {
            "raw": round(atr_series[-1], 4),
            "normalized": round(vol_norm, 2) if vol_norm is not None else None,
            "note": "ATR%/realized vol vs history (inverted — high vol = fear)",
        }
        if vol_norm is not None:
            scores["volatility"] = vol_norm
        else:
            skipped["volatility"] = "normalization failed"
    else:
        skipped["volatility"] = "insufficient volatility history"

    # --- volume ---
    if any(v > 0 for v in volumes):
        vol_signed = _volume_signed_series(closes, volumes)
        if len(vol_signed) >= 10:
            v_norm = normalize_series(vol_signed, invert=False)
            indicators["volume"] = {
                "raw": round(vol_signed[-1], 4),
                "normalized": round(v_norm, 2) if v_norm is not None else None,
                "note": "Volume vs 20d avg × price direction",
            }
            if v_norm is not None:
                scores["volume"] = v_norm
            else:
                skipped["volume"] = "normalization failed"
        else:
            skipped["volume"] = "insufficient volume history"
    else:
        skipped["volume"] = "volume not available"

    # --- open interest ---
    # Latest OI interpretation is absolute (four-way map → 0–100). Blend with
    # history percentile so persistent regimes still move away from 50.
    if any(o > 0 for o in ois):
        oi_series = _oi_raw_series(closes, ois)
        if len(oi_series) >= 5:
            last = interpret_oi(
                closes[-1] - closes[-2] if len(closes) >= 2 else 0.0,
                ois[-1] - ois[-2] if len(ois) >= 2 else 0.0,
            )
            oi_pct = normalize_series(oi_series, invert=False)
            oi_norm = 0.65 * float(last["normalized"]) + 0.35 * (
                oi_pct if oi_pct is not None else float(last["normalized"])
            )
            indicators["open_interest"] = {
                "raw": round(oi_series[-1], 4),
                "normalized": round(oi_norm, 2),
                "note": last["note"],
                "oi_label": last["label"],
            }
            scores["open_interest"] = oi_norm
        else:
            skipped["open_interest"] = "insufficient OI history"
    else:
        skipped["open_interest"] = "open interest not available"

    # --- futures premium ---
    spot_src = spot_candles if spot_candles is not None else None
    fut_src = fut_candles if fut_candles is not None else None
    if spot_src and fut_src:
        spot_c = _closes(spot_src)
        fut_c = _closes(fut_src)
        # Align by taking the overlapping tail lengths
        n = min(len(spot_c), len(fut_c))
        prem_series = _premium_annualized_series(spot_c[-n:], fut_c[-n:], days_to_expiry)
        if len(prem_series) >= 10:
            p_norm = normalize_series(prem_series, invert=False)
            indicators["futures_premium"] = {
                "raw": round(prem_series[-1], 4),
                "normalized": round(p_norm, 2) if p_norm is not None else None,
                "note": "Annualized futures premium/discount vs history",
            }
            if p_norm is not None:
                scores["futures_premium"] = p_norm
            else:
                skipped["futures_premium"] = "normalization failed"
        else:
            skipped["futures_premium"] = "insufficient premium history"
    else:
        skipped["futures_premium"] = "spot or futures unavailable"

    # --- relative strength ---
    if benchmark_candles:
        bench_c = _closes(benchmark_candles)
        n = min(len(closes), len(bench_c))
        rs = _rs_series(closes[-n:], bench_c[-n:], RS_LOOKBACK)
        if len(rs) >= 10:
            rs_norm = normalize_series(rs, invert=False)
            indicators["relative_strength"] = {
                "raw": round(rs[-1], 4),
                "normalized": round(rs_norm, 2) if rs_norm is not None else None,
                "note": f"{RS_LOOKBACK}d relative return vs benchmark",
            }
            if rs_norm is not None:
                scores["relative_strength"] = rs_norm
            else:
                skipped["relative_strength"] = "normalization failed"
        else:
            skipped["relative_strength"] = "insufficient relative-strength history"
    else:
        skipped["relative_strength"] = "benchmark unavailable"

    # PCR / IV skew — not wired (option chain heavy); always skip + redistribute.
    skipped["put_call_ratio"] = "option PCR/IV skew not enabled; weight redistributed"

    score, effective_weights = combine_scores(scores, weights)
    zone = map_zone(score)

    # Attach weights onto indicator rows
    for key, row in indicators.items():
        row["weight"] = round(effective_weights.get(key, 0.0), 4)

    return {
        "score": score,
        "zone": zone,
        "indicators": indicators,
        "skipped": skipped,
        "weights_used": {k: round(v, 4) for k, v in effective_weights.items()},
        "bars_used": len(closes),
    }
