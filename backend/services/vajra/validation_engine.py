"""
Automated discretionary validation checklist (5m structure + market context).
Returns pass / warn / fail with confidence, metrics, and tooltips — no trade blocking.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.services.vajra.engine import RSI_LEN, _rsi_wilder
from backend.services.vajra.indicators import cumulative_vwap, ema_series, wma_series
from backend.services.vajra.transition import (
    EMA_EXEC_LEN,
    RSI_EMA_LEN,
    RSI_WMA_LEN,
    compute_extension_risk,
    compute_pullback_quality,
)

EvalStatus = str  # "pass" | "warn" | "fail"

STRUCTURE_KEYS = (
    "vwap_reclaimed",
    "ema_reclaimed",
    "hilega_milega",
    "pullback_shallow",
    "no_vertical_exhaustion",
    "candle_spread_healthy",
    "not_into_major_level",
    "reclaim_candle_strong",
)
MARKET_KEYS = (
    "market_structure_supportive",
    "sector_not_conflicting",
    "volume_acceptable",
    "not_extended_vwap",
)

LABELS: Dict[str, str] = {
    "vwap_reclaimed": "VWAP reclaimed",
    "ema_reclaimed": "EMA reclaimed",
    "hilega_milega": "Hilega-Milega forming",
    "pullback_shallow": "Pullback shallow",
    "no_vertical_exhaustion": "No vertical exhaustion",
    "candle_spread_healthy": "Candle spread healthy",
    "not_into_major_level": "Not entering into major resistance/support",
    "reclaim_candle_strong": "Reclaim candle closed strong",
    "market_structure_supportive": "Market structure supportive",
    "sector_not_conflicting": "Sector not conflicting",
    "volume_acceptable": "Volume acceptable",
    "not_extended_vwap": "Not extended from VWAP",
}

TOOLTIPS: Dict[str, str] = {
    "vwap_reclaimed": "Price reclaimed VWAP with acceptance.",
    "ema_reclaimed": "Price reclaimed EMA(5) with recent acceptance.",
    "hilega_milega": "RSI momentum structure (Hilega-Milega) aligns with direction.",
    "pullback_shallow": "Pullback remains controlled and structurally healthy.",
    "no_vertical_exhaustion": "Move may already be extended/exhausted.",
    "candle_spread_healthy": "Reclaim candle shows healthy directional conviction.",
    "not_into_major_level": "Insufficient upside room before nearby resistance.",
    "reclaim_candle_strong": "Reclaim candle showed strong acceptance.",
    "market_structure_supportive": "Broader market structure alignment.",
    "sector_not_conflicting": "Sector structure does not oppose trade direction.",
    "volume_acceptable": "Move supported by acceptable participation.",
    "not_extended_vwap": "Entry not excessively stretched from VWAP.",
}


def _item(
    key: str,
    section: str,
    status: EvalStatus,
    confidence: float,
    metric: str,
    *,
    tooltip: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    conf = max(0.0, min(100.0, float(confidence)))
    row: Dict[str, Any] = {
        "key": key,
        "label": LABELS.get(key, key),
        "section": section,
        "status": status,
        "confidence": round(conf, 0),
        "metric": metric,
        "tooltip": tooltip or TOOLTIPS.get(key, ""),
        "passed": status == "pass",
    }
    if extra:
        row.update(extra)
    return row


def extension_risk_level(score: float) -> str:
    if score < 40:
        return "LOW"
    if score < 65:
        return "MEDIUM"
    return "HIGH"


def _body_ratio(o: float, h: float, l: float, c: float) -> float:
    rng = h - l
    if rng <= 0:
        return 0.0
    return abs(c - o) / rng


def _close_strength(h: float, l: float, c: float, bull: bool) -> float:
    rng = h - l
    if rng <= 0:
        return 0.5
    if bull:
        return (c - l) / rng
    return (h - c) / rng


def _reclaimed_recently(
    closes: Sequence[float],
    level_series: Sequence[float],
    i: int,
    *,
    bull: bool,
    lookback: int = 3,
    near_pct: float = 0.0015,
) -> Tuple[bool, bool]:
    """(reclaimed_now, recent_reclaim)"""
    close = closes[i]
    lvl = level_series[i]
    if bull:
        now = close > lvl
        if not now:
            return False, False
        for j in range(max(0, i - lookback), i):
            prev_c, prev_l = closes[j], level_series[j]
            if prev_c <= prev_l * (1 + near_pct):
                return True, True
        return True, closes[i - 1] <= level_series[i - 1] * (1 + near_pct) if i > 0 else True
    now = close < lvl
    if not now:
        return False, False
    for j in range(max(0, i - lookback), i):
        prev_c, prev_l = closes[j], level_series[j]
        if prev_c >= prev_l * (1 - near_pct):
            return True, True
    return True, closes[i - 1] >= level_series[i - 1] * (1 - near_pct) if i > 0 else True


def _eval_vwap(
    bull: bool,
    close: float,
    vwap: float,
    closes: Sequence[float],
    vwap_s: Sequence[float],
    i: int,
) -> Dict[str, Any]:
    now, recent = _reclaimed_recently(closes, vwap_s, i, bull=bull)
    dist_pct = abs(close - vwap) / close * 100 if close else 99.0
    if bull:
        if now and recent:
            status, conf = "pass", 88 - min(20, dist_pct * 8)
            metric = f"{dist_pct:.2f}% above VWAP"
        elif now:
            status, conf = "warn", 62
            metric = f"Above VWAP; weak reclaim ({dist_pct:.2f}%)"
        else:
            status, conf = "fail", 25
            metric = f"Below VWAP ({dist_pct:.2f}%)"
    else:
        if now and recent:
            status, conf = "pass", 88 - min(20, dist_pct * 8)
            metric = f"{dist_pct:.2f}% below VWAP"
        elif now:
            status, conf = "warn", 62
            metric = f"Below VWAP; weak reclaim"
        else:
            status, conf = "fail", 25
            metric = f"Above VWAP ({dist_pct:.2f}%)"
    return _item("vwap_reclaimed", "structure", status, conf, metric)


def _eval_ema(
    bull: bool,
    close: float,
    ema5: Sequence[float],
    opens: Sequence[float],
    closes: Sequence[float],
    i: int,
) -> Dict[str, Any]:
    ema = ema5[i]
    now, recent = _reclaimed_recently(closes, ema5, i, bull=bull, lookback=3)
    body = abs(closes[i] - opens[i])
    decisive = body > abs(closes[i] - ema) * 0.15 if ema else body > 0
    if bull:
        if now and recent and decisive:
            status, conf = "pass", 85
            metric = "EMA(5) reclaimed (decisive)"
        elif now and recent:
            status, conf = "warn", 68
            metric = "EMA(5) reclaimed (marginal)"
        elif now:
            status, conf = "warn", 55
            metric = "Above EMA; no recent cross"
        else:
            status, conf = "fail", 22
            metric = "Below EMA(5)"
    else:
        if now and recent and decisive:
            status, conf = "pass", 85
            metric = "EMA(5) reclaimed (decisive)"
        elif now and recent:
            status, conf = "warn", 68
            metric = "EMA(5) reclaimed (marginal)"
        elif now:
            status, conf = "warn", 55
            metric = "Below EMA; no recent cross"
        else:
            status, conf = "fail", 22
            metric = "Above EMA(5)"
    return _item("ema_reclaimed", "structure", status, conf, metric)


def _eval_hilega(
    bull: bool,
    rsi: float,
    rsi_ema: float,
    rsi_wma: Optional[float],
    rsi_series: Sequence[Optional[float]],
    i: int,
) -> Dict[str, Any]:
    prev_rsi = rsi_series[i - 1] if i > 0 else rsi
    slope_up = rsi > (prev_rsi or rsi)
    slope_down = rsi < (prev_rsi or rsi)
    if bull:
        core = rsi > 50 and rsi_ema > 50
        stack = rsi_wma is None or rsi_ema > (rsi_wma or 0)
        slopes = slope_up
        if core and stack and slopes:
            status, conf = "pass", min(95, 70 + (rsi - 50) * 0.8)
            metric = f"RSI {rsi:.0f} · EMA(RSI) {rsi_ema:.0f}"
        elif core:
            status, conf = "warn", 58
            metric = f"RSI {rsi:.0f} — slopes/stack weak"
        else:
            status, conf = "fail", 30
            metric = f"RSI {rsi:.0f} (need >50)"
    else:
        core = rsi < 50 and rsi_ema < 50
        stack = rsi_wma is None or rsi_ema < (rsi_wma or 100)
        slopes = slope_down
        if core and stack and slopes:
            status, conf = "pass", min(95, 70 + (50 - rsi) * 0.8)
            metric = f"RSI {rsi:.0f} · EMA(RSI) {rsi_ema:.0f}"
        elif core:
            status, conf = "warn", 58
            metric = f"RSI {rsi:.0f} — slopes/stack weak"
        else:
            status, conf = "fail", 30
            metric = f"RSI {rsi:.0f} (need <50)"
    return _item("hilega_milega", "structure", status, conf, metric)


def _eval_pullback(
    bull: bool,
    pullback_depth: float,
    pb_q: float,
) -> Dict[str, Any]:
    pct = pullback_depth * 100
    if pullback_depth < 0.4:
        status, conf = "pass", min(95, pb_q)
        metric = f"{pct:.0f}% retracement"
    elif pullback_depth <= 0.55:
        status, conf = "warn", max(40, pb_q * 0.85)
        metric = f"{pct:.0f}% retracement (deep)"
    else:
        status, conf = "fail", max(20, 50 - pullback_depth * 40)
        metric = f"{pct:.0f}% retracement (too deep)"
    return _item("pullback_shallow", "structure", status, conf, metric, extra={"pullback_depth": round(pullback_depth, 3)})


def _eval_exhaustion(
    bull: bool,
    ext_score: float,
    vwap_dist_pct: float,
    ema_dist_pct: float,
    expansion_count: int,
    rsi: float,
    upper_wick_reject: bool,
) -> Dict[str, Any]:
    level = extension_risk_level(ext_score)
    fails = []
    if vwap_dist_pct > 2.0:
        fails.append("VWAP stretch")
    if ema_dist_pct > 1.5:
        fails.append("EMA stretch")
    if expansion_count >= 3:
        fails.append(f"{expansion_count} expansion bars")
    if bull and rsi > 72:
        fails.append(f"RSI {rsi:.0f}")
    if not bull and rsi < 28:
        fails.append(f"RSI {rsi:.0f}")
    if upper_wick_reject:
        fails.append("wick rejection")

    if level == "LOW":
        status, conf = "pass", max(70, 100 - ext_score * 0.5)
    elif level == "MEDIUM":
        status, conf = "warn", max(45, 70 - ext_score * 0.3)
    else:
        status, conf = "fail", max(25, 40 - (ext_score - 65) * 0.5)

    metric = f"Extension {level} ({ext_score:.0f})"
    if fails:
        metric += " · " + ", ".join(fails[:2])
    return _item(
        "no_vertical_exhaustion",
        "structure",
        status,
        conf,
        metric,
        extra={"extension_risk_level": level, "extension_risk_score": round(ext_score, 1)},
    )


def _eval_candle_spread(
    bull: bool,
    o: float,
    h: float,
    l: float,
    c: float,
    avg_body: float,
    avg_range: float,
) -> Dict[str, Any]:
    br = _body_ratio(o, h, l, c)
    rng = h - l
    close_pos = (c - l) / rng if rng > 0 else 0.5
    upper_wick = (h - max(c, o)) / rng if rng > 0 else 0
    spread_ok = rng >= avg_range * 0.85 if avg_range > 0 else True

    if bull:
        top_close = close_pos >= 0.8
        healthy = br > 0.6 and top_close and upper_wick < 0.25 and spread_ok
        marginal = br >= 0.45 and close_pos >= 0.6
    else:
        top_close = close_pos <= 0.2 if rng > 0 else False
        lower_close = (h - c) / rng >= 0.8 if rng > 0 else False
        healthy = br > 0.6 and lower_close and upper_wick < 0.25 and spread_ok
        marginal = br >= 0.45 and close_pos <= 0.4

    if healthy:
        status, conf = "pass", min(92, 60 + br * 40)
        metric = f"Body {br * 100:.0f}% of range"
    elif marginal:
        status, conf = "warn", 55
        metric = f"Body {br * 100:.0f}% — marginal"
    else:
        status, conf = "fail", 28
        if br < 0.35:
            metric = "Doji / indecision"
        elif upper_wick > 0.4:
            metric = "Long upper wick"
        else:
            metric = f"Weak body {br * 100:.0f}%"
    return _item("candle_spread_healthy", "structure", status, conf, metric)


def _eval_major_level(
    bull: bool,
    close: float,
    atr: float,
    highs: Sequence[float],
    lows: Sequence[float],
    i: int,
) -> Dict[str, Any]:
    risk_unit = max(atr, close * 0.002)
    look = max(0, i - 48)
    swing_hi = max(highs[look : i + 1])
    swing_lo = min(lows[look : i + 1])
    round_lvl = round(close / 50) * 50
    prev_day_hi = max(highs[max(0, i - 78) : i + 1]) if i >= 20 else swing_hi
    prev_day_lo = min(lows[max(0, i - 78) : i + 1]) if i >= 20 else swing_lo

    if bull:
        resistances = [swing_hi, prev_day_hi, round_lvl if round_lvl > close else swing_hi]
        dist = min(abs(close - r) for r in resistances if r >= close * 0.998)
        dist_pct = dist / close * 100 if close else 99
        dist_r = dist / risk_unit if risk_unit else 99
        if dist_r < 0.5 or dist_pct < 0.7:
            status, conf = "fail", 30
            metric = f"Resistance {dist_pct:.2f}% / {dist_r:.1f}R away"
        elif dist_r < 1.0 or dist_pct < 1.2:
            status, conf = "warn", 58
            metric = f"Nearby resistance {dist_pct:.2f}%"
        else:
            status, conf = "pass", min(90, 50 + dist_r * 15)
            metric = f"Room {dist_pct:.2f}% ({dist_r:.1f}R)"
    else:
        supports = [swing_lo, prev_day_lo, round_lvl if round_lvl < close else swing_lo]
        dist = min(abs(close - s) for s in supports if s <= close * 1.002)
        dist_pct = dist / close * 100 if close else 99
        dist_r = dist / risk_unit if risk_unit else 99
        if dist_r < 0.5 or dist_pct < 0.7:
            status, conf = "fail", 30
            metric = f"Support {dist_pct:.2f}% / {dist_r:.1f}R away"
        elif dist_r < 1.0 or dist_pct < 1.2:
            status, conf = "warn", 58
            metric = f"Nearby support {dist_pct:.2f}%"
        else:
            status, conf = "pass", min(90, 50 + dist_r * 15)
            metric = f"Room {dist_pct:.2f}% ({dist_r:.1f}R)"
    return _item("not_into_major_level", "structure", status, conf, metric)


def _eval_reclaim_strong(
    bull: bool,
    close: float,
    vwap: float,
    ema: float,
    o: float,
    h: float,
    l: float,
    c: float,
) -> Dict[str, Any]:
    strength = _close_strength(h, l, c, bull)
    br = _body_ratio(o, h, l, c)
    rng = h - l
    upper_wick = (h - max(c, o)) / rng if rng > 0 else 0
    above = (close > vwap and close > ema) if bull else (close < vwap and close < ema)

    if strength > 0.8 and br > 0.5 and above and upper_wick < 0.25:
        status, conf = "pass", min(92, strength * 100)
        metric = f"Close strength {strength * 100:.0f}%"
    elif strength >= 0.6 and above:
        status, conf = "warn", 60
        metric = f"Close strength {strength * 100:.0f}%"
    else:
        status, conf = "fail", 30
        metric = f"Close strength {strength * 100:.0f}%"
    return _item("reclaim_candle_strong", "structure", status, conf, metric)


def _eval_market(
    bull: bool,
    nifty_pct: float,
    bank_pct: float,
) -> Dict[str, Any]:
    if bull:
        both_pos = nifty_pct >= 0 and bank_pct >= 0
        mixed = (nifty_pct >= 0) != (bank_pct >= 0)
        collapse = nifty_pct < -0.35 and bank_pct < -0.35
    else:
        both_pos = nifty_pct <= 0 and bank_pct <= 0
        mixed = (nifty_pct <= 0) != (bank_pct <= 0)
        collapse = nifty_pct > 0.35 and bank_pct > 0.35

    if both_pos and not collapse:
        status, conf = "pass", 78
        metric = f"NIFTY {nifty_pct:+.2f}% · BANKNIFTY {bank_pct:+.2f}%"
    elif mixed or abs(nifty_pct) < 0.08:
        status, conf = "warn", 55
        metric = f"Mixed · NIFTY {nifty_pct:+.2f}%"
    else:
        status, conf = "fail", 32
        metric = f"Conflicting · NIFTY {nifty_pct:+.2f}%"
    return _item("market_structure_supportive", "market", status, conf, metric)


def _eval_sector(aligned: str) -> Dict[str, Any]:
    if aligned == "aligned":
        return _item("sector_not_conflicting", "market", "pass", 80, "Sector aligned")
    if aligned == "neutral":
        return _item("sector_not_conflicting", "market", "warn", 55, "Sector neutral")
    return _item("sector_not_conflicting", "market", "fail", 35, "Sector conflicting")


def _eval_volume(
    bull: bool,
    vol_ratio: float,
    breakout_gt_pullback: bool,
) -> Dict[str, Any]:
    if vol_ratio >= 1.2 and breakout_gt_pullback:
        status, conf = "pass", min(90, 55 + vol_ratio * 15)
        metric = f"Vol {vol_ratio:.2f}× avg · breakout OK"
    elif vol_ratio >= 1.0:
        status, conf = "warn", 52
        metric = f"Vol {vol_ratio:.2f}× avg (average)"
    else:
        status, conf = "fail", 28
        metric = f"Vol {vol_ratio:.2f}× avg (weak)"
    return _item("volume_acceptable", "market", status, conf, metric)


def _eval_vwap_extension(bull: bool, vwap_dist_pct: float) -> Dict[str, Any]:
    if vwap_dist_pct < 1.2:
        status, conf = "pass", max(75, 90 - vwap_dist_pct * 10)
        metric = f"{vwap_dist_pct:.2f}% from VWAP"
    elif vwap_dist_pct <= 2.0:
        status, conf = "warn", 55
        metric = f"{vwap_dist_pct:.2f}% from VWAP (stretched)"
    else:
        status, conf = "fail", 25
        metric = f"{vwap_dist_pct:.2f}% from VWAP (extended)"
    return _item("not_extended_vwap", "market", status, conf, metric)


def evaluate_checklist(
    candles: Sequence[Dict[str, Any]],
    *,
    direction: str,
    market_index: Optional[Dict[str, float]] = None,
    sector_alignment: str = "neutral",
) -> Dict[str, Any]:
    """
    Full automated checklist from 5m candles.
    market_index: {nifty_pct, bank_pct}
    sector_alignment: aligned | neutral | conflicting
    """
    bull = str(direction or "").upper().startswith("L")
    if len(candles) < 30:
        return {"items": [], "checklist": {}, "auto_available": False}

    opens = [float(c.get("open") or 0) for c in candles]
    highs = [float(c.get("high") or 0) for c in candles]
    lows = [float(c.get("low") or 0) for c in candles]
    closes = [float(c.get("close") or 0) for c in candles]
    volumes = [float(c.get("volume") or 0) for c in candles]
    i = len(closes) - 1
    close = closes[i]
    o, h, l, c = opens[i], highs[i], lows[i], closes[i]

    vwap_s = cumulative_vwap(highs, lows, closes, volumes)
    vwap = vwap_s[i]
    ema5 = ema_series(closes, EMA_EXEC_LEN)
    rsi_series = _rsi_wilder(closes, RSI_LEN)
    rsi = float(rsi_series[i] or 50.0)
    rsi_ema_s = ema_series([float(r or 50.0) for r in rsi_series], RSI_EMA_LEN)
    rsi_wma_s = wma_series([float(r or 50.0) for r in rsi_series], RSI_WMA_LEN)
    rsi_ema = rsi_ema_s[i]
    rsi_wma = rsi_wma_s[i]

    atr = max(1e-6, sum(highs[j] - lows[j] for j in range(max(0, i - 13), i + 1)) / min(14, i + 1))
    pb_q = compute_pullback_quality(opens, highs, lows, closes, ema5, atr, bull_dir=bull)

    impulse_hi = max(highs[max(0, i - 12) : i + 1])
    impulse_lo = min(lows[max(0, i - 12) : i + 1])
    impulse_rng = max(impulse_hi - impulse_lo, atr)
    pullback_depth = (
        (impulse_hi - lows[i]) / impulse_rng
        if bull
        else (highs[i] - impulse_lo) / impulse_rng
    )

    vwap_dist_pct = abs(close - vwap) / close * 100 if close else 99.0
    ema_dist_pct = abs(close - ema5[i]) / close * 100 if close else 99.0

    recent = list(range(max(0, i - 4), i + 1))
    expansion_count = 0
    for j in recent:
        rng = highs[j] - lows[j]
        body = abs(closes[j] - opens[j])
        if rng > atr * 1.1 and body / rng > 0.6 if rng > 0 else False:
            if bull and closes[j] > opens[j]:
                expansion_count += 1
            elif not bull and closes[j] < opens[j]:
                expansion_count += 1

    rng_i = highs[i] - lows[i]
    upper_wick_reject = (
        (highs[i] - max(closes[i], opens[i])) / rng_i > 0.45 if rng_i > 0 and bull else False
    )

    ext_score = compute_extension_risk(
        close, vwap, ema5[i], atr, rsi, highs, lows, closes, opens, bull_dir=bull
    )
    ext_level = extension_risk_level(ext_score)

    bodies = [abs(closes[j] - opens[j]) for j in recent]
    ranges = [highs[j] - lows[j] for j in recent]
    avg_body = sum(bodies) / len(bodies) if bodies else 0
    avg_range = sum(ranges) / len(ranges) if ranges else 0

    vol_ma = sum(volumes[max(0, i - 19) : i + 1]) / min(20, i + 1)
    vol_ratio = volumes[i] / vol_ma if vol_ma > 0 else 0.0
    pb_vol = sum(volumes[max(0, i - 5) : i]) / max(1, min(5, i))
    bo_vol = volumes[i]
    breakout_gt_pullback = bo_vol >= pb_vol * 0.95

    mi = market_index or {}
    nifty_pct = float(mi.get("nifty_pct") or 0)
    bank_pct = float(mi.get("bank_pct") or 0)

    items: List[Dict[str, Any]] = [
        _eval_vwap(bull, close, vwap, closes, vwap_s, i),
        _eval_ema(bull, close, ema5, opens, closes, i),
        _eval_hilega(bull, rsi, rsi_ema, rsi_wma, rsi_series, i),
        _eval_pullback(bull, pullback_depth, pb_q),
        _eval_exhaustion(
            bull, ext_score, vwap_dist_pct, ema_dist_pct, expansion_count, rsi, upper_wick_reject
        ),
        _eval_candle_spread(bull, o, h, l, c, avg_body, avg_range),
        _eval_major_level(bull, close, atr, highs, lows, i),
        _eval_reclaim_strong(bull, close, vwap, ema5[i], o, h, l, c),
        _eval_market(bull, nifty_pct, bank_pct),
        _eval_sector(sector_alignment),
        _eval_volume(bull, vol_ratio, breakout_gt_pullback),
        _eval_vwap_extension(bull, vwap_dist_pct),
    ]

    checklist = {it["key"]: bool(it["passed"]) for it in items}
    return {
        "items": items,
        "checklist": checklist,
        "auto_available": True,
        "pullback_depth": round(pullback_depth, 3),
        "vwap_distance_pct": round(vwap_dist_pct, 3),
        "extension_risk_level": ext_level,
        "extension_risk_score": round(ext_score, 1),
        "pullback_quality_score": round(pb_q, 1),
    }
