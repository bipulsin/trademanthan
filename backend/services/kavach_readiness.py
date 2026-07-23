"""Kavach v3.0 readiness classification + nearer-of-EMA10/VWAP pullback counts.

Mirrors TWCTO Kavach Pine v3.0 banner/row-1 readiness (not ENTER unanimity gates):
  READY TO LONG / READY TO SHORT / WATCHING / NOT READY
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.services.kavach_engine import BEARISH_STATES, BULLISH_STATES
from backend.services.relative_strength_scanner import _parse_ist_date, _sorted_candles
from backend.services.vajra.indicators import cumulative_vwap, ema_series

MAX_PCT_FROM_OPEN = 3.0
MIN_PULLBACKS = 0
MAX_PULLBACKS = 2
VOL_ENTER_MIN = 0.8
PULLBACK_RESET_BARS = 3
PANEL_EMA_LEN = 9
EXIT_EMA_LEN = 10

# Pine Layer 3 — Trade Eligibility (defaults match TWCTO_Kavach_v3_0 inputs).
MIN_EMA_VWAP_CONFIRM_GAP_PCT = 0.15
MAX_PREV_EMA_VWAP_GAP_PCT = 0.50
VWAP_CLOSE_CONFIRM_BARS = 2


def pine_layer3_eligible(
    *,
    close: float,
    vwap: float,
    panel_ema: float,
    prev_panel_ema: float,
    prev_vwap: float,
    macd: float,
    macd_signal: float,
    macd_histogram: float,
    st_bullish: Optional[bool],
    closes_10m: List[float],
    vwaps_at_10m: List[float],
    min_gap_pct: float = MIN_EMA_VWAP_CONFIRM_GAP_PCT,
    max_prev_gap_pct: float = MAX_PREV_EMA_VWAP_GAP_PCT,
    vwap_confirm_bars: int = VWAP_CLOSE_CONFIRM_BARS,
) -> Dict[str, Any]:
    """Pine ``buyEligible`` / ``sellEligible`` (signal count ≥2 + VWAP close streak)."""
    if vwap is None or not vwap:
        return {
            "buy_eligible": False,
            "sell_eligible": False,
            "buy_signal_count": 0,
            "sell_signal_count": 0,
            "buy_vwap_confirmed": False,
            "sell_vwap_confirmed": False,
        }

    macd_hist_pos = float(macd_histogram) > 0
    macd_hist_neg = float(macd_histogram) < 0
    macd_line_above = float(macd) > float(macd_signal)
    macd_line_below = float(macd) < float(macd_signal)
    st_bull = st_bullish is True
    st_bear = st_bullish is False

    ema_gap_pct = (float(panel_ema) - float(vwap)) / float(vwap) * 100.0
    prev_gap_pct = (
        (float(prev_panel_ema) - float(prev_vwap)) / float(prev_vwap) * 100.0
        if prev_vwap
        else 0.0
    )

    buy_sign1 = macd_hist_pos and macd_line_above
    buy_sign2 = st_bull
    buy_sign3 = float(close) > float(panel_ema) and float(close) > float(vwap)
    buy_sign4 = ema_gap_pct >= min_gap_pct and abs(prev_gap_pct) <= max_prev_gap_pct
    buy_count = sum(1 for s in (buy_sign1, buy_sign2, buy_sign3, buy_sign4) if s)

    sell_sign1 = macd_hist_neg and macd_line_below
    sell_sign2 = st_bear
    sell_sign3 = float(close) < float(panel_ema) and float(close) < float(vwap)
    sell_sign4 = (-ema_gap_pct) >= min_gap_pct and abs(prev_gap_pct) <= max_prev_gap_pct
    sell_count = sum(1 for s in (sell_sign1, sell_sign2, sell_sign3, sell_sign4) if s)

    n = min(len(closes_10m), len(vwaps_at_10m))
    buy_streak = 0
    sell_streak = 0
    if n > 0 and float(closes_10m[-1]) > float(vwaps_at_10m[-1]):
        buy_streak = 1
        for i in range(1, vwap_confirm_bars):
            if n - 1 - i < 0:
                break
            if float(closes_10m[-1 - i]) > float(vwaps_at_10m[-1 - i]):
                buy_streak += 1
            else:
                break
    if n > 0 and float(closes_10m[-1]) < float(vwaps_at_10m[-1]):
        sell_streak = 1
        for i in range(1, vwap_confirm_bars):
            if n - 1 - i < 0:
                break
            if float(closes_10m[-1 - i]) < float(vwaps_at_10m[-1 - i]):
                sell_streak += 1
            else:
                break

    buy_vwap_ok = buy_streak >= vwap_confirm_bars
    sell_vwap_ok = sell_streak >= vwap_confirm_bars
    return {
        "buy_eligible": buy_count >= 2 and buy_vwap_ok,
        "sell_eligible": sell_count >= 2 and sell_vwap_ok,
        "buy_signal_count": buy_count,
        "sell_signal_count": sell_count,
        "buy_vwap_confirmed": buy_vwap_ok,
        "sell_vwap_confirmed": sell_vwap_ok,
        "buy_vwap_streak": buy_streak,
        "sell_vwap_streak": sell_streak,
    }


def _grade_ready_level(confidence_display: str) -> int:
    """Pine gradeReadyLevel — stretch-marked A!/B! do not qualify (exact match)."""
    g = (confidence_display or "").strip()
    if g in ("A+", "A"):
        return 2
    if g in ("B", "B*"):
        return 1
    return 0


def count_nearer_pullbacks(
    candles: List[Dict],
    *,
    session_date: str,
) -> Tuple[int, int]:
    """Pullback counts for long/short legs (Pine v3.0 nearer EMA10/VWAP + 3-bar reset).

    Returns ``(pullback_count_long, pullback_count_short)``.
    """
    from backend.services.kavach_10m import aggregate_10m_bars, last_closed_10m_pair_end_idx

    if not candles:
        return 0, 0
    candles = _sorted_candles(candles)
    pair_end = last_closed_10m_pair_end_idx(candles)
    if pair_end < 0:
        return 0, 0
    bars_all = [b for b in aggregate_10m_bars(candles) if b["end_5m_idx"] <= pair_end]
    if len(bars_all) < 3:
        return 0, 0

    closes = [float(b["close"]) for b in bars_all]
    highs = [float(b["high"]) for b in bars_all]
    lows = [float(b["low"]) for b in bars_all]
    ema10_s = ema_series(closes, EXIT_EMA_LEN)
    panel_ema_s = ema_series(closes, PANEL_EMA_LEN)

    # Session VWAP at each 10m bar end (recomputed from day's 5m through end_idx).
    vwaps: List[float] = []
    for b in bars_all:
        end_idx = int(b["end_5m_idx"])
        d = _parse_ist_date(candles[end_idx].get("timestamp"))
        first = 0
        for i, c in enumerate(candles):
            if _parse_ist_date(c.get("timestamp")) == d:
                first = i
                break
        t_highs = [float(c.get("high") or 0) for c in candles[first : end_idx + 1]]
        t_lows = [float(c.get("low") or 0) for c in candles[first : end_idx + 1]]
        t_closes = [float(c.get("close") or 0) for c in candles[first : end_idx + 1]]
        t_vols = [float(c.get("volume") or 0) for c in candles[first : end_idx + 1]]
        series = cumulative_vwap(t_highs, t_lows, t_closes, t_vols) if t_closes else [closes[len(vwaps)]]
        vwaps.append(float(series[-1]))

    pb_long = 0
    pb_short = 0
    in_pull_long = False
    in_pull_short = False
    bars_since_long = 0
    bars_since_short = 0
    ema_above_prev: Optional[bool] = None

    for i in range(len(bars_all)):
        end_idx = int(bars_all[i]["end_5m_idx"])
        bar_date = _parse_ist_date(candles[end_idx].get("timestamp"))
        # Warm indicators on full history; only count/reset on session_date bars.
        if bar_date != session_date:
            ema_above_prev = panel_ema_s[i] > vwaps[i] if vwaps[i] else False
            in_pull_long = False
            in_pull_short = False
            continue

        close = closes[i]
        vwap = vwaps[i]
        ema10 = ema10_s[i]
        panel_ema = panel_ema_s[i]
        ema_above = panel_ema > vwap if vwap else False
        ema_below = panel_ema < vwap if vwap else False

        # Cross resets (Pine: emaVwapCrossUp / Down).
        if ema_above_prev is not None:
            if ema_above and not ema_above_prev:
                pb_long = 0
                in_pull_long = False
                bars_since_long = 0
            if ema_below and ema_above_prev:
                pb_short = 0
                in_pull_short = False
                bars_since_short = 0
        ema_above_prev = ema_above

        nearer = ema10 if abs(close - ema10) <= abs(close - vwap) else vwap
        nearer_prev = (
            (ema10_s[i - 1] if abs(closes[i - 1] - ema10_s[i - 1]) <= abs(closes[i - 1] - vwaps[i - 1]) else vwaps[i - 1])
            if i > 0
            else nearer
        )

        if ema_above:
            if (
                not in_pull_long
                and lows[i] <= nearer
                and i > 0
                and closes[i - 1] > nearer_prev
            ):
                in_pull_long = True
            if in_pull_long and close > nearer:
                pb_long += 1
                in_pull_long = False
                bars_since_long = 0
            else:
                bars_since_long += 1
                if bars_since_long >= PULLBACK_RESET_BARS and pb_long > 0:
                    pb_long = 0
        else:
            in_pull_long = False
            bars_since_long = 0

        if ema_below:
            if (
                not in_pull_short
                and highs[i] >= nearer
                and i > 0
                and closes[i - 1] < nearer_prev
            ):
                in_pull_short = True
            if in_pull_short and close < nearer:
                pb_short += 1
                in_pull_short = False
                bars_since_short = 0
            else:
                bars_since_short += 1
                if bars_since_short >= PULLBACK_RESET_BARS and pb_short > 0:
                    pb_short = 0
        else:
            in_pull_short = False
            bars_since_short = 0

    return pb_long, pb_short


def classify_kavach_readiness(
    *,
    confidence_display: str,
    trade_score: float,
    panel_trend: Optional[str],
    kavach_state: Optional[str],
    pct_from_open: Optional[float],
    pullback_long: int,
    pullback_short: int,
    volume_ratio_for_enter: Optional[float],
    vol_decel_3: bool,
    buy_eligible: Optional[bool] = None,
    sell_eligible: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return Pine v3.0 readiness text + gate flags.

    Direction mirrors Pine: ``kavachDirLong = buyEligible`` (Layer 3), with
    ``trendReadBullish`` as OR fallback. When eligibility is not supplied,
    falls back to ``kavach_state ∈ BULLISH/BEARISH`` (legacy).
    """
    grade_lvl = _grade_ready_level(confidence_display)
    score = float(trade_score or 0)

    trend_bull = (panel_trend or "") == "Bullish"
    trend_bear = (panel_trend or "") == "Bearish"
    kav = (kavach_state or "").upper()
    if buy_eligible is not None or sell_eligible is not None:
        kavach_long = bool(buy_eligible)
        kavach_short = bool(sell_eligible)
    else:
        kavach_long = kav in BULLISH_STATES
        kavach_short = kav in BEARISH_STATES
    dir_long = kavach_long or trend_bull
    dir_short = kavach_short or trend_bear

    pct = float(pct_from_open) if pct_from_open is not None else None
    pct_long_ok = pct is not None and pct < MAX_PCT_FROM_OPEN
    pct_short_ok = pct is not None and (-pct) < MAX_PCT_FROM_OPEN

    pull_long_ok = MIN_PULLBACKS <= int(pullback_long) <= MAX_PULLBACKS
    pull_short_ok = MIN_PULLBACKS <= int(pullback_short) <= MAX_PULLBACKS

    vol_ratio = float(volume_ratio_for_enter) if volume_ratio_for_enter is not None else 0.0
    vol_ok = vol_ratio >= VOL_ENTER_MIN and not vol_decel_3

    ready_long = dir_long and pct_long_ok and pull_long_ok and vol_ok
    ready_short = dir_short and pct_short_ok and pull_short_ok and vol_ok

    if grade_lvl >= 1 and score >= 65 and ready_long and not ready_short:
        text = "READY TO LONG"
    elif grade_lvl >= 1 and score >= 65 and ready_short and not ready_long:
        text = "READY TO SHORT"
    elif grade_lvl >= 1 and score >= 50:
        text = "WATCHING"
    else:
        text = "NOT READY"

    return {
        "readiness": text,
        "grade_ready_level": grade_lvl,
        "dir_long": dir_long,
        "dir_short": dir_short,
        "buy_eligible": buy_eligible,
        "sell_eligible": sell_eligible,
        "pct_from_open": pct,
        "pct_long_ok": pct_long_ok,
        "pct_short_ok": pct_short_ok,
        "pullback_long": int(pullback_long),
        "pullback_short": int(pullback_short),
        "pull_long_ok": pull_long_ok,
        "pull_short_ok": pull_short_ok,
        "vol_enter_ok": vol_ok,
        "vol_decel_3": bool(vol_decel_3),
        "ready_long_practical": ready_long,
        "ready_short_practical": ready_short,
    }


def vol_decel_3_from_10m(volumes_10m: List[float]) -> bool:
    """True when last 4 confirmed 10m volumes are strictly decelerating."""
    if len(volumes_10m) < 4:
        return False
    a, b, c, d = volumes_10m[-4:]
    return d < c < b < a


def attach_readiness_to_stock(
    stock: Dict[str, Any],
    candles: List[Dict],
    *,
    session_date: str,
    session_open: Optional[float],
    metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Compute pullbacks + readiness; mutate stock; return readiness dict."""
    pb_long, pb_short = count_nearer_pullbacks(candles, session_date=session_date)
    is_long = (stock.get("direction") or "LONG").upper() != "SHORT"
    pb = pb_long if is_long else pb_short
    stock["pullback_count"] = pb
    stock["pullback_count_long"] = pb_long
    stock["pullback_count_short"] = pb_short

    price = None
    if metrics:
        price = metrics.get("price")
    if price is None:
        price = stock.get("live_candle_price") or stock.get("ltp") or stock.get("price")
    try:
        price_f = float(price) if price is not None else None
    except (TypeError, ValueError):
        price_f = None
    pct_open = None
    if price_f is not None and session_open and float(session_open) > 0:
        pct_open = (price_f - float(session_open)) / float(session_open) * 100.0
    stock["pct_from_open"] = round(pct_open, 4) if pct_open is not None else None

    conf = str(
        stock.get("confidence")
        or (metrics or {}).get("confidence_grade")
        or ""
    ).strip()
    score = stock.get("trade_score")
    if score is None and metrics:
        score = metrics.get("trade_score")
    if score is None:
        score = stock.get("dashboard_score") or 0

    panel_trend = stock.get("trend") or (metrics or {}).get("panel_trend")
    kav_state = stock.get("dashboard_kavach_live") or (metrics or {}).get("kavach_state")

    vol_ratio = None
    vols_10m: List[float] = []
    if metrics:
        tod = metrics.get("volume_tod_ratio")
        bar = metrics.get("volume_ratio")
        vol_ratio = tod if tod is not None else bar
        vols_10m = list(metrics.get("volumes_10m") or [])
    if vol_ratio is None:
        try:
            vol_ratio = float(stock.get("vol_multiplier") or 0)
        except (TypeError, ValueError):
            vol_ratio = 0.0

    decel = vol_decel_3_from_10m(vols_10m)
    result = classify_kavach_readiness(
        confidence_display=conf,
        trade_score=float(score or 0),
        panel_trend=panel_trend,
        kavach_state=kav_state,
        pct_from_open=pct_open,
        pullback_long=pb_long,
        pullback_short=pb_short,
        volume_ratio_for_enter=vol_ratio,
        vol_decel_3=decel,
        buy_eligible=(metrics or {}).get("buy_eligible"),
        sell_eligible=(metrics or {}).get("sell_eligible"),
    )
    stock["pine_readiness"] = result["readiness"]
    stock["pine_readiness_detail"] = result
    if pb >= 3:
        stock["pullback_label"] = f"{pb}+ pullback"
    elif pb == 1:
        stock["pullback_label"] = "1st pullback"
    elif pb == 2:
        stock["pullback_label"] = "2nd pullback"
    elif pb == 0:
        stock["pullback_label"] = "0 pullback"
    else:
        stock["pullback_label"] = ""
    return result
