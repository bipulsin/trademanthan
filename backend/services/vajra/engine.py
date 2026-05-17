"""
TWCTO Vajra trade qualification engine — port of TWCTO_Vajra Pine script.

Primary timeframe: 15-minute bars. Higher TF: 60-minute for EMA(50) bias.
Volume score uses traded value (close × volume) vs its MA.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

EMA_FAST_LEN = 20
EMA_SLOW_LEN = 50
RSI_LEN = 14
ADX_LEN = 14
ATR_LEN = 14
VOL_LEN = 20
OBV_MA_LEN = 30
POC_LEN = 30
POC_BUCKETS = 10
POC_PROX_PCT = 0.3
RR_MIN = 2.0
SWING_LB = 10


def _ema_series(values: Sequence[float], period: int) -> List[float]:
    if not values:
        return []
    p = max(1, int(period))
    k = 2.0 / (p + 1.0)
    out: List[float] = []
    ema_v = float(values[0])
    for v in values:
        ema_v = float(v) * k + ema_v * (1.0 - k)
        out.append(ema_v)
    return out


def _sma_at(values: Sequence[float], period: int, idx: int) -> Optional[float]:
    p = max(1, int(period))
    if idx + 1 < p:
        return None
    window = values[idx - p + 1 : idx + 1]
    return sum(float(x) for x in window) / float(p)


def _rsi_wilder(closes: Sequence[float], period: int = RSI_LEN) -> List[Optional[float]]:
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    if n < period + 1:
        return out
    gains = [0.0]
    losses = [0.0]
    for i in range(1, n):
        d = float(closes[i]) - float(closes[i - 1])
        gains.append(max(0.0, d))
        losses.append(max(0.0, -d))
    avg_g = sum(gains[1 : period + 1]) / float(period)
    avg_l = sum(losses[1 : period + 1]) / float(period)
    out[period] = 100.0 if avg_l == 0 else 100.0 - (100.0 / (1.0 + avg_g / avg_l))
    for i in range(period + 1, n):
        avg_g = (avg_g * (period - 1) + gains[i]) / float(period)
        avg_l = (avg_l * (period - 1) + losses[i]) / float(period)
        if avg_l == 0:
            out[i] = 100.0
        else:
            rs = avg_g / avg_l
            out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def _true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def _wilder_atr(
    highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int
) -> List[Optional[float]]:
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    if n < period + 1:
        return out
    trs: List[float] = []
    for i in range(1, n):
        trs.append(_true_range(highs[i], lows[i], closes[i - 1]))
    atr = sum(trs[:period]) / float(period)
    out[period] = atr
    for j in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[j]) / float(period)
        out[j + 1] = atr
    return out


def _dmi_adx(
    highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], length: int
) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    """Wilder DMI: returns (+DI, -DI, ADX) series aligned to bar index."""
    n = len(closes)
    di_p: List[Optional[float]] = [None] * n
    di_m: List[Optional[float]] = [None] * n
    adx: List[Optional[float]] = [None] * n
    if n < length * 2:
        return di_p, di_m, adx
    plus_dm = [0.0]
    minus_dm = [0.0]
    tr_list = [0.0]
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        tr_list.append(_true_range(highs[i], lows[i], closes[i - 1]))
    tr_rma = _rma_series(tr_list, length)
    pdm_rma = _rma_series(plus_dm, length)
    mdm_rma = _rma_series(minus_dm, length)
    dx_vals: List[Optional[float]] = [None] * n
    for i in range(n):
        trv = tr_rma[i]
        if trv is None or trv <= 0:
            continue
        p = pdm_rma[i] or 0.0
        m = mdm_rma[i] or 0.0
        di_p[i] = 100.0 * p / trv
        di_m[i] = 100.0 * m / trv
        s = (di_p[i] or 0.0) + (di_m[i] or 0.0)
        if s > 0:
            dx_vals[i] = 100.0 * abs((di_p[i] or 0.0) - (di_m[i] or 0.0)) / s
    adx_seed: List[float] = [v for v in dx_vals[length : length * 2] if v is not None]
    if len(adx_seed) < length:
        return di_p, di_m, adx
    adx_v = sum(adx_seed[:length]) / float(length)
    adx[length * 2 - 1] = adx_v
    for i in range(length * 2, n):
        dxi = dx_vals[i]
        if dxi is None:
            continue
        adx_v = (adx_v * (length - 1) + dxi) / float(length)
        adx[i] = adx_v
    return di_p, di_m, adx


def _rma_series(values: Sequence[float], period: int) -> List[Optional[float]]:
    p = max(1, int(period))
    out: List[Optional[float]] = [None] * len(values)
    if len(values) < p:
        return out
    seed = sum(float(x) for x in values[:p]) / float(p)
    out[p - 1] = seed
    prev = seed
    for i in range(p, len(values)):
        prev = (prev * (p - 1) + float(values[i])) / float(p)
        out[i] = prev
    return out


def _obv_series(closes: Sequence[float], volumes: Sequence[float]) -> List[float]:
    out = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            out.append(out[-1] + float(volumes[i]))
        elif closes[i] < closes[i - 1]:
            out.append(out[-1] - float(volumes[i]))
        else:
            out.append(out[-1])
    return out


def _compute_poc(
    highs: Sequence[float],
    lows: Sequence[float],
    volumes: Sequence[float],
    length: int,
    nbuckets: int,
) -> float:
    n = min(len(highs), len(lows), len(volumes), length)
    if n < 2:
        return (float(highs[-1]) + float(lows[-1])) / 2.0 if highs else 0.0
    seg_h = highs[-n:]
    seg_l = lows[-n:]
    seg_v = volumes[-n:]
    hi = max(seg_h)
    lo = min(seg_l)
    rng = hi - lo
    if rng <= 0:
        return (hi + lo) / 2.0
    bsize = rng / float(nbuckets)
    max_v = 0.0
    poc = (hi + lo) / 2.0
    for bi in range(nbuckets):
        blo = lo + bi * bsize
        bhi = blo + bsize
        bvol = 0.0
        for j in range(n):
            mid = (seg_h[j] + seg_l[j]) / 2.0
            if blo <= mid <= bhi:
                bvol += float(seg_v[j])
        if bvol > max_v:
            max_v = bvol
            poc = (blo + bhi) / 2.0
    return poc


def _update_pivots(
    highs: Sequence[float],
    lows: Sequence[float],
    lb: int,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    p_h1 = p_h2 = p_l1 = p_l2 = None
    n = len(highs)
    for i in range(lb, n - lb):
        wh = highs[i - lb : i + lb + 1]
        wl = lows[i - lb : i + lb + 1]
        if highs[i] == max(wh):
            p_h2, p_h1 = p_h1, float(highs[i])
        if lows[i] == min(wl):
            p_l2, p_l1 = p_l1, float(lows[i])
    return p_h1, p_h2, p_l1, p_l2


def _pass_fail(v: bool) -> str:
    return "✔ PASS" if v else "✘ FAIL"


def _obv_label(obv_bull: bool, obv_bear: bool) -> str:
    if obv_bull:
        return "↑ ABOVE MA · RISING"
    if obv_bear:
        return "↓ BELOW MA · FALLING"
    return "→ FLAT"


@dataclass
class VajraRating:
    trade_type: str
    confidence: float
    bull_score: float
    bear_score: float
    structure_pass: bool
    momentum_pass: bool
    trend_pass: bool
    volume_pass: bool
    obv_label: str
    market_phase: str
    reversal_risk: str

    def to_row_dict(self) -> Dict[str, Any]:
        return {
            "trade_type": self.trade_type,
            "confidence": round(self.confidence, 1),
            "bull_score": round(self.bull_score, 1),
            "bear_score": round(self.bear_score, 1),
            "structure": _pass_fail(self.structure_pass),
            "structure_pass": self.structure_pass,
            "momentum": _pass_fail(self.momentum_pass),
            "momentum_pass": self.momentum_pass,
            "trend": _pass_fail(self.trend_pass),
            "trend_pass": self.trend_pass,
            "volume": _pass_fail(self.volume_pass),
            "volume_pass": self.volume_pass,
            "obv": self.obv_label,
            "market_phase": self.market_phase,
            "reversal_risk": self.reversal_risk,
        }


def compute_vajra_rating(
    candles_15m: Sequence[Dict[str, Any]],
    candles_60m: Optional[Sequence[Dict[str, Any]]] = None,
) -> Optional[VajraRating]:
    """
    Compute Vajra rating from OHLCV candles (oldest → newest).
    Requires at least ~80 completed 15m bars for stable indicators.
    """
    if not candles_15m or len(candles_15m) < 60:
        return None

    opens = [float(c.get("open") or 0) for c in candles_15m]
    highs = [float(c.get("high") or 0) for c in candles_15m]
    lows = [float(c.get("low") or 0) for c in candles_15m]
    closes = [float(c.get("close") or 0) for c in candles_15m]
    volumes = [float(c.get("volume") or 0) for c in candles_15m]
    traded_values = [closes[i] * volumes[i] for i in range(len(closes))]

    n = len(closes)
    i = n - 1

    ema_fast = _ema_series(closes, EMA_FAST_LEN)
    ema_slow = _ema_series(closes, EMA_SLOW_LEN)
    rsi_series = _rsi_wilder(closes, RSI_LEN)
    rsi = rsi_series[i]
    if rsi is None:
        return None

    di_plus, di_minus, adx_series = _dmi_adx(highs, lows, closes, ADX_LEN)
    adx_val = adx_series[i]
    if adx_val is None:
        return None
    adx_3 = adx_series[i - 3] if i >= 3 and adx_series[i - 3] is not None else adx_val

    atr_series = _wilder_atr(highs, lows, closes, ATR_LEN)
    atr = atr_series[i]
    if atr is None:
        return None
    atr_avg_vals = []
    for j in range(max(ATR_LEN, i - 19), i + 1):
        v = atr_series[j]
        if v is not None:
            atr_avg_vals.append(v)
    atr_avg = sum(atr_avg_vals) / len(atr_avg_vals) if atr_avg_vals else atr

    vol_ma = _sma_at(traded_values, VOL_LEN, i)
    if vol_ma is None:
        return None

    obv_raw = _obv_series(closes, volumes)
    obv_ma = _sma_at(obv_raw, OBV_MA_LEN, i)
    if obv_ma is None:
        return None
    obv_bull = obv_raw[i] > obv_ma and obv_raw[i] > obv_raw[max(0, i - 3)]
    obv_bear = obv_raw[i] < obv_ma and obv_raw[i] < obv_raw[max(0, i - 3)]

    poc = _compute_poc(highs, lows, volumes, POC_LEN, POC_BUCKETS)
    prev_poc = _compute_poc(highs[:-POC_LEN], lows[:-POC_LEN], volumes[:-POC_LEN], POC_LEN, POC_BUCKETS) if n > POC_LEN * 2 else poc
    close = closes[i]
    poc_prox = abs(close - poc) / poc * 100 <= POC_PROX_PCT if poc else False
    above_poc = close > poc
    below_poc = close < poc

    p_h1, p_h2, p_l1, p_l2 = _update_pivots(highs, lows, SWING_LB)
    hhhl = (
        p_h1 is not None
        and p_h2 is not None
        and p_l1 is not None
        and p_l2 is not None
        and p_h1 > p_h2
        and p_l1 > p_l2
    )
    lhll = (
        p_h1 is not None
        and p_h2 is not None
        and p_l1 is not None
        and p_l2 is not None
        and p_h1 < p_h2
        and p_l1 < p_l2
    )
    structure_bull = hhhl and close > ema_fast[i] and ema_fast[i] > ema_slow[i] and above_poc
    structure_bear = lhll and close < ema_fast[i] and ema_fast[i] < ema_slow[i] and below_poc

    dip = di_plus[i] or 0.0
    dim = di_minus[i] or 0.0
    bull_momentum = rsi > 55 and adx_val > 20 and dip > dim
    bear_momentum = rsi < 45 and adx_val > 20 and dim > dip
    trend_bull = adx_val > 20 and dip > dim
    trend_bear = adx_val > 20 and dim > dip

    vol_bull = traded_values[i] > vol_ma and close > opens[i]
    vol_bear = traded_values[i] > vol_ma and close < opens[i]

    highest_high = max(highs[max(0, i - SWING_LB) : i]) if i > 0 else highs[i]
    lowest_low = min(lows[max(0, i - SWING_LB) : i]) if i > 0 else lows[i]
    bull_breakout = close > highest_high
    bear_breakout = close < lowest_low
    prev_hi = max(highs[max(0, i - SWING_LB - 1) : i - 1]) if i > 1 else highs[0]
    prev_lo = min(lows[max(0, i - SWING_LB - 1) : i - 1]) if i > 1 else lows[0]
    failed_bull = i > 0 and closes[i - 1] > prev_hi and close < prev_hi
    failed_bear = i > 0 and closes[i - 1] < prev_lo and close > prev_lo

    htf_bullish = False
    htf_bearish = False
    if candles_60m and len(candles_60m) >= EMA_SLOW_LEN + 5:
        htf_closes = [float(c.get("close") or 0) for c in candles_60m]
        htf_ema = _ema_series(htf_closes, EMA_SLOW_LEN)
        htf_bullish = htf_closes[-1] > htf_ema[-1]
        htf_bearish = htf_closes[-1] < htf_ema[-1]

    bull_sl = min(lows[max(0, i - SWING_LB + 1) : i + 1])
    bull_risk = close - bull_sl
    bull_tgt = close + bull_risk * RR_MIN
    bull_rr = (bull_tgt - close) / bull_risk if bull_risk > 0 else 0.0
    bear_sl = max(highs[max(0, i - SWING_LB + 1) : i + 1])
    bear_risk = bear_sl - close
    bear_tgt = close - bear_risk * RR_MIN
    bear_rr = (close - bear_tgt) / bear_risk if bear_risk > 0 else 0.0
    rr_bull = bull_rr >= RR_MIN
    rr_bear = bear_rr >= RR_MIN

    candle_range = highs[i] - lows[i]
    body_size = abs(close - opens[i])
    upper_wick = highs[i] - max(close, opens[i])
    lower_wick = min(close, opens[i]) - lows[i]
    body_ratio = body_size / candle_range if candle_range > 0 else 0.0
    upper_wick_pct = upper_wick / candle_range if candle_range > 0 else 0.0
    lower_wick_pct = lower_wick / candle_range if candle_range > 0 else 0.0
    bull_rejection = upper_wick_pct > 0.4 and body_ratio < 0.3
    bear_rejection = lower_wick_pct > 0.4 and body_ratio < 0.3

    lb2 = SWING_LB * 2
    seg_start = max(0, i - lb2 + 1)
    price_hh_prev = max(highs[seg_start : i + 1])
    price_ll_prev = min(lows[seg_start : i + 1])
    rsi_seg = [r for r in rsi_series[seg_start : i + 1] if r is not None]
    rsi_hh_prev = max(rsi_seg) if rsi_seg else rsi
    rsi_ll_prev = min(rsi_seg) if rsi_seg else rsi
    bear_diverg = highs[i] >= price_hh_prev * 0.99 and rsi < rsi_hh_prev * 0.97
    bull_diverg = lows[i] <= price_ll_prev * 1.01 and rsi > rsi_ll_prev * 1.03

    adx_declining = adx_val < adx_3 and adx_val < 30
    vol_climax = traded_values[i] > vol_ma * 3.0
    rev_score = (
        (1 if bear_diverg or bull_diverg else 0)
        + (1 if bull_rejection or bear_rejection else 0)
        + (1 if adx_declining else 0)
        + (1 if vol_climax else 0)
    )
    rev_risk = "HIGH" if rev_score >= 3 else ("MEDIUM" if rev_score >= 2 else "LOW")

    compression = atr < atr_avg and abs(close - ema_fast[i]) < atr * 0.5
    expansion_bull = atr > atr_avg and bull_momentum and close > highs[i - 1] if i > 0 else False
    expansion_bear = atr > atr_avg and bear_momentum and close < lows[i - 1] if i > 0 else False
    exhaustion = atr < atr_avg and abs(close - opens[i]) < atr * 0.2 and adx_val < 18
    failure_phase = failed_bull or failed_bear
    weakening_phase = adx_declining and not compression and not exhaustion

    if failure_phase:
        phase = "FAILURE"
    elif weakening_phase:
        phase = "WEAKENING"
    elif compression:
        phase = "COMPRESSION"
    elif expansion_bull:
        phase = "BULL EXPANSION"
    elif expansion_bear:
        phase = "BEAR EXPANSION"
    elif exhaustion:
        phase = "EXHAUSTION"
    else:
        phase = "ROTATIONAL"

    b_s_struct = 20 if structure_bull else 0
    b_s_mom = 15 if bull_momentum else 0
    b_s_trend = 10 if trend_bull else 0
    b_s_vol = 10 if vol_bull else 0
    b_s_obv = 5 if obv_bull else 0
    b_s_brk = 15 if bull_breakout else 0
    b_s_rr = 15 if rr_bull else 0
    b_s_poc = 10 if above_poc and not poc_prox else 0
    b_s_htf = 5 if htf_bullish else 0
    b_s_p1 = 20 if failed_bull else 0
    b_s_p2 = 15 if bear_diverg else 0
    b_s_p3 = 10 if adx_declining else 0

    e_s_struct = 20 if structure_bear else 0
    e_s_mom = 15 if bear_momentum else 0
    e_s_trend = 10 if trend_bear else 0
    e_s_vol = 10 if vol_bear else 0
    e_s_obv = 5 if obv_bear else 0
    e_s_brk = 15 if bear_breakout else 0
    e_s_rr = 15 if rr_bear else 0
    e_s_poc = 10 if below_poc and not poc_prox else 0
    e_s_htf = 5 if htf_bearish else 0
    e_s_p1 = 20 if failed_bear else 0
    e_s_p2 = 15 if bull_diverg else 0
    e_s_p3 = 10 if adx_declining else 0

    raw_bull = (
        b_s_struct + b_s_mom + b_s_trend + b_s_vol + b_s_obv + b_s_brk + b_s_rr + b_s_poc + b_s_htf
        - b_s_p1 - b_s_p2 - b_s_p3
    )
    raw_bear = (
        e_s_struct + e_s_mom + e_s_trend + e_s_vol + e_s_obv + e_s_brk + e_s_rr + e_s_poc + e_s_htf
        - e_s_p1 - e_s_p2 - e_s_p3
    )
    bull_score = max(0.0, min(100.0, float(raw_bull)))
    bear_score = max(0.0, min(100.0, float(raw_bear)))

    is_long = bull_score > bear_score and bull_score >= 75
    is_short = bear_score > bull_score and bear_score >= 75
    confidence = bull_score if is_long else (bear_score if is_short else max(bull_score, bear_score))

    if is_long:
        trade_type = "LONG  [A+]" if bull_score >= 85 else "LONG"
    elif is_short:
        trade_type = "SHORT [A+]" if bear_score >= 85 else "SHORT"
    elif bull_score >= 60:
        trade_type = "LONG WATCH"
    elif bear_score >= 60:
        trade_type = "SHORT WATCH"
    else:
        trade_type = "REJECT"

    bull_dir = bull_score >= bear_score
    p_struct = structure_bull if bull_dir else structure_bear
    p_mom = bull_momentum if bull_dir else bear_momentum
    p_trend = trend_bull if bull_dir else trend_bear
    p_vol = vol_bull if bull_dir else vol_bear

    return VajraRating(
        trade_type=trade_type,
        confidence=float(confidence),
        bull_score=bull_score,
        bear_score=bear_score,
        structure_pass=bool(p_struct),
        momentum_pass=bool(p_mom),
        trend_pass=bool(p_trend),
        volume_pass=bool(p_vol),
        obv_label=_obv_label(obv_bull, obv_bear),
        market_phase=phase,
        reversal_risk=rev_risk,
    )


TRADE_TYPE_SORT_ORDER = {
    "LONG  [A+]": 0,
    "LONG": 1,
    "SHORT [A+]": 2,
    "SHORT": 3,
    "LONG WATCH": 4,
    "SHORT WATCH": 5,
    "REJECT": 6,
}


def sort_vajra_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _key(r: Dict[str, Any]) -> Tuple[int, float, str]:
        tt = str(r.get("trade_type") or "REJECT")
        conf = float(r.get("confidence") or 0)
        sec = str(r.get("security") or r.get("stock") or "")
        return (TRADE_TYPE_SORT_ORDER.get(tt, 99), -conf, sec)

    return sorted(rows, key=_key)
