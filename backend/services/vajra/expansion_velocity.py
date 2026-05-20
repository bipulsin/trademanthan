"""Expansion Velocity Score (EVS) — breakout ignition detection."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from backend.services.vajra.engine import ADX_LEN, _dmi_adx, _rsi_wilder, _wilder_atr
from backend.services.vajra.indicators import cumulative_vwap, ema_series


def _ohlcv(candles: Sequence[Dict[str, Any]]) -> tuple[List[float], ...]:
    o = [float(c.get("open") or 0) for c in candles]
    h = [float(c.get("high") or 0) for c in candles]
    l = [float(c.get("low") or 0) for c in candles]
    c = [float(c.get("close") or 0) for c in candles]
    v = [float(c.get("volume") or 0) for c in candles]
    return o, h, l, c, v


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


@dataclass
class ExpansionVelocityResult:
    evs_score: float
    candle_spread_score: float
    compression_escape_score: float
    vwap_displacement_score: float
    adx_slope_score: float
    di_spread_velocity_score: float
    obv_acceleration_score: float
    impulse_efficiency_score: float
    compression_broken: bool
    adx_accelerating: bool
    range_expanding: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "evs_score": round(self.evs_score, 1),
            "candle_spread_score": round(self.candle_spread_score, 1),
            "compression_escape_score": round(self.compression_escape_score, 1),
            "vwap_displacement_score": round(self.vwap_displacement_score, 1),
            "adx_slope_score": round(self.adx_slope_score, 1),
            "di_spread_velocity_score": round(self.di_spread_velocity_score, 1),
            "obv_acceleration_score": round(self.obv_acceleration_score, 1),
            "impulse_efficiency_score": round(self.impulse_efficiency_score, 1),
            "compression_broken": self.compression_broken,
            "adx_accelerating": self.adx_accelerating,
            "range_expanding": self.range_expanding,
        }


def _candle_spread_score(h: List[float], l: List[float], c: List[float], i: int) -> tuple[float, bool]:
    if i < 8:
        return 50.0, False
    ranges = [h[j] - l[j] for j in range(i - 7, i + 1)]
    avg = sum(ranges[:-1]) / max(1, len(ranges) - 1)
    cur = ranges[-1]
    if avg <= 0:
        return 50.0, False
    ratio = cur / avg
    expanding = ratio >= 1.12
    if ratio >= 1.35:
        return 85.0, expanding
    if ratio >= 1.12:
        return 70.0, expanding
    if ratio >= 0.95:
        return 55.0, False
    return 40.0, False


def _compression_escape(h: List[float], l: List[float], c: List[float], i: int, *, bull_dir: bool) -> tuple[float, bool]:
    if i < 12:
        return 50.0, False
    look = max(0, i - 10)
    range_hi = max(h[look:i])
    range_lo = min(l[look:i])
    rng = range_hi - range_lo
    if rng <= 0:
        return 50.0, False
    tight = rng / max(1e-9, sum(h[j] - l[j] for j in range(look, i)) / max(1, i - look))
    compressed = tight < 1.15
    close = c[i]
    if bull_dir:
        broken = close > range_hi * 0.997
        escape = (close - range_hi) / rng if broken else (close - range_lo) / rng
    else:
        broken = close < range_lo * 1.003
        escape = (range_lo - close) / rng if broken else (range_hi - close) / rng
    score = 45.0
    if compressed and broken:
        score = 82.0
    elif broken:
        score = 68.0
    elif escape > 0.55:
        score = 58.0
    return _clamp(score), broken


def _vwap_displacement(h, l, c, v, i: int, *, bull_dir: bool) -> float:
    vwap_s = cumulative_vwap(h, l, c, v)
    if i < 3 or vwap_s[i] == 0:
        return 50.0
    dist_now = (c[i] - vwap_s[i]) / vwap_s[i] * 100
    dist_prev = (c[i - 2] - vwap_s[i - 2]) / vwap_s[i - 2] * 100 if vwap_s[i - 2] else dist_now
    delta = dist_now - dist_prev if bull_dir else dist_prev - dist_now
    if bull_dir and c[i] <= vwap_s[i]:
        return 35.0
    if not bull_dir and c[i] >= vwap_s[i]:
        return 35.0
    return _clamp(52 + delta * 25)


def _adx_di_scores(h, l, c, i: int, *, bull_dir: bool) -> tuple[float, float, bool]:
    if i < ADX_LEN + 6:
        return 50.0, 50.0, False
    di_p, di_m, adx = _dmi_adx(h, l, c, ADX_LEN)
    adx_now = float(adx[i] or 0)
    adx_prev = float(adx[i - 3] or adx_now)
    adx_slope = adx_now - adx_prev
    adx_accel = adx_slope > 1.5 and adx_now >= 16
    adx_score = _clamp(50 + adx_slope * 4 + (8 if adx_now >= 20 else 0))

    dip = float(di_p[i] or 0)
    dim = float(di_m[i] or 0)
    dip_p = float(di_p[i - 3] or dip)
    dim_p = float(di_m[i - 3] or dim)
    spread = (dip - dim) if bull_dir else (dim - dip)
    spread_prev = (dip_p - dim_p) if bull_dir else (dim_p - dip_p)
    spread_vel = spread - spread_prev
    di_score = _clamp(50 + spread * 0.8 + spread_vel * 2.5)
    return adx_score, di_score, adx_accel


def _obv_acceleration(c: List[float], v: List[float], i: int, *, bull_dir: bool) -> float:
    if i < 6:
        return 50.0
    obv = 0.0
    series = []
    for j in range(max(0, i - 12), i + 1):
        if j > 0:
            if c[j] > c[j - 1]:
                obv += v[j]
            elif c[j] < c[j - 1]:
                obv -= v[j]
        series.append(obv)
    if len(series) < 4:
        return 50.0
    vel = series[-1] - series[-3]
    prev_vel = series[-3] - series[-5] if len(series) >= 5 else 0
    accel = vel - prev_vel
    if bull_dir:
        return _clamp(50 + (vel / max(1, abs(series[-1]) or 1)) * 30 + accel * 0.000001 * 100)
    return _clamp(50 + (-vel / max(1, abs(series[-1]) or 1)) * 30)


def _impulse_efficiency(o, h, l, c, i: int, atr: float, *, bull_dir: bool) -> float:
    if i < 4 or atr <= 0:
        return 50.0
    move = (c[i] - c[i - 3]) if bull_dir else (c[i - 3] - c[i])
    path = sum(abs(c[j] - c[j - 1]) for j in range(i - 2, i + 1))
    if path <= 0:
        return 50.0
    efficiency = move / path if bull_dir else move / path
    body = abs(c[i] - o[i])
    rng = h[i] - l[i]
    close_pos = (c[i] - l[i]) / rng if rng > 0 else 0.5
    score = 45.0
    if efficiency > 0.55 and move > atr * 0.4:
        score += 25
    if bull_dir and close_pos > 0.6:
        score += 15
    elif not bull_dir and close_pos < 0.4:
        score += 15
    if body / rng > 0.55 if rng > 0 else False:
        score += 10
    return _clamp(score)


def compute_expansion_velocity(
    candles: Optional[Sequence[Dict[str, Any]]],
    *,
    bull_dir: bool = True,
) -> Optional[ExpansionVelocityResult]:
    if not candles or len(candles) < 30:
        return None
    o, h, l, c, v = _ohlcv(candles)
    i = len(c) - 1
    atr_s = _wilder_atr(h, l, c, 14)
    atr = float(atr_s[i] or 0)

    spread, range_exp = _candle_spread_score(h, l, c, i)
    escape, compression_broken = _compression_escape(h, l, c, i, bull_dir=bull_dir)
    vwap_disp = _vwap_displacement(h, l, c, v, i, bull_dir=bull_dir)
    adx_sc, di_sc, adx_accel = _adx_di_scores(h, l, c, i, bull_dir=bull_dir)
    obv_sc = _obv_acceleration(c, v, i, bull_dir=bull_dir)
    impulse = _impulse_efficiency(o, h, l, c, i, atr, bull_dir=bull_dir)

    evs = (
        spread * 0.18
        + escape * 0.22
        + vwap_disp * 0.18
        + adx_sc * 0.14
        + di_sc * 0.12
        + obv_sc * 0.08
        + impulse * 0.08
    )
    if compression_broken and range_exp:
        evs = min(100.0, evs + 8)
    if adx_accel and vwap_disp >= 55:
        evs = min(100.0, evs + 6)

    return ExpansionVelocityResult(
        evs_score=_clamp(evs),
        candle_spread_score=spread,
        compression_escape_score=escape,
        vwap_displacement_score=vwap_disp,
        adx_slope_score=adx_sc,
        di_spread_velocity_score=di_sc,
        obv_acceleration_score=obv_sc,
        impulse_efficiency_score=impulse,
        compression_broken=compression_broken,
        adx_accelerating=adx_accel,
        range_expanding=range_exp,
    )
