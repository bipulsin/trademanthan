"""Garuda screener — Part 1 imbalance + Part 2 direction/strength/trend/momentum."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import time as dtime
from typing import Any, Dict, List, Optional, Sequence

from backend.services.garuda_screener.config import GarudaConfig, RANK_METHOD, TOP_N, VWAP_SLOPE_THRESHOLD
from backend.services.garuda_screener.indicators import (
    avg_range,
    avg_volume,
    close_position,
    consecutive_directional_bars,
    efficiency_ratio,
    n_bar_high,
    n_bar_low,
    percentile_rank,
    roc,
    rolling_beta,
    sign,
    vwap_slope_score_from_series,
)


@dataclass
class GarudaBarContext:
    """Per-symbol bar series sliced up to evaluation index."""

    symbol: str
    idx: int
    bars: List[Dict[str, Any]]
    prior_session_close: float
    nifty_day_pct: float
    nifty_window_pct: float
    sector_window_pct: Optional[float]
    peer_window_pct: Optional[float]
    atr_daily_pct: float
    beta: Optional[float]
    bar_minutes: int = 10
    session_open_price: Optional[float] = None
    gap_filled: bool = False


def _series(bars: List[Dict[str, Any]], key: str) -> List[float]:
    return [float(b[key]) for b in bars]


def _imbalance_legs(
    ctx: GarudaBarContext,
    cfg: GarudaConfig,
) -> Dict[str, Any]:
    bars = ctx.bars
    i = ctx.idx
    b = bars[i]
    o, h, l, c = b["open"], b["high"], b["low"], b["close"]
    vol = float(b.get("volume") or 0)
    highs, lows, closes, opens, vols = (
        _series(bars, "high"),
        _series(bars, "low"),
        _series(bars, "close"),
        _series(bars, "open"),
        _series(bars, "volume"),
    )

    avg_r = avg_range(highs, lows, i, cfg.atr_len)
    bar_rng = h - l
    legs: Dict[str, Dict[str, Any]] = {}

    # 1 — range expansion
    re_long = re_short = False
    if avg_r and avg_r > 0:
        expanded = bar_rng >= cfg.range_expansion_mult * avg_r
        cp = close_position(c, h, l)
        if expanded and cp is not None:
            re_long = cp >= cfg.close_position_long
            re_short = cp <= cfg.close_position_short
    legs["range_expansion"] = {
        "pass_long": re_long,
        "pass_short": re_short,
        "ratio": round(bar_rng / avg_r, 3) if avg_r and avg_r > 0 else None,
    }

    # 2 — close position (standalone; often overlaps range_expansion)
    cp = close_position(c, h, l)
    cp_long = cp is not None and cp >= cfg.close_position_long
    cp_short = cp is not None and cp <= cfg.close_position_short
    legs["close_position"] = {
        "pass_long": cp_long,
        "pass_short": cp_short,
        "value": round(cp, 4) if cp is not None else None,
    }

    # 3 — volume breakout
    prev_hi = n_bar_high(highs, i, cfg.atr_len)
    prev_lo = n_bar_low(lows, i, cfg.atr_len)
    avg_v = avg_volume(vols, i, cfg.atr_len)
    vol_ok = avg_v and avg_v > 0 and vol >= cfg.volume_breakout_mult * avg_v
    vb_long = vb_short = False
    if vol_ok and prev_hi is not None and prev_lo is not None:
        vb_long = c > prev_hi
        vb_short = c < prev_lo
    legs["volume_breakout"] = {
        "pass_long": vb_long,
        "pass_short": vb_short,
        "vol_ratio": round(vol / avg_v, 3) if avg_v and avg_v > 0 else None,
    }

    # 4 — consecutive directional bars
    bull_n, bear_n = consecutive_directional_bars(opens, highs, lows, closes, i)
    cd_long = bull_n >= cfg.consec_dir_bars
    cd_short = bear_n >= cfg.consec_dir_bars
    legs["consecutive_direction"] = {
        "pass_long": cd_long,
        "pass_short": cd_short,
        "bull_count": bull_n,
        "bear_count": bear_n,
    }

    # 5 — gap-and-hold (bonus, pre-10:00)
    gap_long = gap_short = False
    bar_time = b["bar_end"].time() if hasattr(b["bar_end"], "time") else None
    pre_10 = bar_time is not None and bar_time <= dtime(10, 0)
    if pre_10 and ctx.prior_session_close and ctx.session_open_price:
        gap_pct = (ctx.session_open_price - ctx.prior_session_close) / ctx.prior_session_close * 100.0
        gap_atr = ctx.atr_daily_pct * cfg.gap_atr_mult
        if not ctx.gap_filled:
            if gap_pct >= gap_atr:
                gap_long = True
            elif gap_pct <= -gap_atr:
                gap_short = True
    legs["gap_and_hold"] = {
        "pass_long": gap_long,
        "pass_short": gap_short,
        "bonus_only": True,
        "applicable": pre_10,
    }

    # 6 — RS / sector divergence
    stock_win = None
    if i >= cfg.rs_window_bars:
        base = closes[i - cfg.rs_window_bars]
        if base:
            stock_win = (c - base) / base * 100.0
    rs_long = rs_short = False
    if stock_win is not None:
        nifty_excess = stock_win - ctx.nifty_window_pct
        sector_ref = ctx.sector_window_pct if ctx.sector_window_pct is not None else ctx.peer_window_pct
        sector_excess = (stock_win - sector_ref) if sector_ref is not None else nifty_excess
        rs_long = nifty_excess > 0 and sector_excess > 0
        rs_short = nifty_excess < 0 and sector_excess < 0
    legs["rs_sector_divergence"] = {
        "pass_long": rs_long,
        "pass_short": rs_short,
        "stock_window_pct": round(stock_win, 4) if stock_win is not None else None,
        "nifty_window_pct": round(ctx.nifty_window_pct, 4),
        "sector_window_pct": round(ctx.sector_window_pct, 4) if ctx.sector_window_pct is not None else None,
    }

    return legs


def _imbalance_confirmed(legs: Dict[str, Dict[str, Any]]) -> tuple[bool, str, List[str]]:
    required = ["range_expansion", "close_position", "volume_breakout", "consecutive_direction", "rs_sector_divergence"]
    long_hits = [k for k in required if legs[k]["pass_long"]]
    short_hits = [k for k in required if legs[k]["pass_short"]]
    if len(long_hits) >= 2:
        return True, "LONG", long_hits
    if len(short_hits) >= 2:
        return True, "SHORT", short_hits
    return False, "NEUTRAL", []


def _direction_read(bars: List[Dict[str, Any]], idx: int, lookback: int) -> Dict[str, Any]:
    b = bars[idx]
    c = b["close"]
    vwap = b.get("vwap")
    ema5 = b.get("ema5")
    ema10 = b.get("ema10")
    closes = _series(bars, "close")

    s_vwap = sign(c - float(vwap)) if vwap is not None else 0
    s_ema = sign(float(ema5) - float(ema10)) if ema5 is not None and ema10 is not None else 0
    s_roc = 0
    if idx >= lookback:
        s_roc = sign(c - closes[idx - lookback])

    signs = [s for s in (s_vwap, s_ema, s_roc) if s != 0]
    agree = len(set(signs)) == 1 and len(signs) >= 2
    side = "LONG" if sum(signs) > 0 else ("SHORT" if sum(signs) < 0 else "NEUTRAL")

    return {
        "sign_vwap": s_vwap,
        "sign_ema5_10": s_ema,
        "sign_close_n": s_roc,
        "agreement": agree,
        "side": side,
    }


def evaluate_symbol(
    ctx: GarudaBarContext,
    *,
    cfg: Optional[GarudaConfig] = None,
    cross_section: Optional[Dict[str, List[float]]] = None,
) -> Optional[Dict[str, Any]]:
    """Evaluate one symbol at ctx.idx. cross_section supplies universe lists for percentiles."""
    cfg = cfg or GarudaConfig()
    bars = ctx.bars
    i = ctx.idx
    if i < cfg.atr_len or i >= len(bars):
        return None

    b = bars[i]
    legs = _imbalance_legs(ctx, cfg)
    confirmed, imb_side, imb_hits = _imbalance_confirmed(legs)

    direction = _direction_read(bars, i, cfg.direction_lookback)
    closes = _series(bars, "close")
    c = b["close"]

    # Strength — day RS + window RS
    day_rs = None
    if ctx.prior_session_close:
        stock_day = (c - ctx.prior_session_close) / ctx.prior_session_close * 100.0
        day_rs = stock_day - ctx.nifty_day_pct

    beta = ctx.beta if ctx.beta is not None else 1.0
    beta_adj_rs = None
    if day_rs is not None:
        beta_adj_rs = day_rs - beta * ctx.nifty_day_pct

    strength_pct = None
    if cross_section and day_rs is not None:
        strength_pct = percentile_rank(day_rs, cross_section.get("day_rs") or [])

    # Trend
    adx = b.get("adx")
    adx_f = float(adx) if adx is not None else None
    adx_prev = bars[i - cfg.adx_slope_lookback].get("adx") if i >= cfg.adx_slope_lookback else None
    adx_slope = (adx_f - float(adx_prev)) if adx_f is not None and adx_prev is not None else None
    er = efficiency_ratio(closes, i, cfg.er_len)

    # Momentum
    roc3 = roc(closes, i, cfg.roc_len_primary)
    roc5 = roc(closes, i, cfg.roc_len_alt)
    roc3_prev = roc(closes, i - 1, cfg.roc_len_primary) if i > cfg.roc_len_primary else None
    acceleration = (roc3 - roc3_prev) if roc3 is not None and roc3_prev is not None else None
    avg_v = avg_volume(_series(bars, "volume"), i, cfg.atr_len)
    vol = float(b.get("volume") or 0)
    vol_w_mom = (roc3 * (vol / avg_v)) if roc3 is not None and avg_v and avg_v > 0 else None

    momentum_pct = None
    if cross_section and roc3 is not None:
        momentum_pct = percentile_rank(roc3, cross_section.get("roc3") or [], higher_is_better=True)
        # For shorts, high negative ROC is good — use signed magnitude via abs rank on negative side
        if imb_side == "SHORT":
            momentum_pct = percentile_rank(-roc3, cross_section.get("roc3_neg") or [-x for x in (cross_section.get("roc3") or []) if x is not None])

    vwap_s = _series(bars, "vwap") if all(b.get("vwap") is not None for b in bars[: i + 1]) else []
    vwap_steep = 0.0
    if vwap_s:
        vwap_steep = vwap_slope_score_from_series(vwap_s, i, c, ctx.atr_daily_pct)

    side = imb_side if confirmed else direction["side"]
    if side == "NEUTRAL" and confirmed:
        side = imb_side

    rank_score = None
    if strength_pct is not None and momentum_pct is not None:
        rank_score = (strength_pct + momentum_pct) / 2.0

    return {
        "symbol": ctx.symbol,
        "bar_end": b["bar_end"].isoformat() if hasattr(b["bar_end"], "isoformat") else str(b["bar_end"]),
        "price": round(c, 4),
        "imbalance_confirmed": confirmed,
        "imbalance_side": imb_side,
        "imbalance_hits": imb_hits,
        "imbalance_legs": legs,
        "side": side,
        "direction": direction,
        "strength": {
            "day_rs": round(day_rs, 4) if day_rs is not None else None,
            "beta": round(beta, 4) if ctx.beta is not None else None,
            "beta_fallback": ctx.beta is None,
            "beta_adj_rs": round(beta_adj_rs, 4) if beta_adj_rs is not None else None,
            "percentile": round(strength_pct, 2) if strength_pct is not None else None,
        },
        "trend": {
            "adx": round(adx_f, 3) if adx_f is not None else None,
            "adx_slope": round(adx_slope, 3) if adx_slope is not None else None,
            "efficiency_ratio": round(er, 4) if er is not None else None,
        },
        "momentum": {
            "roc3": round(roc3, 6) if roc3 is not None else None,
            "roc5": round(roc5, 6) if roc5 is not None else None,
            "acceleration": round(acceleration, 6) if acceleration is not None else None,
            "vol_weighted": round(vol_w_mom, 6) if vol_w_mom is not None else None,
            "percentile_roc3": round(momentum_pct, 2) if momentum_pct is not None else None,
            "vwap_slope_score": round(vwap_steep, 2),
            "vwap_slope_pass_50": vwap_steep >= VWAP_SLOPE_THRESHOLD,
        },
        "rank_score": round(rank_score, 2) if rank_score is not None else None,
        "ema10": b.get("ema10"),
        "atr14_pct": ctx.atr_daily_pct,
    }


def rank_top_n(
    rows: List[Dict[str, Any]],
    *,
    top_n: int = TOP_N,
    rank_method: str = RANK_METHOD,
) -> Dict[str, Any]:
    """Rank imbalance-confirmed symbols; return Top-N overall (long+short combined)."""
    pool = [r for r in rows if r.get("imbalance_confirmed")]
    pool.sort(
        key=lambda r: (
            r.get("rank_score") if r.get("rank_score") is not None else -1,
            r.get("strength", {}).get("percentile") or 0,
            r.get("momentum", {}).get("percentile_roc3") or 0,
        ),
        reverse=True,
    )
    top = []
    for rank, r in enumerate(pool[:top_n], 1):
        top.append({**r, "rank": rank})
    return {
        "rank_method": rank_method,
        "n_evaluated": len(rows),
        "n_imbalance_confirmed": len(pool),
        "top_n": top,
        "top_symbols": [r["symbol"] for r in top],
    }
