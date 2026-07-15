"""Kavach engine — reusable Kavach state + composite Trade Score scoring.

This module is intentionally I/O-free and dependency-light so it can be reused by
the Relative Strength Scanner, Alerts, Strategy Builder, Backtesting and Mobile APIs.

All functions are pure and unit-testable: feed them already-computed indicator
values and they return Kavach state / score components.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# Kavach states
STATE_BUY = "BUY"
STATE_READY = "READY"
STATE_WATCH = "WATCH"
STATE_SELL = "SELL"
STATE_READY_SHORT = "READY SHORT"
STATE_WATCH_SHORT = "WATCH SHORT"
STATE_NEUTRAL = "NEUTRAL"

BULLISH_STATES = (STATE_BUY, STATE_READY, STATE_WATCH)
BEARISH_STATES = (STATE_SELL, STATE_READY_SHORT, STATE_WATCH_SHORT)

RANKING_BULLISH = "BULLISH"
RANKING_BEARISH = "BEARISH"


@dataclass
class KavachInput:
    """Indicator snapshot for one symbol at scan time."""

    price: float
    ema5: float
    ema9: float
    ema9_slope: float
    vwap: float
    supertrend_bullish: Optional[bool]  # True=bull, False=bear, None=unknown
    macd: float
    macd_signal: float
    macd_histogram: float
    adx: float
    volume_ratio: float


@dataclass
class KavachResult:
    bullish_count: int
    bearish_count: int
    state: str
    strength: int  # condition count backing the assigned state


def evaluate_kavach(inp: KavachInput) -> KavachResult:
    """Count Bullish / Bearish conditions and assign a Kavach state.

    The 10 directional/confirmation conditions follow the Kavach spec. ADX>20 and
    Volume Ratio>1 are non-directional confirmations and so count for both sides.
    """
    bullish = [
        inp.ema5 > inp.vwap,
        inp.ema5 > inp.ema9,
        inp.ema9_slope > 0,
        inp.price > inp.ema5,
        inp.price > inp.vwap,
        inp.supertrend_bullish is True,
        inp.macd > inp.macd_signal,
        inp.macd_histogram > 0,
        inp.adx > 20,
        inp.volume_ratio > 1,
    ]
    bearish = [
        inp.ema5 < inp.vwap,
        inp.ema5 < inp.ema9,
        inp.ema9_slope < 0,
        inp.price < inp.ema5,
        inp.price < inp.vwap,
        inp.supertrend_bullish is False,
        inp.macd < inp.macd_signal,
        inp.macd_histogram < 0,
        inp.adx > 20,
        inp.volume_ratio > 1,
    ]
    bull_n = sum(1 for c in bullish if c)
    bear_n = sum(1 for c in bearish if c)

    # Bullish branch takes precedence (per spec ordering); directional conditions
    # are mutually exclusive so overlap is limited to the 2 shared confirmations.
    if bull_n >= 7:
        return KavachResult(bull_n, bear_n, STATE_BUY, bull_n)
    if bull_n >= 5:
        return KavachResult(bull_n, bear_n, STATE_READY, bull_n)
    if bear_n >= 7:
        return KavachResult(bull_n, bear_n, STATE_SELL, bear_n)
    if bear_n >= 5:
        return KavachResult(bull_n, bear_n, STATE_READY_SHORT, bear_n)
    if bull_n >= 3:
        return KavachResult(bull_n, bear_n, STATE_WATCH, bull_n)
    if bear_n >= 3:
        return KavachResult(bull_n, bear_n, STATE_WATCH_SHORT, bear_n)
    return KavachResult(bull_n, bear_n, STATE_NEUTRAL, max(bull_n, bear_n))


# --- Trade Score components (max 100) ---------------------------------------
#
# The score measures signal strength for the *ranking direction*. For BEARISH
# ranking we mirror the directional components (use |RS| and reward price below
# VWAP) so the 60–90+ colour bands and ranking remain meaningful on both cards;
# otherwise every short would cap at ~55 and read as "grey". Non-directional
# components (Kavach / Volume / ADX) are identical for both sides.


def relative_strength_score(rs: float, ranking_type: str) -> int:
    """RS component, out of 40. Uses signed RS for bullish, |RS| for bearish."""
    v = rs if ranking_type == RANKING_BULLISH else -rs
    if v > 1.0:
        return 40
    if v >= 0.75:
        return 35
    if v >= 0.50:
        return 30
    if v >= 0.25:
        return 20
    if v >= 0.0:
        return 10
    return 0


def kavach_score(state: str) -> int:
    """Kavach component, out of 30."""
    return {
        STATE_BUY: 30,
        STATE_READY: 22,
        STATE_WATCH: 12,
        STATE_SELL: 30,
        STATE_READY_SHORT: 22,
        STATE_WATCH_SHORT: 12,
    }.get(state, 0)


def volume_ratio_score(volume_ratio: float) -> int:
    """Volume component, out of 15."""
    if volume_ratio > 2.0:
        return 15
    if volume_ratio >= 1.5:
        return 12
    if volume_ratio >= 1.0:
        return 8
    return 0


def adx_score(adx: float) -> int:
    """ADX component, out of 10."""
    if adx > 30.0:
        return 10
    if adx >= 25.0:
        return 8
    if adx >= 20.0:
        return 5
    return 0


def vwap_score(price: float, vwap: float, ranking_type: str) -> int:
    """VWAP component, out of 5 (rewards favourable side for the direction)."""
    if ranking_type == RANKING_BULLISH:
        return 5 if price > vwap else 0
    return 5 if price < vwap else 0


def compute_trade_score(
    *,
    rs: float,
    state: str,
    volume_ratio: float,
    adx: float,
    price: float,
    vwap: float,
    ranking_type: str,
    vwap_steep_persist_bars: int = 0,
) -> int:
    """Composite Trade Score (0–100) for the given ranking direction.

    Optional ``vwap_steep_persist_bars``: when ≥3 consecutive 5m bars hold
    slope ≥50, apply a small additive bump (VWAP_PERSIST_SCORE_BUMP, default 5).
    """
    total = (
        relative_strength_score(rs, ranking_type)
        + kavach_score(state)
        + volume_ratio_score(volume_ratio)
        + adx_score(adx)
        + vwap_score(price, vwap, ranking_type)
    )
    if int(vwap_steep_persist_bars or 0) >= 3:
        try:
            from backend.services.vwap_adx_promotion import vwap_persist_score_bump

            total += int(vwap_persist_score_bump())
        except Exception:
            total += 5
    return min(100, total)
