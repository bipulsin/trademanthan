"""Open Low High screening rules (Phase A first candle + Phase B confirmation).

Interpretation of confirmation (documented per product spec):
  Keep only confirmed setups after the completed 09:20 five-minute candle:
  - Bullish: KEEP if 09:20 **high** > 09:15 **high**; REMOVE otherwise.
  - Bearish: KEEP if 09:20 **low** < 09:15 **low**; REMOVE otherwise.

  After confirmation, a 09:20 wick/body filter applies (see
  ``passes_second_candle_wick_filter``).

Phase A uses close-vs-prev-day extreme (not wick) plus VWAP + candle color.
Doji (close == open) is allowed: bearish accepts close <= open, bullish
close >= open. This differs from the OpenHighLow *backtest* gates that use
first.high/low wick.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class PhaseAResult:
    side: str  # BULLISH | BEARISH
    first_open: float
    first_high: float
    first_low: float
    first_close: float
    vwap_0915: float
    prev_high: float
    prev_low: float
    prev_close: Optional[float] = None


def evaluate_phase_a(
    *,
    first_open: float,
    first_high: float,
    first_low: float,
    first_close: float,
    vwap_0915: float,
    prev_high: float,
    prev_low: float,
    prev_close: Optional[float] = None,
) -> Optional[PhaseAResult]:
    """Qualify on the completed 09:15 five-minute candle only.

    BULLISH: close > prev day high AND close > VWAP AND non-red (close >= open).
    BEARISH: close < prev day low AND close < VWAP AND non-green (close <= open).
    Doji (close == open) is accepted for both sides when the other gates pass.
    If both would qualify (pathological), return None (ambiguous).
    """
    fo, fh, fl, fc = float(first_open), float(first_high), float(first_low), float(first_close)
    vwap = float(vwap_0915)
    ph, pl = float(prev_high), float(prev_low)

    non_red = fc >= fo  # green or doji
    non_green = fc <= fo  # red or doji
    bullish = fc > ph and fc > vwap and non_red
    bearish = fc < pl and fc < vwap and non_green

    if bullish and bearish:
        return None
    if bullish:
        return PhaseAResult(
            side="BULLISH",
            first_open=fo,
            first_high=fh,
            first_low=fl,
            first_close=fc,
            vwap_0915=vwap,
            prev_high=ph,
            prev_low=pl,
            prev_close=float(prev_close) if prev_close is not None else None,
        )
    if bearish:
        return PhaseAResult(
            side="BEARISH",
            first_open=fo,
            first_high=fh,
            first_low=fl,
            first_close=fc,
            vwap_0915=vwap,
            prev_high=ph,
            prev_low=pl,
            prev_close=float(prev_close) if prev_close is not None else None,
        )
    return None


def confirm_phase_b(
    side: str,
    *,
    first_high: float,
    first_low: float,
    second_high: float,
    second_low: float,
) -> bool:
    """Return True to KEEP the card; False to REMOVE (failed extreme confirmation).

    Bullish: 09:20 high must make a higher high vs 09:15 high.
    Bearish: 09:20 low must make a lower low vs 09:15 low.
    """
    side_u = str(side or "").upper()
    if side_u == "BULLISH":
        return float(second_high) > float(first_high)
    if side_u == "BEARISH":
        return float(second_low) < float(first_low)
    return False


def passes_second_candle_wick_filter(
    side: str,
    *,
    second_open: float,
    second_high: float,
    second_low: float,
    second_close: float,
    max_wick_body_ratio: float = 0.35,
) -> bool:
    """09:20 wick/body filter — True to KEEP, False to REMOVE.

    Body = abs(close - open). Zero-body (doji) fails the filter.
    Bullish: upper_wick / body <= max ratio (default 0.35).
    Bearish: lower_wick / body <= max ratio (default 0.35).
    """
    o = float(second_open)
    h = float(second_high)
    l = float(second_low)
    c = float(second_close)
    body = abs(c - o)
    if body == 0:
        return False
    side_u = str(side or "").upper()
    limit = float(max_wick_body_ratio)
    if side_u == "BULLISH":
        upper_wick = h - max(o, c)
        return (upper_wick / body) <= limit
    if side_u == "BEARISH":
        lower_wick = min(o, c) - l
        return (lower_wick / body) <= limit
    return False


def freeze_tags_immutable(
    tags: Dict[str, Any],
    *,
    frozen: bool,
) -> Dict[str, Any]:
    """After freeze, sector tags must not mutate even if live sector ranks move.

    When ``frozen`` is True, return a shallow copy; callers must treat the
    returned dict as the sole source of truth for the day (no live re-badge).
    """
    out = dict(tags or {})
    out["frozen"] = bool(frozen)
    # Snapshot display fields so UI never recomputes from live movers.
    badge = out.get("sector_badge")
    star = bool(out.get("sector_star"))
    if badge and not out.get("sector_badge_display"):
        out["sector_badge_display"] = f"{badge}*" if star else str(badge)
    return out


def apply_confirmation_filter(
    phase_a: PhaseAResult,
    *,
    second_open: Optional[float],
    second_high: Optional[float],
    second_low: Optional[float],
    second_close: Optional[float],
) -> Optional[PhaseAResult]:
    """Keep phase-A result only when Phase B extremes + 09:20 wick filter pass."""
    if (
        second_open is None
        or second_high is None
        or second_low is None
        or second_close is None
    ):
        return None
    if not confirm_phase_b(
        phase_a.side,
        first_high=phase_a.first_high,
        first_low=phase_a.first_low,
        second_high=float(second_high),
        second_low=float(second_low),
    ):
        return None
    if not passes_second_candle_wick_filter(
        phase_a.side,
        second_open=float(second_open),
        second_high=float(second_high),
        second_low=float(second_low),
        second_close=float(second_close),
    ):
        return None
    return phase_a


def traditional_pivot_levels(
    prev_high: float,
    prev_low: float,
    prev_close: float,
) -> Dict[str, float]:
    """Classic floor trader pivots from prior-day H/L/C.

    PP = (H + L + C) / 3
    R1 = 2*PP - L;  R2 = PP + (H - L);  R3 = H + 2*(PP - L)
    S1 = 2*PP - H;  S2 = PP - (H - L);  S3 = L - 2*(H - PP)
    """
    h, l, c = float(prev_high), float(prev_low), float(prev_close)
    pp = (h + l + c) / 3.0
    r1 = 2.0 * pp - l
    s1 = 2.0 * pp - h
    r2 = pp + (h - l)
    s2 = pp - (h - l)
    r3 = h + 2.0 * (pp - l)
    s3 = l - 2.0 * (h - pp)
    return {
        "pp": pp,
        "r1": r1,
        "r2": r2,
        "r3": r3,
        "s1": s1,
        "s2": s2,
        "s3": s3,
    }


def next_pivot_targets(
    side: str,
    *,
    prev_high: float,
    prev_low: float,
    prev_close: float,
    reference: float,
    count: int = 2,
) -> Tuple[Optional[float], Optional[float]]:
    """Next ``count`` traditional pivot levels in the trade direction.

    Bullish (long): ascending levels strictly above ``reference`` (resistances / PP).
    Bearish (short): descending levels strictly below ``reference`` (supports / PP).
    Returns (t1, t2) — missing slots are None when fewer levels remain.
    """
    levels = traditional_pivot_levels(prev_high, prev_low, prev_close)
    ordered = sorted(
        {round(v, 6) for v in levels.values()},
        key=lambda x: x,
    )
    ref = float(reference)
    side_u = str(side or "").upper()
    if side_u == "BULLISH":
        ahead = [v for v in ordered if v > ref]
    elif side_u == "BEARISH":
        ahead = [v for v in reversed(ordered) if v < ref]
    else:
        return None, None
    t1 = ahead[0] if len(ahead) >= 1 else None
    t2 = ahead[1] if len(ahead) >= 2 else None
    if count <= 1:
        return t1, None
    return t1, t2
