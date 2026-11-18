"""Open Low High screening rules (Phase A first candle + Phase B confirmation).

Interpretation of confirmation (documented per product spec):
  Keep only confirmed setups after the completed 09:20 five-minute candle:
  - Bullish: KEEP if 09:20 **close** >= 09:15 **close**; REMOVE otherwise.
  - Bearish: KEEP if 09:20 **close** <= 09:15 **close**; REMOVE otherwise.

  After close confirmation, a 09:20 wick/body filter applies (see
  ``passes_second_candle_wick_filter``).

Phase A uses close-vs-prev-day extreme (not wick) plus VWAP + candle color.
Doji (close == open) is allowed: bearish accepts close <= open, bullish
close >= open. This differs from the OpenHighLow *backtest* gates that use
first.high/low wick.

O=L / O=H badge (equity 09:15 only):
  - Bullish & open == low  → "O=L"
  - Bearish & open == high → "O=H"

NIFTY header cue (index as of ~09:20):
  - Gap: 09:15 open vs prior session close (up / down / flat).
  - Bias: 09:20 close vs 09:15 close (bullish / bearish / flat).
  - Show green "NIFTY" on Bullish header when bias=bullish and gap=up.
  - Show red "NIFTY" on Bearish header when bias=bearish and gap=down.
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


@dataclass(frozen=True)
class PhaseBResult:
    ok: bool
    fail_reason: Optional[str] = None


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
    first_close: float,
    second_close: float,
    # Legacy kwargs ignored (kept so older call sites don't explode during rollout).
    first_high: Optional[float] = None,
    first_low: Optional[float] = None,
    second_high: Optional[float] = None,
    second_low: Optional[float] = None,
) -> bool:
    """Return True to KEEP the card; False to REMOVE (failed close confirmation).

    Bullish: 09:20 close must be >= 09:15 close.
    Bearish: 09:20 close must be <= 09:15 close.
    """
    del first_high, first_low, second_high, second_low
    side_u = str(side or "").upper()
    c15 = float(first_close)
    c20 = float(second_close)
    if side_u == "BULLISH":
        return c20 >= c15
    if side_u == "BEARISH":
        return c20 <= c15
    return False


def evaluate_phase_b(
    side: str,
    *,
    first_close: float,
    second_open: Optional[float],
    second_high: Optional[float],
    second_low: Optional[float],
    second_close: Optional[float],
    max_wick_body_ratio: float = 0.35,
) -> PhaseBResult:
    """Full Phase B gate with a Trade Log–friendly fail reason."""
    if (
        second_open is None
        or second_high is None
        or second_low is None
        or second_close is None
    ):
        return PhaseBResult(ok=False, fail_reason="missing 09:20 bar")
    if not confirm_phase_b(
        side,
        first_close=float(first_close),
        second_close=float(second_close),
    ):
        return PhaseBResult(ok=False, fail_reason="close not confirming")
    if not passes_second_candle_wick_filter(
        side,
        second_open=float(second_open),
        second_high=float(second_high),
        second_low=float(second_low),
        second_close=float(second_close),
        max_wick_body_ratio=max_wick_body_ratio,
    ):
        return PhaseBResult(ok=False, fail_reason="wick too large")
    return PhaseBResult(ok=True, fail_reason=None)


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


def open_extreme_badge(
    side: str,
    *,
    first_open: float,
    first_high: float,
    first_low: float,
) -> Optional[str]:
    """Equity 09:15 O=L (bullish) / O=H (bearish) marker, else None."""
    side_u = str(side or "").upper()
    o = float(first_open)
    h = float(first_high)
    l = float(first_low)
    if side_u == "BULLISH" and o == l:
        return "O=L"
    if side_u == "BEARISH" and o == h:
        return "O=H"
    return None


def evaluate_nifty_cue(
    *,
    prev_close: Optional[float],
    first_open: Optional[float],
    first_close: Optional[float],
    second_close: Optional[float],
) -> Dict[str, Any]:
    """NIFTY gap + bias as of ~09:20 for subsection header badges.

    Rules:
      gap_up   → 09:15 open > prior close
      gap_down → 09:15 open < prior close
      flat     → open == prior close (or missing inputs)
      bullish  → 09:20 close > 09:15 close
      bearish  → 09:20 close < 09:15 close
      flat     → equal closes (or missing)

    Badge:
      show on Bullish header when bullish + gap_up
      show on Bearish header when bearish + gap_down
    """
    gap = "flat"
    bias = "flat"
    if (
        prev_close is not None
        and first_open is not None
        and float(prev_close) != 0
    ):
        o = float(first_open)
        pc = float(prev_close)
        if o > pc:
            gap = "up"
        elif o < pc:
            gap = "down"
    if first_close is not None and second_close is not None:
        c15 = float(first_close)
        c20 = float(second_close)
        if c20 > c15:
            bias = "bullish"
        elif c20 < c15:
            bias = "bearish"

    show_on: Optional[str] = None
    if bias == "bullish" and gap == "up":
        show_on = "BULLISH"
    elif bias == "bearish" and gap == "down":
        show_on = "BEARISH"

    return {
        "nifty_gap": gap,
        "nifty_bias": bias,
        "nifty_show_on": show_on,
        "nifty_prev_close": float(prev_close) if prev_close is not None else None,
        "nifty_0915_open": float(first_open) if first_open is not None else None,
        "nifty_0915_close": float(first_close) if first_close is not None else None,
        "nifty_0920_close": float(second_close) if second_close is not None else None,
    }


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
    """Keep phase-A result only when Phase B close confirm + 09:20 wick filter pass."""
    pb = evaluate_phase_b(
        phase_a.side,
        first_close=phase_a.first_close,
        second_open=second_open,
        second_high=second_high,
        second_low=second_low,
        second_close=second_close,
    )
    return phase_a if pb.ok else None


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
