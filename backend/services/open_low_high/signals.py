"""Open Low High screening rules (Phase A first candle + Phase B confirmation).

Interpretation of confirmation (documented per product spec):
  User wording said "remove where second closes above/below" — we interpret that as
  **remove cards that FAIL confirmation**, i.e. keep only confirmed setups:
  - Bullish: KEEP if second candle close > first candle high; REMOVE otherwise.
  - Bearish: KEEP if second candle close < first candle low; REMOVE otherwise.

Phase A uses close-vs-prev-day extreme (not wick) plus VWAP + candle color,
which differs from the OpenHighLow *backtest* gates that use first.high/low wick.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


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

    BULLISH: close > prev day high AND close > VWAP AND green (close > open).
    BEARISH: close < prev day low AND close < VWAP AND red (close < open).
    If both would qualify (pathological), return None (ambiguous).
    """
    fo, fh, fl, fc = float(first_open), float(first_high), float(first_low), float(first_close)
    vwap = float(vwap_0915)
    ph, pl = float(prev_high), float(prev_low)

    green = fc > fo
    red = fc < fo
    bullish = fc > ph and fc > vwap and green
    bearish = fc < pl and fc < vwap and red

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
    second_close: float,
) -> bool:
    """Return True to KEEP the card; False to REMOVE (failed confirmation)."""
    side_u = str(side or "").upper()
    sc = float(second_close)
    if side_u == "BULLISH":
        return sc > float(first_high)
    if side_u == "BEARISH":
        return sc < float(first_low)
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
    second_close: Optional[float],
) -> Optional[PhaseAResult]:
    """Keep phase-A result only when Phase B confirmation passes."""
    if second_close is None:
        return None
    if not confirm_phase_b(
        phase_a.side,
        first_high=phase_a.first_high,
        first_low=phase_a.first_low,
        second_close=float(second_close),
    ):
        return None
    return phase_a

