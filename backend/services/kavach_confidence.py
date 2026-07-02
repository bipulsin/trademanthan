"""Kavach unified confidence grade — trade score + volume + VWAP purity."""
from __future__ import annotations

from typing import List, Literal, Optional, Tuple

REGIME_TREND = "TREND"
REGIME_TRANSITION = "TRANSITION"

VolumeLabel = Literal["High", "Average", "Low"]
PurityProven = bool  # >= 60%


def compute_vwap_purity_pct(
    closes: List[float],
    vwap_series: List[float],
    *,
    direction: str,
    num_bars: int = 8,
    bar_size: int = 2,
) -> float:
    """Count last ``num_bars`` completed synthetic 10m bars on signal side of VWAP.

    Resamples ``bar_size`` consecutive closes (default 2×5m → 10m). Uses bar close
    vs session VWAP at that bar's end.
    """
    if not closes or not vwap_series or len(closes) != len(vwap_series):
        return 0.0
    is_long = (direction or "LONG").upper() != "SHORT"
    n = len(closes)
    if n < bar_size:
        return 0.0
    # Build 10m-equivalent bar indices (end index of each pair).
    ends: List[int] = []
    i = bar_size - 1
    while i < n:
        ends.append(i)
        i += bar_size
    if not ends:
        return 0.0
    sample = ends[-num_bars:]
    on_side = 0
    for idx in sample:
        c = closes[idx]
        v = vwap_series[idx]
        if v <= 0:
            continue
        if is_long and c > v:
            on_side += 1
        elif not is_long and c < v:
            on_side += 1
    return round(on_side / len(sample) * 100.0, 1) if sample else 0.0


def detect_market_regime(
    *,
    st_prev: Optional[bool],
    st_curr: Optional[bool],
    macd_prev: float,
    macd_sig_prev: float,
    macd_curr: float,
    macd_sig_curr: float,
    ema5_prev: float,
    vwap_prev: float,
    ema5_curr: float,
    vwap_curr: float,
) -> str:
    """TRANSITION if ST, MACD, or EMA/VWAP relationship flipped this bar."""
    if st_prev is not None and st_curr is not None and st_prev != st_curr:
        return REGIME_TRANSITION
    macd_bull_prev = macd_prev > macd_sig_prev
    macd_bull_curr = macd_curr > macd_sig_curr
    if macd_bull_prev != macd_bull_curr:
        return REGIME_TRANSITION
    ema_above_prev = ema5_prev > vwap_prev if vwap_prev else False
    ema_above_curr = ema5_curr > vwap_curr if vwap_curr else False
    if ema_above_prev != ema_above_curr:
        return REGIME_TRANSITION
    return REGIME_TREND


def _purity_proven(purity_pct: float) -> bool:
    return purity_pct >= 60.0


def compute_confidence_grade(
    score: float,
    volume_label: VolumeLabel,
    purity_pct: float,
    regime: str = REGIME_TREND,
) -> Tuple[str, bool]:
    """Return (grade, transition_floor_applied).

    Grades: A+, A, B, C, D. Display C* when transition floor applied.
    """
    s = int(round(score))
    vol = volume_label
    pure = _purity_proven(purity_pct)
    transition_floor = False

    if s < 65:
        grade = "D"
    elif vol == "Low" and not pure:
        grade = "D"
    elif vol == "High" and pure and s >= 95:
        grade = "A+"
    elif vol == "High" and pure and s >= 85:
        grade = "A"
    elif vol == "High" and pure and s >= 75:
        grade = "B"
    elif vol == "Average" and pure and s >= 85:
        grade = "B"
    elif vol == "High" and not pure and s >= 85:
        grade = "C"
    elif vol == "Average" and pure and s >= 75:
        grade = "C"
    elif vol == "Low" and pure and s >= 85:
        grade = "C"
    else:
        grade = "D"

    if (
        grade == "D"
        and regime == REGIME_TRANSITION
        and s >= 75
    ):
        grade = "C"
        transition_floor = True

    return grade, transition_floor


def format_confidence_display(grade: str, transition_floor: bool) -> str:
    if transition_floor and grade == "C":
        return "C*"
    return grade


def format_quality_row(
    volume_label: VolumeLabel,
    purity_pct: float,
    score: float,
    regime: str,
) -> str:
    vol_s = "Vol+" if volume_label == "High" else ("Vol~" if volume_label == "Average" else "Vol-")
    pure_s = "Pure+" if _purity_proven(purity_pct) else "Pure-"
    regime_s = " TRANS" if regime == REGIME_TRANSITION else ""
    return f"{vol_s} {pure_s} {int(round(score))}{regime_s}".strip()


def confidence_passes_gate(
    grade: str,
    *,
    counter_rs: bool = False,
    maturity_a_only: bool = False,
    climactic: bool = False,
) -> bool:
    """Whether confidence satisfies checklist gate for with-RS entries."""
    g = (grade or "").replace("*", "").upper()
    if g == "D":
        return False
    if counter_rs or maturity_a_only or climactic:
        return g in ("A+", "A")
    return g in ("A+", "A", "B", "C")
