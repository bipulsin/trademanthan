"""Score components 0–100 for Volume Mismatch signals."""
from __future__ import annotations

from typing import Optional

from backend.services.volume_mismatch.constants import DEFAULT_GAP_THRESHOLD_PCT


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def gap_score(gap_percent: float, threshold: float = DEFAULT_GAP_THRESHOLD_PCT) -> float:
    """0–25 from absolute gap magnitude vs threshold."""
    if threshold <= 0:
        return 0.0
    ratio = abs(gap_percent) / threshold
    return round(_clamp(ratio * 12.5, 0, 25), 2)


def mismatch_score(net_volume: float, first_15m_volume: float) -> float:
    """0–35 from net volume share of first 15m volume."""
    if first_15m_volume <= 0:
        return 0.0
    share = abs(net_volume) / first_15m_volume
    return round(_clamp(share * 35, 0, 35), 2)


def relative_volume_score(relative_volume: Optional[float]) -> float:
    """0–25 from today's first 15m vs 20-day average."""
    if relative_volume is None or relative_volume <= 0:
        return 0.0
    # 1.0x -> 10, 2.0x -> 20, 3.0x+ -> 25
    return round(_clamp((relative_volume - 0.5) * 12.5, 0, 25), 2)


def range_score(first_open: float, first_high: float, first_low: float) -> float:
    """0–15 from first 15m range expansion."""
    if first_open <= 0 or first_high <= first_low:
        return 0.0
    range_pct = ((first_high - first_low) / first_open) * 100.0
    return round(_clamp(range_pct * 3.0, 0, 15), 2)


def total_score(
    gap_percent: float,
    net_volume: float,
    first_15m_volume: float,
    relative_volume: Optional[float],
    first_open: float,
    first_high: float,
    first_low: float,
    *,
    gap_threshold: float = DEFAULT_GAP_THRESHOLD_PCT,
) -> float:
    gs = gap_score(gap_percent, gap_threshold)
    ms = mismatch_score(net_volume, first_15m_volume)
    rs = relative_volume_score(relative_volume)
    rng = range_score(first_open, first_high, first_low)
    return round(gs + ms + rs + rng, 2)
