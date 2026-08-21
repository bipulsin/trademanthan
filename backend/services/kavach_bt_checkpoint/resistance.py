"""BT-2 — resistance confluence (warning-only), ±0.2% pivot zones."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.services.kavach_bt_checkpoint.config import (
    CLUSTER_MIN_INTERACTIONS,
    DANGER_ZONE_PCT,
    PIVOT_LEFT,
    PIVOT_RIGHT,
    PIVOT_ZONE_PCT,
)


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def detect_pivot_levels(
    bars: List[Dict[str, Any]],
    *,
    left: int = PIVOT_LEFT,
    right: int = PIVOT_RIGHT,
) -> List[Dict[str, Any]]:
    """Swing high/low pivots confirmed with `right` bars after the pivot bar."""
    highs = [_f(b["high"]) or 0.0 for b in bars]
    lows = [_f(b["low"]) or 0.0 for b in bars]
    n = len(bars)
    pivots: List[Dict[str, Any]] = []
    for i in range(left, n - right):
        h = highs[i]
        l = lows[i]
        if all(h >= highs[i - j] for j in range(1, left + 1)) and all(
            h > highs[i + j] for j in range(1, right + 1)
        ):
            pivots.append(
                {
                    "idx": i,
                    "level": h,
                    "kind": "resistance",
                    "bar_end": bars[i].get("bar_end"),
                }
            )
        if all(l <= lows[i - j] for j in range(1, left + 1)) and all(
            l < lows[i + j] for j in range(1, right + 1)
        ):
            pivots.append(
                {
                    "idx": i,
                    "level": l,
                    "kind": "support",
                    "bar_end": bars[i].get("bar_end"),
                }
            )
    return pivots


def _zone_hits(
    bars: List[Dict[str, Any]],
    level: float,
    *,
    up_to_idx: int,
    zone_pct: float = PIVOT_ZONE_PCT,
) -> int:
    """Count prior open/close interactions near the pivot zone."""
    if level <= 0:
        return 0
    lo = level * (1.0 - zone_pct / 100.0)
    hi = level * (1.0 + zone_pct / 100.0)
    hits = 0
    for i in range(max(0, up_to_idx)):
        o = _f(bars[i].get("open"))
        c = _f(bars[i].get("close"))
        for px in (o, c):
            if px is not None and lo <= px <= hi:
                hits += 1
                break
    return hits


def evaluate_resistance_confluence(
    bars: List[Dict[str, Any]],
    *,
    entry_idx: int,
    entry_price: float,
    direction: str,
) -> Dict[str, Any]:
    """Warning-only resistance confluence at entry.

    Active when a relevant pivot is within DANGER_ZONE_PCT of entry and
    ≥ CLUSTER_MIN_INTERACTIONS prior open/close touches exist in the ±0.2% zone.
    """
    is_long = str(direction).upper() in ("LONG", "BUY", "B")
    if entry_idx < 0 or not bars or entry_price <= 0:
        return {
            "res_confluence": False,
            "nearest_pivot": None,
            "pivot_kind": None,
            "pivot_zone_pct": PIVOT_ZONE_PCT,
            "cluster_n": 0,
            "dist_pct": None,
        }

    # Only use pivots confirmed at or before entry
    slice_bars = bars[: entry_idx + 1]
    pivots = detect_pivot_levels(slice_bars)
    relevant = []
    for p in pivots:
        if is_long and p["kind"] != "resistance":
            continue
        if not is_long and p["kind"] != "support":
            continue
        # For longs, resistance above or near entry; for shorts, support below/near
        level = float(p["level"])
        dist_pct = abs(level - entry_price) / entry_price * 100.0
        if dist_pct > DANGER_ZONE_PCT * 3:  # ignore far levels early filter
            continue
        # Directional relevance: long cares about levels above or slightly below
        if is_long and level < entry_price * (1 - DANGER_ZONE_PCT / 100.0):
            continue
        if not is_long and level > entry_price * (1 + DANGER_ZONE_PCT / 100.0):
            continue
        cluster_n = _zone_hits(slice_bars, level, up_to_idx=entry_idx)
        relevant.append({**p, "dist_pct": dist_pct, "cluster_n": cluster_n})

    if not relevant:
        return {
            "res_confluence": False,
            "nearest_pivot": None,
            "pivot_kind": None,
            "pivot_zone_pct": PIVOT_ZONE_PCT,
            "cluster_n": 0,
            "dist_pct": None,
        }

    relevant.sort(key=lambda x: x["dist_pct"])
    nearest = relevant[0]
    in_danger = nearest["dist_pct"] <= DANGER_ZONE_PCT
    cluster_ok = nearest["cluster_n"] >= CLUSTER_MIN_INTERACTIONS
    active = bool(in_danger and cluster_ok)

    return {
        "res_confluence": active,
        "nearest_pivot": round(float(nearest["level"]), 4),
        "pivot_kind": nearest["kind"],
        "pivot_zone_pct": PIVOT_ZONE_PCT,
        "cluster_n": int(nearest["cluster_n"]),
        "dist_pct": round(float(nearest["dist_pct"]), 4),
        "warning_only": True,
    }
