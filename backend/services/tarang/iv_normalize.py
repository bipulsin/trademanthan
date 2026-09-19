"""IV unit normalization for Kosmic Tarang.

Stores both raw venue value + unit and a normalized decimal IV in [0, ~5].
"""
from __future__ import annotations

from typing import Optional, Tuple


def normalize_iv(raw: Optional[float], unit_hint: Optional[str] = None) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """Return (iv_decimal, iv_raw, iv_unit).

    Rules:
    - If unit_hint is ``percent`` (or raw looks like percent > 1.5 and <= 500), treat as percent.
    - If unit_hint is ``decimal`` or raw in (0, 1.5], treat as decimal.
    - Values <= 0 or absurdly large return (None, raw, unit).
    """
    if raw is None:
        return None, None, unit_hint
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None, None, unit_hint

    hint = (unit_hint or "").strip().lower() or None
    if hint in ("pct", "%", "percent", "percentage"):
        unit = "percent"
        dec = v / 100.0
    elif hint in ("decimal", "frac", "fraction"):
        unit = "decimal"
        dec = v
    else:
        # Heuristic: Upstox Option Greek IV is typically decimal (0.43);
        # some feeds use percent (43). Delta mark_vol is decimal.
        if 1.5 < abs(v) <= 500:
            unit = "percent"
            dec = v / 100.0
        else:
            unit = "decimal"
            dec = v

    if dec is None or dec <= 0 or dec > 5.0:
        return None, v, unit
    return dec, v, unit
