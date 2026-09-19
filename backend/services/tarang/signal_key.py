"""Canonical signal key for one-open-forward-test-per-structure."""
from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Sequence


def canonical_signal_key(
    *,
    underlying: Optional[str],
    expiry: Optional[str],
    structure: Optional[str],
    legs: Sequence[Dict[str, Any]] | None = None,
    strikes: Iterable[Any] | None = None,
) -> str:
    und = str(underlying or "").strip().upper()
    exp = str(expiry or "").strip()
    st = str(structure or "").strip().lower()
    vals = []
    if strikes is not None:
        vals = [s for s in strikes if s is not None]
    else:
        for leg in legs or []:
            if leg.get("strike") is not None:
                vals.append(leg.get("strike"))
    nums = sorted({float(v) for v in vals})
    strike_part = ",".join(f"{n:g}" for n in nums)
    return f"{und}|{exp}|{st}|{strike_part}"
