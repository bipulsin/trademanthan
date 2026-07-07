"""Detect silent volume=0 regressions across WS and OI heatmap pipelines."""
from __future__ import annotations

import logging
from typing import List, Optional, Sequence

logger = logging.getLogger(__name__)


def audit_zero_volume_fraction(
    volumes: Sequence[int],
    *,
    context: str,
    warn_fraction: float = 0.90,
    min_samples: int = 20,
) -> Optional[str]:
    """
    Return a warning message when an unusually large share of symbols report volume=0.
    Mirrors checklist lock-coverage audit — surfaces pipeline regressions in logs.
    """
    if not volumes or len(volumes) < min_samples:
        return None
    zero = sum(1 for v in volumes if int(v or 0) <= 0)
    frac = zero / float(len(volumes))
    if frac >= warn_fraction:
        return (
            f"{context}: volume=0 on {zero}/{len(volumes)} symbols "
            f"({frac * 100:.0f}%) — check WS ltq aggregation / REST quote volume fields"
        )
    return None


def log_volume_audit(volumes: Sequence[int], *, context: str) -> None:
    msg = audit_zero_volume_fraction(volumes, context=context)
    if msg:
        logger.warning(msg)
