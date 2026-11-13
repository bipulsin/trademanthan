"""Fear & Greed score (0–100) for a single trading instrument.

Context / filter only — not an entry or exit trigger.
"""
from backend.services.fear_greed.compute import (
    DEFAULT_WEIGHTS,
    ZONE_LABELS,
    combine_scores,
    compute_fear_greed_from_candles,
    interpret_oi,
    map_zone,
    normalize_series,
    percentile_rank,
    rebalance_weights,
)
from backend.services.fear_greed.service import get_fear_greed

__all__ = [
    "DEFAULT_WEIGHTS",
    "ZONE_LABELS",
    "combine_scores",
    "compute_fear_greed_from_candles",
    "get_fear_greed",
    "interpret_oi",
    "map_zone",
    "normalize_series",
    "percentile_rank",
    "rebalance_weights",
]
