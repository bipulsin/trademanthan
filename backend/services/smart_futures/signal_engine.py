"""Exit checks on 1m Renko (half brick)."""
from __future__ import annotations

import logging
from typing import Optional

from backend.services.smart_futures.data_service import closes_from_candles, get_1m_candles
from backend.services.smart_futures.renko_engine import (
    build_traditional_renko,
    exit_two_opposite_bricks,
)

logger = logging.getLogger(__name__)


def should_exit_position(
    instrument_key: str,
    direction: str,
    main_brick_size: float,
) -> bool:
    """
    Exit: 2 consecutive opposite-colour bricks on 1m Renko with brick = 0.5 * main brick.
    """
    half = (main_brick_size or 0.0) * 0.5
    if half <= 0:
        return False
    candles = get_1m_candles(instrument_key, days_back=3)
    if not candles or len(candles) < 10:
        return False
    closes = closes_from_candles(candles)
    bricks = build_traditional_renko(closes, half)
    if len(bricks) < 2:
        return False
    return exit_two_opposite_bricks(bricks, direction)
