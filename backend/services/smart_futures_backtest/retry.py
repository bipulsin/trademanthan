"""Bounded retries for third-party API calls used only by the backtest runner."""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional, Tuple, TypeVar

T = TypeVar("T")


def call_with_retries(
    logger: logging.Logger,
    label: str,
    fn: Callable[[], T],
    max_tries: int = 2,
) -> Tuple[Optional[T], Optional[str]]:
    """
    Run ``fn`` up to ``max_tries`` times. On failure after all tries, log and return (None, err).
    """
    last_err: Optional[str] = None
    for attempt in range(1, max_tries + 1):
        try:
            return fn(), None
        except Exception as e:
            last_err = str(e)
            logger.warning("%s attempt %s/%s failed: %s", label, attempt, max_tries, e)
    logger.error("%s aborted after %s tries: %s", label, max_tries, last_err)
    return None, last_err
