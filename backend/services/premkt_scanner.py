"""
Pre-market F&O scanner (Top N watchlist).

Schedule and scoring live in ``premarket_watchlist_job`` (Upstox-only). This module is the
stable import path referenced by ops/docs (`premkt_scanner.run`, `fetch_rows`).
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

from backend.services.premarket_watchlist_job import (
    fetch_premarket_watchlist_for_date,
    run_premarket_watchlist_job,
)


def run() -> Dict[str, Any]:
    """Run the pre-market scan (same as the scheduled job)."""
    return run_premarket_watchlist_job()


def fetch_rows(session_date: date) -> List[Dict[str, Any]]:
    """Rows for ``premarket_watchlist`` for the given session date."""
    return fetch_premarket_watchlist_for_date(session_date)
