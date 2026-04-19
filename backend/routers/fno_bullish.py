"""
Public read-only API for the F&O Bullish Trend scanner backtest artifact.

The artifact is produced by
``backend/scripts/run_fno_bullish_backtest.py`` and cached on disk at
``/home/ubuntu/trademanthan/data/fno_bullish_backtest.json`` on EC2 (with a
local fallback under ``backend/data/`` for development).

The 15:15 second-scan variant is ``fno_bullish_backtest_1515.json`` (see
``/data-1515``).

No authentication: the page at ``/fno-bullish.html`` is publicly viewable.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)

router = APIRouter(tags=["fno-bullish"])


def _candidate_paths(filename: str) -> tuple[Path, ...]:
    return (
        Path("/home/ubuntu/trademanthan/data") / filename,
        Path(__file__).resolve().parents[2] / "backend" / "data" / filename,
        Path(__file__).resolve().parents[2] / "data" / filename,
    )


def _find_data_file(filename: str) -> Optional[Path]:
    for p in _candidate_paths(filename):
        try:
            if p.is_file():
                return p
        except OSError:
            continue
    return None


def _load_json_path(path: Path) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except Exception as e:
        logger.warning("fno_bullish: failed to read %s: %s", path, e)
        raise HTTPException(status_code=500, detail=f"Could not read artifact: {e}")
    if isinstance(doc, dict):
        doc["artifact_path"] = str(path)
    return doc


@router.get("/data")
def get_fno_bullish_data() -> Dict[str, Any]:
    """Return the cached F&O Bullish Trend backtest document (default rules)."""
    path = _find_data_file("fno_bullish_backtest.json")
    if path is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "F&O Bullish Trend backtest artifact not found. Run "
                "`python3 backend/scripts/run_fno_bullish_backtest.py` to "
                "generate it."
            ),
        )
    return _load_json_path(path)


@router.get("/data-1515")
def get_fno_bullish_data_1515_second_scan() -> Dict[str, Any]:
    """15:15-only variant: 2+ scans per streak, entry 5 min after the **second** scan."""
    path = _find_data_file("fno_bullish_backtest_1515.json")
    if path is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "F&O 15:15 second-scan artifact not found. Run "
                "`python3 backend/scripts/run_fno_bullish_backtest.py --1515-second-scan`."
            ),
        )
    return _load_json_path(path)
