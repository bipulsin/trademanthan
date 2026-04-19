"""
Public read-only API for the F&O Bullish Trend scanner backtest artifact.

The artifact is produced by
``backend/scripts/run_fno_bullish_backtest.py`` and cached on disk at
``/home/ubuntu/trademanthan/data/fno_bullish_backtest.json`` on EC2 (with a
local fallback under ``backend/data/`` for development).

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


def _candidate_paths() -> tuple[Path, ...]:
    fname = "fno_bullish_backtest.json"
    return (
        Path(f"/home/ubuntu/trademanthan/data/{fname}"),
        Path(__file__).resolve().parents[2] / "backend" / "data" / fname,
        Path(__file__).resolve().parents[2] / "data" / fname,
    )


def _find_data_file() -> Optional[Path]:
    for p in _candidate_paths():
        try:
            if p.is_file():
                return p
        except OSError:
            continue
    return None


@router.get("/data")
def get_fno_bullish_data() -> Dict[str, Any]:
    """Return the cached F&O Bullish Trend backtest document."""
    path = _find_data_file()
    if path is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "F&O Bullish Trend backtest artifact not found. Run "
                "`python3 backend/scripts/run_fno_bullish_backtest.py` to "
                "generate it."
            ),
        )
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except Exception as e:
        logger.warning("fno_bullish: failed to read %s: %s", path, e)
        raise HTTPException(status_code=500, detail=f"Could not read artifact: {e}")
    if isinstance(doc, dict):
        doc["artifact_path"] = str(path)
    return doc
