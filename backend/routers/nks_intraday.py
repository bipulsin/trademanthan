"""
Public read-only API for the NKS intraday momentum backtest artifact.

The data JSON is produced by
``backend/scripts/run_nks_intraday_backtest.py`` and cached on disk at
``/home/ubuntu/trademanthan/data/nks_intraday_backtest.json`` (default) or a
local fallback under ``backend/data/`` for development. This router simply
reads the cached file and returns it verbatim.

No authentication: the page at ``/nks-intraday.html`` is intended to be
publicly viewable.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)

router = APIRouter(tags=["nks-intraday"])


_CANDIDATE_PATHS = (
    Path("/home/ubuntu/trademanthan/data/nks_intraday_backtest.json"),
    Path(__file__).resolve().parents[2] / "backend" / "data" / "nks_intraday_backtest.json",
    Path(__file__).resolve().parents[2] / "data" / "nks_intraday_backtest.json",
)


def _find_data_file() -> Optional[Path]:
    for p in _CANDIDATE_PATHS:
        try:
            if p.is_file():
                return p
        except OSError:
            continue
    return None


@router.get("/data")
def get_nks_intraday_data() -> Dict[str, Any]:
    """Return the cached NKS intraday backtest document."""
    path = _find_data_file()
    if path is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "NKS intraday backtest artifact not found. Run "
                "`python3 backend/scripts/run_nks_intraday_backtest.py` to generate it."
            ),
        )
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except Exception as e:
        logger.warning("nks_intraday: failed to read %s: %s", path, e)
        raise HTTPException(status_code=500, detail=f"Could not read artifact: {e}")
    if isinstance(doc, dict):
        doc["artifact_path"] = str(path)
    return doc
