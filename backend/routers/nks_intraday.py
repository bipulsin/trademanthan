"""
Public read-only API for the NKS intraday momentum backtest artifacts.

Two artifacts are produced by
``backend/scripts/run_nks_intraday_backtest.py`` and cached on disk:

- ``nks_intraday_backtest.json``        — prices from the **same day** as the CSV shortlist date
- ``nks_intraday_backtest_nextday.json`` — prices from the **next trading day**

Default location on EC2: ``/home/ubuntu/trademanthan/data/``. A local fallback
under ``backend/data/`` is checked for development.

No authentication: the pages at ``/nks-intraday.html`` and
``/nks-intraday-next.html`` are publicly viewable.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

router = APIRouter(tags=["nks-intraday"])


def _candidate_paths(day: str) -> tuple[Path, ...]:
    if day == "next":
        suffix = "_nextday"
    elif day == "v2":
        suffix = "_v2"
    else:
        suffix = ""
    fname = f"nks_intraday_backtest{suffix}.json"
    return (
        Path(f"/home/ubuntu/trademanthan/data/{fname}"),
        Path(__file__).resolve().parents[2] / "backend" / "data" / fname,
        Path(__file__).resolve().parents[2] / "data" / fname,
    )


def _find_data_file(day: str) -> Optional[Path]:
    for p in _candidate_paths(day):
        try:
            if p.is_file():
                return p
        except OSError:
            continue
    return None


@router.get("/data")
def get_nks_intraday_data(
    day: str = Query("same", regex="^(same|next|v2)$"),
) -> Dict[str, Any]:
    """Return the cached NKS intraday backtest document.

    ``day=same`` (default) returns prices from the shortlist date itself;
    ``day=next`` returns prices from the next trading session after it.
    """
    path = _find_data_file(day)
    if path is None:
        raise HTTPException(
            status_code=503,
            detail=(
                f"NKS intraday backtest artifact (day={day}) not found. Run "
                "`python3 backend/scripts/run_nks_intraday_backtest.py --mode both` "
                "to generate it."
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
        doc.setdefault("day_mode", day)
    return doc
