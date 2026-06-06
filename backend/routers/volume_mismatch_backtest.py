"""
Public read-only API for Gap + Bollinger Band Futures backtest artifact.

No authentication — page at ``/volumemismatch-backtest.html``.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/volume-mismatch-backtest", tags=["volume-mismatch-backtest"])

_ARTIFACT = "volume_mismatch_backtest.json"


def _candidate_paths() -> tuple[Path, ...]:
    root = Path(__file__).resolve().parents[2]
    return (
        Path("/home/ubuntu/trademanthan/data") / _ARTIFACT,
        root / "backend" / "data" / _ARTIFACT,
        root / "data" / _ARTIFACT,
    )


def _find_artifact() -> Optional[Path]:
    for p in _candidate_paths():
        try:
            if p.is_file():
                return p
        except OSError:
            continue
    return None


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except Exception as e:
        logger.warning("volume_mismatch_backtest read %s: %s", path, e)
        raise HTTPException(status_code=500, detail=f"Could not read artifact: {e}")
    if isinstance(doc, dict):
        doc["artifact_path"] = str(path)
    return doc


@router.get("/data")
def get_volume_mismatch_backtest_data() -> Dict[str, Any]:
    """Return cached backtest JSON (signals since from_date)."""
    path = _find_artifact()
    if path is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Gap + BB backtest artifact not found. Run "
                "`python3 backend/scripts/run_volume_mismatch_backtest.py` "
                "to generate it."
            ),
        )
    return _load_json(path)


@router.get("/health")
def backtest_health() -> Dict[str, str]:
    path = _find_artifact()
    return {"status": "ok" if path else "missing", "artifact": str(path or "")}
